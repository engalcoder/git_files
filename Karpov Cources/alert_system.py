from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
from datetime import date
import telegram
import matplotlib.pyplot as plt
import numpy as np
import io
import seaborn as sns
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# connection to DB
con = {'host': 'https://clickhouse.lab.karpov.courses',
              'database': '*************',
              'user': '********',
              'password': '*********'
              }

# telegram bot data
bot_token = '********************************'
bot = telegram.Bot(token=bot_token)
chat_id = ########

# Feed metrics for control:
metrics_feed = ['users_feed', 'views', 'likes']

# Messages metrics for control:
metrics_msg = ['users_messages', 'messages_sent']

# anomalies detection base settings
a = 4
n = 5


# algorythm 1 for anomalies detection
def check_anomaly_iqr(df, metric, a=3, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['upper'] = df['q75'] + a * df['iqr']
    df['bottom'] = df['q25'] - a * df['iqr']
    df['upper'] = df['upper'].rolling(n, center=True, min_periods=1).mean()
    df['bottom'] = df['bottom'].rolling(n, center=True, min_periods=1).mean()

    if df[metric].iloc[-1] < df['bottom'].iloc[-1] \
            or df[metric].iloc[-1] > df['upper'].iloc[-1] \
            or df[metric].iloc[-1] <= 0:
        is_alert_iqr = 1
    else:
        is_alert_iqr = 0

    return is_alert_iqr, df


# algorythm 2 for anomalies detection
def check_anomaly_sigma(df, metric, a=4, n=5):

    a += 1
    df['rol_mean'] = df[metric].rolling(n).mean()
    df['std'] = np.std(df[metric].tail(n))

    df['upper_sigma'] = df['rol_mean'] + a * df['std']
    df['bottom_sigma'] = df['rol_mean'] - a * df['std']

    if df[metric].iloc[-1] < df['bottom_sigma'].iloc[-1] \
        or df[metric].iloc[-1] > df['upper_sigma'].iloc[-1] \
        or df[metric].iloc[-1] <= 0:
        is_alert_sigma = 1
    else:
        is_alert_sigma = 0

    return is_alert_sigma, df


# message about detected outlier for metric
def creating_message(df, metric, chat_id=799996443):
    msg = '''Метрика {metric}:
        текущее значение {current_val:.2f}
        отклонение от предыдущего значения {last_val_diff:.2%}'''. \
        format(metric=metric,
               current_val=df[metric].iloc[-1],
               last_val_diff=1 - (df[metric].iloc[-1] / df[metric].iloc[-2]))
    bot.sendMessage(chat_id=chat_id, text=msg)
    return msg


# drawing the graphs
def graph_drw(df, metric, chat_id=799996443):
    sns.set(rc={'figure.figsize': (16, 10)})
    plt.tight_layout()

    ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric', linewidth=2)
    ax = sns.lineplot(x=df['ts'], y=df['upper'], label='upper')
    ax = sns.lineplot(x=df['ts'], y=df['upper_sigma'], label='upper_sigma')
    ax = sns.lineplot(x=df['ts'], y=df['bottom'], label='bottom')
    ax = sns.lineplot(x=df['ts'], y=df['bottom_sigma'], label='bottom_sigma')

    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 2 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)
        ax.set(xlabel='time')
        ax.set(ylabel=metric)

        ax.set_title(metric)
        ax.set(ylim=(0, None))

    # creating .png file
    plot_obj = io.BytesIO()
    plt.savefig(plot_obj)
    plot_obj.seek(0)
    plot_obj.name = '{0}.png'.format(metric)
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_obj)


# Airflow
schedule_interval = '/15 * * * *'

default_args = {
    'owner': 'd-engalychev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 25),
}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def d_engalychev_alert_system():

    @task
    # getting feed data from DB via SQL query
    def get_data_feed():
        data = ph.read_clickhouse(f"""select
                                        toStartOfFifteenMinutes(time) as ts,
                                        toDate(time) as date,
                                        formatDateTime(ts, '%R') as hm,
                                        uniqExact(user_id) as users_feed,
                                        countIf(user_id, action='view') as views,
                                        countIf(user_id, action='like') as likes
                                        from simulator_20230220.feed_actions
                                    where time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                                    group by ts, date, hm
                                    order by ts
                                    """, connection=con)
        return data

    @task
    # getting messages data from DB via SQL query
    def get_data_messages():
        data = ph.read_clickhouse(f"""
                    select toStartOfFifteenMinutes(time) as ts,
                           toDate(time) as date,
                           formatDateTime(ts, '%R') as hm,
                           uniqExact(user_id) as users_messages,
                           count(*) as messages_sent
                    from simulator_20230220.message_actions
                    where time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                    group by ts, date, hm
                    order by ts""", connection=con)
        return data

    @task
    # alert system
    def run_alerts(data, metrics_list, chat_id=799996443, a=3, n=5):

        for metric in metrics_list:
            # print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert_iqr, df = check_anomaly_iqr(df, metric, a, n)
            is_alert_sigma, df = check_anomaly_sigma(df, metric, a, n)

            if is_alert_iqr == 1 and is_alert_sigma == 1:
                creating_message(df, metric, chat_id)
                graph_drw(df, metric, chat_id)

    # TASKS EXECUTION
    data_feed = get_data_feed()
    run_alerts(data_feed, metrics_feed, chat_id, a, n)
    data_msg = get_data_messages()
    run_alerts(data_msg, metrics_msg, chat_id, a, n)

d_engalychev_alert_system = d_engalychev_alert_system()