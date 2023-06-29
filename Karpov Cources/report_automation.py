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
import asyncio
import matplotlib.dates as mdates
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# connection to DB
con = {'host': 'https://clickhouse.lab.karpov.courses',
              'database': '*************',
              'user': '********',
              'password': '*********'
              }

bot_token = '********************************'
bot = telegram.Bot(token=bot_token)
chat_id = -########


# Airflow part
default_args = {
    'owner': 'd-engalychev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 19),
}

schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def d_engalychev_report():

    @task
    def get_data_feed():
        df_feed = ph.read_clickhouse(f"""
                    select *
                    from
                    (select
                    toDate(time) as day,
                    uniq(user_id) as dau,
                    countIf(action = 'view') as views, 
                    countIf(action = 'like') as likes
                    from
                    simulator_20230220.feed_actions
                    group by day) as t1
                    join
                    (select start_day as day, uniq(user_id) new_users
                    from
                    (select 
                    user_id,
                    min(toDate(time)) as start_day
                    from
                    simulator_20230220.feed_actions
                    group by user_id)
                    group by day) as t2
                    using day
                    having day >= today()-7 and day < today()""", connection=con)

        return df_feed

    @task
    def get_data_msg():
        df_msg = ph.read_clickhouse(f"""
                    select *
                    from
                    (select
                    toDate(time) day,
                    uniq(user_id) as dau,
                    count(user_id) as messages_sent
                    from
                    simulator_20230220.message_actions
                    group by day) t1
                    join
                    (select registration_day as day, uniq(user_id) new_users
                    from
                    (select 
                    user_id,
                    min(toDate(time)) as registration_day
                    from
                    simulator_20230220.message_actions
                    group by user_id)
                    group by day) t2
                    using day
                    having day >= today()-7 and day < today()""", connection=con)

        return df_msg

    @task
    def merge_dfs(df_feed, df_msg):
        df_union = pd.merge(df_feed, df_msg, on=['day'], how='inner')
        return df_union

    @task
    def creating_report(df_union):
        yesterday = str(date.today() - timedelta(days=1))
        data_yesterday = df_union[df_union.day == yesterday]
        dau_feed = data_yesterday.dau_x.to_list()[0]
        dau_msg = data_yesterday.dau_y.to_list()[0]
        new_users_feed = data_yesterday.new_users_x.to_list()[0]
        new_users_msg = data_yesterday.new_users_y.to_list()[0]
        likes = round(data_yesterday.likes.to_list()[0], 2)
        views = round(data_yesterday.views.to_list()[0], 2)
        messages_sent = round(data_yesterday.messages_sent.to_list()[0], 2)

        msg = """
        Метрики за вчерашний день {yesterday}

    Активные пользователи: 1) лента новостей - {dau_feed} 2) мессенджер - {dau_msg}

    Новые пользователи: 1) лента новостей - {new_users_feed} 2) мессенджер - {new_users_msg}

    Действия на одного пользователя: likes - {likes}, views - {views}, messages - {messages_sent}
        """.format(yesterday=yesterday,
                   dau_feed=dau_feed, dau_msg=dau_msg,
                   new_users_feed=new_users_feed, new_users_msg=new_users_msg,
                   likes=likes, views=views, messages_sent=messages_sent)

        return msg

    @task
    def plot_dau(data):
        data_dau = pd.melt(data[['day', 'day_x', 'dau_y']], id_vars=['day'], value_name='amount')

        sns.set(rc={'figure.figsize': (7, 12)})
        dau_plot = sns.lineplot(x='day', y='amount', hue='variable', data=data_dau)
        dau_plot.set_title('DAU за последние 7 дней')
        dau_plot.set(xlabel='day', ylabel='amount')
        dau_plot.tick_params(axis='x', rotation=30)
        leg = dau_plot.axes.get_legend()
        new_title = 'App'
        leg.set_title(new_title)
        new_labels = ['Feed', 'Messanger']
        for t, l in zip(leg.texts, new_labels):
            t.set_text(l)

        dau_plot = io.BytesIO()
        plt.savefig(dau_plot)
        dau_plot.seek(0)
        dau_plot.name = 'dau_plot.png'
        plt.close()

        return dau_plot

    @task
    def plot_new_users(data):
        data_new_users = pd.melt(data[['day', 'new_users_x', 'new_users_y']], id_vars=['day'], value_name='amount')

        sns.set(rc={'figure.figsize': (7, 12)})
        new_users_plot = sns.lineplot(x='day', y='amount', hue='variable', data=data_new_users)
        new_users_plot.set_title('Новые пользователи за последние 7 дней')
        new_users_plot.set(xlabel='day', ylabel='amount')
        new_users_plot.tick_params(axis='x', rotation=30)
        leg = new_users_plot.axes.get_legend()
        new_title = 'App'
        leg.set_title(new_title)
        new_labels = ['Feed', 'Messanger']
        for t, l in zip(leg.texts, new_labels):
            t.set_text(l)

        new_users_plot = io.BytesIO()
        plt.savefig(new_users_plot)
        new_users_plot.seek(0)
        new_users_plot.name = 'new_users_plot.png'
        plt.close()

        return new_users_plot

    @task
    def plot_actions(data):
        data_actions = pd.melt(data[['day', 'views', 'likes', 'message_sent']], id_vars=['day'], value_name='amount')

        sns.set(rc={'figure.figsize': (7, 12)})
        actions_plot = sns.lineplot(x='day', y='amount', hue='variable', data=data_actions)
        actions_plot.set_title('Действия на пользователя')
        actions_plot.set(xlabel='day', ylabel='amount')
        actions_plot.tick_params(axis='x', rotation=30)
        leg = actions_plot.axes.get_legend()
        new_title = 'Действия'
        leg.set_title(new_title)
        new_labels = ['просмотры', 'лайки', 'сообщения']
        for t, l in zip(leg.texts, new_labels):
            t.set_text(l)

        actions_plot = io.BytesIO()
        plt.savefig(actions_plot)
        actions_plot.seek(0)
        actions_plot.name = 'actions_plot.png'
        plt.close()

        return actions_plot

    @task
    def bot_report(msg, dau_plot, new_users_plot, actions_plot):
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=dau_plot)
        bot.sendPhoto(chat_id=chat_id, photo=new_users_plot)
        bot.sendPhoto(chat_id=chat_id, photo=actions_plot)

    # TASKS EXECUTION
    df_feed = get_data_feed()
    df_msg = get_data_msg()
    df_union = merge_dfs(df_feed, df_msg)

    msg = creating_report(df_union)
    dau_plot = plot_dau(df_union)
    new_users_plot = plot_new_users(df_union)
    actions_plot = plot_actions(df_union)

    bot_report(msg, dau_plot, new_users_plot, actions_plot)


d_engalychev_report = d_engalychev_report()
