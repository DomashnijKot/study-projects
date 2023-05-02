import pandas as pd
import pandahouse as ph
import datetime as dt
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import telegram
import io

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230120',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
default_args = {
    'owner': 'a-volnenko-15',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 14),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

my_token = '5961271723:AAFqSqJLUGMdZUaR6lhy5HDbGqkjA7jeu40'
bot = telegram.Bot(token = my_token)

report_template = '''Отчет по всему приложению за {today_date}

Оба сервиса:
Всего событий: {events:,}
DAU: {users:,} ({users_comp_day_ago:.2%} к дню назад, {users_comp_week_ago:.2%} к неделе назад)
DAU by platform:
    iOS: {users_ios:,} ({users_ios_comp_day_ago:.2%} к дню назад, {users_ios_comp_week_ago:.2%} к неделе назад)
    Android: {users_android:,} ({users_android_comp_day_ago:.2%} к дню назад, {users_android_comp_week_ago:.2%} к неделе назад)
New users: {new_users:,} ({new_users_comp_day_ago:.2%} к дню назад, {new_users_comp_week_ago:.2%} к неделе назад)
New users by source:
    ads: {new_users_ads:,} ({new_users_ads_comp_day_ago:.2%} к дню назад, {new_users_ads_comp_week_ago:.2%} к неделе назад)
    organic: {new_users_organic:,} ({new_users_organic_comp_day_ago:.2%} к дню назад, {new_users_organic_comp_week_ago:.2%} к неделе назад)
    
Лента:
DAU: {feed_users:,} ({feed_users_comp_day_ago:.2%} к дню назад, {feed_users_comp_week_ago:.2%} к неделе назад)
Likes: {likes:,} ({likes_comp_day_ago:.2%} к дню назад, {likes_comp_week_ago:.2%} к неделе назад)
Views: {views:,} ({views_comp_day_ago:.2%} к дню назад, {views_comp_week_ago:.2%} к неделе назад)
CTR: {ctr:.2f} ({ctr_comp_day_ago:.2%} к дню назад, {ctr_comp_week_ago:.2%} к неделе назад)
Likes per user: {lpu:.2} ({lpu_comp_day_ago:.2%} к дню назад, {lpu_comp_week_ago:.2%} к неделе назад)

Мессенджер:
DAU: {msg_users:,} ({msg_users_comp_day_ago:.2%} к дню назад, {msg_users_comp_week_ago:.2%} к неделе назад)
Messages: {msgs:,} ({msg_comp_day_ago:.2%} к дню назад, {msg_comp_week_ago:.2%} к неделе назад)
Messages per user: {mpu:.2} ({mpu_comp_day_ago:.2%} к дню назад, {mpu_comp_week_ago:.2%} к неделе назад)
'''
today = pd.Timestamp('now')
yesterday = pd.Timestamp('now') - pd.DateOffset(days=1)
day_ago = yesterday - pd.DateOffset(days=1)
week_ago = yesterday - pd.DateOffset(days=7)


def prev_date_comp(df, metric, prev='day'):
    day = df[df.date == yesterday.date()][metric].astype('float').iloc[0]
    if prev == 'day':
        prev_date =  df[df.date == day_ago.date()][metric].astype('float').iloc[0]
    else:
        prev_date = df[df.date == week_ago.date()][metric].astype('float').iloc[0]
    return (day - prev_date) / prev_date

def yesterday_values(df, metric):
    return df[df.date == yesterday.date()][metric].iloc[0]
     
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def volnenko_app_report_dag():
    
    @task
    def get_feed_data():
        query = '''
                SELECT 
                    toDate(time) as date,
                    uniqExact(user_id) as users_feed,
                    countIf(user_id, action='view') as views,
                    countIf(user_id, action='like') as likes,
                    views + likes as events,
                    likes / views * 100 as CTR,
                    likes / users_feed as LPU,
                    views / users_feed as VPU
                FROM simulator_20230120.feed_actions
                WHERE toDate(time) between today() - 8 and today() - 1
                GROUP BY date
                ORDER BY date
                '''
        df = ph.read_clickhouse(query, connection=connection)
        df.date = pd.to_datetime(df.date).dt.date
        return df

    @task
    def get_messages_data():
        query= '''
                SELECT 
                    toDate(time) as date,
                    uniqExact(user_id) as users_msg,
                    count(user_id) as messages,
                    messages / users_msg as MPU
                FROM simulator_20230120.message_actions
                WHERE toDate(time) between today() - 8 and today() - 1
                GROUP BY date
                ORDER BY date    
        '''

        df = ph.read_clickhouse(query, connection=connection)
        df.date = pd.to_datetime(df.date).dt.date
        return df
    
    @task
    def get_users_by_os():
        query='''
                WITH 
            feed_events AS
                (SELECT
                    toDate(time) as date,
                    user_id,
                    os
                FROM simulator_20230120.feed_actions
                WHERE date between today() - 8 and today() - 1),
            messages_events AS
                (SELECT
                    toDate(time) as date,
                    user_id,
                    os
                FROM simulator_20230120.message_actions
                WHERE toDate(time) between today() - 8 and today() - 1),
            all_events AS
                (select * from feed_events
                union all
                select * from messages_events)

            SELECT 
                date, 
                uniqExact(user_id) as users,
                uniqExactIf(user_id, os='iOS') as users_ios,
                uniqExactIf(user_id, os='Android') as users_android
            FROM all_events
            GROUP BY date
            ORDER BY date
        '''
        df = ph.read_clickhouse(query, connection=connection)
        df.date = pd.to_datetime(df.date).dt.date
        return df

    @task
    def get_new_users_source():
        query='''
            WITH 
            feed_events AS
                (SELECT
                    user_id,
                    toDate(time) as date,
                    min(toDate(time)) OVER (PARTITION BY user_id) as min_date,
                    source
                FROM simulator_20230120.feed_actions
                WHERE date between today() - 90 and today() - 1),
            messages_events AS
                (SELECT
                    user_id,
                    toDate(time) as date,
                    min(toDate(time)) OVER (PARTITION BY user_id) as min_date,
                    user_id,
                    source
                FROM simulator_20230120.message_actions
                WHERE toDate(time) between today() - 90 and today() - 1),
            all_events AS
                (select * from feed_events
                union all
                select * from messages_events)

            SELECT 
                min_date as date, 
                uniqExact(user_id) as users_new,
                uniqExactIf(user_id, source='ads') as users_ads,
                uniqExactIf(user_id, source='organic') as users_organic
            FROM all_events
            WHERE min_date between today() - 8 and today() - 1
            GROUP BY date
            ORDER BY date
            '''
        df = ph.read_clickhouse(query, connection=connection)
        df.date = pd.to_datetime(df.date).dt.date
        return df
    
    @task
    def get_report(users_source, users_os, messages_data, feed_data):
        report = (report_template
                .format(today_date=today.strftime('%Y-%m-%d'),
                       events=yesterday_values(feed_data, 'events') + yesterday_values(messages_data, 'messages'),
                       users = yesterday_values(users_os, 'users'),
                       users_comp_day_ago = prev_date_comp(users_os, 'users', 'day'),
                       users_comp_week_ago = prev_date_comp(users_os, 'users', 'week'),
                       users_ios = yesterday_values(users_os, 'users_ios'),
                       users_ios_comp_day_ago = prev_date_comp(users_os, 'users_ios', 'day'),
                       users_ios_comp_week_ago = prev_date_comp(users_os, 'users_ios', 'week'),
                       users_android = yesterday_values(users_os, 'users_android'),
                       users_android_comp_day_ago = prev_date_comp(users_os, 'users_android', 'day'),
                       users_android_comp_week_ago = prev_date_comp(users_os, 'users_android', 'week'),
                       new_users = yesterday_values(users_source, 'users_new'),
                       new_users_comp_day_ago = prev_date_comp(users_source, 'users_new', 'day'),
                       new_users_comp_week_ago = prev_date_comp(users_source, 'users_new', 'week'),
                       new_users_ads = yesterday_values(users_source, 'users_ads'),
                       new_users_ads_comp_day_ago = prev_date_comp(users_source, 'users_ads', 'day'),
                       new_users_ads_comp_week_ago = prev_date_comp(users_source, 'users_ads', 'week'),
                       new_users_organic = yesterday_values(users_source, 'users_organic'),
                       new_users_organic_comp_day_ago = prev_date_comp(users_source, 'users_organic', 'day'),
                       new_users_organic_comp_week_ago = prev_date_comp(users_source, 'users_organic', 'week'),

                       feed_users = yesterday_values(feed_data, 'users_feed'),
                       feed_users_comp_day_ago = prev_date_comp(feed_data, 'users_feed', 'day'),
                       feed_users_comp_week_ago = prev_date_comp(feed_data, 'users_feed', 'week'),
                       likes = yesterday_values(feed_data, 'likes'),
                       likes_comp_day_ago = prev_date_comp(feed_data, 'likes', 'day'),
                       likes_comp_week_ago = prev_date_comp(feed_data, 'likes', 'week'),
                       views = yesterday_values(feed_data, 'views'),
                       views_comp_day_ago = prev_date_comp(feed_data, 'views', 'day'),
                       views_comp_week_ago = prev_date_comp(feed_data, 'views', 'week'),
                       ctr = yesterday_values(feed_data, 'CTR'),
                       ctr_comp_day_ago = prev_date_comp(feed_data, 'CTR', 'day'),
                       ctr_comp_week_ago = prev_date_comp(feed_data, 'CTR', 'week'),
                       lpu = yesterday_values(feed_data, 'LPU'),
                       lpu_comp_day_ago = prev_date_comp(feed_data, 'LPU', 'day'),
                       lpu_comp_week_ago = prev_date_comp(feed_data, 'LPU', 'week'),

                       msg_users = yesterday_values(messages_data, 'users_msg'),
                       msg_users_comp_day_ago = prev_date_comp(messages_data, 'users_msg', 'day'),
                       msg_users_comp_week_ago = prev_date_comp(messages_data, 'users_msg', 'week'),
                       msgs = yesterday_values(messages_data, 'messages'),
                       msg_comp_day_ago = prev_date_comp(messages_data, 'messages', 'day'),
                       msg_comp_week_ago = prev_date_comp(messages_data, 'messages', 'week'),
                       mpu = yesterday_values(messages_data, 'MPU'),
                       mpu_comp_day_ago = prev_date_comp(messages_data, 'MPU', 'day'),
                       mpu_comp_week_ago = prev_date_comp(messages_data, 'MPU', 'week')))
        return report
      
    @task
    def get_plots(users_source, users_os, messages_data, feed_data):
        data = pd.merge(feed_data, messages_data, on='date')
        data = pd.merge(data, users_os, on='date')
        data = pd.merge(data, users_source, on='date')
        data['all_events'] = data.events + data.messages

        plot_objects = []

        #Графики по всему приложению
        fig, axes = plt.subplots(3, figsize=(10, 16))
        app_plot_dict = {0: {'y': ['all_events'], 'title': 'Events'},
                         1: {'y': ['users', 'users_ios', 'users_android'], 'title': 'DAU'},
                         2: {'y': ['users_new', 'users_ads', 'users_organic'], 'title': 'New users'}}
        time = pd.to_datetime(data.date).dt.strftime('%e %b').to_list()
        for i in range(3):
            for y in app_plot_dict[i]['y']:
                if app_plot_dict[i]['title'] == 'Events':
                    axes[i].plot(data[y] / 1000)
                    axes[i].set_ylabel('Кол-во (тысячи)')
                else:
                    axes[i].plot(data[y])
                axes[i].set_xticks(data[y].index)
            axes[i].set_title(app_plot_dict[i]['title'], fontsize=16)
            axes[i].set_xticklabels(time, rotation=15)
            axes[i].grid(alpha=0.5)
            axes[i].spines['top'].set_visible(False)
            axes[i].spines['right'].set_visible(False)

        today_date = datetime.now().strftime('%Y-%m-%d')
        fig.suptitle(f'Метрики по всему приложению за 7 дней \n {today_date}', fontsize=20)

        app_object = io.BytesIO()
        plt.savefig(app_object)
        app_object.seek(0)
        app_object.name = '7 day app metrics.png'
        plt.close()
        plot_objects.append(app_object)

        #Графики по ленте
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        axes = axes.flatten()
        feed_plot_dict = {0: {'metric': 'users_feed', 'title': 'DAU'},
                          1: {'metric': 'views', 'title': 'Просмотры'},
                          2: {'metric': 'likes', 'title': 'Лайки'},
                          3: {'metric': 'CTR', 'title': 'CTR'}}
        time = pd.to_datetime(data.date).dt.strftime('%e %b').to_list()
        for i, ax in enumerate(axes):
            metric = feed_plot_dict[i]['metric']
            title = feed_plot_dict[i]['title']
            if title != 'CTR':
                ax.plot(data[metric] / 1000)
                ax.set_ylabel('Кол-во (тысячи)')
            else:
                ax.plot(data[metric])
                ax.set_ylabel('Значение')
            ax.set_xticks(data[metric].index)
            ax.set_title(title, fontsize=16)
            ax.set_xticklabels(time, rotation=15)
            ax.grid(alpha=0.5)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)

        today_date = datetime.now().strftime('%Y-%m-%d')
        fig.suptitle(f'Метрики Ленты за 7 дней \n {today_date}', fontsize=20)

        feed_object = io.BytesIO()
        plt.savefig(feed_object)
        feed_object.seek(0)
        feed_object.name = '7 day feed metrics.png'
        plt.close()
        plot_objects.append(feed_object)

        #Графики по мессенжеру
        fig, axes = plt.subplots(3, figsize=(10, 18))
        axes = axes.flatten()
        feed_plot_dict = {0: {'metric': 'users_msg', 'title': 'DAU'},
                          1: {'metric': 'messages', 'title': 'Кол-во сообщений'},
                          2: {'metric': 'MPU', 'title': 'Кол-во сообещинй на пользователя'}}
        time = pd.to_datetime(data.date).dt.strftime('%e %b').to_list()
        for i, ax in enumerate(axes):
            metric = feed_plot_dict[i]['metric']
            title = feed_plot_dict[i]['title']
            ax.plot(data[metric])
            ax.ticklabel_format(useOffset=False)
            ax.set_xticks(data[metric].index)
            ax.set_title(title, fontsize=16)
            ax.set_xticklabels(time, rotation=15)
            ax.grid(alpha=0.5)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)

        today_date = datetime.now().strftime('%Y-%m-%d')
        fig.suptitle(f'Метрики Мессенджера за 7 дней \n {today_date}', fontsize=20)

        msg_object = io.BytesIO()
        plt.savefig(msg_object)
        msg_object.seek(0)
        msg_object.name = '7 day messenger metrics.png'
        plt.close()
        plot_objects.append(msg_object)

        return plot_objects

    @task
    def send_report(report, chat_id = -677113209):
        bot.sendMessage(chat_id=chat_id, text=report)

    @task
    def send_plots(plot_objects, chat_id = -677113209):
        for x in plot_objects:
            bot.sendPhoto(chat_id=chat_id, photo=x)
            
    users_source = get_new_users_source()
    users_os = get_users_by_os()
    messages_data = get_messages_data()
    feed_data = get_feed_data()
    report = get_report(users_source, users_os, messages_data, feed_data)
    plots = get_plots(users_source, users_os, messages_data, feed_data)
    send_report(report)
    send_plots(plots)
    
volnenko_app_report_dag = volnenko_app_report_dag()    