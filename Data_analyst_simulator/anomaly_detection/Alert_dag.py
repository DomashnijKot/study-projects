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

my_token = '5961271723:AAFqSqJLUGMdZUaR6lhy5HDbGqkjA7jeu40'
bot = telegram.Bot(token = my_token)
# id беседы: -520311152
# id лс: 477287800
chat_id = -520311152

default_args = {
    'owner': 'a-volnenko-15',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 14),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

def check_anamaly_iqr(history_data, x):
    q75, q25 = np.percentile(history_data, [75 ,25])
    iqr = q75 - q25
    return (x < q25 - iqr * 1.5) or (x > q75 + iqr * 1.5)


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def volnenko_alert_dag():
    
    @task
    def extract_messages_data():
        query = '''
                SELECT *,
                DATE_SUB(MINUTE, 15 * 5, now()) as interval_start,
                NOW() as interval_end
                FROM simulator_20230120.message_actions
                WHERE time >= DATE_SUB(MINUTE, 15 * 5, now())
                '''
        messages = ph.read_clickhouse(query, connection=connection)
        m_start = messages.interval_start.iloc[0]
        m_end = messages.interval_end.iloc[0]
        messages = messages[(messages.time >= m_start) & (messages.time < m_end)]
        messages.drop(['interval_start', 'interval_end'], inplace=True, axis=1)
        messages_data = (messages, m_start)
        return messages_data
    
    
    @task
    def extract_feed_data():
        query = '''
                SELECT *, 
                DATE_SUB(MINUTE, 15 * 5, now()) as interval_start,
                NOW() as interval_end
                FROM simulator_20230120.feed_actions
                WHERE time >= DATE_SUB(MINUTE, 15 * 5, now())
            '''

        feed = ph.read_clickhouse(query, connection=connection)
        f_start = feed.interval_start.iloc[0]
        f_end = feed.interval_end.iloc[0]
        feed = feed[(feed.time >= f_start) & (feed.time < f_end)]
        feed.drop(['interval_start', 'interval_end'], inplace=True, axis=1)
        feed_data = (feed, f_start)
        return feed_data
    
    
    @task
    def check_feed_anomaly(data, spltis=['source', 'os']):
        df, time_origin = data
        alerts = []
        for split_group in spltis:
            categories = df[split_group].unique()
            for cat in categories:
                sl = df[df[split_group] == cat]
                res={}
                res['Активные пользователи в ленте'] = sl.groupby(pd.Grouper(key='time', freq='15min', origin=time_origin))['user_id'].nunique()
                res['Просмотры'] = sl[sl.action == 'view'].groupby(pd.Grouper(key='time', freq='15min', origin=time_origin)).size()
                res['Лайки'] = sl[sl.action == 'like'].groupby(pd.Grouper(key='time', freq='15min', origin=time_origin)).size()
                res['CTR'] = round(res['Лайки'] / res['Просмотры'] * 100, 2)
                for metric in res.keys():
                    history, current = res[metric].iloc[:4].values, res[metric].iloc[-1]
                    if check_anamaly_iqr(history, current):
                        alert = {'metric': metric, 'split': split_group, 'category': cat, 
                                 'history': history, 'current_x': current}
                        alerts.append(alert)
        return alerts
    
    
    @task
    def check_message_anomaly(data, spltis=['source', 'os']):
        df, time_origin = data
        alerts = []
        for split_group in spltis:
            categories = df[split_group].unique()
            for cat in categories:
                sl = df[df[split_group] == cat]
                res={}
                res['Активные пользователи в мессенджере'] = sl.groupby(pd.Grouper(key='time', freq='15min', origin=time_origin))['user_id'].nunique()
                res['Количество отправленных сообщений'] = sl.groupby(pd.Grouper(key='time', freq='15min', origin=time_origin)).size()
                for metric in res.keys():
                    history, current = res[metric].iloc[:4].values, res[metric].iloc[-1]
                    if check_anamaly_iqr(history, current):
                        alert = {'metric': metric, 'split': split_group, 'category': cat, 
                                 'history': history, 'current_x': current}
                        alerts.append(alert)
        return alerts
    
    @task
    def send_alerts(feed_alerts, messages_alerts):
        alerts = feed_alerts + messages_alerts
        for alert in alerts:
            message = ('Alert!\n'
                      f'Метрика "{alert["metric"]}" в срезе {alert["split"]} - {alert["category"]} \n'
                      f'Текущее значение {alert["current_x"]} не попало в доверительный интервал, построенный с помощью IQR \n')
            bot.sendMessage(chat_id=chat_id, text=message)
    
    
    messages_data = extract_messages_data()
    feed_data = extract_feed_data()
    feed_alerts = check_feed_anomaly(feed_data)
    messages_alerts = check_message_anomaly(messages_data)
    send_alerts(feed_alerts, messages_alerts)

    
    
volnenko_alert_dag = volnenko_alert_dag() 
    
    