import telegram
import io
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import datetime as dt
import seaborn as sns
import matplotlib.pyplot as plt

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


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
chat_id = -677113209


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_report_dag_volnenko():
  
    @task
    def extract_7day_data():
        query = '''
        SELECT *
        from simulator_20230120.feed_actions
        WHERE toDate(time) >= today() - 7
            AND toDate(time) != today()
        '''

        return ph.read_clickhouse(query, connection=connection)
    
    
    @task
    def make_yesterday_report(df7):
        yesterday = datetime.now() - timedelta(1)
        df = df7[df7.time.dt.date == yesterday.date()]
        res = {}
        res['dau'] = df.user_id.nunique()
        res['views'] = df[df.action == 'view'].shape[0]
        res['likes'] = df[df.action == 'like'].shape[0]
        #res['ctr'] = res['likes'] / res['views']
        res['ctr'] = round(res['likes'] / res['views'] * 100, 2)

        ydate = yesterday.strftime('%Y-%m-%d')
        report = (f'Метрики за {ydate}: \n'
                  f'DAU: {res["dau"]:,} \n'
                  f'Просмотры: {res["views"]:,} \n'
                  f'Лайки: {res["likes"]:,} \n'
                  f'CTR: {res["ctr"]}%')

        return report

    
    
    @task
    def make_metrics_7day(df):
        res={}
        res['DAU'] = df.groupby(df.time.dt.date)['user_id'].nunique()
        res['Просмотры'] = df[df.action == 'view'].groupby(df.time.dt.date).size()
        res['Лайки'] = df[df.action == 'like'].groupby(df.time.dt.date).size()
        res['CTR'] = round(res['Лайки'] / res['Просмотры'] * 100, 2)

        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        axes = axes.flatten()

        time = pd.to_datetime(res['DAU'].index.to_series()).dt.strftime('%e %b')
        for ax, metric in zip(axes, res.items()):
            name = metric[0]
            values = metric[1]
            if name != 'CTR':
                ax.plot(values.index, 
                        values.values / 1000)
                ax.set_ylabel('Кол-во (тысячи)')
            else:
                ax.plot(values.index, 
                        values.values)
                ax.set_ylabel('Значение (проценты)')
            ax.set_xticks(values.index)
            ax.set_xticklabels(time.to_list(), rotation=15)
            #ax.tick_params('x', labelrotation=15)
            ax.set_title(name, fontsize=16)
            ax.grid(alpha=0.5)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)


        today_date = datetime.now().strftime('%Y-%m-%d')
        fig.suptitle(f'Метрики Ленты за 7 дней \n {today_date}', fontsize=20)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = '7 day metrics.png'
        plt.close()
        return plot_object
    
    @task
    def send_report(report, chat_id = -677113209):
        bot.sendMessage(chat_id=chat_id, text=report)
    
    @task
    def send_metrics(plot_object, chat_id = -677113209):
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        
    df7 = extract_7day_data()
    
    report = make_yesterday_report(df7)
    plot_object = make_metrics_7day(df7)
    
    send_report(report)
    send_metrics(plot_object)
    

feed_report_dag_volnenko = feed_report_dag_volnenko()  