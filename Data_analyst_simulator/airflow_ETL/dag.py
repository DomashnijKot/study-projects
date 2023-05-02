import pandas as pd
import pandahouse as ph
import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230120',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                           'database':'test',
                           'user':'student-rw',
                           'password':'656e2b0c9c'
                  }

default_args = {
    'owner': 'a-volnenko-15',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5),
    'start_date': dt.datetime(2023, 2, 11),
}


schedule_interval = '30 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def volnenko_dag_stats():
    
    @task 
    def extract_feed():
        # В feed_actions для каждого юзера посчитаем число просмотров и лайков контента

        query = '''
        SELECT toDate(time) as event_date, user_id, countIf(action='like') as likes,
            countIf(action='view') as views, gender, age, os
        FROM simulator_20230120.feed_actions
        WHERE event_date = yesterday()
        GROUP BY event_date, user_id, gender, age, os
        '''

        return ph.read_clickhouse(query, connection=connection)

    @task
    def extract_messages():
        #В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений,
        #скольким людям он пишет, сколько людей пишут ему
        query = '''
        WITH
        -- Считаем кол-во отправленных сообщений и число получателей
        source AS
            (SELECT toDate(time) as event_date, user_id, count() as messages_sent, uniqExact(reciever_id) as users_sent,
                gender, age, os
            FROM simulator_20230120.message_actions
            WHERE event_date = yesterday()
            GROUP BY user_id, event_date, gender, age, os),

        -- Считаем кол-во принятных сообщений и число отправителей
        reciever AS
            (SELECT toDate(time) as event_date, reciever_id as user_id, count() as messages_received, uniqExact(user_id) as users_received,
                gender, age, os
            FROM simulator_20230120.message_actions
            WHERE event_date = yesterday()
            GROUP BY reciever_id, event_date, gender, age, os)

        SELECT event_date, user_id, s.messages_sent, s.users_sent, r.messages_received, r.users_received,
            gender, age, os
        FROM source s
        FULL JOIN reciever r
        USING user_id, event_date, gender, age, os
        '''

        return ph.read_clickhouse(query, connection=connection)

    
    @task
    def join_tables(df1, df2):
        # Соединяем две таблицы
        res = pd.merge(df1, df2, on=['event_date', 'user_id', 'gender', 'age', 'os'], how='outer')
        res.fillna(0, inplace=True)
        float_cl = res.select_dtypes(include='float64').columns
        res[float_cl] = res[float_cl].astype('int64')
        return res    
    
    @task
    def gender_split(df):
        # Срез по полу

        split_col = 'gender'
        features = ['views', 'likes', 'messages_received', 'messages_sent', 
                                 'users_received', 'users_sent']
        new_df = (df
                     .groupby(['event_date',split_col])[features]
                     .sum()
                     .reset_index()
                     .rename(columns={split_col: 'dimension_value'}))
        new_df['dimension'] = split_col
        new_df = new_df[['event_date', 'dimension', 'dimension_value'] + features]
        return new_df
    
    @task
    def age_split(df):
        # Срез по возрасту

        split_col = 'age'
        features = ['views', 'likes', 'messages_received', 'messages_sent', 
                                 'users_received', 'users_sent']
        new_df = (df
                     .groupby(['event_date',split_col])[features]
                     .sum()
                     .reset_index()
                     .rename(columns={split_col: 'dimension_value'}))
        new_df['dimension'] = split_col
        new_df = new_df[['event_date', 'dimension', 'dimension_value'] + features]
        return new_df
    
    @task
    def os_split(df):
        # Срез по os

        split_col = 'os'
        features = ['views', 'likes', 'messages_received', 'messages_sent', 
                                 'users_received', 'users_sent']
        new_df = (df
                     .groupby(['event_date',split_col])[features]
                     .sum()
                     .reset_index()
                     .rename(columns={split_col: 'dimension_value'}))
        new_df['dimension'] = split_col
        new_df = new_df[['event_date', 'dimension', 'dimension_value'] + features]
        return new_df

    @task
    def df_concat(df1, df2, df3):
        return pd.concat([df1, df2, df3], axis=0)
    
    @task
    def load(df):

        query_create_table = """
        CREATE TABLE IF NOT EXISTS test.a_volnenko (
            event_date Date,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_received UInt64,
            messages_sent UInt64,
            users_received UInt64,
            users_sent UInt64
        )
        ENGINE = MergeTree()
        ORDER BY event_date
            """

        ph.execute(query_create_table, connection=connection_test)
        
        ph.to_clickhouse(df, 'a_volnenko', index=False, connection=connection_test)
    
    
    feed = extract_feed()
    messages = extract_messages()
    merged = join_tables(messages, feed)
    gender_df = gender_split(merged)
    age_df = age_split(merged)
    os_df = os_split(merged)
    df_res = df_concat(gender_df, age_df, os_df)
    load(df_res)
    

volnenko_dag_stats = volnenko_dag_stats()