#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import time
from datetime import datetime, timedelta, date
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.odbc import pyodbc as odbc

import requests
import pandas as pd
import sys


url = 'https://webapi.bps.go.id/v1/api/list/model/data/domain/3300/var/87/key/206dc477af8073658139f101fc153b32/'
default_args = {
	'owner': 'rahmawaminggu',
	'depends_on_past': False,
        'email' : ['indrar0104@gmail.com'],
    	'email_on_failuer': False,
    	'email_on_retry': False,
    	'retries': 0
}

with DAG(
    dag_id='dag_BPS_Jateng_kependudukan',
    schedule_interval='0 3,15 * * *',
    start_date=datetime(2022, 5, 1),
    catchup=False,
    tags=['bps_jateng_penduduk'],
    default_args=default_args

) as dag:

    start = DummyOperator(
        task_id='start_job',
    )

    def fetch_api(url):
        z = requests.get(url).json()
        return z

    fetch_json = PythonOperator(
        task_id='fetch_json',
        python_callable=fetch_api,
        op_kwargs={'url': url}
    )

    def dim_gender(z):
        turvar = pd.DataFrame(z['turvar'])
        conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.139.209.176;DATABASE=NBA_External_Analytics;UID=sa;PWD=Nb4sqlserver@')
        cursor = conn.cursor()
        cursor.execute("delete from bps_dim_gender")

        for index, row in turvar.iterrows():
            cursor.execute("""insert into bps_dim_gender
            values(?,?)
            """,row.val
                ,row.label)
        conn.commit()
        cursor.close()
    
    run_dim_gender = PythonOperator(
        task_id='run_dim_gender',
        python_callable=dim_gender,
        op_kwargs={'z': z}
    )

    def dim_kota_kab(z):
        vervar = pd.DataFrame(z['vervar'])
        vervar = vervar.drop(vervar[(vervar.val == 3300 )].index)
        vervar[['jenis_kota_kab','desk_kota_kab']] = vervar['label'].str.split(" ",expand=True)
        conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.139.209.176;DATABASE=NBA_External_Analytics;UID=sa;PWD=Nb4sqlserver@')
        cursor = conn.cursor()
        cursor.execute("delete from bps_dim_kota_kab")

        for index, row in vervar.iterrows():
            cursor.execute("""insert into bps_dim_kota_kab(id_kota_kab,jenis_kota_kab,desk_kota_kab)
            values(?,?,?)
            """,row.val
                ,row.jenis_kota_kab
            ,row.desk_kota_kab)
        conn.commit()
        cursor.close()

    run_dim_kota_kab = PythonOperator(
        task_id='run_dim_kota_kab',
        python_callable=dim_kota_kab,
        op_kwargs={'z' : z}
    )

    def dim_tahun(z):
        tahun = pd.DataFrame(z['tahun'])
        conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.139.209.176;DATABASE=NBA_External_Analytics;UID=sa;PWD=Nb4sqlserver@')
        cursor = conn.cursor()
        cursor.execute("delete from bps_dim_tahun")

        for index, row in tahun.iterrows():
            cursor.execute("""insert into bps_dim_tahun
            values(?,?)
            """,row.val
                ,row.label)
        conn.commit()
        cursor.close()

    run_dim_tahun = PythonOperator(
        task_id='run_dim_tahun',
        python_callable=dim_tahun,
        op_kwargs={'z' : z}
    )

    step_2 = BashOperator(
        task_id='print_status',
        bash_command='echo dim tables successfully inserted',
        trigger_rule='all_success'
    )

    def fact_table(z):
        datacontent = pd.DataFrame(z['datacontent'], index=[0]).transpose().reset_index()
        datacontent.rename(columns={"index":"id", 0:"jumlah_penduduk"},inplace=True)
        kota_kab = datacontent['id'].str.slice(start=0,stop=4)
        jenis_kelamin = datacontent['id'].str.slice(start=6,stop=8)
        tahun = datacontent['id'].str.slice(start=8,stop=-1)
        datacontent = datacontent.assign(kota_kab = kota_kab, jenis_kelamin=jenis_kelamin,tahun=tahun)
        conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.139.209.176;DATABASE=NBA_External_Analytics;UID=sa;PWD=Nb4sqlserver@')
        cursor = conn.cursor()
        cursor.execute("delete from bps_fact_jumlah_penduduk")

        for index, row in datacontent.iterrows():
            cursor.execute("""insert into bps_fact_jumlah_penduduk
            values(?,?,?,?,?)
            """,row.id
                ,row.jumlah_penduduk
            ,row.kota_kab
            ,row.jenis_kelamin
            ,row.tahun)
        conn.commit()
        cursor.close()

    run_fact_table = PythonOperator(
        task_id='run_fact_table',
        python_callable=fact_table,
        op_kwargs={'z' : z}
    )

    step_3 = BashOperator(
        task_id='print_status_finish',
        bash_command='echo all jobs already finish',
        trigger_rule='all_success'
    )


    start >> fetch_json >> [run_dim_gender, run_dim_kota_kab, run_dim_tahun] >> step_2 >> run_fact_table
    run_fact_table >> step_3
    
    # [END howto_operator_python_kwargs]

    # # [ get date ]
# get_date = date.today() + timedelta(days=-1)
# curr_date = get_date.strftime("%Y-%m-%d")
# curr_month = get_date.strftime("%Y%m")
# curr_year = get_date.strftime("%Y")
# # [ end get date ]