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

"""Example DAG demonstrating the usage of the PythonOperator."""
import time
from datetime import datetime, timedelta, date
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from scripts.main import main

url = 'https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab'
default_args = {
	'owner': 'rahmawaminggu',
	'depends_on_past': False,
    	'email_on_failuer': False,
    	'email_on_retry': False,
    	'retries': 0
}

with DAG(
    dag_id='dag_final_project',
    schedule_interval='0 3,15 * * *',
    start_date=datetime(2022, 5, 1),
    catchup=False,
    tags=['de'],
    default_args=default_args
) as dag:

    start = DummyOperator(
        task_id='start_job',
    )

    fetch_json = PythonOperator(
        task_id='fetch_json',
        python_callable=main,
        op_kwargs={'url': url}
    )
    # [END howto_operator_python]

    run_ddl = PostgresOperator(
        task_id="run_ddl",
        postgres_conn_id='pg_dwh',
        sql='sql/ddl_final_prj.sql'
    )

    run_dim_case = PostgresOperator(
        task_id="run_dim_case",
        postgres_conn_id='pg_dwh',
        sql='sql/load_case_dim.sql'
    )

    run_dim_province = PostgresOperator(
        task_id="run_dim_province",
        postgres_conn_id='pg_dwh',
        sql='sql/load_province_dim.sql'
    )

    run_dim_district = PostgresOperator(
        task_id="run_dim_district",
        postgres_conn_id='pg_dwh',
        sql='sql/load_district_dim.sql'
    )

    step_2 = BashOperator(
        task_id='print_status',
        bash_command='echo dim tables successfully inserted',
        trigger_rule='all_success'
    )

    run_fact_province_district = PostgresOperator(
        task_id="run_fact_province_district",
        postgres_conn_id='pg_dwh',
        sql='sql/load_province_district_dly.sql'
    )

    # [ START PROVINCE AGG ]

    run_fact_province_dly = PostgresOperator(
        task_id="run_fact_province_dly",
        postgres_conn_id='pg_dwh',
        sql='sql/load_province_dly.sql'
    )

    run_fact_province_mth = PostgresOperator(
        task_id="run_fact_province_mth",
        postgres_conn_id='pg_dwh',
        sql='sql/load_province_mth.sql'
    )

    run_fact_province_yearly = PostgresOperator(
        task_id="run_fact_province_yearly",
        postgres_conn_id='pg_dwh',
        sql='sql/load_province_yearly.sql'
    )

    # [ END PROVINCE AGG ]

    # [START DISTRICT AGG ]
    run_fact_district_dly = PostgresOperator(
        task_id="run_fact_district_dly",
        postgres_conn_id='pg_dwh',
        sql='sql/load_district_dly.sql'
    )

    run_fact_district_mth = PostgresOperator(
        task_id="run_fact_district_mth",
        postgres_conn_id='pg_dwh',
        sql='sql/load_district_mth.sql'
    )

    run_fact_district_yearly = PostgresOperator(
        task_id="run_fact_district_yearly",
        postgres_conn_id='pg_dwh',
        sql='sql/load_district_yearly.sql'
    )
    # [END DISTRICT AGG ]


    # [ finish ]
    step_3 = BashOperator(
        task_id='print_status_finish',
        bash_command='echo all jobs already finish',
        trigger_rule='all_success'
    )
    # [end ]


    start >> fetch_json >> run_ddl >> [run_dim_case, run_dim_province, run_dim_district] >> step_2 >> run_fact_province_district
    run_fact_province_district >> run_fact_province_dly >> run_fact_province_mth >> run_fact_province_yearly >> step_3
    run_fact_province_district >> run_fact_district_dly >> run_fact_district_mth >> run_fact_district_yearly >> step_3
    
    # [END howto_operator_python_kwargs]






    # # [ get date ]
# get_date = date.today() + timedelta(days=-1)
# curr_date = get_date.strftime("%Y-%m-%d")
# curr_month = get_date.strftime("%Y%m")
# curr_year = get_date.strftime("%Y")
# # [ end get date ]
