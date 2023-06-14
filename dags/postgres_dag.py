# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client import set_producer

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}
CONNECTION = "postgres_conn"

with DAG(
    'postgres',
    schedule_interval=None,
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
):


    drop_tables = PostgresOperator(
        task_id='postgres_drop_if_not_exists',
        postgres_conn_id=CONNECTION,
        sql='''
            DROP TABLE IF EXISTS users, trips
        '''
    )

    create_users_table = PostgresOperator(
        task_id='postgres_create_users_table',
        postgres_conn_id=CONNECTION,
        sql='''
        CREATE TABLE users (
            user_uuid varchar(36) primary key,
            name varchar(255)
        )
        '''
    )

    create_trips_table = PostgresOperator(
        task_id='postgres_create_trips_table',
        postgres_conn_id=CONNECTION,
        sql='''
        CREATE TABLE trips (
            user_uuid varchar(36) primary key references users,
            name varchar(255),
            address varchar(255),
            trip_date timestamp
        )
        '''
    )

    create_daily_users_table = PostgresOperator(
        task_id='postgres_daily_users_table',
        postgres_conn_id=CONNECTION,
        sql="""
        CREATE TABLE daily_users AS 
        SELECT u.user_uuid, u.name, DATE(t.trip_date) as date, COUNT(*) as count
        FROM users u
        JOIN trips t ON u.user_uuid=t.user_uuid  
        GROUP BY u.user_uuid, u.name, DATE(t.trip_date)
        """
    )

    drop_tables >> [
        create_users_table, 
        create_trips_table, 
    ] >> create_daily_users_table
