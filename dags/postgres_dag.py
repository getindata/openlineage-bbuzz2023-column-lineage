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

dag = DAG(
    'postgres',
    schedule_interval=None,
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)

CONNECTION = "postgres_conn"

drop = PostgresOperator(
    task_id='postgres_drop_if_not_exists',
    postgres_conn_id=CONNECTION,
    sql='''
        DROP TABLE IF EXISTS jelly_colors, jelly_transactions, jelly_users, jelly_favorite_colors, jelly_active_daily_users, jelly_daily_users_green_jellys
    ''',
    dag=dag
)

create_jelly_colors = PostgresOperator(
    task_id='postgres_create_jelly_colors_table',
    postgres_conn_id=CONNECTION,
    sql='''
    CREATE TABLE jelly_colors (
        color_uuid varchar(36) primary key,
        name varchar(255)
    )
    ''',
    dag=dag
)

create_jelly_users = PostgresOperator(
    task_id='postgres_create_jelly_users_table',
    postgres_conn_id=CONNECTION,
    sql='''
    CREATE TABLE jelly_users (
        user_uuid varchar(36) primary key,
        name varchar(255)
    )
    ''',
    dag=dag
)

create_jelly_favorite_colors = PostgresOperator(
    task_id='postgres_create_jelly_favorite_colors_table',
    postgres_conn_id=CONNECTION,
    sql='''
    CREATE TABLE jelly_favorite_colors (
        user_uuid varchar(36) references jelly_users,
        color_uuid varchar(36) references jelly_colors
    )
    ''',
    dag=dag
)


# Unfortunately not a double-entry transactions table
create_jelly_transactions_table = PostgresOperator(
    task_id='postgres_create_jelly_transactions_table',
    postgres_conn_id=CONNECTION,
    sql='''
    CREATE TABLE jelly_transactions (
        from_user_uuid varchar(36) references jelly_users,
        to_user_uuid varchar(36) references jelly_users,
        exchanged_color_uuid varchar(36) references jelly_colors,
        amount_exchanged integer,
        lat float,
        lon float,
        exchange_date timestamp
    )
    ''',
    dag=dag
)

create_jelly_daily_users_view = PostgresOperator(
    task_id='postgres_daily_users_table',
    postgres_conn_id=CONNECTION,
    sql="""
    CREATE TABLE jelly_active_daily_users AS 
    SELECT u.user_uuid, u.name, SUM(amount_exchanged)
    FROM jelly_users u
    JOIN jelly_transactions t ON (u.user_uuid=t.from_user_uuid OR u.user_uuid=t.to_user_uuid) 
    GROUP BY u.user_uuid, u.name
    """,
    dag=dag
)


create_jelly_daily_users_green_jellys = PostgresOperator(
    task_id='postgres_daily_users_green_jellys_table',
    postgres_conn_id=CONNECTION,
    sql="""
    CREATE TABLE jelly_daily_users_green_jellys AS 
    SELECT u.name, c.name as color
    FROM jelly_active_daily_users u
    JOIN jelly_favorite_colors f ON f.user_uuid = u.user_uuid
    JOIN jelly_colors c ON c.color_uuid=f.color_uuid
    WHERE c.name = 'green'
    """,
    dag=dag
)

drop >> [
    create_jelly_colors, 
    create_jelly_users, 
] >> create_jelly_favorite_colors >> \
create_jelly_transactions_table >> \
create_jelly_daily_users_view >> create_jelly_daily_users_green_jellys
