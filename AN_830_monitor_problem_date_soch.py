from datetime import datetime
import logging
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

table_name = "monitor_problem_date_soch"
schema_name = "dwh_mid"
dwh_db = "dwh-postgres"


# check if table exists
def check_table_exists(connection):
    try:
        check_query = f"SELECT EXISTS(select from information_schema.tables where table_name = '{table_name}' and table_schema = '{schema_name}')"
        exist_state = pd.read_sql(check_query, con=connection).iloc[0, 0] == 1
    except Exception as ex:
        logging.error(f"Failed to connect to {dwh_db} database: {ex}")
        raise
    return exist_state


# table creation
def create_table(cursor):
    cre_tab_query = f"""
        create table {schema_name}.{table_name}(
        t1_id bigint,
        pd_soch int)
        """

    try:
        cursor.execute(cre_tab_query)
    except Exception as ex:
        logging.error(f"create_table failure {dwh_db} database: {ex}")
        raise


# indexes creation
def create_indexes(cursor):
    cre_idx_query = f"""
        CREATE INDEX {table_name}_t1_id_index ON {schema_name}.{table_name} (t1_id);
        """


    try:
        cursor.execute(cre_idx_query)
    except Exception as ex:
        logging.error(f"create_indexes failure {dwh_db} database: {ex}")
        raise


# fill in entity
def fill_data(cursor):
    fill_data_query = f"""insert into {schema_name}.{table_name}
        SELECT t1.t1_id                                     AS t1_id,
               CASE WHEN t1.date = t1.period_end THEN 1 END AS pd_soch
        FROM dwh_bi.monitor_sor_soch_by_dates t1
        WHERE t1.type = 'soch'
        GROUP BY t1.t1_id, CASE WHEN t1.date = t1.period_end THEN 1 END
        HAVING CASE WHEN t1.date = t1.period_end THEN 1 END IS NOT NULL;
        """


    try:
        cursor.execute(fill_data_query)
    except Exception as ex:
        logging.error(f"fill_data failure {dwh_db} database: {ex}")
        raise


# main step - refresh data in entity
@task
def refresh_data():
    logging.info(f"Attempting to connect to {dwh_db} database")
    pg_hook = PostgresHook(postgres_conn_id=dwh_db)
    try:
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        logging.info(f"Successfully connected to {dwh_db}")
        logging.info(f"Checking table {table_name} exists...")

        state = check_table_exists(connection)

        if not state:
            create_table(cursor)
            create_indexes(cursor)
        else:
            cursor.execute(f"truncate table {schema_name}.{table_name}")

        fill_data(cursor)

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as ex:
        logging.error(f"Failed to connect to {dwh_db} database: {ex}")
        raise


with DAG(
        'AN830_monitor_problem_date_soch',
        start_date=datetime(2024, 7, 11, 00, 00, 00),
        schedule_interval='30 23 * * *',  # '*/10 * * * *', None
        catchup=False,
        tags=["AN830_monitor_sor_soch"]
) as dag:
    data_sche_time = refresh_data()
