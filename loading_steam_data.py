from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import snowflake.connector


SNOWFLAKE_CONN_ID = "snowflake_default"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def loading_data():
    snowflake_conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    conn = snowflake.connector.connect(
        user=snowflake_conn.login,
        password=snowflake_conn.password,
        region=snowflake_conn.extra_dejson.get("region"),
        account=snowflake_conn.extra_dejson.get("account"),
        warehouse=snowflake_conn.extra_dejson.get("warehouse"),
        database=snowflake_conn.extra_dejson.get("database"),
        schema=snowflake_conn.extra_dejson.get("schema")
        )
    cursor = conn.cursor()
    cursor.execute("PUT file:///opt/airflow/data/dataset.csv @INTERNAL_STAGE;")
    cursor.execute("TRUNCATE TABLE raw_data;")
    cursor.execute("""
        INSERT INTO raw_data (raw_col)
        SELECT 
          V
        FROM @INTERNAL_STAGE (FILE_FORMAT => TEXT_FORMAT, PATTERN => '.*.csv.gz') STG
        -- Lateral join to call our UDTF
        JOIN LATERAL PARSE_CSV(STG.$1, ',', '"');
    """)
    conn.close()

def filling_struct_table():
    snowflake_conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    conn = snowflake.connector.connect(
        user=snowflake_conn.login,
        password=snowflake_conn.password,
        region=snowflake_conn.extra_dejson.get("region"),
        account=snowflake_conn.extra_dejson.get("account"),
        warehouse=snowflake_conn.extra_dejson.get("warehouse"),
        database=snowflake_conn.extra_dejson.get("database"),
        schema=snowflake_conn.extra_dejson.get("schema")
        )
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE struct_data;")
    cursor.execute("""
    INSERT INTO STRUCT_DATA (ID, NAME, YEAR, METACRITIC_RATING, REVIEWER_RATING,
        POSITIVITY_RATIO, TO_BEAT_MAIN, TO_BEAT_EXTRA, TO_BEAT_COMPLETIONIST, EXTRA_CONTENT_LENGTH, TAGS)
    WITH VARIANT_DATA AS (
    SELECT raw_col V
    FROM raw_data
    )
    SELECT 
      V:ID::INT AS ID,
      V:NAME::VARCHAR AS NAME,
      V:YEAR::INT AS YEAR,
      V:METACRITIC_RATING::INT AS METACRITIC_RATING,
      V:REVIEWER_RATING::INT AS REVIEWER_RATING,
      V:POSITIVITY_RATIO::NUMBER(20,15) AS POSITIVITY_RATIO,
      V:TO_BEAT_MAIN::NUMBER(7,2) AS TO_BEAT_MAIN,
      V:TO_BEAT_EXTRA::NUMBER(7,2) AS TO_BEAT_EXTRA,
      V:TO_BEAT_COMPLETIONIST::NUMBER(7,2) AS TO_BEAT_COMPLETIONIST,
      V:EXTRA_CONTENT_LENGTH::NUMBER(20,15) AS EXTRA_CONTENT_LENGTH,
      V:TAGS::VARCHAR as TAGS
    FROM VARIANT_DATA;""")
    conn.close()

def filling_ratings_table():
    snowflake_conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    conn = snowflake.connector.connect(
        user=snowflake_conn.login,
        password=snowflake_conn.password,
        region=snowflake_conn.extra_dejson.get("region"),
        account=snowflake_conn.extra_dejson.get("account"),
        warehouse=snowflake_conn.extra_dejson.get("warehouse"),
        database=snowflake_conn.extra_dejson.get("database"),
        schema=snowflake_conn.extra_dejson.get("schema")
        )
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE rating_by_year;")
    cursor.execute("""
        INSERT INTO rating_by_year
        SELECT
            year,
            max(metacritic_rating) as max_mr,
            max(reviewer_rating) as max_rr
        FROM struct_data
        GROUP BY 1;""")
    conn.close()

with DAG(
    'loading_steam_data', 
    default_args=default_args,
    schedule_interval='@once',
    ) as dag:
    loading_data = PythonOperator(
        task_id='loading_data',
        python_callable=loading_data,
        dag=dag
    )
    filling_struct_table = PythonOperator(
        task_id='filling_struct_table',
        python_callable=filling_struct_table,
        dag=dag
    )
    filling_ratings_table = PythonOperator(
        task_id='filling_ratings_table',
        python_callable=filling_ratings_table,
        dag=dag
        )

loading_data >> filling_struct_table >> filling_ratings_table



