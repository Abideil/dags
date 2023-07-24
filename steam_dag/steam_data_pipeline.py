from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from sf_conn import snowflake_args, SNOWFLAKE_CONN_ID
import snowflake.connector


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}


def loading_data():
    conn = snowflake.connector.connect(**snowflake_args)
    cursor = conn.cursor()
    cursor.execute("PUT file:///opt/airflow/data/dataset.csv @INTERNAL_STAGE/steam;")
    cursor.execute("TRUNCATE TABLE raw_data;")
    cursor.execute("""
        INSERT INTO raw_data (raw_col)
        SELECT 
          V
        FROM @INTERNAL_STAGE/steam (FILE_FORMAT => TEXT_FORMAT, PATTERN => '.*.csv.gz') STG
        -- Lateral join to call our UDTF
        JOIN LATERAL PARSE_CSV(STG.$1, ',', '"');
    """)
    cursor.close()
    conn.close()

def filling_struct_table():
    conn = snowflake.connector.connect(**snowflake_args)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE struct_data;")
    cursor.execute("""
        INSERT INTO STRUCT_DATA (ID, NAME, YEAR, METACRITIC_RATING, REVIEWER_RATING,
            POSITIVITY_RATIO, TO_BEAT_MAIN, TO_BEAT_EXTRA, TO_BEAT_COMPLETIONIST, EXTRA_CONTENT_LENGTH, TAGS)
        SELECT 
          $1:ID::INT AS ID,
          $1:NAME::VARCHAR AS NAME,
          $1:YEAR::INT AS YEAR,
          $1:METACRITIC_RATING::INT AS METACRITIC_RATING,
          $1:REVIEWER_RATING::INT AS REVIEWER_RATING,
          $1:POSITIVITY_RATIO::NUMBER(20,15) AS POSITIVITY_RATIO,
          $1:TO_BEAT_MAIN::NUMBER(7,2) AS TO_BEAT_MAIN,
          $1:TO_BEAT_EXTRA::NUMBER(7,2) AS TO_BEAT_EXTRA,
          $1:TO_BEAT_COMPLETIONIST::NUMBER(7,2) AS TO_BEAT_COMPLETIONIST,
          $1:EXTRA_CONTENT_LENGTH::NUMBER(20,15) AS EXTRA_CONTENT_LENGTH,
          $1:TAGS::VARCHAR as TAGS
        FROM raw_data;""")
    cursor.close()
    conn.close()

def filling_ratings_table():
    conn = snowflake.connector.connect(**snowflake_args)
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
    cursor.close()
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



