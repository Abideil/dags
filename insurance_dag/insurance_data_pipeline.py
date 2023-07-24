from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from sf_conn import snowflake_args, SNOWFLAKE_CONN_ID
import snowflake.connector


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 24),
}


def loading_raw_data():
    conn = snowflake.connector.connect(**snowflake_args)
    cursor = conn.cursor()
    cursor.execute("PUT file:///opt/airflow/data/insurance.csv @INTERNAL_STAGE/insurance;")
    cursor.execute("TRUNCATE TABLE raw_insurance_data;")
    cursor.execute("""
        INSERT INTO raw_insurance_data (raw_col)
        SELECT 
          V
        FROM @INTERNAL_STAGE/insurance (FILE_FORMAT => TEXT_FORMAT, PATTERN => '.*.csv.gz') STG
        -- Lateral join to call our UDTF
        JOIN LATERAL PARSE_CSV(STG.$1, ',', '"');
    """)
    cursor.close()
    conn.close()

def loading_struct_data():
    conn = snowflake.connector.connect(**snowflake_args)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE struct_insurance_data;")

    # Data cleaning (actualy it depends on what we can call an insufficient data) and then loading into a structured table
    cursor.execute("""
        INSERT INTO struct_insurance_data (age, sex, bmi, children, smoker, region, charges)
        SELECT DISTINCT
          $1:AGE::INT AS AGE,
          $1:SEX::VARCHAR AS SEX,
          $1:BMI::NUMBER(5,2) AS BMI,
          $1:CHILDREN::INT AS CHILDREN,
          $1:SMOKER::VARCHAR AS SMOKER,
          $1:REGION::VARCHAR AS REGION,
          $1:CHARGES::NUMBER(7,2) AS CHARGES
        FROM raw_insurance_data
        WHERE CHARGES IS NOT NULL;""")
    cursor.close()
    conn.close()

def loading_smokers_stats():
    conn = snowflake.connector.connect(**snowflake_args)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE SMOKER_INSURANCE_STAT;")

    # statistics about smokers depending on the region
    cursor.execute("""
        INSERT INTO SMOKER_INSURANCE_STAT
        SELECT
            region,
            min(charges),
            max(charges)
        FROM struct_insurance_data
        WHERE SMOKER = 'yes'
        GROUP BY 1;""")
    cursor.close()
    conn.close()

def loading_obesity_stats():
    conn = snowflake.connector.connect(**snowflake_args)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE OBESITY_INSURANCE_STAT;")

    # statistics about obesity depending on the region (bmi over 30)
    cursor.execute("""
        INSERT INTO OBESITY_INSURANCE_STAT
        SELECT
            region,
            min(charges),
            max(charges)
        FROM struct_insurance_data
        WHERE BMI >= 30
        GROUP BY 1;""")
    cursor.close()
    conn.close()

def loading_age_stats():
    conn = snowflake.connector.connect(**snowflake_args)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE AGE_INSURANCE_STAT;")

    # statistics about insurance payouts depending on age (w/o group concatenation)
    cursor.execute("""
        INSERT INTO AGE_INSURANCE_STAT
        SELECT
            age,
            min(charges),
            max(charges)
        FROM struct_insurance_data
        GROUP BY 1;""")
    cursor.close()
    conn.close()

def check_tables():
    conn = snowflake.connector.connect(**snowflake_args)
    tables_to_check = ['SMOKER_INSURANCE_STAT', 'OBESITY_INSURANCE_STAT', 'AGE_INSURANCE_STAT']
    for table in tables_to_check:
        cursor = conn.cursor()
        cursor.execute(f"select count(*) from {table}")
        count = cursor.fetchone()[0]
        cursor.close()
        if count == 0:
            raise ValueError(f"Table '{table}' is empty.")
    conn.close()

with DAG(
    'insuranse_data_pipeline', 
    default_args=default_args,
    schedule_interval='@once',
    ) as dag:
    loading_raw_data_task = PythonOperator(
        task_id='loading_raw_data',
        python_callable=loading_raw_data,
        dag=dag
    )

    loading_struct_data_task = PythonOperator(
        task_id='loading_struct_data',
        python_callable=loading_struct_data,
        dag=dag
    )

    loading_smokers_stats_task = PythonOperator(
        task_id='loading_smokers_stats',
        python_callable=loading_smokers_stats,
        dag=dag
    )

    loading_obesity_stats_task = PythonOperator(
        task_id='loading_obesity_stats',
        python_callable=loading_obesity_stats,
        dag=dag
    )

    loading_age_stats_task = PythonOperator(
        task_id='loading_age_stats',
        python_callable=loading_age_stats,
        dag=dag
    )

    check_tables_not_empty = PythonOperator(
        task_id='check_tables',
        python_callable=check_tables,
        dag=dag
    )

loading_raw_data_task >> loading_struct_data_task >> [loading_smokers_stats_task, loading_obesity_stats_task, loading_age_stats_task] >> check_tables_not_empty


