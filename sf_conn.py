from airflow.hooks.base_hook import BaseHook


SNOWFLAKE_CONN_ID = "snowflake_default"

snowflake_conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)

snowflake_args = {
    'user': snowflake_conn.login,
    'password': snowflake_conn.password,
    'region': snowflake_conn.extra_dejson.get("region"),
    'account': snowflake_conn.extra_dejson.get("account"),
    'warehouse': snowflake_conn.extra_dejson.get("warehouse"),
    'database': snowflake_conn.extra_dejson.get("database"),
    'schema': snowflake_conn.extra_dejson.get("schema")
}