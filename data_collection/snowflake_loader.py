import os
import snowflake.connector
from dotenv import load_dotenv

# Load env vars (for local dev)
load_dotenv()

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

if __name__ == "__main__":
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    print("Successfully connected to:", cursor.fetchone()[0])
    conn.close()