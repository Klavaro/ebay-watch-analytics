from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import requests
import json
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping,SnowflakePrivateKeyPemProfileMapping
from cosmos.operators import DbtSnapshotOperator 



# Constants
EBAY_API_URL = "https://api.sandbox.ebay.com/buy/browse/v1/item_summary/search"
QUERY_PARAMS = {"q": "laptop", "limit": "100"}
SNOWFLAKE_CONN_ID = "snowflake_rsa"
TABLE_NAME = "EBAY_ANALYTICS.RAW.EBAY_RAW"
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt-ebay/"


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def get_ebay_oauth_token():
    url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }
    auth = (
        Variable.get("ebay_app_id"),
        Variable.get("ebay_cert_id")
    )
    response = requests.post(url, headers=headers, data=data, auth=auth)
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_and_load_ebay_raw():
    token = get_ebay_oauth_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.get(EBAY_API_URL, headers=headers, params=QUERY_PARAMS)
    response.raise_for_status()
    data = response.json().get("itemSummaries", [])

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"SELECT MAX(ITEM_CREATION_DATE) FROM {TABLE_NAME}")
    result = cursor.fetchone()
    max_creation_date = result[0].replace(tzinfo=timezone.utc) if result[0] else datetime.min.replace(tzinfo=timezone.utc)

    for item in data:
        item_creation_str = item.get("itemCreationDate")
        if not item_creation_str:
            continue
        item_creation_date = datetime.fromisoformat(item_creation_str.replace("Z", "+00:00"))
        if item_creation_date <= max_creation_date:
            continue

        image = item.get("image", {})
        price = item.get("price", {})
        seller = item.get("seller", {})
        item_location = item.get("itemLocation", {})

        row = {
            "ITEM_ID": item.get("itemId"),
            "TITLE": item.get("title"),
            "LEAF_CATEGORY_IDS": json.dumps(item.get("leafCategoryIds", [])),
            "CATEGORIES": json.dumps(item.get("categories", [])),
            "ITEM_HREF": item.get("itemHref"),
            "CONDITION": item.get("condition"),
            "CONDITION_ID": item.get("conditionId"),
            "THUMBNAIL_IMAGES": json.dumps(item.get("thumbnailImages", [])),
            "SHIPPING_OPTIONS": json.dumps(item.get("shippingOptions", [])),
            "BUYING_OPTIONS": json.dumps(item.get("buyingOptions", [])),
            "ITEM_WEB_URL": item.get("itemWebUrl"),
            "ADDITIONAL_IMAGES": json.dumps(item.get("additionalImages", [])),
            "ADULT_ONLY": item.get("adultOnly"),
            "LEGACY_ITEM_ID": item.get("legacyItemId"),
            "AVAILABLE_COUPONS": item.get("availableCoupons"),
            "ITEM_ORIGIN_DATE": item.get("itemOriginDate"),
            "ITEM_CREATION_DATE": item.get("itemCreationDate"),
            "TOP_RATED_BUYING_EXPERIENCE": item.get("topRatedBuyingExperience"),
            "PRIORITY_LISTING": item.get("priorityListing"),
            "LISTING_MARKETPLACE_ID": item.get("listingMarketplaceId"),
            "IMAGE_URL": image.get("imageUrl"),
            "PRICE_VALUE": price.get("value"),
            "PRICE_CURRENCY": price.get("currency"),
            "SELLER_USERNAME": seller.get("username"),
            "SELLER_FEEDBACK_PERCENTAGE": seller.get("feedbackPercentage"),
            "SELLER_FEEDBACK_SCORE": seller.get("feedbackScore"),
            "ITEM_LOCATION_COUNTRY": item_location.get("country"),
            "LOAD_TIMESTAMP": datetime.utcnow(),
        }

        sql = f"""
            INSERT INTO {TABLE_NAME} (
                ITEM_ID, TITLE, LEAF_CATEGORY_IDS, CATEGORIES, ITEM_HREF,
                CONDITION, CONDITION_ID, THUMBNAIL_IMAGES, SHIPPING_OPTIONS, BUYING_OPTIONS,
                ITEM_WEB_URL, ADDITIONAL_IMAGES, ADULT_ONLY,
                LEGACY_ITEM_ID, AVAILABLE_COUPONS, ITEM_ORIGIN_DATE, ITEM_CREATION_DATE,
                TOP_RATED_BUYING_EXPERIENCE, PRIORITY_LISTING, LISTING_MARKETPLACE_ID,
                IMAGE_URL, PRICE_VALUE, PRICE_CURRENCY, SELLER_USERNAME,
                SELLER_FEEDBACK_PERCENTAGE, SELLER_FEEDBACK_SCORE, ITEM_LOCATION_COUNTRY,
                LOAD_TIMESTAMP
            ) VALUES (
                %(ITEM_ID)s, %(TITLE)s, %(LEAF_CATEGORY_IDS)s, %(CATEGORIES)s, %(ITEM_HREF)s,
                %(CONDITION)s, %(CONDITION_ID)s, %(THUMBNAIL_IMAGES)s, %(SHIPPING_OPTIONS)s, %(BUYING_OPTIONS)s,
                %(ITEM_WEB_URL)s, %(ADDITIONAL_IMAGES)s, %(ADULT_ONLY)s,
                %(LEGACY_ITEM_ID)s, %(AVAILABLE_COUPONS)s, %(ITEM_ORIGIN_DATE)s, %(ITEM_CREATION_DATE)s,
                %(TOP_RATED_BUYING_EXPERIENCE)s, %(PRIORITY_LISTING)s, %(LISTING_MARKETPLACE_ID)s,
                %(IMAGE_URL)s, %(PRICE_VALUE)s, %(PRICE_CURRENCY)s, %(SELLER_USERNAME)s,
                %(SELLER_FEEDBACK_PERCENTAGE)s, %(SELLER_FEEDBACK_SCORE)s, %(ITEM_LOCATION_COUNTRY)s,
                %(LOAD_TIMESTAMP)s
            )
        """
        cursor.execute(sql, row)

    cursor.close()
    conn.close()

private_key_pem = Variable.get("rsa_private")
# Define DAG
with DAG(
    dag_id="ebay_to_snowflake_raw",
    default_args=default_args,
    description="Fetch raw eBay listings and load to Snowflake with exact schema match",
    start_date=datetime(2025, 7, 15),
    schedule_interval="@daily",
    catchup=False,
    tags=["ebay", "snowflake", "raw"],
) as dag:

    fetch_and_load = PythonOperator(
        task_id="fetch_and_load_ebay_raw",
        python_callable=fetch_and_load_ebay_raw,
    )

    profile_config = ProfileConfig(
        profile_name="snowflake",
        target_name="dev",
        profile_mapping=SnowflakePrivateKeyPemProfileMapping(
            conn_id=SNOWFLAKE_CONN_ID,
            profile_args={
                "database": "EBAY_ANALYTICS",
                "schema": "RAW",
                "warehouse": "EBAY_WH",
                "role": "EBAY_DEVELOPER",
                "private_key": private_key_pem  # pass key here inside profile_args
            },
        ),
    )

    dbt_snapshot = DbtSnapshotOperator(
        task_id="run_dbt_snapshot",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        install_deps=True,
    )


    # Single dbt run using DbtTaskGroup
    with DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
            "select": ["staging", "core", "marts"],
        },
    ) as dbt_tg:
        pass 

    fetch_and_load >> dbt_tg
