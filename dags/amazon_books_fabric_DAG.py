# amazon_books_fabric_dag.py
# Airflow DAG for Microsoft Fabric
# Extracts Amazon book data → cleans with pandas → uploads CSV to OneLake
# Optional: triggers a Fabric notebook or pipeline for downstream analytics

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.microsoft.fabric.operators.fabric import FabricRunItemOperator
import requests
import pandas as pd
from bs4 import BeautifulSoup
#from azure.storage.blob import BlobServiceClient
import os

# ────────────────────────────────────────────────
# CONFIG — replace placeholders
# ────────────────────────────────────────────────
NUM_BOOKS = 50
FABRIC_CONN_ID = "amazon-books"             # connection name in Airflow UI
#FABRIC_WORKSPACE_ID = "<YOUR_FABRIC_WORKSPACE_ID>"
#FABRIC_NOTEBOOK_ITEM_ID = "<YOUR_FABRIC_NOTEBOOK_OR_PIPELINE_ITEM_ID>"
ONELAKE_PATH = "abfss://Project1_apacheAirflow@onelake.dfs.fabric.microsoft.com/amazon.Lakehouse/Files/raw_data/books.csv"


HEADERS = {
    "Referer": "https://www.amazon.com/",
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    "User-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        " AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/107.0.0.0 Safari/537.36"
    ),
}

# ────────────────────────────────────────────────
# ETL TASKS
# ────────────────────────────────────────────────

from airflow.exceptions import AirflowFailException
import time, random

USE_OPENLIBRARY_FALLBACK = True

def fetch_amazon_books(num_books, **context):
    ti = context["ti"]
    base_url = "https://www.amazon.com/s?k=data+engineering+books"
    books, seen = [], set()
    session = requests.Session()
    session.headers.update(HEADERS)

    page = 1
    while len(books) < num_books and page <= 5:  # cap pages to avoid long crawls
        url = f"{base_url}&page={page}"
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.content, "html.parser")
        # slightly more permissive selectors
        for item in soup.select("div.s-result-item"):
            title_el = item.select_one("h2 a span.a-text-normal") or item.select_one("span.a-text-normal")
            author_el = item.select_one("a.a-size-base")
            price_whole = item.select_one("span.a-price-whole")
            price_frac  = item.select_one("span.a-price-fraction")
            rating_el = item.select_one("span.a-icon-alt")

            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            if title in seen:
                continue
            seen.add(title)

            price = None
            if price_whole:
                pw = price_whole.get_text(strip=True).replace(",", "")
                pf = price_frac.get_text(strip=True) if price_frac else "0"
                price = f"{pw}.{pf}"

            rating = rating_el.get_text(strip=True) if rating_el else None
            author = author_el.get_text(strip=True) if author_el else None

            books.append({
                "Title": title,
                "Author": author,
                "Price": price,
                "Rating": rating,
            })

        page += 1
        time.sleep(random.uniform(1.0, 2.0))  # be polite

    # If nothing came back, optionally fall back to OpenLibrary (public API)
    if len(books) == 0 and USE_OPENLIBRARY_FALLBACK:
        ol = requests.get(
            "https://openlibrary.org/search.json",
            params={"q": "data engineering", "limit": num_books},
            timeout=30
        ).json()
        docs = ol.get("docs", [])
        for d in docs:
            books.append({
                "Title": d.get("title"),
                "Author": ", ".join(d.get("author_name", [])[:1]) or None,
                "Price": None,
                "Rating": d.get("ratings_average"),  # may be None
            })

    # Fail fast if still nothing (so downstream doesn't run pointlessly)
    if len(books) == 0:
        raise AirflowFailException("Fetch returned 0 rows (site blocked / structure changed).")

    df = pd.DataFrame(books[:num_books]).drop_duplicates(subset="Title")
    ti.xcom_push(key="book_data", value=df.to_dict("records"))
    print(f"✅ Extracted {len(df)} rows")



def clean_book_data(**context):
    ti = context["ti"]
    book_data = ti.xcom_pull(key="book_data", task_ids="fetch_amazon_books")
    if not book_data:
        raise ValueError("No book data found for cleaning")

    df = pd.DataFrame(book_data)
    df["etl_processed_at"] = datetime.utcnow().isoformat()
    df["Price"] = (
    df["Price"].astype(str)
        .str.replace(",", "", regex=False)
        .replace("None", pd.NA)
    )
    df["Rating"] = (
        df["Rating"].astype(str)
        .str.extract(r"(\d+\.\d+)", expand=False)
        .astype(float)
        .fillna(0)
    )


    # Push back to XCom instead of saving locally
    ti.xcom_push(key="cleaned_books", value=df.to_dict("records"))
    print(f"✅ Cleaned {len(df)} books")


def upload_to_onelake(**context):
    ti = context["ti"]
    cleaned_books = ti.xcom_pull(key="cleaned_books", task_ids="clean_book_data")
    if not cleaned_books:
        raise ValueError("No cleaned book data found")

    df = pd.DataFrame(cleaned_books)
    local_path = "/lakehouse/amzon/Files/raw_data/books.csv"  # mounted path in Fabric
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    df.to_csv(local_path, index=False)

    print(f"✅ Uploaded to OneLake: {local_path}")


# ────────────────────────────────────────────────
# DAG DEFINITION
# ────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="amazon_books_fabric_dag",
    default_args=default_args,
    description="Extract Amazon book data and store it in OneLake via Fabric Airflow Job",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["fabric", "onelake", "amazon", "etl"],
) as dag:

    fetch_amazon_books_task = PythonOperator(
        task_id="fetch_amazon_books",
        python_callable=fetch_amazon_books,
        op_args=[NUM_BOOKS],
    )

    clean_book_data_task = PythonOperator(
        task_id="clean_book_data",
        python_callable=clean_book_data,
    )

    upload_to_onelake_task = PythonOperator(
        task_id="upload_to_onelake",
        python_callable=upload_to_onelake,
    )

    #trigger_fabric_notebook = FabricRunItemOperator(
     #   task_id="trigger_fabric_notebook",
      #  fabric_conn_id=FABRIC_CONN_ID,
       # workspace_id=FABRIC_WORKSPACE_ID,
        #item_id=FABRIC_NOTEBOOK_ITEM_ID,
        #check_interval_seconds=10,
        #timeout_seconds=3600,
    

    fetch_amazon_books_task >> clean_book_data_task >> upload_to_onelake_task


