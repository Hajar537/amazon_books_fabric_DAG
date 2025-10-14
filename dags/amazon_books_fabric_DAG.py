# amazon_books_fabric_dag_fixed.py
# Airflow DAG for Microsoft Fabric
# Extracts Amazon book data → cleans with pandas → uploads CSV to OneLake
# Optional: triggers a Fabric notebook or pipeline for downstream analytics

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import os
import time, random
from airflow.exceptions import AirflowFailException

# ✅ NEW IMPORTS for OneLake upload
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# ────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────
NUM_BOOKS = 50
CONTAINER_NAME = "Project1_apacheAirflow"
DIRECTORY_PATH = "amazon.Lakehouse/Files/raw_data"
FILE_NAME = "books.csv"

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

USE_OPENLIBRARY_FALLBACK = True

# ────────────────────────────────────────────────
# ETL TASKS
# ────────────────────────────────────────────────

def fetch_amazon_books(num_books, **context):
    """Scrape book data from Amazon or fallback to OpenLibrary."""
    ti = context["ti"]
    base_url = "https://www.amazon.com/s?k=data+engineering+books"
    books, seen = [], set()
    session = requests.Session()
    session.headers.update(HEADERS)

    page = 1
    while len(books) < num_books and page <= 5:
        url = f"{base_url}&page={page}"
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.content, "html.parser")
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
        time.sleep(random.uniform(1.0, 2.0))

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
                "Rating": d.get("ratings_average"),
            })

    if len(books) == 0:
        raise AirflowFailException("❌ No books fetched from either source.")

    df = pd.DataFrame(books[:num_books]).drop_duplicates(subset="Title")
    ti.xcom_push(key="book_data", value=df.to_dict("records"))
    print(f"✅ Extracted {len(df)} books from Amazon/OpenLibrary.")


def clean_book_data(**context):
    """Clean book data for consistency."""
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

    ti.xcom_push(key="cleaned_books", value=df.to_dict("records"))
    print(f"✅ Cleaned {len(df)} books successfully.")



def upload_to_onelake(**context):
    """
    Upload cleaned CSV to OneLake (ADLS Gen2 compatible) with robust logging:
    - Prefer SAS via ONELAKE_SAS_TOKEN if present (quickest to validate)
    - Else use DefaultAzureCredential (Managed Identity / SP)
    - Verifies file system and directory, creates directory if needed
    """
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import DefaultAzureCredential
    import os, traceback

    ti = context["ti"]
    cleaned_books = ti.xcom_pull(key="cleaned_books", task_ids="clean_book_data")
    if not cleaned_books:
        raise ValueError("No cleaned book data found")

    # 0) Write local CSV
    import pandas as pd
    df = pd.DataFrame(cleaned_books)
    local_csv = "/tmp/books.csv"
    df.to_csv(local_csv, index=False)
    print(f"[upload] local_csv={local_csv} rows={len(df)}")

    ACCOUNT_URL = "https://onelake.dfs.fabric.microsoft.com"
    FILE_SYSTEM = "Project1_apacheAirflow"                 # your workspace "container"
    DIRECTORY   = "amazon.Lakehouse/Files/raw_data"        # lakehouse + Files path
    FILE_NAME   = "books.csv"

    # 1) Choose credential
    sas = os.getenv("ONELAKE_SAS_TOKEN")
    if sas:
        print("[upload] Using SAS token from ONELAKE_SAS_TOKEN")
        service_client = DataLakeServiceClient(account_url=ACCOUNT_URL, credential=sas)
    else:
        print("[upload] Using DefaultAzureCredential()")
        cred = DefaultAzureCredential(exclude_interactive_browser_credential=True)
        service_client = DataLakeServiceClient(account_url=ACCOUNT_URL, credential=cred)

    try:
        # 2) Verify file system (container = workspace)
        fs_client = service_client.get_file_system_client(FILE_SYSTEM)
        exists = False
        try:
            # list_paths will fail if FS doesn't exist or permission denied
            _ = next(fs_client.get_paths(path="", max_results=1), None)
            exists = True
        except Exception as e:
            print(f"[upload] file system check error: {type(e).__name__}: {e}")
        if not exists:
            raise RuntimeError(
                f"File system '{FILE_SYSTEM}' not accessible. "
                "Check that this name EXACTLY matches your Fabric workspace "
                "and that the identity has permissions."
            )
        print(f"[upload] file system OK: {FILE_SYSTEM}")

        # 3) Ensure directory
        dir_client = fs_client.get_directory_client(DIRECTORY)
        try:
            dir_client.create_directory()
            print(f"[upload] created directory: {DIRECTORY}")
        except Exception:
            print(f"[upload] directory exists: {DIRECTORY}")

        # 4) Create/overwrite file
        file_client = dir_client.create_file(FILE_NAME)
        with open(local_csv, "rb") as f:
            data = f.read()
            file_client.append_data(data=data, offset=0, length=len(data))
            file_client.flush_data(len(data))
        print(f"[upload] ✅ uploaded to onelake: {FILE_SYSTEM}/{DIRECTORY}/{FILE_NAME}")

        # Optional: push the ABFS path for downstream tasks
        abfs_path = f"abfss://{FILE_SYSTEM}@onelake.dfs.fabric.microsoft.com/{DIRECTORY}/{FILE_NAME}"
        ti.xcom_push(key="onelake_abfs_path", value=abfs_path)
        print(f"[upload] abfs_path={abfs_path}")

    except Exception as e:
        print("[upload] ❌ upload failed")
        traceback.print_exc()
        raise


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
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    clean_book_data_task = PythonOperator(
        task_id="clean_book_data",
        python_callable=clean_book_data,
    )

    upload_to_onelake_task = PythonOperator(
        task_id="upload_to_onelake",
        python_callable=upload_to_onelake,
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    fetch_amazon_books_task >> clean_book_data_task >> upload_to_onelake_task
