import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import random
from datetime import datetime, timezone
import os

DB_URL = os.getenv("DB_URL")
maximum_company_id = 100000  # adjust as needed

# HTTP headers to identify as a regular browser
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hy-AM,ru-RU,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Referer": "https://www.spyur.am/",
}

def get_db_connection():
    try:
        if not DB_URL:
            print("[DB] Error: DB_URL environment variable is not set.")
            return None
        return psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        print(f"[DB] OperationalError while connecting: {e}")
        return None
    except Exception as e:
        print(f"[DB] Unexpected error while connecting: {e}")
        return None


def create_tables():
    conn = get_db_connection()
    if conn is None:
        print("[DB] create_tables skipped due to connection error.")
        return
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS spyur (
        id BIGINT PRIMARY KEY,
        name TEXT,
        owner TEXT,
        address TEXT,
        phones TEXT[],
        categories TEXT[],
        founded_year TEXT,
        scraped_at TIMESTAMP DEFAULT now()
    );
    """)

    conn.commit()
    cur.close()
    conn.close()


def get_last_checkpoint():
    conn = get_db_connection()
    if conn is None:
        print("[DB] get_last_checkpoint failed: no DB connection.")
        return 0
    cur = conn.cursor()

    cur.execute("SELECT last_id FROM scraper_checkpoint WHERE id = 3;")
    result = cur.fetchone()
    cur.close()
    conn.close()

    return result[0] if result else 0


def update_checkpoint(last_id):
    conn = get_db_connection()
    if conn is None:
        print("[DB] update_checkpoint skipped: no DB connection.")
        return
    cur = conn.cursor()

    cur.execute("""
        UPDATE scraper_checkpoint
        SET last_id=%s, updated_at=%s
        WHERE id=3;
    """, (last_id, datetime.utcnow()))

    conn.commit()
    cur.close()
    conn.close()


def scrape_company(company_id: int):
    url = f"https://www.spyur.am/am/companies/{company_id}/"

    # Basic retry for transient errors
    transient_statuses = {429, 500, 502, 503, 504}
    max_retries = 3
    backoff = 1.0

    response = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
        except requests.RequestException as e:
            print(f"[HTTP] Request error for ID {company_id}: {e}")
            if attempt == max_retries:
                return {"id": company_id, "status": None, "error": str(e)}
            time.sleep(backoff)
            backoff *= 2
            continue

        if response.status_code in transient_statuses and attempt < max_retries:
            print(f"[HTTP] {response.status_code} for ID {company_id}, retry {attempt}/{max_retries}...")
            time.sleep(backoff)
            backoff *= 2
            continue
        break

    if response is None:
        return {"id": company_id, "status": None}

    if response.status_code == 403:
        print(f"[HTTP] 403 Forbidden for ID {company_id}. Access likely blocked.")
        return {"id": company_id, "status": 403}
    if response.status_code == 404:
        print(f"[HTTP] 404 Not Found for ID {company_id}.")
        return {"id": company_id, "status": 404}
    if response.status_code != 200:
        print(f"[HTTP] {response.status_code} for ID {company_id}.")
        return {"id": company_id, "status": response.status_code}

    soup = BeautifulSoup(response.text, "html.parser")

    company_name = soup.select_one(".page_title")
    owner = soup.select_one(".lead_info.text_block")
    address = soup.select_one(".address_block")
    phones = soup.select(".phone_info")
    categories = soup.select(".info_content *")

 # 🔥 Extract founding year
    founded_year = None  

    for item in soup.select("ul.info_list li"):
        title = item.select_one(".inner_subtitle")
        value = item.select_one(".text_block")

        if title and "Հիմնադրման տարի" in title.get_text(strip=True):
            founded_year = value.get_text(strip=True).replace("\n", "").strip()
            break  # stop at first match

    return {
        "id": company_id,
        "status": 200,
        "name": company_name.get_text(strip=True) if company_name else None,
        "owner": owner.get_text(strip=True) if owner else None,
        "address": address.get_text(strip=True) if address else None,
        "phones": list({p.get_text(strip=True) for p in phones}) if phones else [],
        "categories": list({c.get_text(strip=True) for c in categories}) if categories else [],
        "founded_year": founded_year
    }


def save_company(data):
    conn = get_db_connection()
    if conn is None:
        print("[DB] save_company skipped: no DB connection.")
        return
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO spyur (id, name, owner, address, phones, categories, founded_year)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name=EXCLUDED.name,
            owner=EXCLUDED.owner,
            address=EXCLUDED.address,
            phones=EXCLUDED.phones,
            categories=EXCLUDED.categories,
            founded_year=EXCLUDED.founded_year;
    """, (
        data["id"],
        data["name"],
        data["owner"],
        data["address"],
        data["phones"],
        data["categories"],
        data["founded_year"]
    ))

    conn.commit()
    cur.close()
    conn.close()


from datetime import datetime, timezone

if __name__ == "__main__":
    create_tables()

    company_id = get_last_checkpoint()
    print(f"Resuming from {company_id}...")

    while company_id < maximum_company_id:
        company_id = get_last_checkpoint()+1
        data = scrape_company(company_id)
        status = data.get("status") if isinstance(data, dict) else None

        if status == 403:
            print(f"ID {company_id} -> access blocked (403), skipping.")
        elif status == 404:
            print(f"ID {company_id} -> not found (404), skipping.")
        elif status and status != 200:
            print(f"ID {company_id} -> HTTP {status}, skipping.")
        elif not data or data.get("name") == "ՍԽԱ՛Լ Է":
            print(f"ID {company_id} -> invalid page content, skipping DB.")
        else:
            save_company(data)
            print(f"Saved: {data['name']} ({company_id})")

        # UPDATED: use timezone-aware UTC datetime
        conn = get_db_connection()
        if conn is None:
            print("[DB] Checkpoint update skipped: no DB connection.")
            time.sleep(random.uniform(0.5, 1.5))
            continue
        cur = conn.cursor()
        cur.execute("""
            UPDATE scraper_checkpoint
            SET last_id=%s, updated_at=%s
            WHERE id=3;
        """, (company_id, datetime.now(timezone.utc)))
        conn.commit()
        cur.close()
        conn.close()

        time.sleep(random.uniform(0.5, 1.5))

