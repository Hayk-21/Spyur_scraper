from curl_cffi import requests as curl_requests
from bs4 import BeautifulSoup
import psycopg2
import time
import random
from datetime import datetime, timezone
import os

# DB_URL = os.getenv("DB_URL")
DB_URL = "postgresql://neondb_owner:npg_jY8oIh0trUwX@ep-misty-paper-adycpb8w-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

maximum_company_id = 100000  # adjust as needed

# cloudscraper/requests use Python's TLS stack; many WAFs return 403. curl_cffi impersonates Chrome's TLS + HTTP/2.
_http = curl_requests.Session()
_IMPERSONATE = "chrome"

_PAGE_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9,hy;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
}


def warmup_http():
    """Load the English homepage first so cookies / same-origin navigation look real."""
    _http.get(
        "https://www.spyur.am/en/",
        impersonate=_IMPERSONATE,
        timeout=60,
        headers={**_PAGE_HEADERS, "Sec-Fetch-Site": "none"},
    )


def get_db_connection():
    return psycopg2.connect(DB_URL)


def create_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS spyur_en (
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
    cur = conn.cursor()

    cur.execute("SELECT last_id FROM scraper_checkpoint WHERE id = 4;")
    result = cur.fetchone()
    cur.close()
    conn.close()

    return result[0] if result else 0


def update_checkpoint(last_id):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE scraper_checkpoint
        SET last_id=%s, updated_at=%s
        WHERE id=4;
    """, (last_id, datetime.utcnow()))

    conn.commit()
    cur.close()
    conn.close()


def scrape_company(company_id: int):
    if company_id < 1:
        return None

    url = f"https://www.spyur.am/en/companies/{company_id}/"
    response = _http.get(
        url,
        impersonate=_IMPERSONATE,
        timeout=60,
        headers={
            **_PAGE_HEADERS,
            "Referer": "https://www.spyur.am/en/companies/",
            "Sec-Fetch-Site": "same-origin",
        },
    )
    print("response: ", response.status_code)
    if response.status_code != 200:
        return None  # not found or bad request

    soup = BeautifulSoup(response.text, "html.parser")

    company_name = soup.select_one(".page_title")
    owner = soup.select_one(".lead_info.text_block")
    address = soup.select_one(".address_block")
    phones = soup.select(".phone_info")
    categories = soup.select(".info_content *")

    founded_year = None  

    for item in soup.select("ul.info_list li"):
        title = item.select_one(".inner_subtitle")
        value = item.select_one(".text_block")

        if title and "Year established" in title.get_text(strip=True):
            founded_year = value.get_text(strip=True).replace("\n", "").strip()
            break  # stop at first match

    return {
        "id": company_id,
        "name": company_name.get_text(strip=True) if company_name else None,
        "owner": owner.get_text(strip=True) if owner else None,
        "address": address.get_text(strip=True) if address else None,
        "phones": list({p.get_text(strip=True) for p in phones}) if phones else [],
        "categories": list({c.get_text(strip=True) for c in categories}) if categories else [],
        "founded_year": founded_year
    }


def save_company(data):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO spyur_en (id, name, owner, address, phones, categories, founded_year)
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


if __name__ == "__main__":
    create_tables()

    start_id = get_last_checkpoint()
    print(f"Resuming from {start_id}...")

    try:
        warmup_http()
        print("Session warmup OK (homepage).")
    except Exception as e:
        print(f"Session warmup failed (continuing anyway): {e}")

    for company_id in range(start_id, maximum_company_id):  # adjust range if you want
        data = scrape_company(company_id)
        if not data or data["name"] == "ERROR!":
            print(f"ID {company_id} -> invalid / not found, skipping DB.")
        else:
            save_company(data)
            print(f"Saved: {data['name']} ({company_id})")

        # UPDATED: use timezone-aware UTC datetime
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE scraper_checkpoint
            SET last_id=%s, updated_at=%s
            WHERE id=4;
        """, (company_id, datetime.now(timezone.utc)))
        conn.commit()
        cur.close()
        conn.close()

        time.sleep(random.uniform(0.1, 0.2))

