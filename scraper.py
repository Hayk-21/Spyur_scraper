import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import random
from datetime import datetime, timezone
import os

# DB_URL = os.getenv("DB_URL")
DB_URL = "postgresql://neondb_owner:npg_jY8oIh0trUwX@ep-misty-paper-adycpb8w-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

maximum_company_id = 100000  # adjust as needed

# Bare requests.get() looks like a script; spyur.am returns 403 without browser-like headers.
_http = requests.Session()
_http.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,hy;q=0.8",
        "Referer": "https://www.spyur.am/",
        "Upgrade-Insecure-Requests": "1",
    }
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
    url = f"https://www.spyur.am/en/companies/{company_id}/"
    response = _http.get(url, timeout=60)
    print("response: ", response.status_code)
    if response.status_code != 200:
        return None  # not found or bad request

    soup = BeautifulSoup(response.text, "html.parser")

    company_name = soup.select_one(".page_title")
    print("company_name: ", company_name)
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

    start_id = get_last_checkpoint()
    print(f"Resuming from {start_id}...")

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

