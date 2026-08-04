"""Scrapes English-language company listings from spyur.am into `spyur_en`
(plan Step 3.0). Config comes from environment variables / .env - see
.env.example.

IMPORTANT: spyur.am currently serves a Cloudflare "Just a moment..." JS
challenge to plain HTTP clients - `requests` alone gets 200 OK with challenge
HTML, not the real page, so every ID looked "not found" even though the
company existed. `cloudscraper` solves the JS challenge for most Cloudflare
"low" / "managed" challenge tiers; if spyur.am is on a stricter tier this may
still fail; `_looks_like_challenge_page()` detects that case explicitly so
the crawl logs it clearly and backs off, instead of silently mis-recording
every ID as invalid the way the previous plain-requests version did.
"""

import os
import random
import time
from datetime import datetime, timezone

import cloudscraper
import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL is not set (env or .env) - refusing to run without a target database")

CHECKPOINT_ID = int(os.getenv("SCRAPER_CHECKPOINT_ID", "4"))
MAX_COMPANY_ID = int(os.getenv("MAX_COMPANY_ID", "100000"))
REQUEST_DELAY_MIN = float(os.getenv("REQUEST_DELAY_MIN", "0.5"))
REQUEST_DELAY_MAX = float(os.getenv("REQUEST_DELAY_MAX", "1.5"))
MAX_CONSECUTIVE_CHALLENGES = int(os.getenv("MAX_CONSECUTIVE_CHALLENGES", "10"))

_SCRAPER = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})

# Category selector is best-effort: spyur's markup wasn't directly
# inspectable while this was written (Cloudflare-blocked), so we try a
# short list of plausible anchor-list selectors in order and log which one
# actually matched, rather than the previous `.info_content *` (which
# selects every descendant element and produces fragmented/duplicated text).
_CATEGORY_SELECTORS = [
    ".info_content a",
    ".classifier_list a",
    "ul.classifier a",
    ".categories a",
    ".info_content li",
]


def get_db_connection():
    return psycopg2.connect(DB_URL)


def create_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS spyur_en (
            id BIGINT PRIMARY KEY,
            name TEXT,
            owner TEXT,
            address TEXT,
            phones TEXT[],
            categories TEXT[],
            founded_year TEXT,
            scraped_at TIMESTAMP DEFAULT now()
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scraper_checkpoint (
            id INTEGER PRIMARY KEY,
            last_id INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    cur.execute(
        "INSERT INTO scraper_checkpoint (id, last_id) VALUES (%s, 0) ON CONFLICT (id) DO NOTHING",
        (CHECKPOINT_ID,),
    )


def get_last_checkpoint(cur) -> int:
    cur.execute("SELECT last_id FROM scraper_checkpoint WHERE id = %s;", (CHECKPOINT_ID,))
    row = cur.fetchone()
    return row[0] if row else 0


def update_checkpoint(cur, last_id: int) -> None:
    cur.execute(
        "UPDATE scraper_checkpoint SET last_id = %s, updated_at = %s WHERE id = %s;",
        (last_id, datetime.now(timezone.utc), CHECKPOINT_ID),
    )


def _looks_like_challenge_page(html: str) -> bool:
    lowered = html[:2000].lower()
    return "just a moment" in lowered or "cf-mitigated" in lowered or "cf_chl_opt" in lowered


def _extract_categories(soup: BeautifulSoup) -> tuple[list[str], str | None]:
    for selector in _CATEGORY_SELECTORS:
        found = soup.select(selector)
        if found:
            texts = [c.get_text(strip=True) for c in found if c.get_text(strip=True)]
            if texts:
                return list(dict.fromkeys(texts)), selector
    return [], None


def scrape_company(company_id: int) -> dict | None:
    url = f"https://www.spyur.am/en/companies/{company_id}/"
    response = _SCRAPER.get(url, timeout=20)

    if response.status_code != 200:
        return None

    if _looks_like_challenge_page(response.text):
        raise RuntimeError(f"Cloudflare challenge page returned for {url} - scraper is blocked")

    soup = BeautifulSoup(response.text, "html.parser")

    company_name = soup.select_one(".page_title")
    if not company_name:
        return None

    owner = soup.select_one(".lead_info.text_block")
    address = soup.select_one(".address_block")
    phones = soup.select(".phone_info")
    categories, matched_selector = _extract_categories(soup)
    if company_id == 1 or matched_selector:
        print(f"  (category selector matched: {matched_selector!r})")

    founded_year = None
    for item in soup.select("ul.info_list li"):
        title = item.select_one(".inner_subtitle")
        value = item.select_one(".text_block")
        if title and "Year established" in title.get_text(strip=True) and value:
            founded_year = value.get_text(strip=True).replace("\n", "").strip()
            break

    return {
        "id": company_id,
        "name": company_name.get_text(strip=True),
        "owner": owner.get_text(strip=True) if owner else None,
        "address": address.get_text(strip=True) if address else None,
        "phones": list({p.get_text(strip=True) for p in phones}) if phones else [],
        "categories": categories,
        "founded_year": founded_year,
    }


def save_company(cur, data: dict) -> None:
    cur.execute(
        """
        INSERT INTO spyur_en (id, name, owner, address, phones, categories, founded_year)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, owner = EXCLUDED.owner, address = EXCLUDED.address,
            phones = EXCLUDED.phones, categories = EXCLUDED.categories,
            founded_year = EXCLUDED.founded_year;
        """,
        (data["id"], data["name"], data["owner"], data["address"],
         data["phones"], data["categories"], data["founded_year"]),
    )


def main():
    conn = get_db_connection()
    cur = conn.cursor()
    create_tables(cur)
    conn.commit()

    start_id = get_last_checkpoint(cur)
    print(f"Resuming from {start_id}...")

    consecutive_challenges = 0
    for company_id in range(start_id, MAX_COMPANY_ID):
        try:
            data = scrape_company(company_id)
            consecutive_challenges = 0
        except RuntimeError as exc:
            consecutive_challenges += 1
            print(f"ID {company_id} -> {exc} ({consecutive_challenges}/{MAX_CONSECUTIVE_CHALLENGES})")
            if consecutive_challenges >= MAX_CONSECUTIVE_CHALLENGES:
                print("Too many consecutive Cloudflare challenges - stopping without advancing checkpoint.")
                break
            time.sleep(10 * consecutive_challenges)
            continue

        if not data:
            print(f"ID {company_id} -> not found, skipping DB.")
        else:
            save_company(cur, data)
            print(f"Saved: {data['name']} ({company_id})")

        update_checkpoint(cur, company_id)
        conn.commit()
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
