"""Scrapes English-language company data from spyur.am into `spyur_en`
via the yellow-pages category tree (plan Step 8).

Why the category tree instead of the old 0..100000 ID sweep:
- the ID sweep wasted ~75% of requests on non-existent IDs and got the IP
  captcha-flagged; the tree only touches real, classified companies;
- every listing page yields 20 (company, category) pairs in one request -
  the categories are exactly what the classification pipeline needs;
- multi-category membership is captured because a company appears in each
  of its category listings.

Anti-bot notes (verified 2026-08-15): spyur.am sits behind Cloudflare but
serves real pages to plain `requests` with a browser User-Agent. If a
challenge page appears mid-crawl we switch the transport to `cloudscraper`
once, and if challenges persist the run aborts with status='blocked' in
`scraper_runs` (checkpoint preserved, so the next run resumes where we
stopped instead of re-hammering the site).

Run modes:
    python scraper.py --once      one crawl now, then exit
    python scraper.py --service   long-running weekly service (Railway) -
                                  checks hourly, crawls when the last OK run
                                  is >= SCRAPE_INTERVAL_DAYS old. No cron.
"""

import argparse
import math
import os
import random
import re
import sys
import time
from datetime import datetime, timezone

import requests
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL is not set (env or .env) - refusing to run without a target database")

BASE = "https://www.spyur.am"
SOURCE = "spyur"

CHECKPOINT_ID = int(os.getenv("SCRAPER_CHECKPOINT_ID", "4"))  # last completed leaf ypid
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "25"))
REQUEST_DELAY_MIN = float(os.getenv("REQUEST_DELAY_MIN", "1.5"))
REQUEST_DELAY_MAX = float(os.getenv("REQUEST_DELAY_MAX", "3.0"))
TREE_DELAY = float(os.getenv("TREE_DELAY", "0.7"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
MAX_CONSECUTIVE_CHALLENGES = int(os.getenv("MAX_CONSECUTIVE_CHALLENGES", "8"))
MAX_LISTING_PAGES_PER_RUN = int(os.getenv("MAX_LISTING_PAGES_PER_RUN", "6000"))
MAX_DETAIL_PER_RUN = int(os.getenv("MAX_DETAIL_PER_RUN", "1500"))
SCRAPE_INTERVAL_DAYS = float(os.getenv("SCRAPE_INTERVAL_DAYS", "7"))
RETRY_CAPPED_HOURS = float(os.getenv("RETRY_CAPPED_HOURS", "24"))
RETRY_FAILED_HOURS = float(os.getenv("RETRY_FAILED_HOURS", "6"))
SERVICE_POLL_SECONDS = int(os.getenv("SERVICE_POLL_SECONDS", "3600"))

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

_COMPANY_HREF_RE = re.compile(r"/en/companies/([^/]+)/(\d+)")
_LEAF_HREF_RE = re.compile(r"/en/yellow_pages/yp/(\d+)")
_COUNT_SUFFIX_RE = re.compile(r"\s*\((\d+)\)\s*$")
# Real category ypids are <= 5 digits. Synonym/alias tree nodes get a synthetic
# id of <alias-prefix><real-ypid zero-padded to 5>; their listing pages contain
# the same companies as the real leaf, so crawling them would multiply the
# request count ~4x for zero new data (verified 2026-08-15: all 11,877 alias
# nodes resolved to one of the 3,905 real leaves).
_REAL_YPID_MAX = 99999


class ChallengeBlocked(RuntimeError):
    """Cloudflare keeps blocking us - the crawl must stop."""


# Residential IPs get plain pages, but datacenter IPs (Railway) receive 403
# from Cloudflare based on IP reputation + the python-requests TLS fingerprint
# (cloudscraper shares that fingerprint, so it doesn't help there). Transport
# escalation ladder:
#   requests    - fastest, works from residential IPs
#   cloudscraper- solves JS challenge pages (challenge HTML with HTTP 200)
#   curl_cffi   - impersonates Chrome's real TLS/JA3 fingerprint; usually the
#                 one that works from datacenter IPs
# If even curl_cffi gets 403, the IP itself is blocked - set PROXY_URL (or
# HTTPS_PROXY) to route through a residential/ISP proxy.
PROXY_URL = os.getenv("PROXY_URL", "").strip()

# Alternative to a paid proxy: the Cloudflare Worker in worker/ forwards
# requests from Cloudflare's own network, which spyur.am's Cloudflare accepts.
# Set both on Railway:
#   WORKER_PROXY_URL=https://spyur-proxy.<subdomain>.workers.dev
#   WORKER_PROXY_TOKEN=<the PROXY_TOKEN secret given to the worker>
WORKER_PROXY_URL = os.getenv("WORKER_PROXY_URL", "").strip().rstrip("/")
WORKER_PROXY_TOKEN = os.getenv("WORKER_PROXY_TOKEN", "").strip()

_BROWSER_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.spyur.am/",
}


class Http:
    _ESCALATION = ["requests", "cloudscraper", "curl_cffi"]

    def __init__(self):
        start = os.getenv("START_TRANSPORT", "requests").strip() or "requests"
        if start not in self._ESCALATION:
            print(f"[http] unknown START_TRANSPORT {start!r} - using 'requests'")
            start = "requests"
        self._transport = start
        self._session = self._make_session(start)
        self._consecutive_challenges = 0

    @staticmethod
    def _make_session(kind: str):
        proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
        if kind == "requests":
            s = requests.Session()
            s.headers.update(_BROWSER_HEADERS)
            if proxies:
                s.proxies.update(proxies)
            return s
        if kind == "cloudscraper":
            import cloudscraper

            s = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
            if proxies:
                s.proxies.update(proxies)
            return s
        if kind == "curl_cffi":
            from curl_cffi import requests as curl_requests

            return curl_requests.Session(impersonate="chrome", proxies=proxies)
        raise ValueError(kind)

    @staticmethod
    def _looks_like_challenge(html: str) -> bool:
        lowered = html[:2500].lower()
        return (
            "just a moment" in lowered
            or "cf-mitigated" in lowered
            or "cf_chl_opt" in lowered
            or "turnstile" in lowered
        )

    def _escalate(self) -> bool:
        """Move to the next transport. False when already on the last one."""
        idx = self._ESCALATION.index(self._transport)
        for nxt in self._ESCALATION[idx + 1:]:
            try:
                self._session = self._make_session(nxt)
            except Exception as exc:  # noqa: BLE001 - missing optional dep etc.
                print(f"[http] transport {nxt} unavailable: {type(exc).__name__}: {exc}")
                continue
            self._transport = nxt
            print(f"[http] switched transport to {nxt}")
            return True
        return False

    @staticmethod
    def _route(url: str, params: dict | None) -> tuple[str, dict | None]:
        """Rewrite the request through the Cloudflare Worker proxy when
        configured (WORKER_PROXY_URL). Params are folded into the target URL
        because the worker takes the full URL as a single query param."""
        if not WORKER_PROXY_URL:
            return url, params
        from urllib.parse import urlencode

        target = url + ("?" + urlencode(params) if params else "")
        return WORKER_PROXY_URL, {"token": WORKER_PROXY_TOKEN, "url": target}

    def get(self, url: str, params: dict | None = None):
        url, params = self._route(url, params)
        network_errors = 0
        while True:
            try:
                resp = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            except Exception as exc:  # requests + curl_cffi raise different types
                network_errors += 1
                if network_errors >= MAX_RETRIES:
                    raise
                time.sleep(min(2 ** network_errors * 2, 15))
                continue
            if resp.status_code in (403, 429, 503) or (
                resp.status_code == 200 and self._looks_like_challenge(resp.text)
            ):
                self._consecutive_challenges += 1
                body_snippet = (resp.text or "")[:180].replace("\n", " ")
                print(
                    f"[http] challenge/{resp.status_code} on {url} via {self._transport} "
                    f"({self._consecutive_challenges}/{MAX_CONSECUTIVE_CHALLENGES}) "
                    f"body: {body_snippet!r}"
                )
                if self._consecutive_challenges >= MAX_CONSECUTIVE_CHALLENGES:
                    raise ChallengeBlocked(
                        f"blocked after {self._consecutive_challenges} challenges at {url} "
                        f"(last transport: {self._transport}"
                        + ("" if PROXY_URL else "; consider setting PROXY_URL")
                        + ")"
                    )
                if not self._escalate():
                    # Already on the last transport - back off, then retry it.
                    time.sleep(min(10 * self._consecutive_challenges, 60))
                continue
            resp.raise_for_status()
            self._consecutive_challenges = 0
            return resp


# --------------------------------------------------------------------------- db

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
    cur.execute("ALTER TABLE spyur_en ADD COLUMN IF NOT EXISTS slug TEXT")
    cur.execute("ALTER TABLE spyur_en ADD COLUMN IF NOT EXISTS detail_scraped_at TIMESTAMP")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS spyur_category (
            ypid BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            parent_path TEXT,
            company_count INTEGER,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
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
    cur.execute("ALTER TABLE spyur_category ALTER COLUMN ypid TYPE BIGINT")
    cur.execute("ALTER TABLE scraper_checkpoint ALTER COLUMN last_id TYPE BIGINT")
    cur.execute(
        "INSERT INTO scraper_checkpoint (id, last_id) VALUES (%s, 0) ON CONFLICT (id) DO NOTHING",
        (CHECKPOINT_ID,),
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scraper_runs (
            id SERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT now(),
            finished_at TIMESTAMP,
            status TEXT NOT NULL DEFAULT 'running',
            pages_fetched INTEGER NOT NULL DEFAULT 0,
            rows_upserted INTEGER NOT NULL DEFAULT 0,
            detail TEXT
        )
        """
    )


def get_checkpoint(cur) -> int:
    cur.execute("SELECT last_id FROM scraper_checkpoint WHERE id = %s", (CHECKPOINT_ID,))
    row = cur.fetchone()
    return row[0] if row else 0


def set_checkpoint(cur, last_id: int) -> None:
    cur.execute(
        "UPDATE scraper_checkpoint SET last_id = %s, updated_at = %s WHERE id = %s",
        (last_id, datetime.now(timezone.utc), CHECKPOINT_ID),
    )


def start_run(cur) -> int:
    # A crash/redeploy can leave a 'running' row behind forever - close it out
    # so the runs table stays a truthful log.
    cur.execute(
        "UPDATE scraper_runs SET status = 'aborted', finished_at = now() "
        "WHERE source = %s AND status = 'running'",
        (SOURCE,),
    )
    cur.execute(
        "INSERT INTO scraper_runs (source, status) VALUES (%s, 'running') RETURNING id",
        (SOURCE,),
    )
    return cur.fetchone()[0]


def finish_run(cur, run_id: int, status: str, pages: int, rows: int, detail: str = "") -> None:
    cur.execute(
        """
        UPDATE scraper_runs
        SET finished_at = now(), status = %s, pages_fetched = %s, rows_upserted = %s, detail = %s
        WHERE id = %s
        """,
        (status, pages, rows, detail[:500], run_id),
    )


def upsert_companies(cur, rows: list[tuple[int, str | None, str | None, list[str]]]) -> None:
    """Batched upsert - one network round trip per leaf instead of one per
    company (local -> Neon latency made per-row writes the bottleneck).
    Callers must pre-dedupe by company id (ON CONFLICT can't touch the same
    row twice in one statement)."""
    if not rows:
        return
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO spyur_en (id, name, slug, categories, scraped_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, spyur_en.name),
            slug = COALESCE(EXCLUDED.slug, spyur_en.slug),
            categories = (
                SELECT COALESCE(array_agg(DISTINCT c ORDER BY c), '{}')
                FROM unnest(COALESCE(spyur_en.categories, '{}') || EXCLUDED.categories) AS c
            ),
            scraped_at = now()
        """,
        rows,
        template="(%s, %s, %s, %s, now())",
    )


# ------------------------------------------------------------------- tree walk

def walk_category_tree(http: Http) -> list[dict]:
    """Return leaf categories: [{ypid, name, parent_path, count}, ...]."""
    roots = http.get(f"{BASE}/tree/yp_en.json").json()
    leaves: list[dict] = []

    def clean(name: str) -> tuple[str, int | None]:
        m = _COUNT_SUFFIX_RE.search(name)
        count = int(m.group(1)) if m else None
        return _COUNT_SUFFIX_RE.sub("", name).strip(), count

    for root in roots:
        prop = root["property"]
        root_name, _ = clean(prop["name"])
        time.sleep(TREE_DELAY)
        level2 = http.get(
            f"{BASE}/load-tree.php",
            params={"type": "yp", "lang": "en", "id": prop["id"], "level": "1"},
        ).json()
        for node in level2:
            l2_name, _ = clean(node["name"])
            time.sleep(TREE_DELAY)
            level3 = http.get(
                f"{BASE}/load-tree.php",
                params={"type": "yp", "lang": "en", "id": node["id"], "level": "2"},
            ).json()
            for leaf in level3:
                href = (leaf.get("a_attr") or {}).get("href") or ""
                m = _LEAF_HREF_RE.search(href)
                if not m:
                    continue  # no listing page (count 0 in practice)
                leaf_name, count = clean(BeautifulSoup(leaf["name"], "html.parser").get_text())
                leaves.append(
                    {
                        "ypid": int(m.group(1)),
                        "name": leaf_name,
                        "parent_path": f"{root_name} > {l2_name}",
                        "count": count,
                    }
                )

    # Drop synonym/alias nodes that point at a real leaf we already have,
    # then dedupe by ypid.
    real_ypids = {l["ypid"] for l in leaves if l["ypid"] <= _REAL_YPID_MAX}
    deduped: dict[int, dict] = {}
    for leaf in leaves:
        if leaf["ypid"] > _REAL_YPID_MAX and int(str(leaf["ypid"])[-5:]) in real_ypids:
            continue
        if leaf["ypid"] not in deduped:
            deduped[leaf["ypid"]] = leaf
    return list(deduped.values())


# ------------------------------------------------------------------- listings

def parse_listing_page(html: str) -> list[tuple[int, str, str]]:
    """[(company_id, slug, clean_name)] from a category listing page."""
    soup = BeautifulSoup(html, "html.parser")
    wrapper = soup.find(id="results_list_wrapper") or soup
    out: list[tuple[int, str, str]] = []
    seen: set[int] = set()
    for a in wrapper.find_all("a", href=True):
        m = _COMPANY_HREF_RE.search(a["href"])
        if not m:
            continue
        company_id = int(m.group(2))
        if company_id in seen:
            continue
        seen.add(company_id)
        name_el = a.select_one(".company_name")
        name = name_el.get_text(strip=True) if name_el else None
        out.append((company_id, m.group(1), name))
    return out


def listing_page_count(html: str) -> int:
    m = re.search(r"Found\s+(\d+)\s+compan", html)
    if not m:
        return 1
    return max(1, math.ceil(int(m.group(1)) / 20))


def crawl_listings(http: Http, conn, cur, leaves: list[dict], pages_budget: int) -> tuple[int, int, bool]:
    """Crawl category listing pages, upserting (company, category) pairs.
    Returns (pages_fetched, rows_upserted, completed_all_leaves)."""
    checkpoint = get_checkpoint(cur)
    if checkpoint:
        print(f"[listings] resuming after leaf ypid {checkpoint}")
    pages = rows = 0
    todo = sorted(
        (l for l in leaves if l["ypid"] > checkpoint and (l["count"] is None or l["count"] > 0)),
        key=lambda l: l["ypid"],
    )

    for leaf in todo:
        if pages >= pages_budget:
            return pages, rows, False
        first = http.get(f"{BASE}/en/yellow_pages/yp/{leaf['ypid']}")
        pages += 1
        total_pages = listing_page_count(first.text)
        page_html = [first.text]
        truncated = False
        for n in range(2, total_pages + 1):
            if pages >= pages_budget:
                truncated = True
                break
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            page_html.append(http.get(f"{BASE}/en/yellow_pages-{n}/yp/{leaf['ypid']}").text)
            pages += 1

        merged: dict[int, tuple[int, str | None, str | None, list[str]]] = {}
        for html in page_html:
            for company_id, slug, name in parse_listing_page(html):
                merged[company_id] = (company_id, name, slug, [leaf["name"]])
        upsert_companies(cur, list(merged.values()))
        leaf_rows = len(merged)
        rows += leaf_rows
        if truncated:
            # Budget ran out mid-leaf: keep the rows, but leave the checkpoint
            # on the previous leaf so the next run re-crawls this one fully.
            conn.commit()
            print(f"[listings] {leaf['ypid']} {leaf['name']}: {leaf_rows} companies (partial, budget hit)")
            return pages, rows, False
        set_checkpoint(cur, leaf["ypid"])
        conn.commit()
        print(f"[listings] {leaf['ypid']} {leaf['name']}: {leaf_rows} companies ({total_pages}p)")
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    set_checkpoint(cur, 0)  # full pass complete - next run starts fresh
    conn.commit()
    return pages, rows, True


# -------------------------------------------------------------------- details

def parse_company_page(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.select_one(".page_title")
    if not title:
        return None
    owner = soup.select_one(".lead_info.text_block")
    address = soup.select_one(".address_block")
    phones = soup.select(".phone_info")

    categories = []
    for a in soup.find_all("a", href=re.compile(r"/en/yellow_pages/yp/\d+")):
        text = a.get_text(strip=True)
        if text and text not in categories and "Types of activity" not in text:
            categories.append(text)

    founded_year = None
    for item in soup.select("ul.info_list li"):
        t = item.select_one(".inner_subtitle")
        v = item.select_one(".text_block")
        if t and v and "Year established" in t.get_text(strip=True):
            founded_year = v.get_text(strip=True).replace("\n", "").strip()
            break

    return {
        "name": title.get_text(strip=True),
        "owner": owner.get_text(strip=True) if owner else None,
        "address": address.get_text(strip=True) if address else None,
        "phones": sorted({p.get_text(strip=True) for p in phones}) if phones else [],
        "categories": categories,
        "founded_year": founded_year,
    }


def crawl_details(http: Http, conn, cur, budget: int) -> tuple[int, int]:
    """Fill owner/address/phones for companies discovered via listings."""
    cur.execute(
        "SELECT id FROM spyur_en WHERE detail_scraped_at IS NULL ORDER BY id LIMIT %s",
        (budget,),
    )
    ids = [r[0] for r in cur.fetchall()]
    pages = rows = 0
    for company_id in ids:
        resp = http.get(f"{BASE}/en/companies/{company_id}/")
        pages += 1
        data = parse_company_page(resp.text)
        if data:
            cur.execute(
                """
                UPDATE spyur_en SET
                    name = COALESCE(%s, name), owner = %s, address = %s, phones = %s,
                    founded_year = %s,
                    categories = (
                        SELECT COALESCE(array_agg(DISTINCT c ORDER BY c), '{}')
                        FROM unnest(COALESCE(categories, '{}') || %s) AS c
                    ),
                    detail_scraped_at = now()
                WHERE id = %s
                """,
                (data["name"], data["owner"], data["address"], data["phones"],
                 data["founded_year"], data["categories"], company_id),
            )
            rows += 1
            print(f"[details] {company_id}: {data['name'][:60]}")
        else:
            cur.execute(
                "UPDATE spyur_en SET detail_scraped_at = now() WHERE id = %s",
                (company_id,),
            )
            print(f"[details] {company_id}: page has no company content - marked visited")
        conn.commit()
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    return pages, rows


# ------------------------------------------------------------------------ runs

def run_crawl() -> str:
    """One full crawl: tree -> listings -> details. Returns final status."""
    conn = get_db_connection()
    cur = conn.cursor()
    create_tables(cur)
    conn.commit()

    run_id = start_run(cur)
    conn.commit()
    http = Http()
    pages = rows = 0
    try:
        leaves = walk_category_tree(http)
        print(f"[tree] {len(leaves)} leaf categories")
        if not leaves:
            finish_run(cur, run_id, "empty", 0, 0, "category tree returned no leaves")
            conn.commit()
            return "empty"
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO spyur_category (ypid, name, parent_path, company_count, updated_at)
            VALUES %s
            ON CONFLICT (ypid) DO UPDATE SET
                name = EXCLUDED.name, parent_path = EXCLUDED.parent_path,
                company_count = EXCLUDED.company_count, updated_at = now()
            """,
            [(l["ypid"], l["name"], l["parent_path"], l["count"]) for l in leaves],
            template="(%s, %s, %s, %s, now())",
        )
        conn.commit()

        l_pages, l_rows, completed = crawl_listings(http, conn, cur, leaves, MAX_LISTING_PAGES_PER_RUN)
        pages += l_pages
        rows += l_rows

        d_pages = d_rows = 0
        if completed:
            d_pages, d_rows = crawl_details(http, conn, cur, MAX_DETAIL_PER_RUN)
            pages += d_pages
            rows += d_rows

        cur.execute("SELECT COUNT(*) FROM spyur_en WHERE detail_scraped_at IS NULL")
        detail_backlog = cur.fetchone()[0]

        if rows == 0:
            status = "empty"
        elif not completed or detail_backlog > 0:
            status = "capped"
        else:
            status = "ok"
        finish_run(
            cur, run_id, status, pages, rows,
            f"listings={l_rows} details={d_rows} detail_backlog={detail_backlog}",
        )
        conn.commit()
        print(f"[run] {status}: {pages} pages, {rows} rows, detail backlog {detail_backlog}")
        return status
    except ChallengeBlocked as exc:
        conn.rollback()
        finish_run(cur, run_id, "blocked", pages, rows, str(exc))
        conn.commit()
        print(f"[run] BLOCKED: {exc}")
        return "blocked"
    except Exception as exc:  # noqa: BLE001 - a run must always be closed out
        conn.rollback()
        finish_run(cur, run_id, "error", pages, rows, f"{type(exc).__name__}: {exc}")
        conn.commit()
        print(f"[run] ERROR: {type(exc).__name__}: {exc}")
        return "error"
    finally:
        cur.close()
        conn.close()


def _seconds_until_due() -> float:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        create_tables(cur)
        conn.commit()
        cur.execute(
            """
            SELECT status, COALESCE(finished_at, started_at) FROM scraper_runs
            WHERE source = %s AND status <> 'running'
            ORDER BY id DESC LIMIT 1
            """,
            (SOURCE,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        return 0
    status, last = row
    age = (datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)).total_seconds()
    if status == "capped":
        wait = RETRY_CAPPED_HOURS * 3600
    elif status in ("error", "blocked", "empty", "aborted"):
        wait = RETRY_FAILED_HOURS * 3600
    else:
        wait = SCRAPE_INTERVAL_DAYS * 86400
    return max(0.0, wait - age)


def service_loop() -> None:
    print(
        f"[service] spyur weekly scraper - interval {SCRAPE_INTERVAL_DAYS}d, "
        f"capped retry {RETRY_CAPPED_HOURS}h, failure retry {RETRY_FAILED_HOURS}h"
    )
    while True:
        try:
            wait = _seconds_until_due()
            if wait <= 0:
                run_crawl()
            else:
                print(f"[service] next run due in {wait / 3600:.1f}h")
        except Exception as exc:  # noqa: BLE001 - the service must survive anything
            print(f"[service] loop error: {type(exc).__name__}: {exc}")
        time.sleep(SERVICE_POLL_SECONDS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="spyur.am English category-tree scraper")
    parser.add_argument("--once", action="store_true", help="run one crawl now and exit")
    parser.add_argument("--service", action="store_true", help="run as a weekly in-process service")
    args = parser.parse_args()
    if args.once:
        sys.exit(0 if run_crawl() in ("ok", "capped") else 1)
    service_loop()
