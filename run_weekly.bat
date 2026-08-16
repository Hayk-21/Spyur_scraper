@echo off
rem Weekly Spyur crawl - run from this PC because spyur.am's Cloudflare
rem hard-blocks datacenter IPs (Railway) and Worker subrequests carry the
rem original client identity. Residential IPs pass. Scheduled via Windows
rem Task Scheduler (task name: DramWiseSpyurScraper).
rem
rem Stages per run: category-tree listings -> per-company details -> ID sweep
rem (ids 1..max+margin never seen in any category listing; dead ids are
rem remembered in spyur_sweep_checked so they cost one request ever).
rem
rem spyur.am throttles after a few hours (ReadTimeout kills the run), so
rem retry with a 5-minute pause - the DB checkpoints make every attempt
rem resume where the previous one stopped.
cd /d C:\Users\sakoy\Documents\AUA\Ideas\dramwise\Spyur_scraper
set MAX_RETRIES=5
set REQUEST_TIMEOUT=40
set MAX_DETAIL_PER_RUN=40000
set MAX_SWEEP_PER_RUN=70000
for /l %%i in (1,1,12) do (
    python scraper.py --once >> scraper_local.log 2>&1
    if not errorlevel 1 goto done
    echo [run_weekly] attempt %%i failed, retrying in 5 min >> scraper_local.log
    timeout /t 300 /nobreak > nul
)
:done
