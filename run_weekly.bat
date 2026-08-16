@echo off
rem Weekly Spyur crawl - run from this PC because spyur.am's Cloudflare
rem hard-blocks datacenter IPs (Railway) and Worker subrequests carry the
rem original client identity. Residential IPs pass. Scheduled via Windows
rem Task Scheduler (task name: DramWiseSpyurScraper).
rem
rem The crawl takes ~3h; transient spyur.am slowness can kill a run with a
rem ReadTimeout, so retry up to 6 times with a 5-minute pause - the DB
rem checkpoint means each retry resumes where the last attempt stopped.
cd /d C:\Users\sakoy\Documents\AUA\Ideas\dramwise\Spyur_scraper
set MAX_RETRIES=5
for /l %%i in (1,1,6) do (
    python scraper.py --once >> scraper_local.log 2>&1
    if not errorlevel 1 goto done
    echo [run_weekly] attempt %%i failed, retrying in 5 min >> scraper_local.log
    timeout /t 300 /nobreak > nul
)
:done
