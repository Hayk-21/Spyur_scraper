@echo off
rem Weekly Spyur crawl - run from this PC because spyur.am's Cloudflare
rem hard-blocks datacenter IPs (Railway) and Worker subrequests carry the
rem original client identity. Residential IPs pass. Scheduled via Windows
rem Task Scheduler (task name: DramWiseSpyurScraper).
cd /d C:\Users\sakoy\Documents\AUA\Ideas\dramwise\Spyur_scraper
python scraper.py --once >> scraper_local.log 2>&1
