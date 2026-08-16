# Daily scraper runner (Windows Task Scheduler: DramWiseDailyScrapers, 00:00).
# Runs Spyur then Yell incrementally: each checks what's new up to the last
# known id and fetches only that (checkpoints + done-markers in the DB make
# every run resume/skip automatically). Skips a scraper if an instance is
# already running (e.g. a long backfill from the previous night).
$root = "C:\Users\sakoy\Documents\AUA\Ideas\dramwise"
$log = "$root\daily_runner.log"
$spyurLog = "$root\Spyur_scraper\scraper_local.log"
$yellLog = "$root\Yell_scraper\yell_local.log"

function Test-ScraperRunning($pattern) {
    $procs = Get-CimInstance Win32_Process -Filter "Name like 'python%'" |
        Where-Object { $_.CommandLine -match $pattern }
    return ($procs | Measure-Object).Count -gt 0
}

Add-Content $log "[daily] start $(Get-Date -Format s)"

# ---- Spyur (local only - spyur.am blocks datacenter IPs) ----
if (Test-ScraperRunning 'scraper\.py') {
    Add-Content $log "[daily] spyur already running - skipped"
} else {
    $env:MAX_RETRIES = "5"
    $env:REQUEST_TIMEOUT = "40"
    $env:MAX_DETAIL_PER_RUN = "40000"
    $env:MAX_SWEEP_PER_RUN = "70000"
    Set-Location "$root\Spyur_scraper"
    for ($i = 1; $i -le 12; $i++) {
        python scraper.py --once *>> $spyurLog
        if ($LASTEXITCODE -eq 0) { break }
        Add-Content $log "[daily] spyur attempt $i failed, retry in 5 min"
        Start-Sleep -Seconds 300
    }
}

# ---- Yell (also runs on Railway; local daily run helps clear the backlog -
# both are idempotent via the yell_scraped_en marker table) ----
if (Test-ScraperRunning 'yell\.py') {
    Add-Content $log "[daily] yell already running - skipped"
} else {
    $env:MAX_PAGES_PER_RUN = "25000"
    Set-Location "$root\Yell_scraper"
    for ($i = 1; $i -le 6; $i++) {
        python yell.py --once *>> $yellLog
        if ($LASTEXITCODE -eq 0) { break }
        Add-Content $log "[daily] yell attempt $i failed, retry in 5 min"
        Start-Sleep -Seconds 300
    }
}

Add-Content $log "[daily] end $(Get-Date -Format s)"
