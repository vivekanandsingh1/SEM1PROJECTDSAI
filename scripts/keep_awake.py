import sys

from playwright.sync_api import sync_playwright

URL = sys.argv[1] if len(sys.argv) > 1 else "https://sem1dsaiproject.streamlit.app/"

with sync_playwright() as p:
    page = p.chromium.launch().new_page()
    page.goto(URL, wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(10_000)
    wake = page.get_by_role("button", name="Yes, get this app back up!")
    if wake.count():
        wake.click()
        print("app was asleep, wake requested")
        page.wait_for_timeout(90_000)
    else:
        page.wait_for_timeout(30_000)
        print("app is awake")
