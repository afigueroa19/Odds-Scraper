from playwright.sync_api import sync_playwright
from time import sleep
import os

url_base = "https://www.livescore.com/en/"
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto(url_base)
    page.locator("css=button").click()
    #sleep(30)
    #page.screenshot(path="Bet-Open.png")
    #sleep(5)
