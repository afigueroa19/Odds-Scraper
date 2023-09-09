from playwright.sync_api import sync_playwright
from time import sleep
import os
import json

array_dict_campeonatos = [\
{ "ChampName": "Europa /UEFA Champions League", "ChampId": 4584},\
{ "ChampName": "Europa / UEFA Europa League", "ChampId": 4230},\
#{"ChampName": "Amistosos Internacionales", "ChampId": 3645},\
{"ChampName": "América / Copa Libertadores", "ChampId": 4651},\
{"ChampName": "América / Copa Sudamericana", "ChampId": 6078},\
#{"ChampName": "Primera División", "ChampId": 4632},\
{"ChampName": "España / La Liga", "ChampId": 4486},\
#{"ChampName": "Copa del Rey", "ChampId": 2973},\
#{"ChampName": "Premier League", "ChampId": 2936},\
{"ChampName": "Italia / Serie A", "ChampId": 4484},\
{"ChampName": "Francia / Ligue 1", "ChampId": 4610},\
{"ChampName": "Alemania / Bundesliga", "ChampId": 4261},\
#{"ChampName": "Primera División", "ChampId": 3152},\
#{"ChampName": "Primera División", "ChampId": 3837},\
{"ChampName": "Ecuador / LigaPro", "ChampId": 26888}]


def get_json(p, url_base):
    browser = p.firefox.launch(headless=False)
    context = browser.new_context(
        user_agent= 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.76 Mobile Safari/537.36'
    )
    page = context.new_page()
    page.goto(url_base)
    #page.screenshot(path="Sportbets.png")
    sleep(1)
    text = page.locator("xpath=//body").inner_text()
    
    browser.close()
    #text =text[1:-1]
    #print(text)
    y = json.loads(text)
    return y


with sync_playwright() as p:
    i = 0
    try:
        Liga = array_dict_campeonatos[i]['ChampName']
        print(Liga)
        code = array_dict_campeonatos[i]['ChampId']
        
        url_base = ('''
            https://sport.sportbet.ec/Prematch/GetEventsList?champId={code}&timeFilter=0&langId=13&partnerId=391&countryCode=PA
            ''').format(code=code)
        y = get_json(p, url_base)
        save_file = open("Mercados.json", "w")  
        json.dump(y, save_file, indent = 6)  
        save_file.close()  
        
    except:
        pass    