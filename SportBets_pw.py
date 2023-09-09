from playwright.sync_api import sync_playwright
from time import sleep
import os
import json
import numpy as np
from datetime import datetime, timedelta
from azure.cosmos import CosmosClient
import uuid


now = datetime.now()
fecha_muestra = now.strftime("%Y-%m-%d")
hora_muestra = now.strftime("%H:%M:%S")

cod_pais = "EC"

url_cosmos = 'https://cosmodbolimpolakehouse.documents.azure.com:443/'
key_cosmos = 'NOPE=='

client_cosmos = CosmosClient(url_cosmos, credential=key_cosmos)
database_cosmos = client_cosmos.get_database_client('betsoffice_scraping')
container_olimpo_odds = database_cosmos.get_container_client('olimpo_odds_ec_2023')
container_olimpo_odds_last = database_cosmos.get_container_client('olimpo_odds_last_ec')


array_dict_campeonatos = [\
{ "ChampName": "Europa / UEFA Champions League", "ChampId": 4584},\
{ "ChampName": "Europa / UEFA Europa League", "ChampId": 4230},\
#{"ChampName": "Internacional / Amistosos", "ChampId": 3645},\
{"ChampName": "América / Copa Libertadores", "ChampId": 4651},\
{"ChampName": "América / Copa Sudamericana", "ChampId": 6078},\
{"ChampName": "Perú / Liga 1", "ChampId": 5354},\
{"ChampName": "España / La Liga", "ChampId": 4486},\
#{"ChampName": "España / Copa del Rey", "ChampId": 2973},\
{"ChampName": "Inglaterra / Premier League", "ChampId": 391},\
{"ChampName": "Italia / Serie A", "ChampId": 4484},\
{"ChampName": "Francia / Ligue 1", "ChampId": 4610},\
{"ChampName": "Alemania / Bundesliga", "ChampId": 4261},\
#{"ChampName": "Primera División", "ChampId": 3152},\
#{"ChampName": "Primera División", "ChampId": 3837},\
{"ChampName": "Ecuador / Liga Pro", "ChampId": 26888}]


def get_json(p, url_base):
    browser = p.firefox.launch(headless=True)
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
    for i in range(len(array_dict_campeonatos)):
        try:
            nombre_liga = array_dict_campeonatos[i]['ChampName']
            print(nombre_liga)
            code = array_dict_campeonatos[i]['ChampId']
            
            url_base = ('''
                https://sport.sportbet.ec/Prematch/GetEventsList?champId={code}&timeFilter=0&langId=13&partnerId=391&countryCode=PA
                ''').format(code=code)
            y = get_json(p, url_base)
            for j in range(len(y)):
                #print(y)
                nombre_evento = y[j]['N']
                fecha = y[j]['D']
                #print(fecha)
                #print(HomeTeam, AwayTeam, sep=' - ')
                odd_1 = y[j]['StakeTypes'][0]['Stakes'][0]['F']
                odd_x = y[j]['StakeTypes'][0]['Stakes'][1]['F']
                odd_2 = y[j]['StakeTypes'][0]['Stakes'][2]['F']
                #print(odd1, oddx,odd2, sep=' - ')
                #print('-'*30)

                if fecha_muestra>= fecha:
                    en_vivo = 'SI'
                else:
                    en_vivo = 'NO'
                ###Insercion a cosmos2023-05-30T
                print({
                        "id":str(uuid.uuid4()),
                        "cod_pais":"EC",
                        "fecha_evento": fecha[:10],
                        "hora_evento": fecha[11:-1],
                        "fecha_hora_evento": fecha,
                        "deporte": "Fútbol",
                        "liga": nombre_liga,
                        "evento": nombre_evento,
                        "odd_1": odd_1,
                        "odd_x": odd_x,
                        "odd_2": odd_2,
                        "en_vivo": en_vivo,
                        "proveedor": "SportBets",
                        "fecha_muestra": fecha_muestra,
                        "hora_muestra": hora_muestra,
                        "fecha_hora_muestra": fecha_muestra+"T"+hora_muestra+"Z"
                    })
            sleep(5)
            print('-'*30)
        except Exception as e:
            print(e)
            continue        
    #sleep(5)
