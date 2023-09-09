from playwright.sync_api import sync_playwright
from time import sleep
import os
import json
import numpy as np
from datetime import datetime, timedelta
from azure.cosmos import CosmosClient
import uuid
from lib_normalize import normalize


now = datetime.now()
fecha_muestra = now.strftime("%Y-%m-%d")
hora_muestra = now.strftime("%H:%M:%S")

cod_pais = "PTY"



array_dict_campeonatos = [\
#{ "ChampName": "Europa / UEFA Champions League", "ChampId": 4584},\
#{ "ChampName": "Europa / UEFA Europa League", "ChampId": 4230},\
#{"ChampName": "Internacional / Amistosos", "ChampId": 3645},\
#{"ChampName": "América / Copa Libertadores", "ChampId": 4651},\
#{"ChampName": "América / Copa Sudamericana", "ChampId": 6078},\
#{"ChampName": "Perú / Liga 1", "ChampId": 5354},\
{"ChampName": "España / La Liga", "ChampId": 2903511051},\
#{"ChampName": "España / Copa del Rey", "ChampId": 2973},\
#{"ChampName": "Inglaterra / Premier League", "ChampId": 391},\
#{"ChampName": "Italia / Serie A", "ChampId": 4484},\
#{"ChampName": "Francia / Ligue 1", "ChampId": 4610},\
#{"ChampName": "Alemania / Bundesliga", "ChampId": 4261},\
#{"ChampName": "Primera División", "ChampId": 3152},\
#{"ChampName": "Primera División", "ChampId": 3837},\
#{"ChampName": "Ecuador / Liga Pro", "ChampId": 26888}
]


def get_json(p, url_base):
    browser = p.firefox.launch(headless=True)
    context = browser.new_context(
        user_agent= 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.76 Mobile Safari/537.36'
    )
    page = context.new_page()
    page.goto(url_base)
    #page.screenshot(path="Codere.png")
    sleep(1)
    text = page.locator("xpath=//body/pre").inner_text()
    
    browser.close()
    y = json.loads(text)
    return y


with sync_playwright() as p:
    for i in range(len(array_dict_campeonatos)):
        try:
            nombre_liga = array_dict_campeonatos[i]['ChampName']
            print(nombre_liga)
            code = array_dict_campeonatos[i]['ChampId']
            
            url_base = ('''
                https://m.codere.pa/NavigationService/Home/GetEvents?parentId={code}&gameTypes=1;18
                ''').format(code=code)
            y = get_json(p, url_base)
            for j in range(len(y)):
                nombre_evento = y[j]['Name']
                nombre_evento = normalize(nombre_evento)
                #print(nombre_evento)
                fecha = int(y[j]['StartDate'][6:-2])
                fecha = datetime.fromtimestamp(fecha/1000)
                
                #print(fecha)
                odd_1 = y[j]['Games'][0]['Results'][0]['Odd']
                odd_x = y[j]['Games'][0]['Results'][1]['Odd']
                odd_2 = y[j]['Games'][0]['Results'][2]['Odd']

                if now>= fecha:
                    en_vivo = 'SI'
                else:
                    en_vivo = 'NO'
                
                item=({
                        "id":str(uuid.uuid4()),
                        "cod_pais":cod_pais,
                        "fecha_evento": fecha.strftime('%Y-%m-%d'),
                        "hora_evento": fecha.strftime('%H-%M-%S'),
                        "fecha_hora_evento": fecha.strftime('%Y-%m-%dT%H-%M-%SZ'),
                        "deporte": "Fútbol",
                        "liga": nombre_liga,
                        "evento": nombre_evento,
                        "odd_1": odd_1,
                        "odd_x": odd_x,
                        "odd_2": odd_2,
                        "en_vivo": en_vivo,
                        "proveedor": "Codere",
                        "fecha_muestra": fecha_muestra,
                        "hora_muestra": hora_muestra,
                        "fecha_hora_muestra": fecha_muestra+"T"+hora_muestra+"Z"
                    })
                print(item)
                print('-'*30)\
            sleep(5)
        except Exception as e:
            print(e)
            continue        
    #sleep(5)
