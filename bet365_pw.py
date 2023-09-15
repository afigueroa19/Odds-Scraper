from playwright.sync_api import sync_playwright
from time import sleep
import os
import numpy as np
import uuid
from datetime import datetime, timedelta
import random
from azure.cosmos import CosmosClient
from lib_normalize import normalize

##List of Leagues to Scrape
array_dict_campeonatos = [\
#{"ChampName": "Italia / Serie A", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E76509991/G40/'},\
#{ "ChampName": "Europa / UEFA Champions League", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E79147586/G40/'},\
#{ "ChampName": "Europa / UEFA Europa League", "url_base": 4230},\
#{"ChampName": "Amistosos Internacionales", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E91000949/G40/'},\
#{#"ChampName": "América / Copa Libertadores", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E85780589/G40/'}
#{"ChampName": "América / Copa Sudamericana", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E88156539/G40/'},\
#{"ChampName": "Inglaterra / Premier League", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E91422157/G40/'}\
{"ChampName": "España / La Liga", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E91721703/G40/H^1/'}\
#{"ChampName": "Copa del Rey", "url_base": 2973},\
#{"ChampName": "Premier League", "url_base": 2936},\
#{"ChampName": "Primera División", "url_base": 3152},\
#{"ChampName": "Perú / Liga 1", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E91602171/G40/H^1/'}\
#{"ChampName": "Ecuador / LigaPro", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E85996192/G40/'},\
#{"ChampName": "Francia / Ligue 1", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E76468858/G40/'},\
#{"ChampName": "Alemania / Bundesliga", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E90725075/G40/'}
]

cod_pais = "PTY"

now = datetime.now()
fecha_muestra = now.strftime("%Y-%m-%d")
hora_muestra = now.strftime("%H:%M:%S")

'''
The following two functions are used to parse the data and format it
1. get_info will capture the datetime of the game, league, participants and if the event have starts,
    the get_fecha function  is used within get_info for parcing the datetime data
2. get_odds will capture the odds for 1x2(Home-Tie-Visitor) bets
'''
def get_info(parent_element, liga):
    eventos = []
    Equipos = []

    if parent_element:
        descendant_elements = parent_element.query_selector_all('div')
        #print(len(descendant_elements))
        for descendant_element in descendant_elements:
            if ((descendant_element.get_attribute('class')=='rcl-MarketHeaderLabel rcl-MarketHeaderLabel-isdate ')or(descendant_element.get_attribute('class')=='rcl-MarketHeaderLabel-isdate rcl-MarketHeaderLabel ')):
                #print('Fecha: ', descendant_element.text_content())
                Fecha = get_fecha(descendant_element.text_content()[4:])
                
            if ((descendant_element.get_attribute('class')=='rcl-ParticipantFixtureDetails_BookCloses ')):
                #print('Hora: ', descendant_element.text_content())
                    en_vivo = 'NO'
                    Hora = descendant_element.text_content()
            elif ((descendant_element.get_attribute('class')=='pi-CouponParticipantClockInPlay_GameTimerWrapper ')):
                    Hora = descendant_element.text_content()
                    Hora = (now - timedelta(minutes=int(Hora[:2]))).strftime('%H:%M')
                    en_vivo = 'SI'
                

            if ((descendant_element.get_attribute('class')=='rcl-ParticipantFixtureDetailsTeam_TeamName ')):
                #print('Equipo: ', descendant_element.text_content())
                Equipos.append(descendant_element.text_content())
            if len(Equipos)==2:
                evento = Equipos[0]+' - '+Equipos[1]
                evento = normalize(evento)
                eventos.append({
                    'id': str(uuid.uuid4()),
                    'cod_pais': cod_pais,
                    'fecha_evento': Fecha,
                    'hora_evento': Hora,
                    'fecha_hora_evento': Fecha+'T'+Hora+':00Z',
                    'liga': liga,
                    'evento': evento,
                    "en_vivo": en_vivo
                })
                Equipos.clear()
    return eventos

def get_odds(parent_element, index):
    odd = []
    if parent_element:
        descendant_elements = parent_element.query_selector_all('span')
        for descendant_element in descendant_elements:
                
            if ((descendant_element.get_attribute('class')=='sgl-ParticipantOddsOnly80_Odds')):
                odd.append({f"Odd_{index}": descendant_element.text_content()})
    return odd

def get_fecha(fecha):
    dict_meses = {
        'ene': '01',
        'feb': '02',
        'mar': '03',
        'abr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'ago': '08',
        'sep': '09',
        'oct': '10',
        'nov': '11',
        'dic': '12'}
    dia = fecha[:2]
    mes = dict_meses.get(fecha[3:], '07')
    
    return '2023'+'-'+str(mes)+'-'+str(dia)
    

with sync_playwright() as p:

    for i in range(len(array_dict_campeonatos)):
        
        browser = p.firefox.launch(headless=True)
        context = browser.new_context(
            user_agent= 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.76 Mobile Safari/537.36'
        )
        page = context.new_page()
    
        print(i)
        url_base, liga = array_dict_campeonatos[i]['url_base'], array_dict_campeonatos[i]['ChampName']
        print(url_base, liga, sep= ' ; ')
        try:
            page.goto(url_base)
            time_to_sleep = random.randint(3, 9)
            print('time_to_sleep : ', time_to_sleep)
            sleep(time_to_sleep)
            parent_selector = 'div.sgl-MarketFixtureDetailsLabel.gl-Market_General.gl-Market_General-columnheader.gl-Market_General-haslabels'
            
            parent_element = page.query_selector(parent_selector)
            
            eventos = get_info(parent_element, liga)

            parent_selector = 'div.sgl-MarketOddsExpand.gl-Market_General.gl-Market_General-columnheader'
            parent_element = page.query_selector_all(parent_selector)

            odd_1 = get_odds(parent_element[0],1)
            odd_x = get_odds(parent_element[1],'x')
            odd_2 = get_odds(parent_element[2],2)
            
            
            for i in range(len(eventos)):
                data_muestra = {
                "proveedor": "Bet365",
                "fecha_muestra": fecha_muestra,
                "hora_muestra": hora_muestra,
                "fecha_hora_muestra": fecha_muestra+"T"+hora_muestra+"Z"}

                eventos[i] = eventos[i]|odd_1[i]|odd_x[i]|odd_2[i]|data_muestra
                print(eventos[i])


            
        
        except Exception as e:
            print('Hubo un error: ')
            print(e)
            continue

        finally:
            browser.close()
            time_to_sleep = random.randint(5, 12)
            print('time_to_sleep : ', time_to_sleep)
            sleep(time_to_sleep)

