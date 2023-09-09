from playwright.sync_api import sync_playwright
from time import sleep
import os
import numpy as np
import uuid
from datetime import datetime, timedelta
import random
url_base = "https://www.coolbet.com/pe/deportes/futbol/per%C3%BA/primera-division-peruana"

array_dict_campeonatos = [\
#{"ChampName": "Italia / Serie A", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E76509991/G40/'},\
#{ "ChampName": "Europa / UEFA Champions League", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E79147586/G40/'},\
#{ "ChampName": "Europa / UEFA Europa League", "url_base": 4230},\
#{"ChampName": "Amistosos Internacionales", "url_base": 'https://www.coolbet.com/pe/deportes/futbol/internacional/amistosos-internacionales-fifa'},\
#{"ChampName": "América / Copa Libertadores", "url_base": 'https://www.coolbet.com/pe/deportes/futbol/am%C3%A9ricas/copa-libertadores'},\
#{"ChampName": "América / Copa Sudamericana", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E88156539/G40/'},\
#{"ChampName": "Primera División", "url_base": 4632},\
#{"ChampName": "España / La Liga", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E76473591/G40/H^1/'},\
#{"ChampName": "Copa del Rey", "url_base": 2973},\
#{"ChampName": "Premier League", "url_base": 'https://www.coolbet.com/pe/deportes/futbol/inglaterra/premier-league'},\
#{"ChampName": "Primera División", "url_base": 3152},\
{"ChampName": "Perú / Liga 1", "url_base": 'https://www.coolbet.com/pe/deportes/futbol/per%C3%BA/primera-division-peruana'}\
#{"ChampName": "Ecuador / LigaPro", "url_base": 'https://www.coolbet.com/pe/deportes/futbol/ecuador/liga-pro'},\
#{"ChampName": "Francia / Ligue 1", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E76468858/G40/'},\
#{"ChampName": "Alemania / Bundesliga", "url_base": 'https://www.bet365.com/#/AC/B1/C1/D1002/E90725075/G40/'}
]

cod_pais = 'EC'
now = datetime.now()
fecha_muestra = now.strftime("%Y-%m-%d")
hora_muestra = now.strftime("%H:%M:%S")

with sync_playwright() as p:
    for i in range(len(array_dict_campeonatos)):
        try:
            browser = p.firefox.launch(headless=False)
            context = browser.new_context(
                user_agent= 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.76 Mobile Safari/537.36'
            )
            page = context.new_page()
            url_base, liga = array_dict_campeonatos[i]['url_base'], array_dict_campeonatos[i]['ChampName']
            print(url_base, liga, sep= ' ; ')

            page.goto(url_base)

            page.wait_for_selector("span.outcome-value")

            time_to_sleep = random.randint(3, 7)
            print('time_to_sleep : ', time_to_sleep)
            sleep(time_to_sleep)
            print('Sleep is Over')
            parent_element = page.locator("xpath = //div[@class = 'sc-jVODtj htEOCE']")
        except Exception as e:
            print('Hubo un error iniciando la pagina')
            print(e)
            browser.close()
            continue

        for i in range(parent_element.count()):
            try:
                #Nombre de Eventos
                Home = parent_element.nth(i).locator("xpath=//div[@class = 'match-teams']/div[@class = 'team-home']/div[@class = 'name']").text_content()
                
                Away = parent_element.nth(i).locator("xpath=//div[@class = 'match-teams']/div[@class = 'team-away']/div[@class = 'name']").text_content()

                print(Home, Away, sep = ' - ')

                #Fechas
                Fechas = parent_element.nth(i).locator("xpath=//div[@class = 'match-time']//span")
                Fecha = Fechas.nth(0).text_content()
                Hora = Fechas.nth(1).text_content()
                
                print(Fecha, Hora, sep = ' ')

                #Cuotas de Eventos
                Odds = parent_element.nth(i).locator("xpath=//div[@class = 'match-market-outcome animated']//span[@class = 'outcome-value']")
                Odd_1, Odd_x, Odd_2 = Odds.nth(0).text_content(),Odds.nth(1).text_content(), Odds.nth(2).text_content()
                print(Odd_1, Odd_x, Odd_2, sep = ' - ')
                print('-'*30)
            except Exception as e:
                print('Hubo un error')
                print(e)
                continue

        browser.close()