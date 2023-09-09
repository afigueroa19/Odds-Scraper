from selenium import webdriver
from decimal import Decimal
import uuid
import csv
import sys
from os.path import join, dirname
from selenium import webdriver
from slugify import slugify
import warnings
import time, gc, io
import pandas as pd
##from selenium.common.exceptions import NoSuchElementException
from urllib.request import urlopen
from azure.cosmos import CosmosClient
import json
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import re
from selenium.webdriver import ActionChains
from datetime import datetime, timedelta
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import numpy as np
from datetime import datetime, timedelta
from time import sleep
from lib_normalize import normalize

cod_pais = "PTY"

url_cosmos = 'https://proyecto-cuotas-pty.documents.azure.com:443/'
key_cosmos = 'NOPE=='

client_cosmos = CosmosClient(url_cosmos, credential=key_cosmos)
database_cosmos = client_cosmos.get_database_client('sc_pty')
container_olimpo_odds = database_cosmos.get_container_client('cuotas')


##Funciones de Scrapping
def get_fecha_evento(evento):
    try:
        hora = evento.find_element(By.XPATH, ".//span[@class = 'time']").text
        fecha = evento.find_element(By.XPATH, ".//span[@class = 'date']").text
        fecha = correct_fecha(fecha)      
    except:
        fecha, hora = ('0', '0')
        pass
    return fecha, hora

def get_nombre_evento(evento):
    try:
        eventos_container = evento.find_elements(By.XPATH, "./td[@class = 'seln ']")
        home = eventos_container[0].find_element(By.XPATH, ".//span[@class = 'seln-name']").text        
        visitor = eventos_container[1].find_element(By.XPATH, ".//span[@class = 'seln-name']").text        
        name= home + " - "+ visitor              
    except:
        name = ''
        pass
    try:
        home_odd = eventos_container[0].find_element(By.XPATH, ".//span[@class = 'price us']").text
        visitor_odd = eventos_container[1].find_element(By.XPATH, ".//span[@class = 'price us']").text
        x_odd = evento.find_element(By.XPATH, "./td[@class = 'seln seln_sort-D']//span[@class = 'price us']").text
    except:
        home_odd, x_odd, visitor_odd = 0,0,0
        pass
    return name, get_odds_transformation(int(home_odd)), get_odds_transformation(int(x_odd)), get_odds_transformation(int(visitor_odd))

def get_odds_transformation(odd):  
    if odd>0:
        odd = (odd+100)/100
    else:
        odd = odd*-1
        odd = (odd+100)/(odd)
    return round(odd,4)

def get_odds(evento):
    odds = evento.find_elements(By.XPATH, ".//app-money-line/div//span/span")
    if len(odds)==3:
        odd_1, odd_2, odd_x = [get_odds_transformation(int(odds[0].text)), get_odds_transformation(int(odds[1].text)), get_odds_transformation(int(odds[2].text))]
    else :
        odd_1, odd_2, odd_x = None, None, None
    return odd_1, odd_2, odd_x

def correct_fecha(fecha):
    dict_meses = {
        'Ene': '01',
        'Feb': '02',
        'Mar': '03',
        'Abr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Ago': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dic': '12'}
    dia = fecha[:2]
    mes = dict_meses.get(fecha[3:], '10')
    
    return '2023'+'-'+str(mes)+'-'+str(dia)

#driverRuta = r"C:\Users\afigu\Desktop\Selenium\chromedriver.exe"

navegador = None
tiempoMaximo = 10
tiempoMedio = 4
tiempoMinimo = 1
tiempoSMinimo = 0.5
maxIntentos = 3
urlBase = "https://be.betcris.pa/es/deportes"

##iniciarNavegador()
##INICIAR NAVEGADOR
warnings.filterwarnings("ignore")
opciones = webdriver.ChromeOptions()
opciones.add_argument('--start-maximized')
opciones.add_argument('--dns-prefetch-disable')
opciones.add_argument('--no-sandbox')
opciones.add_argument("--window-size=1920x1080")
opciones.add_argument('--disable-notifications')
opciones.add_argument('--disable-dev-shm-usage') #aaaaa
#opciones.add_argument('--headless') #para evitar gui
opciones.add_argument('--disable-gpu')
opciones.add_argument("user-agent=My user Agent")
#navegador = webdriver.Chrome(driverRuta, options = opciones)
navegador = webdriver.Chrome(options = opciones)
navegador.implicitly_wait(tiempoMaximo)#Tiempo de espera del driver para encontrar un elemento

now = datetime.now()
fecha_muestra = now.strftime("%Y-%m-%d")
hora_muestra = now.strftime("%H:%M:%S")
df_eventos_=[]
##Inicio de pagina e ingreso de usuario

#navegador.save_screenshot("/home/root/scraping/estado_betcris.png")

enlaces = [("Inglaterra / Premier League","https://sports.betcha.pa/es_PA/t/19157/Inglaterra-Premier-League")#,
          #("Francia / Ligue 1","https://be.betcris.pa/es/deportes/futbol/ligas-top/francia-liga-1/"),
          #("Alemania / Bundesliga","https://be.betcris.pa/es/deportes/futbol/ligas-top/alemania-bundesliga/"),
          #("Italia / Serie A","https://be.betcris.pa/es/deportes/futbol/ligas-top/italia-serie-a/"),
          #("España / La Liga","https://be.betcris.pa/es/deportes/futbol/ligas-top/espana-la-liga/"),
          #("Europa / UEFA Champions League","https://be.betcris.pa/es/deportes/futbol/ligas-top/uefa-liga-de-campeones/"),
          #("Europa / UEFA Europa League","https://be.betcris.pa/es/deportes/futbol/ligas-top/uefa-liga-europea/"),
          #("Americas / Copa Libertadores","https://be.betcris.pa/es/deportes/futbol/ligas-top/copa-libertadores/"),
          #("Ecuador / Liga Pro","https://be.betcris.pa/es/deportes/futbol/america-del-sur/ecuador-liga-serie-a/"),
          #("Perú / Liga 1","https://be.betcris.pa/es/deportes/futbol/america-del-sur/peru-primera-division/"),
          #("Americas / Copa Libertadores","https://be.betcris.pa/es/deportes/futbol/clubes-internacionales/copa-sudamericana/")
          ]
          
          
for i in range(len(enlaces)):
    (nombreItem, enlace) = enlaces[i]
    nombre_liga = nombreItem
    print(nombre_liga)
    navegador.get(enlace)
    sleep(3)
    eventos = navegador.find_elements(By.XPATH,"//div[@class = 'pager-page']//tbody/tr")
    print(len(eventos))
    sleep(3)
    for evento in eventos:

        #nombre_liga = re.sub("\nEN VIVO", "", nombre_liga)

        nombre_evento, odd_1, odd_x, odd_2 = get_nombre_evento(evento)
        nombre_evento = normalize(nombre_evento)
        #print('evento: ',nombre_evento)
        #print(odd_1, odd_x, odd_2, sep=' --- ')
        if ((odd_1 ==None) or (odd_x ==None) or (odd_2 ==None)):
            continue
        fecha, hora = get_fecha_evento(evento)
        #print(fecha,hora,sep=' *** ')
        en_vivo = 'NO'
        if fecha_muestra+"T"+hora_muestra+"Z" >= fecha+"T"+hora+"Z":
            en_vivo = 'SI'
        cod_pais='PTY'
        
        item=({
                "id":str(uuid.uuid4()),
                "cod_pais":cod_pais,
                "fecha_evento": fecha,
                "hora_evento": hora,
                "fecha_hora_evento": fecha+"T"+hora+"Z",
                "deporte": "Fútbol",
                "liga": nombre_liga,
                "evento": nombre_evento,
                "odd_1": odd_1,
                "odd_x": odd_x,
                "odd_2": odd_2,
                "en_vivo": en_vivo,
                "proveedor": "BetCha",
                "fecha_muestra": fecha_muestra,
                "hora_muestra": hora_muestra,
                "fecha_hora_muestra": fecha_muestra+"T"+hora_muestra+"Z"
            })
        print(item)
        #container_olimpo_odds.upsert_item(element)

if navegador != None:
    navegador.close()
    navegador.quit()
    navegador = None    
gc.collect()    

sys.exit()