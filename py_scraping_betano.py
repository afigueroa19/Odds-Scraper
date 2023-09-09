from selenium import webdriver
from decimal import Decimal
import uuid
import csv
import sys
import subprocess
import os
import datetime, calendar
import mysql.connector
from os.path import join, dirname
#from dotenv import load_dotenv
from selenium import webdriver
# from seleniumwire import webdriver
# import seleniumwire.undetected_chromedriver as webdriver
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

if sys.version_info[0] >= 3:
    unicode = str

url_cosmos = 'https://cosmodbolimpolakehouse.documents.azure.com:443/'
key_cosmos = 'NOPE=='

client_cosmos = CosmosClient(url_cosmos, credential=key_cosmos)
database_cosmos = client_cosmos.get_database_client('betsoffice_scraping')
container_olimpo_odds = database_cosmos.get_container_client('olimpo_odds_ec_2023')
container_olimpo_odds_last = database_cosmos.get_container_client('olimpo_odds_last_ec')

## INICIAR VARIABLES DEL DRIVER
##driverRuta = "/home/stageperu/kambi/python/scraping/chromedriver"
##driverRuta = r"C:\\Users\\EduardoEspinoza\\chromedriver.exe"
##driverRuta = r"C:\Users\EduardoEspinoza\OneDrive - NG ENTERTAINMENT PERÚ II\\chromedriver.exe"
driverRuta = "/root/chromedriver"

navegador = None
tiempoMaximo = 25
tiempoIntermedio = 20 
tiempoMedio = 10
tiempoMinimo = 8
tiempoSMinimo = 5
maxIntentos = 3
urlBase = "https://ec.betano.com/sport/futbol/"

##INICIAR NAVEGADOR

warnings.filterwarnings("ignore")
opciones = webdriver.ChromeOptions()
opciones.add_argument('--start-maximized')
opciones.add_argument('--dns-prefetch-disable')
opciones.add_argument('--no-sandbox')
opciones.add_argument("--window-size=1920x1080")
opciones.add_argument('--disable-notifications')
opciones.add_argument('--disable-dev-shm-usage') #aaaaa
opciones.add_argument('--headless') #para evitar gui
opciones.add_argument('--disable-gpu')
navegador = webdriver.Chrome(driverRuta, chrome_options = opciones)
navegador.implicitly_wait(tiempoMaximo)#Tiempo de espera del driver para encontrar un elemento

#navegador.get(urlBase)
#navegador.implicitly_wait(tiempoMaximo)


try:
    boton = navegador.find_element(By.XPATH,"//button[contains(@class, 'sb-modal__close__btn') and contains(@class, 'uk-modal-close-default')]")
    boton.click()
except:
    pass


try:
    boton = navegador.find_element(By.XPATH,"//button[contains(@class, 'uk-button sticky-notification__cta') and contains(@class, 'sticky-notification__cta--secondary')]")
    boton.click()
except:
    pass
    
    
# li_football = None
# all_li = navegador.find_elements(By.XPATH, "//li")
# for li_iter in all_li:
    # if "FOOT" in li_iter.get_attribute('class'):
        # li_football = li_iter
        # print("GTM-sidebar_FOOT")
        # break

urls_a_visitar_liga = [\
'sport/futbol/campeonatos/copa-libertadores/189817/',\
'sport/futbol/campeonatos/copa-sudamericana/189818/',\
'sport/futbol/campeonatos/liga-de-campeones/188566/',\
'sport/futbol/campeonatos/liga-de-europa/188567/',\
'sport/futbol/campeonatos/liga-europa-conferencia/189602/',\
'sport/futbol/campeonatos/euro-2024/189663/']

urls_a_visitar_pais = [\
'sport/futbol/campeonatos/alemania/24/',\
'sport/futbol/campeonatos/espana/2/',\
'sport/futbol/campeonatos/peru/11380/',\
'sport/futbol/campeonatos/inglaterra/1/',\
'sport/futbol/campeonatos/argentina/11319/',\
'sport/futbol/campeonatos/brasil/10004/',\
'sport/futbol/campeonatos/francia/23/',\
'sport/futbol/campeonatos/italia/87/',\
'sport/futbol/campeonatos/clubes-internacionales/11396/',\
'sport/futbol/campeonatos/ecuador/11340/',\
'sport/futbol/campeonatos/uefa/188975/',\
'sport/futbol/campeonatos/mexico/11375/',\
'sport/futbol/campeonatos/internacional/11364/']



# ##SECUNDARIOS
# divs_secundarios = li_football.find_elements(By.XPATH, "//div")

# for div_iter in divs_secundarios:
    # if "sport-picker__item sport-picker__secondary" == div_iter.get_attribute('class'):
        # #link = div_iter.findElement(By.TAG_NAME,"a")
        # #print(link.get_attribute('href'))
        # html_interno = BeautifulSoup(div_iter.get_attribute('innerHTML'))
        # print(div_iter.text.split("\n"))
        # print(html_interno.find("a").get("href"))
        # urls_a_visitar.append(html_interno.find("a").get("href"))
        
# ##EXPANDIBLES
# i = 0
# for div_iter in divs_secundarios:
    # if "sport-picker__item sport-picker__item--expandable" == div_iter.get_attribute('class'):        
        # lista_otros = ["COMPETENCIAS PRINCIPALES","INTERNACIONAL"]
        # if div_iter.text.strip() in lista_otros:
            # i = i +1
            # print(div_iter.text)
            # div_iter.click()
            # links = navegador.find_elements(By.XPATH,"//div[contains(@class, 'sport-picker__item') and contains(@class, 'sport-picker__item--primary')]")
            # for i_link in links:
                # print(i_link.text)
                # html_interno = BeautifulSoup(i_link.get_attribute('innerHTML'))
                # print(html_interno.find("a").get("href"))
                # urls_a_visitar.append(html_interno.find("a").get("href"))
        # if i == 2:
            # break
            # #print(html_interno)

VISITAR_LIGAS = True
VISITAR_GRUPOS_PAISES = True

milisegundos = int(round(time.time() * 1000))
fechahora_muestra = datetime.datetime.fromtimestamp(milisegundos / 1000.0)
cad_fecha_muestra = fechahora_muestra.strftime('%Y-%m-%d')
cad_hora_muestra = fechahora_muestra.strftime('%H:%M')
cad_fechahora_muestra = fechahora_muestra.strftime('%Y-%m-%dT%H:%M:%SZ')


if (VISITAR_LIGAS):
    print("--VISITAR_LIGAS--")
    
    for url_iter in urls_a_visitar_liga:
        try:
            navegador.get("https://ec.betano.com/"+url_iter)
            navegador.implicitly_wait(tiempoMaximo)

            breadcrumb = navegador.find_element(By.XPATH,"//div[contains(@class, 'section-title')]")
            breadcrumb_list = breadcrumb.text.split("-")
            #print(breadcrumb.text.split("-"))
            cad_pais = breadcrumb_list[0].strip()
            cad_liga = breadcrumb_list[1].strip()
            print("------",cad_pais,"|",cad_liga)
            print("A")
            tabla = navegador.find_element(By.XPATH,"//div[@class='events-list__grid']")
            rows = tabla.find_elements(By.XPATH,"//div[@class='events-list__grid__event']")
            for i_row_ in rows:
                
                liga= cad_pais + " / " + cad_liga

                html_interno = BeautifulSoup(i_row_.get_attribute('innerHTML'))
                print("--------------------")
                cad_dia_mes=""
                cad_hora=""
                cad_local=""
                cad_visitante=""

                i_cursor =0
                ths_evento = html_interno.find("div",{"class": "events-list__grid__info"})
                data_evento = ths_evento.text.split("\n")
                for data_ in data_evento:
                    if data_.strip() != "":
                        if i_cursor==0:
                            cad_dia_mes=data_.strip()
                        if i_cursor==1:
                            cad_hora=data_.strip()
                        if i_cursor==2:
                            cad_local=data_.strip()
                        if i_cursor==3:
                            cad_visitante=data_.strip()
                        i_cursor=i_cursor+1
                print("------",cad_pais,"-",cad_liga)
                print(cad_dia_mes,"-",cad_hora,"-",cad_local,"-",cad_visitante)

                fecha = "2023-" +  cad_dia_mes[3:6] +"-"+cad_dia_mes[0:2] +"T" + cad_hora + ":00Z"
                fechaEvento = datetime.datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%SZ')
                cadFechaEvento = fechaEvento.strftime('%Y-%m-%d')
                cadHoraEvento = fechaEvento.strftime('%H:%M')
                cadFechaHoraEvento = fechaEvento.strftime('%Y-%m-%dT%H:%M:%SZ')
                fecha = fecha.lower().replace('t', ' ').replace('z', '')
                
                evento = cad_local+" - "+ cad_visitante

                ths_full_time = html_interno.find("div",{"class":"table__markets__market"})

                cad_1=""
                cad_x=""
                cad_2=""
                i_cursor =0
                for i_row_market in ths_full_time:
                    spans = ths_full_time.find_all("span",{"class":"selections__selection__odd"})
                    for ispan in spans:
                        iispan = ispan.text.split("\n")
                        print(iispan)
                        for iii in iispan:
                            if iii.strip() != "":
                                if i_cursor==0:
                                    cad_1=iii.strip()
                                if i_cursor==1:
                                    cad_x=iii.strip()
                                if i_cursor==2:
                                    cad_2=iii.strip()
                                i_cursor=i_cursor+1
                                #print(iii.strip())

                    break
                print(cad_1,"-",cad_x,"-",cad_2)
                
                odd_1 = float(cad_1)
                odd_x = float(cad_x)
                odd_2 = float(cad_2)

                print("--------------------")
                en_vivo = 'NO'
                print("Z")
                if fecha == None:
                    pass
                elif fecha != None:
                    if cad_fechahora_muestra >= cadFechaHoraEvento:
                        en_vivo = 'SI'
                        
                    ##
                    ##ID ALEATORIO
                    container_olimpo_odds.upsert_item({
                        "id":str(uuid.uuid4()),
                        "cod_pais":"EC",
                        "fecha_evento": cadFechaEvento,
                        "hora_evento": cadHoraEvento,
                        "fecha_hora_evento": cadFechaHoraEvento,
                        "deporte": "Fútbol",
                        "liga": liga,
                        "evento": evento,
                        "odd_1": odd_1,
                        "odd_x": odd_x,
                        "odd_2": odd_2,
                        "en_vivo": en_vivo,
                        "proveedor": "Betano",
                        "fecha_muestra": cad_fecha_muestra,
                        "hora_muestra": cad_hora_muestra,
                        "fecha_hora_muestra": cad_fechahora_muestra
                    })
                    print("Z1")
                    id_unico = "EC-" + str(cadFechaEvento) + "-FUTBOL-" + str(slugify(liga)) + "-" + str(slugify(evento)) +str(en_vivo.upper())+"-BETANO"
                    print("Z2")
                    container_olimpo_odds_last.upsert_item({
                        "id":id_unico,
                        "cod_pais": "EC",
                        "fecha_evento": cadFechaEvento,
                        "hora_evento": cadHoraEvento,
                        "fecha_hora_evento": cadFechaHoraEvento,
                        "deporte": "Fútbol",
                        "liga": liga,
                        "evento": evento,
                        "odd_1": odd_1,
                        "odd_x": odd_x,
                        "odd_2": odd_2,
                        "en_vivo": en_vivo,
                        "proveedor": "Betano",
                        "fecha_muestra": cad_fecha_muestra,
                        "hora_muestra": cad_hora_muestra,
                        "fecha_hora_muestra": cad_fechahora_muestra
                    })            
                
                
        except Exception as e:
            print(str(e))
            pass    
    
    

if (VISITAR_GRUPOS_PAISES):
    print("--VISITAR_GRUPOS_PAISES--")
    for url_iter in urls_a_visitar_pais:
        try:
            navegador.get("https://ec.betano.com/"+url_iter)
            navegador.implicitly_wait(tiempoMaximo)

            breadcrumb = navegador.find_element(By.XPATH,"//div[contains(@class, 'section-title')]")
            breadcrumb_list = breadcrumb.text.split("-")
            #print(breadcrumb.text.split("-"))
            #cad_pais = breadcrumb_list[0].strip()
            #cad_liga = breadcrumb_list[1].strip()
            cad_deporte = breadcrumb_list[0].strip()
            cad_pais = breadcrumb_list[1].strip()
            print("------",cad_deporte,"-",cad_pais)
            print("A")
            
            
            ##bloques = navegador.find_elements(By.XPATH,"//div[@class='league-block']")
            bloques = navegador.find_elements(By.CLASS_NAME,"league-block")
            for i_bloque_ in bloques:
                html_interno_bloque = BeautifulSoup(i_bloque_.get_attribute('innerHTML'))
                ##print(html_interno_bloque)
               
                #cad_liga = i_bloque_.find_element(By.XPATH,"//div[@class='league-block__header']").text.strip()
                #cad_liga = i_bloque_.find_element(By.CSS_SELECTOR,"div.league-block__header").text.strip()
                cad_liga = html_interno_bloque.find("h2").text.strip()
                
                
                #if cad_liga=="":
                #    cad_liga = i_bloque_.find_element(By.CLASS_NAME,"league-block__header").text.strip()
                #    
                #print("BLOQUE:",cad_liga)
                #if cad_liga=="":
                #    break
                
                
                print("------",cad_pais,"-",cad_liga)
                
              

                #tabla = i_bloque_.find_element(By.XPATH,"//table[@class='events-list__grid']")
                tabla = i_bloque_.find_element(By.CLASS_NAME,"events-list__grid")
                
                #rows = tabla.find_elements(By.XPATH,"//tr[@class='events-list__grid__event']")
                rows = tabla.find_elements(By.CLASS_NAME,"events-list__grid__event")
                for i_row_ in rows:
                    
                    liga= cad_pais + " / " + cad_liga

                    html_interno = BeautifulSoup(i_row_.get_attribute('innerHTML'))
                    print("--------------------")
                    cad_dia_mes=""
                    cad_hora=""
                    cad_local=""
                    cad_visitante=""

                    i_cursor =0
                    ths_evento = html_interno.find("div",{"class": "events-list__grid__info"})
                    data_evento = ths_evento.text.split("\n")
                    for data_ in data_evento:
                        if data_.strip() != "":
                            if i_cursor==0:
                                cad_dia_mes=data_.strip()
                            if i_cursor==1:
                                cad_hora=data_.strip()
                            if i_cursor==2:
                                cad_local=data_.strip()
                            if i_cursor==3:
                                cad_visitante=data_.strip()
                            i_cursor=i_cursor+1
                    print("------",cad_pais,"-",cad_liga)
                    print(cad_dia_mes,"-",cad_hora,"-",cad_local,"-",cad_visitante)

                    fecha = "2023-" +  cad_dia_mes[3:6] +"-"+cad_dia_mes[0:2] +"T" + cad_hora + ":00Z"
                    fechaEvento = datetime.datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%SZ')
                    cadFechaEvento = fechaEvento.strftime('%Y-%m-%d')
                    cadHoraEvento = fechaEvento.strftime('%H:%M')
                    cadFechaHoraEvento = fechaEvento.strftime('%Y-%m-%dT%H:%M:%SZ')
                    fecha = fecha.lower().replace('t', ' ').replace('z', '')
                    
                    evento = cad_local+" - "+ cad_visitante

                    ths_full_time = html_interno.find("div",{"class":"table__markets__market"})

                    cad_1=""
                    cad_x=""
                    cad_2=""
                    i_cursor =0
                    for i_row_market in ths_full_time:
                        spans = ths_full_time.find_all("span",{"class":"selections__selection__odd"})
                        for ispan in spans:
                            iispan = ispan.text.split("\n")
                            for iii in iispan:
                                if iii.strip() != "":
                                    if i_cursor==0:
                                        cad_1=iii.strip()
                                    if i_cursor==1:
                                        cad_x=iii.strip()
                                    if i_cursor==2:
                                        cad_2=iii.strip()
                                    i_cursor=i_cursor+1
                                    #print(iii.strip())

                        break
                    print(cad_1,"-",cad_x,"-",cad_2)
                    
                    odd_1 = float(cad_1)
                    odd_x = float(cad_x)
                    odd_2 = float(cad_2)

                    print("--------------------")
                    en_vivo = 'NO'
                    print("Z")
                    if fecha == None:
                        pass
                    elif fecha != None:
                        if cad_fechahora_muestra >= cadFechaHoraEvento:
                            en_vivo = 'SI'
                            
                        ##
                        ##ID ALEATORIO
                        container_olimpo_odds.upsert_item({
                            "id":str(uuid.uuid4()),
                            "cod_pais":"EC",
                            "fecha_evento": cadFechaEvento,
                            "hora_evento": cadHoraEvento,
                            "fecha_hora_evento": cadFechaHoraEvento,
                            "deporte": "Fútbol",
                            "liga": liga,
                            "evento": evento,
                            "odd_1": odd_1,
                            "odd_x": odd_x,
                            "odd_2": odd_2,
                            "en_vivo": en_vivo,
                            "proveedor": "Betano",
                            "fecha_muestra": cad_fecha_muestra,
                            "hora_muestra": cad_hora_muestra,
                            "fecha_hora_muestra": cad_fechahora_muestra
                        })
                        print("Z1")
                        id_unico = "EC-" + str(cadFechaEvento) + "-FUTBOL-" + str(slugify(liga)) + "-" + str(slugify(evento)) +str(en_vivo.upper())+"-BETANO"
                        print("Z2")
                        container_olimpo_odds_last.upsert_item({
                            "id":id_unico,
                            "cod_pais": "EC",
                            "fecha_evento": cadFechaEvento,
                            "hora_evento": cadHoraEvento,
                            "fecha_hora_evento": cadFechaHoraEvento,
                            "deporte": "Fútbol",
                            "liga": liga,
                            "evento": evento,
                            "odd_1": odd_1,
                            "odd_x": odd_x,
                            "odd_2": odd_2,
                            "en_vivo": en_vivo,
                            "proveedor": "Betano",
                            "fecha_muestra": cad_fecha_muestra,
                            "hora_muestra": cad_hora_muestra,
                            "fecha_hora_muestra": cad_fechahora_muestra
                        })            
                    
                
        except Exception as e:
            print(str(e))
            pass

if navegador != None:
    navegador.close()
    navegador.quit()
    navegador = None
gc.collect()    

sys.exit()