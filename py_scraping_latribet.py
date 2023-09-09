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


## INICIAR VARIABLES DEL DRIVER
##driverRuta = "C:\\Users\\EduardoEspinoza\\chromedriver.exe"
##driverRuta = r"C:\Users\EduardoEspinoza\OneDrive - NG ENTERTAINMENT PERÚ II\\chromedriver.exe"
driverRuta = "/root/chromedriver"

navegador = None
tiempoMaximo = 10
tiempoMedio = 4
tiempoMinimo = 1
tiempoSMinimo = 0.5
maxIntentos = 3
urlBase = "https://latribet.ec/sportsbook/240"

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
opciones.add_argument('--headless') #para evitar gui
opciones.add_argument('--disable-gpu')
navegador = webdriver.Chrome(driverRuta, chrome_options = opciones)
navegador.implicitly_wait(tiempoMaximo)#Tiempo de espera del driver para encontrar un elemento

fechaBase = {'Hoy': datetime.datetime.now().strftime('%Y-%m-%d'), 'Mañana': (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')}
meses = {'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04', \
    'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08', \
    'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'}


milisegundos = int(round(time.time() * 1000))
fechahora_muestra = datetime.datetime.fromtimestamp(milisegundos / 1000.0)
cad_fecha_muestra = fechahora_muestra.strftime('%Y-%m-%d')
cad_hora_muestra = fechahora_muestra.strftime('%H:%M')
cad_fechahora_muestra = fechahora_muestra.strftime('%Y-%m-%dT%H:%M:%SZ')


lista = ["Perú","América","Europa","Mundo","Argentina","Brasil","España","Inglaterra","Alemania","Francia","Italia","Portugal","Colombia","Chile","Ecuador","México"]
#lista = ["Perú"]
navegador.get(urlBase)
##enlaces = obtenerEnlaces()
from selenium.webdriver.common.by import By
multiplicador = 1
navegador.implicitly_wait(tiempoMaximo)
(enlaces, cont) = ([], 0)
while cont < multiplicador * maxIntentos:
    try:
        items = navegador.find_elements(By.XPATH,"//li[@id='sports-menu-item__240']//li")
        #print(items)
        for i in range(0, len(items)):
            nombreItem = items[i].text
            ##enlace = items[i].find_element_by_tag_name("a").get_attribute("href")
            nombreItem = items[i].text
            enlace = items[i].find_element(By.TAG_NAME,"a").get_attribute("href")
            ##find_element
            print(nombreItem, enlace)
            
            ##CAMBIO##
            if nombreItem in lista:
                enlaces.append((nombreItem, enlace))

        if len(enlaces) > 0:
            break

    except Exception as e:
        print(e)
        pass
        
    enlaces = []
    cont += 1
    #print('INTENTO->', cont)
    navegador.refresh()
    time.sleep(tiempoMinimo)

navegador.implicitly_wait(tiempoMedio)

########
def obtenerFechaBase(texto):
    #print("obtenerFechaBase",texto)
    try:
        fecha = fechaBase.get(texto)
        if fecha != None:
            return fecha

        ##fechaActual = Fecha.obtenerFechaLima().replace(tzinfo=None)
        fechaActual = datetime.datetime.now().replace(tzinfo=None)
        words = texto.lower().split(' ')
        fechaString = "{0}-{1}-{2}".format(fechaActual.year, meses.get(words[1]), words[0])
        ##fechaReferencial = Fecha.stringDiaToFecha(fechaString)
        fechaReferencial = datetime.datetime.strptime(fechaString, '%Y-%m-%d')
        
        if fechaReferencial < fechaActual:
            fechaString = "{0}-{1}-{2}".format(fechaActual.year + 1, meses.get(words[1]), words[0])

        return fechaString

    except Exception as e:
        print(e)
        pass

    return None


def obtenerScores(texto):
    lineas = texto.split('\n')
    scores = []
    for linea in lineas:
        valor = linea.strip()
        try:
            numero = float(valor)
            scores.append(numero)
        except:
            pass

    if len(scores) < 3:
        return (None, None, None)

    return (scores[0],scores[1],scores[2])


def obtenerEventos(nombreItem):
    #print('\tNOMBRE->', nombreItem)
    (fechaBase, torneo, cont, data) = ('', '', 0, [])
    while cont < maxIntentos:
        try:
            #contenedor = navegador.find_element_by_css_selector("div.osg-coupon__sport")
            contenedor = navegador.find_element(By.CSS_SELECTOR,"div.osg-coupon__sport")
            
            #elementos = contenedor.find_elements_by_css_selector('h3, h4, div.osg-coupon__events')
            elementos = contenedor.find_elements(By.CSS_SELECTOR,'h3, h4, div.osg-coupon__events')

            #fechaActual = Fecha.fechaHoraString()
            ##fechaActual="2022-10-01"
            fechaActual=datetime.datetime.now().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M")
            for elemento in elementos:
                etiqueta = elemento.get_attribute('outerHTML')[0:4]
                #print(elemento.text)
                if etiqueta == '<h3>':
                    #print(elemento.text)
                    fechaBase = obtenerFechaBase(elemento.text)
                    continue
                if etiqueta == '<h4>':
                    #print(nombreItem,elemento.text)
                    torneo = nombreItem + ' / ' +elemento.text
                    continue

                #contenido = elemento.find_element_by_class_name('osg-coupon__event-description-row').text
                contenido = elemento.find_element(By.CLASS_NAME,'osg-coupon__event-description-row').text
                
                #print("fechaBase",fechaBase)
                #print("contenido",contenido)
                if fechaBase != None:
                    fecha  = "{0} {1}:00".format(fechaBase, contenido[0:5])
                else:
                    fecha = None

                evento = contenido[6:]
                (var1, varx, var2) = obtenerScores(elemento.text)
                
                ###############
                ### AGREGAR OLIMPO_COD_PAIS ###
                cod_pais = "EC"

                url_cosmos = 'https://cosmodbolimpolakehouse.documents.azure.com:443/'
                key_cosmos = 'NOPE=='

                client_cosmos = CosmosClient(url_cosmos, credential=key_cosmos)
                database_cosmos = client_cosmos.get_database_client('betsoffice_scraping')
                container_olimpo_odds = database_cosmos.get_container_client('olimpo_odds_ec_2023')
                container_olimpo_odds_last = database_cosmos.get_container_client('olimpo_odds_last_ec')
                
                en_vivo = 'NO'
                if fecha == None:
                    pass
                elif fecha != None:
                    
                    ##fechaEvento = datetime.datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=5)
                    
                    fechaEvento = datetime.datetime.strptime(fecha, '%Y-%m-%d %H:%M:%S') 
                    ##- datetime.timedelta(hours=5)

                    cadFechaEvento = fechaEvento.strftime('%Y-%m-%d')
                    cadHoraEvento = fechaEvento.strftime('%H:%M')
                    cadFechaHoraEvento = fechaEvento.strftime('%Y-%m-%dT%H:%M:%SZ')

                    fecha = fecha.lower().replace('t', ' ').replace('z', '')

                    print("cadFechaEvento",cadFechaEvento)
                    print("cad_fechahora_muestra",cad_fechahora_muestra)
                    
                    if cad_fechahora_muestra >= cadFechaHoraEvento:
                        en_vivo = 'SI'
                        print("EN VIVO?")
                    if ((var1 is not None) and (varx is not None) and (var2 is not None)):

                        evento=evento.replace("\nCrear apuesta", "" )
                        
                        id_unico = "EC-" + str(cadFechaEvento) + "-FUTBOL-" + slugify(torneo) + "-" + slugify(evento) +str(en_vivo.upper())+"-LATRIBET"

                        ##ID ALEATORIO
                        container_olimpo_odds.upsert_item({
                            "id":str(uuid.uuid4()),
                            "cod_pais":cod_pais,
                            "fecha_evento": cadFechaEvento,
                            "hora_evento": cadHoraEvento,
                            "fecha_hora_evento": cadFechaHoraEvento,
                            "deporte": "Fútbol",
                            "liga": torneo,
                            "evento": evento,
                            "odd_1": var1,
                            "odd_x": varx,
                            "odd_2": var2,
                            "en_vivo": en_vivo,
                            "proveedor": "LaTribet",
                            "fecha_muestra": cad_fecha_muestra,
                            "hora_muestra": cad_hora_muestra,
                            "fecha_hora_muestra": cad_fechahora_muestra
                        })
                        
                        container_olimpo_odds_last.upsert_item({
                            "id": id_unico,
                            "cod_pais": "EC",
                            "fecha_evento": cadFechaEvento,
                            "hora_evento":cadHoraEvento,
                            "fecha_hora_evento":cadFechaHoraEvento,
                            "deporte": "Fútbol",
                            "liga": torneo,
                            "evento": evento,
                            "odd_1":var1,
                            "odd_x":varx,
                            "odd_2":var2,
                            "en_vivo": en_vivo,
                            "proveedor":"LaTribet",
                            "fecha_muestra": cad_fecha_muestra,
                            "hora_muestra": cad_hora_muestra,
                            "fecha_hora_muestra": cad_fechahora_muestra
                        })

                        #print('\t',[fecha, 'Fútbol', torneo, evento, var1, varx, var2,'LaTribet', fechaActual])
                        #data.append([fecha, 'Fútbol', torneo, evento, var1, varx, var2,'Inkabet', fechaActual])

            break

        except Exception as e:
            print(e)
            pass
        cont += 1
        navegador.refresh()
        time.sleep(tiempoMinimo)

    return data


########################
(data, visitados, cont, nEnlaces) = ([], {}, 0, len(enlaces))
while cont < nEnlaces:
    try:
        (nombreItem, enlace) = enlaces[cont]
        if visitados.get(nombreItem) != None:
            #print('YA VISITADO->', nombreItem)
            cont += 1
            continue

        #print('ENLACE->', enlace)
        navegador.get(enlace)
        time.sleep(tiempoMinimo)
        data = data + obtenerEventos(nombreItem)

        visitados[nombreItem] = True
        cont += 1
        continue

    except Exception as e:
        print('ERROR_obtenerData', str(e))
        pass
    navegador.refresh()
    time.sleep(tiempoMinimo)


if navegador != None:
    navegador.close()
    navegador.quit()
    navegador = None
gc.collect()    

sys.exit()