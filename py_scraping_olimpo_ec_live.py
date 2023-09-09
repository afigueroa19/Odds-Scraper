import pandas as pd
import json
import csv
import sys
import subprocess
import os
import datetime, calendar
import mysql.connector
from os.path import join, dirname
from dotenv import load_dotenv
from slugify import slugify
import uuid
#dotenv_path = join('/home/appolimpolakehouse/credentials.env')
#load_dotenv(dotenv_path)

#olimpo_jobs_user = os.getenv('OLIMPO_JOBS_USER')
#olimpo_jobs_password = os.getenv('OLIMPO_JOBS_PASSWORD')
#olimpo_jobs_host = os.getenv('OLIMPO_JOBS_HOST')

from urllib.request import urlopen
from azure.cosmos import CosmosClient
import json
import time

url_cosmos = 'https://cosmodbolimpolakehouse.documents.azure.com:443/'
key_cosmos = 'NOPE=='

client_cosmos = CosmosClient(url_cosmos, credential=key_cosmos)
database_cosmos = client_cosmos.get_database_client('betsoffice_scraping')
#container_olimpo_odds = database_cosmos.get_container_client('olimpo_odds_ec_2023')
container_olimpo_odds = database_cosmos.get_container_client('onepage_scraping_live_testing')


from urllib.request import urlopen  
import json
import time
import datetime

def obtenerOdds(odds):
    try:
        var1 = odds[0]['odds']*1.0 / 1000
    except:
        var1 = None
    try:
        varx = odds[1]['odds']*1.0 / 1000
    except:
        varx = None
    try:
        var2 = odds[2]['odds']*1.0 / 1000
    except:
        var2 = None

    return (var1, varx, var2)

milisegundos = int(round(time.time() * 1000))
fechahora_muestra = datetime.datetime.fromtimestamp(milisegundos/1000.0)
## - datetime.timedelta(hours=5)

cad_fecha_muestra = fechahora_muestra.strftime('%Y-%m-%d')
cad_hora_muestra = fechahora_muestra.strftime('%H:%M')
cad_fechahora_muestra = fechahora_muestra.strftime('%Y-%m-%dT%H:%M:%SZ')



# JSON Ligas importantes
##url = "https://eu-offering.kambicdn.org/offering/v2018/nexus/group/highlight.json?lang=es_PE&market=PE&client_id=2&ncid={0}&depth=0".format(milisegundos)
url = "https://na-offering-api.kambicdn.net/offering/v2018/nexus/group/highlight.json?lang=es_EC&market=EC&client_id=2&ncid={0}&depth=0".format(milisegundos)
#url = "https://na-offering-api.kambicdn.net/offering/v2018/nexus/group/highlight.json?lang=es_EC&market=EC&client_id=2&ncid=1683071293898&depth=0".format(milisegundos)
url_liga = ""
response = urlopen(url)
print("LINK INICIAL:",url)
data_json = json.loads(response.read())
print(response.status)
#print(data_json)
lista_ligas_importantes = []
registros = data_json['groups']

for liga_importante in registros:
    if liga_importante['sport'] == "FOOTBALL":
        print(liga_importante['pathTermId'])
        lista_ligas_importantes.append(liga_importante['pathTermId'])

lista_ligas_importantes.append('/football')
lista_ligas_importantes.append('/football/spain/copa_del_rey')
lista_ligas_importantes.append('/football/england/premier_league')
lista_ligas_importantes.append('/football/spain/la_liga')
lista_ligas_importantes.append('/football/peru/liga_1')
lista_ligas_importantes.append('/football/germany/bundesliga')
lista_ligas_importantes.append('/football/champions_league')
lista_ligas_importantes.append('/football/france/ligue_1')
lista_ligas_importantes.append('/football/europa_league')
lista_ligas_importantes.append('/football/copa_libertadores')
lista_ligas_importantes.append('/football/italy/serie_a')
lista_ligas_importantes.append('/football/international_friendly_matches')
lista_ligas_importantes.append('/football/ecuador/liga_pro')

for url in lista_ligas_importantes:
    try:
        print("LIGA",url)
        ##url_liga = "https://eu-offering.kambicdn.org/offering/v2018/nexus/listView{0}.json?lang=es_PE&market=PE&client_id=2&channel_id=1&ncid={1}&useCombined=true".format(url,milisegundos)
        url_liga = "https://na-offering-api.kambicdn.net/offering/v2018/nexus/listView{0}.json?lang=es_EC&market=EC&client_id=2&channel_id=1&ncid={1}&useCombined=true".format(url,milisegundos)
        
        response = urlopen(url_liga)
        data_json = json.loads(response.read())
        registros = data_json['events']

        for registro in registros:
            evento = registro['event']
            nombreEvento = evento['name']
            fechaEvento = evento['start']
            
            
            fechaEvento = datetime.datetime.strptime(fechaEvento,'%Y-%m-%dT%H:%M:%SZ')
            print(fechaEvento)
            fechaEvento = fechaEvento - datetime.timedelta(hours=5)
            print(fechaEvento)
            
            
            cadFechaEvento = fechaEvento.strftime('%Y-%m-%d')
            cadHoraEvento = fechaEvento.strftime('%H:%M')
            cadFechaHoraEvento = fechaEvento.strftime('%Y-%m-%dT%H:%M:%SZ')
            ##fechaEvento = Fecha.procesarCadenaUtc0ToUtc5(fechaEvento)

            path = evento['path']
            try:
                torneo = ''
                torneo = path[1]['name']
                torneo = torneo + ' / ' + path[2]['name']
            except:
                print("pass 1")
                pass

            try:
                (var1, varx, var2) = (None, None, None)#Odds
                ofertas = registro['betOffers']
                for oferta in ofertas:
                    if oferta['betOfferType']['name'] == 'Partido':
                        (var1, varx, var2) = obtenerOdds(oferta['outcomes'])
                        break
            except:
                pass
                print("pass 2")
            
            en_vivo = 'NO'
            print(fechahora_muestra)
            print(fechaEvento)
            if ( fechahora_muestra>=fechaEvento):
                en_vivo = 'SI'
            print(en_vivo)
            if ((en_vivo =='SI') and ((var1 is not None) or (varx is not None) or (var2 is not None))):
            
                id_unico = "EC-" + cadFechaEvento + "-FUTBOL-" +slugify(torneo)+"-" +slugify(nombreEvento)+str(en_vivo.upper())+"-OLIMPO"

                container_olimpo_odds.upsert_item({
                    "cod_pais": "EC",
                    "fecha_evento": cadFechaEvento,
                    "hora_evento":cadHoraEvento,
                    "fecha_hora_evento":cadFechaHoraEvento,
                    "deporte": "Fútbol",
                    "liga": torneo,
                    "evento": nombreEvento,
                    "odd_1":var1,
                    "odd_x":varx,
                    "odd_2":var2,
                    "en_vivo": en_vivo,
                    "proveedor":"Olimpo",
                    "fecha_muestra": cad_fecha_muestra,
                    "hora_muestra": cad_hora_muestra,
                    "fecha_hora_muestra": cad_fechahora_muestra
                })

                
    except Exception as e:
        print(str(e))
        pass
           
sys.exit()