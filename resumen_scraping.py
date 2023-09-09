import requests
from datetime import date, datetime, timedelta
from azure.cosmos import CosmosClient
import uuid
import pandas as pd
import re
from slugify import slugify
import numpy as np
import re
import func_email
import sys
from lib_normalize import normalize, normalize_liga, funcion_exploracion_ligas, funcion_reporting

def main(argv, arc):
    #LISTA_CORREOS = "walter@betsoffice.com;eduardo@betsoffice.com;isaias@betsoffice.com;grace@betsoffice.com;andrea@betsoffice.com;alberto@macaolatam.com" 
    LISTA_CORREOS = "alberto@macaolatam.com" 
    url_cosmos = "https://cosmodbolimpolakehouse.documents.azure.com:443/"
    key_cosmos = "NOPE=="
    
    client_cosmos = CosmosClient(url_cosmos, credential=key_cosmos)
    database_cosmos = client_cosmos.get_database_client('betsoffice_scraping')
    container_odds = database_cosmos.get_container_client('olimpo_odds_2023')
    container_scraping = database_cosmos.get_container_client('onepage_scraping')

    cad_filter_liga = ["Champions","Amistosos Internacionales","La Liga","Premier",  "Ligue 1",  "Copa Sudamericana",  "Copa del Rey", "Copa Libertadores", "Liga 1", "Serie A", "Bundesliga",  "Europa League"]
    fecha_evento_inicio = datetime.now()- timedelta(days=1)
    fecha_evento_inicio = fecha_evento_inicio.strftime("%Y-%m-%d")


    #fecha_evento_fin = fecha_evento_inicio + timedelta(days=8)
    fecha_evento_fin = datetime.now()- timedelta(days=1) + timedelta(days=31)
    fecha_evento_fin = fecha_evento_fin.strftime("%Y-%m-%d")
    n=30
    #fecha_evento_inicio = "2023-01-01"
    #fecha_evento_fin = "2023-04-30"

    if ((argv[1]!="")&(argv[2]!="")):
        fecha_evento_inicio = argv[1]
        fecha_evento_fin = argv[2]

    print(fecha_evento_inicio)
    print(fecha_evento_fin)

    #func_email.enviar_email(LISTA_CORREOS,"SCRAPING - INICIO ","INICIO SCRAPING", "NINGUNO")
    for nombre_liga in cad_filter_liga:
        try:
            print(nombre_liga)
            Reporte = funcion_reporting(container_odds, fecha_evento_inicio, fecha_evento_fin, nombre_liga, n)
            
            if len(Reporte)!=0:
                print("Se obtuvo Reporte")

                print(Reporte)
                Reporte["compendio"]=Reporte["fecha_evento"]+"|"+Reporte["hora_evento"]+"|"+Reporte["evento_rename"]+"|"+Reporte["liga_rename"]
                Tabla=Reporte.groupby(['fecha_muestra','compendio','proveedor']).mean().reset_index()
                print("Se agrupo") 
                # Tabla["fecha_evento"]=Tabla.apply(fecha_evento, axis=1)
                # Tabla["hora_evento"]=Tabla.apply(hora_evento, axis=1)
                # Tabla["liga_rename"]=Tabla.apply(liga, axis=1)
                name=Tabla["compendio"].str.split('|', expand=True)
                name.columns = ['fecha_evento', 'hora_evento', 'evento_rename', 'liga_rename']
                Tabla = pd.concat([name, Tabla], axis=1)
                print("Se añadieron las columnas fecha_evento, hora_evento, liga") 
                for i in range(len(Tabla)):
                    element = {
                        "id": "PE_"+str(Tabla.loc[i,"fecha_evento"])+"_"+slugify(str(Tabla.loc[i,"liga_rename"]))+"_"+slugify(str(Tabla.loc[i,"evento_rename"]))+"_"+str(Tabla.loc[i,"fecha_muestra"])+"_"+slugify(str(Tabla.loc[i,"proveedor"]))+"_NO_"+str(int(Tabla.loc[i,"n"])),
                        "cod_pais":"PE",
                        "fecha_evento":str(Tabla.loc[i,"fecha_evento"]),
                        "hora_evento":str(Tabla.loc[i,"hora_evento"]),
                        "liga":str(Tabla.loc[i,"liga_rename"]),
                        "evento":str(Tabla.loc[i,"evento_rename"]),
                        "fecha_muestra":str(Tabla.loc[i,"fecha_muestra"]),
                        "proveedor":str(Tabla.loc[i,"proveedor"]),
                        "en_vivo":"NO",
                        "payout":np.round(float(Tabla.loc[i,"payout"]),4),
                        "odd_1":np.round(float(Tabla.loc[i,"odd_1"]),4),
                        "odd_x":np.round(float(Tabla.loc[i,"odd_x"]),4),
                        "odd_2":np.round(float(Tabla.loc[i,"odd_2"]),4),
                        "n_dias":int(Tabla.loc[i,"n"]),
                    }

                    container_scraping.upsert_item(element)

        except Exception as e:
            #func_email.enviar_email(LISTA_CORREOS,"SCRAPING - ERROR - RESUMEN SCRAPING","{}".format(e), "NINGUNO")
            print(e)
            pass

if __name__ == '__main__':
    main(sys.argv, len(sys.argv))
sys.exit()
