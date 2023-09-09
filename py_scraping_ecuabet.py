from urllib.request import urlopen  
import json
import time
import datetime
from slugify import slugify
from azure.cosmos import CosmosClient
import uuid
from random import seed
import requests

start_utc = datetime.datetime.utcnow()
end_utc_7days = datetime.datetime.utcnow() + datetime.timedelta(days=7)
end_utc = datetime.datetime.utcnow() + datetime.timedelta(days=30)

cad_start_utc = start_utc.strftime('%Y-%m-%dT%H%%3A%M%%3A00.000Z')
cad_end_utc_7days = end_utc_7days.strftime('%Y-%m-%dT%H%%3A%M%%3A00.000Z')
cad_end_utc = end_utc.strftime('%Y-%m-%dT%H%%3A%M%%3A00.000Z')


##LISTA CAMPEONATOS 1

endpoint_campeonatos = ('''
https://sb2frontend-altenar2.biahosted.com/api/Sportsbook/GetFavouritesChamps?timezoneOffset=300&langId=4&skinName=ecuabet&configId=1&culture=es-ES&countryCode=PA&deviceType=Mobile&numformat=en&integration=ecuabet&period=periodmonth&sportId={cod_sport}&startDate={cad_fec_hora_ini}&endDate={cad_fec_hora_ini}
''').format(cod_sport=str(66),cad_fec_hora_ini=cad_start_utc,cad_fec_hora_fin=cad_end_utc)


#response = urlopen(endpoint_campeonatos)
#data_json = json.loads(response.read())


campeonatos_ingresados = []

array_dict_campeonatos = [\
{ "SportName": "Fútbol", "CatName": "Europa", "ChampName": "UEFA Champions League", "ChampId": 16808},\
{ "SportName": "Fútbol", "CatName": "Europa", "ChampName": "UEFA Europa League", "ChampId": 16809},\
{ "SportName": "Fútbol", "CatName": "Europa", "ChampName": "Clasif. Camp. Europeo", "ChampId": 3148},\
{ "SportName": "Fútbol", "CatName": "Europa", "ChampName": "UEFA Nations League", "ChampId": 5098},\
{ "SportName": "Fútbol", "CatName": "Europa", "ChampName": "UEFA Europa Conference League", "ChampId": 31608},\
{ "SportName": "Fútbol", "CatName": "Mundo", "ChampName": "Amistosos Internacionales", "ChampId": 3645},\
{ "SportName": "Fútbol", "CatName": "Mundo", "ChampName": "Copa Mundial 2026", "ChampId": 3146},\
{ "SportName": "Fútbol", "CatName": "Americas", "ChampName": "Liga de Naciones CONCACAF", "ChampId": 28427},\
{ "SportName": "Fútbol", "CatName": "Americas", "ChampName": "Copa Libertadores", "ChampId": 3709},\
{ "SportName": "Fútbol", "CatName": "Americas", "ChampName": "Copa Sudamericana", "ChampId": 3108},\
{ "SportName": "Fútbol", "CatName": "Brasil", "ChampName": "Carioca", "ChampId": 3357},\
{ "SportName": "Fútbol", "CatName": "Colombia", "ChampName": "Primera A, Clausura", "ChampId": 3857},\
{ "SportName": "Fútbol", "CatName": "Uruguay", "ChampName": "Primera División", "ChampId": 4632},\
{ "SportName": "Fútbol", "CatName": "España", "ChampName": "La Liga", "ChampId": 2941},\
{ "SportName": "Fútbol", "CatName": "España", "ChampName": "Copa del Rey", "ChampId": 2973},\
{ "SportName": "Fútbol", "CatName": "Inglaterra", "ChampName": "Premier League", "ChampId": 2936},\
{ "SportName": "Fútbol", "CatName": "Italia", "ChampName": "Serie A", "ChampId": 2942},\
{ "SportName": "Fútbol", "CatName": "Francia", "ChampName": "Ligue 1", "ChampId": 2943},\
{ "SportName": "Fútbol", "CatName": "Alemania", "ChampName": "Bundesliga", "ChampId": 2950},\
{ "SportName": "Fútbol", "CatName": "Portugal", "ChampName": "Primera División", "ChampId": 3152},\
{ "SportName": "Fútbol", "CatName": "Argentina", "ChampName": "Liga Profesional", "ChampId": 3075},\
{ "SportName": "Fútbol", "CatName": "Brasil", "ChampName": "Brasileiro, Serie A", "ChampId": 11318},\
{ "SportName": "Fútbol", "CatName": "México", "ChampName": "Liga MX", "ChampId": 10009},\
{ "SportName": "Fútbol", "CatName": "Colombia", "ChampName": "Primera A, Clausura", "ChampId": 3857},\
{ "SportName": "Fútbol", "CatName": "Chile", "ChampName": "Primera División", "ChampId": 3837},\
{ "SportName": "Fútbol", "CatName": "Ecuador", "ChampName": "LigaPro Primera B", "ChampId": 4884},\
{ "SportName": "Fútbol", "CatName": "Europa", "ChampName": "UEFA Nations League", "ChampId": 5098},\
{ "SportName": "Fútbol", "CatName": "Ecuador", "ChampName": "LigaPro Primera A", "ChampId": 4564}]

for iter_ in array_dict_campeonatos:
    llave = iter_["ChampId"]
    if llave not in campeonatos_ingresados:
        campeonatos_ingresados.append(llave)


session_obj = requests.Session()
response = session_obj.get(endpoint_campeonatos, headers={"User-Agent": "Mozilla/5.0"})
data = response.content
data_json = json.loads(data)


campeonatos = data_json['Result']

#campeonatos_ingresados = []

#array_dict_campeonatos = []
for campeonato in campeonatos:
    if campeonato["ChampId"] not in campeonatos_ingresados:
        array_dict_campeonatos.append(
        {
            "SportName":campeonato["SportName"],
            "CatName": campeonato["CatName"],
            "ChampName":campeonato["ChampName"],
            "ChampId":campeonato["ChampId"]
        }
        )
        
##LISTA CAMPEONATOS 2

endpoint_campeonatos = ('''
https://sb2frontend-altenar2.biahosted.com/api/SportsBook/GetMenuBySport?timezoneOffset=300&langId=4&skinName=ecuabet&configId=1&culture=es-ES&countryCode=PA&deviceType=Desktop&numformat=en&integration=ecuabet&sportId={cod_sport}&period=periodall&startDate={cad_fec_hora_ini}&endDate={cad_fec_hora_fin}
''').format(cod_sport=str(66),cad_fec_hora_ini=cad_start_utc,cad_fec_hora_fin=cad_end_utc)

#response = urlopen(endpoint_campeonatos)
#data_json = json.loads(response.read())

session_obj = requests.Session()
response = session_obj.get(endpoint_campeonatos, headers={"User-Agent": "Mozilla/5.0"})
data = response.content
data_json = json.loads(data)

campeonatos = data_json['Result'][0]['Items']
for campeonato in campeonatos:
    if campeonato["Items"][0]["Id"] not in campeonatos_ingresados:
        array_dict_campeonatos.append(
        {
            "SportName": "Fútbol",
            "CatName": campeonato["Name"],
            "ChampName":campeonato["Items"][0]["Name"],
            "ChampId":campeonato["Items"][0]["Id"]
        }
        )

##################



milisegundos = int(round(time.time() * 1000))
fechahora_muestra = datetime.datetime.fromtimestamp(milisegundos/1000.0)

cad_fecha_muestra = fechahora_muestra.strftime('%Y-%m-%d')
cad_hora_muestra = fechahora_muestra.strftime('%H:%M')
cad_fechahora_muestra = fechahora_muestra.strftime('%Y-%m-%dT%H:%M:%SZ')


for iter_campeonato in array_dict_campeonatos:    
    endpoint_eventos = ('''https://sb2frontend-altenar2.biahosted.com/api/Sportsbook/GetEvents?timezoneOffset=300&langId=4&skinName=ecuabet&configId=1&culture=es-ES&countryCode=PA&deviceType=Mobile&numformat=en&integration=ecuabet&sportids=0&categoryids=0&champids={cod_campeonato}&group=AllEvents&period=periodall&withLive=false&outrightsDisplay=none&marketTypeIds=&couponType=0&startDate={cad_fec_hora_ini}&endDate={cad_fec_hora_fin}
    ''').format(cod_campeonato=str(iter_campeonato["ChampId"]),cad_fec_hora_ini=cad_start_utc,cad_fec_hora_fin=cad_end_utc)
    #''').format(cod_campeonato=str(iter_campeonato["ChampId"]),cad_fec_hora_ini=cad_start_utc,cad_fec_hora_fin=cad_end_utc_7days)

    #response = urlopen(endpoint_eventos)
    #data_json = json.loads(response.read())
    
    session_obj = requests.Session()
    response = session_obj.get(endpoint_eventos, headers={"User-Agent": "Mozilla/5.0"})
    data = response.content
    data_json = json.loads(data)

    lista_eventos = data_json['Result']['Items']
    for iter_evento in lista_eventos:
        lista_eventos_deep = iter_evento['Events']
        for iter_evento_deep in lista_eventos_deep:
            evento = iter_evento_deep["Name"].replace("vs.","-")
            print(iter_evento_deep["Name"].replace("vs.","-"))
            #print(iter_evento_deep["EventDate"])
            fechaEvento = datetime.datetime.strptime(iter_evento_deep["EventDate"],'%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=5)
            
            torneo = iter_campeonato["CatName"] + ' / ' + iter_campeonato["ChampName"]
            print(torneo)
            cadFechaEvento = fechaEvento.strftime('%Y-%m-%d')
            cadHoraEvento = fechaEvento.strftime('%H:%M')
            cadFechaHoraEvento = fechaEvento.strftime('%Y-%m-%dT%H:%M:%SZ')
            print(cadFechaHoraEvento)
            for odds_iter_item in  iter_evento_deep['Items']:
                
                if odds_iter_item['Name'] == '1x2':
                    #print (odds_iter_item['Name'])
                    array_odds_1x2 = odds_iter_item["Items"]
                    odd_1 = 0.0
                    odd_x = 0.0
                    odd_2 = 0.0
                    for iter_array_odds_1x2 in array_odds_1x2:
                        if iter_array_odds_1x2['SelectionTypeId']==1:
                            odd_1 = iter_array_odds_1x2['Price']
                        if iter_array_odds_1x2['SelectionTypeId']==2:
                            odd_x = iter_array_odds_1x2['Price']
                        if iter_array_odds_1x2['SelectionTypeId']==3:
                            odd_2 = iter_array_odds_1x2['Price']
                    print(odd_1,odd_x,odd_2)
                    
                    en_vivo = 'NO'
                    if fechaEvento == None:
                        pass
                    elif fechaEvento != None:
                        if cad_fechahora_muestra >= cadFechaHoraEvento:
                            en_vivo = 'SI'
                        if ((odd_1>0) and (odd_x>0) and (odd_2>0)):
                            
                            cod_pais = "EC"
                            url_cosmos = 'https://cosmodbolimpolakehouse.documents.azure.com:443/'
                            key_cosmos = 'NOPE=='

                            client_cosmos = CosmosClient(url_cosmos, credential=key_cosmos)
                            database_cosmos = client_cosmos.get_database_client('betsoffice_scraping')
                            container_olimpo_odds = database_cosmos.get_container_client('olimpo_odds_ec_2023')
                            container_olimpo_odds_last = database_cosmos.get_container_client('olimpo_odds_last_ec')

                            id_unico = "EC-" + str(cadFechaEvento) + "-FUTBOL-" + slugify(torneo) + "-" + slugify(evento) +str(en_vivo.upper())+"-ECUABET"

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
                                "odd_1": odd_1,
                                "odd_x": odd_x,
                                "odd_2": odd_2,
                                "en_vivo": en_vivo,
                                "proveedor": "Ecuabet",
                                "fecha_muestra": cad_fecha_muestra,
                                "hora_muestra": cad_hora_muestra,
                                "fecha_hora_muestra": cad_fechahora_muestra
                            })
                            
                            container_olimpo_odds_last.upsert_item({
                            "id": id_unico,
                            "cod_pais": cod_pais,
                            "fecha_evento": cadFechaEvento,
                            "hora_evento":cadHoraEvento,
                            "fecha_hora_evento":cadFechaHoraEvento,
                            "deporte": "Fútbol",
                            "liga": torneo,
                            "evento": evento,
                            "odd_1":odd_1,
                            "odd_x":odd_x,
                            "odd_2":odd_2,
                            "en_vivo": en_vivo,
                            "proveedor":"Ecuabet",
                            "fecha_muestra": cad_fecha_muestra,
                            "hora_muestra": cad_hora_muestra,
                            "fecha_hora_muestra": cad_fechahora_muestra
                        })


                    break
    #break