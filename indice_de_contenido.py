from urllib.request import urlopen  
import json
import time
from azure.cosmos import CosmosClient
import pandas as pd
import csv
import sys
import subprocess
import calendar
from slugify import slugify
import uuid
from datetime import datetime,date, timedelta
import numpy as np
import mysql.connector

replacements= {
    'World Cup Qualification - CONMEBOL Qualification': 'World Cup Qualification / CONMEBOL',
    
    'World Cup Qualification - UEFA Qualification: 1st Round: Group A': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group B': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group C': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group D': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group E': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group F': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group G': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group H': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group I': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group J': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group K': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: 1st Round: Group L': 'World Cup Qualification / UEFA',
    'World Cup Qualification - UEFA Qualification: Play-off': 'World Cup Qualification / UEFA',
    
    'Euro 2024 Qualification - Qualification: Group A': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group B': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group C': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group D': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group E': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group F': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group G': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group H': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group I': 'Euro 2024 Qualification',
    'Euro 2024 Qualification - Qualification: Group J': 'Euro 2024 Qualification',
    
    'Champions League - Qualification: Preliminary round 2023/24': 'Champions League / Qualification',
    'Champions League - Qualification: Preliminary Round': 'Champions League / Qualification',
    'Champions League - Qualification 2023/24': 'Champions League / Qualification',
    'Champions League - Qualification': 'Champions League / Qualification',
    'Champions League - Group A 2023/24': 'Champions League',
    'Champions League - Group B 2023/24': 'Champions League',
    'Champions League - Group C 2023/24': 'Champions League',
    'Champions League - Group D 2023/24': 'Champions League',
    'Champions League - Group E 2023/24': 'Champions League',
    'Champions League - Group F 2023/24': 'Champions League',
    'Champions League - Group G 2023/24': 'Champions League',
    'Champions League - Group H 2023/24': 'Champions League',
    'Champions League - Round of 16 2023/24': 'Champions League',
    'Champions League - Quarter-Finals 2023/24': 'Champions League',
    'Champions League - Semi-Finals 2023/24': 'Champions League',
    'Champions League - Final 2023/24': 'Champions League',
    
    'Europa League - Qualification': 'Europa League / Qualification',
    'Europa League - Group A': 'Europa League',
    'Europa League - Group B': 'Europa League',
    'Europa League - Group C': 'Europa League',
    'Europa League - Group D': 'Europa League',
    'Europa League - Group E': 'Europa League',
    'Europa League - Group F': 'Europa League',
    'Europa League - Group G': 'Europa League',
    'Europa League - Group H': 'Europa League',
    'Europa League - Knockout Round Play-offs': 'Europa League',
    'Europa League - Quarter-Finals': 'Europa League',
    'Europa League - Semi-Finals': 'Europa League',
    'Europa League - Final': 'Europa League',
    
    'Spain - Copa del Rey Round 1': 'Copa del Rey',
    'Spain - Copa del Rey Round 2': 'Copa del Rey',
    'Spain - Copa del Rey Round 32': 'Copa del Rey',
    'Spain - Copa del Rey Round 16': 'Copa del Rey',
    'Spain - Copa del Rey Quarter-Finals': 'Copa del Rey',
    'Spain - Copa del Rey Semi-Finals': 'Copa del Rey',
    'Spain - Copa del Rey Final': 'Copa del Rey',
    
    'England - FA Cup Round 1': 'Inglaterra / FA Cup',
    'England - FA Cup Round 2': 'Inglaterra / FA Cup',
    'England - FA Cup Round 3': 'Inglaterra / FA Cup',
    'England - FA Cup Round 4': 'Inglaterra / FA Cup',
    'England - FA Cup Round 5': 'Inglaterra / FA Cup',
    'England - FA Cup Quarter Finals': 'Inglaterra / FA Cup',
    'England - FA Cup Semi Finals': 'Inglaterra / FA Cup',
    'England - FA Cup Final': 'Inglaterra / FA Cup',
    
    'Peru - Liga 1: Clausura': 'Peru / Liga 1',
    'Peru - Liga 1: Apertura': 'Peru / Liga 1',
    
    'Copa Libertadores - Group A': 'Copa Libertadores',
    'Copa Libertadores - Group B': 'Copa Libertadores',
    'Copa Libertadores - Group C': 'Copa Libertadores',
    'Copa Libertadores - Group D': 'Copa Libertadores',
    'Copa Libertadores - Group E': 'Copa Libertadores',
    'Copa Libertadores - Group F': 'Copa Libertadores',
    'Copa Libertadores - Group G': 'Copa Libertadores',
    'Copa Libertadores - Group H': 'Copa Libertadores',
    'Copa Libertadores - Qualification': 'Copa Libertadores / Qualification',
    
    'Copa Sudamericana - Group A': 'Copa Sudamericana',
    'Copa Sudamericana - Group B': 'Copa Sudamericana',
    'Copa Sudamericana - Group C': 'Copa Sudamericana',
    'Copa Sudamericana - Group D': 'Copa Sudamericana',
    'Copa Sudamericana - Group E': 'Copa Sudamericana',
    'Copa Sudamericana - Group F': 'Copa Sudamericana',
    'Copa Sudamericana - Group G': 'Copa Sudamericana',
    'Copa Sudamericana - Group H': 'Copa Sudamericana',
    'Copa Sudamericana - Qualification': 'Copa Sudamericana / Qualification',
    
    'England - Premier League 23/24': 'Inglaterra / Premier League',
    'England - Premier League': 'Inglaterra / Premier League',
    'International - Friendlies': 'Amistosos / Internacionales',
    'Spain - LaLiga Santander': 'España / La Liga',
    'Italy - Serie A': 'Italia / Serie A',
    'USA - MLS': 'USA / MLS',
    'Brazil - Serie A': 'Brasil / Serie A',
    'Argentina - Primera Division': 'Argentina / Primera Division',
    'France - Ligue 1': 'Francia / Ligue 1',
    'Germany - Bundesliga': 'Alemania / Bundesliga',
    'Ecuador - Serie A': 'Ecuador / Serie A',
    'Germany - Bundesliga': 'Alemania / Bundesliga',
    
    
    'UEFA Nations League - Final': 'UEFA Nations League',
    'UEFA Nations League - 3rd Place Play-off': 'UEFA Nations League',
    'UEFA Nations League - Semi-finals': 'UEFA Nations League',
    'UEFA Nations League - League A: Group 1': 'UEFA Nations League',
    'UEFA Nations League - League A: Group 2': 'UEFA Nations League',
    'UEFA Nations League - League A: Group 3': 'UEFA Nations League',
    'UEFA Nations League - League A: Group 4': 'UEFA Nations League',
    'UEFA Nations League - League B: Group 1': 'UEFA Nations League',
    'UEFA Nations League - League B: Group 2': 'UEFA Nations League',
    'UEFA Nations League - League B: Group 3': 'UEFA Nations League',
    'UEFA Nations League - League B: Group 4': 'UEFA Nations League', 
    'UEFA Nations League - League C: Group 1': 'UEFA Nations League',
    'UEFA Nations League - League C: Group 2': 'UEFA Nations League',
    'UEFA Nations League - League C: Group 3': 'UEFA Nations League',
    'UEFA Nations League - League C: Group 4': 'UEFA Nations League',
    'UEFA Nations League - League D: Group 1': 'UEFA Nations League',
    'UEFA Nations League - League D: Group 2': 'UEFA Nations League'}

ligas = ['World Cup Qualification - CONMEBOL Qualification',
         
         'World Cup Qualification - UEFA Qualification: 1st Round: Group A', 'World Cup Qualification - UEFA Qualification: 1st Round: Group B',
         'World Cup Qualification - UEFA Qualification: 1st Round: Group C', 'World Cup Qualification - UEFA Qualification: 1st Round: Group D',
         'World Cup Qualification - UEFA Qualification: 1st Round: Group E', 'World Cup Qualification - UEFA Qualification: 1st Round: Group F',
         'World Cup Qualification - UEFA Qualification: 1st Round: Group G', 'World Cup Qualification - UEFA Qualification: 1st Round: Group H',
         'World Cup Qualification - UEFA Qualification: 1st Round: Group I', 'World Cup Qualification - UEFA Qualification: 1st Round: Group J',
         'World Cup Qualification - UEFA Qualification: 1st Round: Group K', 'World Cup Qualification - UEFA Qualification: 1st Round: Group L',
         'World Cup Qualification - UEFA Qualification: Play-off',
         
         'Euro 2024 Qualification - Qualification: Group A', 'Euro 2024 Qualification - Qualification: Group B',
         'Euro 2024 Qualification - Qualification: Group C', 'Euro 2024 Qualification - Qualification: Group D',
         'Euro 2024 Qualification - Qualification: Group E', 'Euro 2024 Qualification - Qualification: Group F',
         'Euro 2024 Qualification - Qualification: Group G', 'Euro 2024 Qualification - Qualification: Group H',
         'Euro 2024 Qualification - Qualification: Group I', 'Euro 2024 Qualification - Qualification: Group J',
         
         'Champions League - Qualification: Preliminary round 2023/24', 'Champions League - Qualification: Preliminary Round',
         'Champions League - Qualification 2023/24', 'Champions League - Qualification', 'Champions League - Group A 2023/24',
         'Champions League - Group B 2023/24', 'Champions League - Group C 2023/24', 'Champions League - Group D 2023/24',
         'Champions League - Group E 2023/24', 'Champions League - Group F 2023/24', 'Champions League - Group G 2023/24',
         'Champions League - Group H 2023/24', 'Champions League - Round of 16 2023/24', 'Champions League - Quarter-Finals 2023/24',
         'Champions League - Semi-Finals 2023/24',  'Champions League - Final 2023/24',
         
         'Europa League - Qualification', 'Europa League - Group A',
         'Europa League - Group B', 'Europa League - Group C', 'Europa League - Group D',
         'Europa League - Group E', 'Europa League - Group F', 'Europa League - Group G',
         'Europa League - Group H', 'Europa League - Knockout Round Play-offs', 'Europa League - Knockout Round Play-offs',
         'Europa League - Quarter-Finals', 'Europa League - Semi-Finals',  'Europa League - Final',
         
         'Spain - Copa del Rey Round 1', 'Spain - Copa del Rey Round 2', 'Spain - Copa del Rey Round of 32',
         'Spain - Copa del Rey Round of 16', 'Spain - Copa del Rey Quarter-Finals', 'Spain - Copa del Rey Semi-Finals',
         'Spain - Copa del Rey Final',
         
         'England - FA Cup Round 1', 'England - FA Cup Round 2', 'England - FA Cup Round 3', 'England - FA Cup Round 4',
         'England - FA Cup Round 5', 'England - FA Cup Quarter Finals', 'England - FA Cup Semi Finals',
         'England - FA Cup Final',
         
         'Peru - Liga 1: Clausura', 'Peru - Liga 1: Apertura',
         
        'Copa Libertadores - Group A', 'Copa Libertadores - Group B', 'Copa Libertadores - Group C',
        'Copa Libertadores - Group D', 'Copa Libertadores - Group E', 'Copa Libertadores - Group F',
        'Copa Libertadores - Group G', 'Copa Libertadores - Group H',
         
         
         'Copa Sudamericana - Group A', 'Copa Sudamericana - Group B', 'Copa Sudamericana - Group C',
         'Copa Sudamericana - Group D', 'Copa Sudamericana - Group E', 'Copa Sudamericana - Group F',
         'Copa Sudamericana - Group G', 'Copa Sudamericana - Group H',
         
         'England - Premier League 23/24',
         'England - Premier League',
         'International - Friendlies',
         'Spain - LaLiga Santander', 
         'Italy - Serie A', 
         'USA - MLS', 
         'Brazil - Serie A', 
         'Argentina - Primera Division',
         'France - Ligue 1', 
         'Germany - Bundesliga', 
         'Ecuador - Serie A', 
         'France - Ligue 1',
         
         'UEFA Nations League - Final', 'UEFA Nations League - 3rd Place Play-off', 'UEFA Nations League - Semi-finals',
         'UEFA Nations League - League A: Group 1',
         'UEFA Nations League - League A: Group 2', 'UEFA Nations League - League A: Group 3', 'UEFA Nations League - League A: Group 4',
         'UEFA Nations League - League B: Group 1',
         'UEFA Nations League - League B: Group 2', 'UEFA Nations League - League B: Group 3', 'UEFA Nations League - League B: Group 4',
         'UEFA Nations League - League C: Group 1',
         'UEFA Nations League - League C: Group 2', 'UEFA Nations League - League C: Group 3', 'UEFA Nations League - League C: Group 4',
         'UEFA Nations League - League D: Group 1',
         'UEFA Nations League - League D: Group 2'
         
         ###########BASKET###################
         'NBA - Play-offs',
         'NBA - Regular season', 
         'NBA - Pre-season', 
         'NBA - All-Star Game',
         'NBA - California Classic Summer League', 
         'NBA - Salt Lake City Summer League', 
         'NBA - Las Vegas Summer League: Play-off', 
         'NBA - Las Vegas Summer League', 
         'NBA - Las Vegas Summer League: Placement Matches', 
         
         'EuroLeague - EuroLeague: Play-off',
         'EuroLeague - EuroLeague: Regular season',
         
         'EuroBasket - Play-off',
         'EuroBasket - Group A',
         'EuroBasket - Group B',
         'EuroBasket - Group C',
         'EuroBasket - Group D',
         'EuroBasket - Qualification: Group A',
         'EuroBasket - Qualification: Group B',
         'EuroBasket - Qualification: Group C',
         'EuroBasket - Qualification: Group D',
         'EuroBasket - Qualification: Group E',
         'EuroBasket - Qualification: Group F',
         'EuroBasket - Qualification: Group G',
         'EuroBasket - Qualification: Group H',
         
         'FIBA Europe Cup - Group A',
         'FIBA Europe Cup - Group B',
         'FIBA Europe Cup - Group C',
         'FIBA Europe Cup - Group D',
         'FIBA Europe Cup - Group E',
         'FIBA Europe Cup - Group F',
         'FIBA Europe Cup - Group G',
         'FIBA Europe Cup - Group H',
         'FIBA Europe Cup - Group I',
         'FIBA Europe Cup - Group J',
         'FIBA Europe Cup - Group J',
         'FIBA Europe Cup - Group K',
         'FIBA Europe Cup - Group L',
         
         'WNBA - WNBA: Regular season',
         'WNBA - WNBA: Play-off',
         'WNBA - All-Star Game',
         
         ###############TENNIS######################
         "Wimbledon - Men's Singles",
         "Wimbledon - Women's Singles",
         "Wimbledon - Men's Doubles",
         "Wimbledon - Women's Doubles",
         "Wimbledon - Mixed Doubles",
         "Wimbledon - Men's Qualification",
         "Wimbledon - Women's Qualification",
         
         "French Open - Men's Singles",
         "French Open - Women's Singles",
         "French Open - Men's Doubles",
         "French Open - Women's Doubles",
         "French Open - Mixed Doubles",
         "French Open - Men's Qualification",
         "French Open - Women's Qualification",
         
         "US Open - Men's Singles",
         "US Open - Women's Singles",
         "US Open - Men's Doubles",
         "US Open - Women's Doubles",
         "US Open - Mixed Doubles",
         "US Open - Men's Qualification",
         "US Open - Women's Qualification",
         
         "Australian Open - Men's Singles",
         "Australian Open - Women's Singles",
         "Australian Open - Men's Doubles",
         "Australian Open - Women's Doubles",
         "Australian Open - Mixed Doubles",
         "Australian Open - Men's Qualification",
         "Australian Open - Women's Qualification",
         
         "ATP 1000 - Canadian Open (Montreal): Qualification",
         "ATP 1000 - Canadian Open (Montreal)",
         "ATP 1000 - Cincinnati",
         "ATP 1000 - Cincinnati: Qualification",
         "ATP 1000 - Canadian Open (Montreal)",
         "ATP 1000 - Indian Wells Masters",
         "ATP 1000 - Indian Wells Masters: Qualification",
         "ATP 1000 - Internazionali BNL d'Italia (Rome)",
         "ATP 1000 - Internazionali BNL d'Italia (Rome): Qualification",
         "ATP 1000 - Madrid Open",
         "ATP 1000 - Madrid Open: Qualification",
         "ATP 1000 - Miami Open",
         "ATP 1000 - Miami Open: Qualification",
         "ATP 1000 - Monte-Carlo Masters",
         "ATP 1000 - Monte-Carlo Masters: Qualification",
         "ATP 1000 - Paris Masters",
         "ATP 1000 - Paris Masters: Qualification",
         
         "ATP 500 - Astana Open: Qualification",
         "ATP 500 - Astana Open",
         "ATP 500 - Barcelona Open",
         "ATP 500 - Barcelona Open: Qualification",
         "ATP 500 - Dubai Tennis Championships",
         "ATP 500 - Dubai Tennis Championships: Qualification",
         "ATP 500 - Erste Bank Open (Vienna)",
         "ATP 500 - Erste Bank Open (Vienna): Qualification",
         "ATP 500 - Halle Open",
         "ATP 500 - Halle Open: Qualification",
         "ATP 500 - Hamburg European Open",
         "ATP 500 - Hamburg European Open: Qualification",
         "ATP 500 - Japan Open (Tokyo)",
         "ATP 500 - Japan Open (Tokyo): Qualification",
         "ATP 500 - Mexican Open (Acapulco)",
         "ATP 500 - Mexican Open (Acapulco): Qualification",
         "ATP 500 - Queen's Club Championships (London)",
         "ATP 500 - Queen's Club Championships (London): Qualification",
         "ATP 500 - Rio Open",
         "ATP 500 - Rio Open: Qualification",
         "ATP 500 - Rotterdam Open",
         "ATP 500 - Rotterdam Open: Qualification",
         "ATP 500 - Swiss Indoors Basel",
         "ATP 500 - Swiss Indoors Basel: Qualification",
         "ATP 500 - Washington Open",
         "ATP 500 - Washington Open: Qualification",
         "ATP 500 - Washington Open: Doubles Qualification",
         
         "Billie Jean King Cup - Group 1: Europe/Africa: Pool A",
         "Billie Jean King Cup - Group 1: Europe/Africa: Pool B",
         "Billie Jean King Cup - Group 1: Americas",
         "Billie Jean King Cup - Group 1: Asia/Oceania",
         "Billie Jean King Cup - Group 1: Europe/Africa: Play-off 1-2",
         "Billie Jean King Cup - Group 1: Europe/Africa: Play-off 5-6",
         "Billie Jean King Cup - Group 1: Europe/Africa: Playoff 7-8",

         "Billie Jean King Cup - Qualification",  
         
         "Davis Cup - World Group 2: Play-offs",  
         "Davis Cup - World Group 1: Play-offs",
         "Davis Cup - Qualification"
        ]
sports= ['soccer', 'basketball', 'tennis']
def normalize_liga(s):

    s = replacements.get(s, s)
    
    return s

fecha_muestra = datetime.now()
cad_fecha_muestra = fecha_muestra.strftime('%Y-%m-%d')

lista = []

inicio = date.today()
cad_inicio = inicio.strftime('%Y-%m-%d')

for sport in sports:
    for day in range(0,90):
        fecha_iter = inicio + timedelta(day)
        fecha_str = fecha_iter.strftime("%Y%m%d")
        url = "https://prod-public-api.livescore.com/v1/api/app/date/{sport}/{fecha_str}/-5?MD=1".format(sport= sport,fecha_str = str(fecha_str))
        response = urlopen(url)
        print("LINK INICIAL:",url)
        data_json = json.loads(response.read())
        print(response.status)
        for i in range(len(data_json['Stages'])):
            try:
                country = data_json['Stages'][i]['Cnm']
            except:
                country = ""
                pass 
            try:
                league = data_json['Stages'][i]['Snm']
            except:
                league = ""
                pass  
            league2 = country + ' - ' + league
            #print(league2)
            if (league2 in ligas):
                #print('\n')
                #print(league)
                #print(league2)
                for j in range(len(data_json['Stages'][i]['Events'])):
                    home_team = data_json['Stages'][i]['Events'][j]['T1'][0]['Nm']
                    #print(home_team)
                    away_team = data_json['Stages'][i]['Events'][j]['T2'][0]['Nm']
                    #print(away_team)
                    fecha = data_json['Stages'][i]['Events'][j]['Esd']
                    fecha_hora_evento = (str(fecha)[:4]+"-"+str(fecha)[4:6]+"-"+str(fecha)[6:8]+
                                         'T'+str(fecha)[8:10]+':'+str(fecha)[10:12]+':'+str(fecha)[12:14]+'Z')
                    lista.append({
                        "deporte": str.capitalize(sport),
                        "liga": league2,
                        "local": home_team,
                        "visita": away_team,
                        "fecha_evento": fecha_hora_evento[:10],
                        "hora_evento": fecha_hora_evento[11:-4],
                        "fecha_hora_evento":str(fecha_hora_evento),
                        "fecha_hora_muestra":str(fecha_muestra)
                    })
    

df = pd.DataFrame(lista)

#df = df.drop(df[((df['liga']=='International - ')&(df['equipo_local']!='Peru')&(df['equipo_visitante']!='Peru'))].index)

#df = df.reset_index(drop = True)

if len(df)>1:
    df['liga']=df['liga'].apply(normalize_liga)
    df['cod_semana']=df['fecha_evento'].apply(lambda x: str(datetime.strptime(x, '%Y-%m-%d').isocalendar()[0]) + '-'+str(datetime.strptime(x, '%Y-%m-%d').isocalendar()[1]))
    df["cod_semana"]=df["cod_semana"].values.astype('str')
    df['cod_mes']=df['fecha_evento'].apply(lambda x: int(str(x[:4])  + str(x[5:7])))
    print(np.unique(df['liga']))
    print(df.tail(10))

    df.to_csv('/home/stageecuador/indice_de_contenido.csv', encoding='utf-8', sep='\t', index=False)


    sys.path.append('/home/stageecuador/credenciales/')
    print("prueba credenciales")
    import no_credenciales as func_credenciales

    sink_jdbcHostname = func_credenciales.sink_jdbcHostname
    sink_user = func_credenciales.sink_user
    sink_password = func_credenciales.sink_password



    cnx_mysql = mysql.connector.connect(user=sink_user, password=sink_password,host=sink_jdbcHostname,auth_plugin='mysql_native_password', allow_local_infile=True)

    cursor_mysql = cnx_mysql.cursor()  

    print('Se Trunca la Tabla Stage')
    cursor_mysql.execute('''
    TRUNCATE TABLE COMERCIAL.stage_indice_contenido;
    ''')
    cnx_mysql.commit()
    print('Se Carga la data en la Tabla Stage')
    sql_command = ('''
    LOAD DATA LOCAL INFILE '/home/stageecuador/indice_de_contenido.csv'
    IGNORE INTO TABLE COMERCIAL.stage_indice_contenido
    FIELDS TERMINATED BY '\t'
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (deporte, liga, equipo_local, equipo_visitante, fecha_evento, hora_evento, fecha_hora_evento, fecha_hora_muestra, cod_semana, cod_mes)
    ''')
    cursor_mysql.execute(sql_command)
    cnx_mysql.commit()

    print('Se Eliminan los eventos con fecha_evento>= inicio de la tabla principal')
    sql_command = '''
        DELETE FROM COMERCIAL.indice_contenido
        WHERE fecha_evento >= "{inicio}"
        AND fecha_evento<="{fin}";
        '''.format(inicio = str(cad_inicio), fin = str(fecha_iter.strftime('%Y-%m-%d')))
    print(sql_command)
    cursor_mysql.execute(sql_command)
    cnx_mysql.commit()

    print('Se Cargan los Eventos de hoy mas 30 dias en el futuro')
    cursor_mysql.execute('''
    INSERT IGNORE INTO COMERCIAL.indice_contenido(
                    deporte, liga, equipo_local, equipo_visitante, fecha_evento, hora_evento, fecha_hora_evento, fecha_hora_muestra, cod_semana, cod_mes
                    )
    SELECT 
        deporte, liga, equipo_local, equipo_visitante, fecha_evento, hora_evento, fecha_hora_evento, fecha_hora_muestra, cod_semana, cod_mes
    FROM COMERCIAL.stage_indice_contenido;
    ''')

    cnx_mysql.commit()
    cursor_mysql.close()
    cnx_mysql.close()