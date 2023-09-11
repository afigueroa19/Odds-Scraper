import requests
from datetime import date, datetime, timedelta
from azure.cosmos import CosmosClient
import uuid
import pandas as pd
import re
from slugify import slugify
import numpy as np
import re
import sys


def normalize(s):
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"), 
        ("ü", "u"),
        ("Á", "A"),
        ("É", "E"),
        ("Í", "I"),
        ("Ó", "O"),
        ("Ú", "U"),
        ("à", "a")   
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.lower(), b.lower())
        s = re.sub(' FC| AC| AFC| BSC| BK| CA| CD| CF| CP| CR| CS| CSD| FK| IA| PR|-PR| RCD| RC|-RJ| RJ| SA| SCO| SC|-SP| SP| SV| Balompie| del| de| SS| BC|Asociacion Deportiva |Deportivo|KV$|CC |Dep. |EM |FBC |Carlos A. |Carlos |Universidad', '', s)
        s = re.sub('^FC|^AC|^AFC|^BSC|^BK|^CA|^CD|^CF|^CP|^CR|^CS|^CSD|^FK|^IA|^PR|^RCD|^RC|^RJ|^SA|^SCO|^SC|^SP|^SV|^UD|^ | $|SSC| Association|Club Deportivo |ADT |Club |^UC |^UD|^SL|AJ | HSC|Olympique |LOSC | OSC|ES |OGC |AD ', '', s)
        s = re.sub('1. |Union |AS | 04| TC| SK|VfL |VfB |FSV |RB | 05|US |Sportiva |Republica |^SD |SD ', '', s)
        s = re.sub(' UC |  | UD | SL ', ' ', s)
        
        ###UEFA CHAMPIONS - EUROPA LEAGUE###
        s = re.sub('Saint[ -]Germain', 'SG', s)
        s = re.sub(' Hotspur', '', s)
        s = re.sub(' Munchen', ' Munich', s)
        s = re.sub(' Espanyol Barcelona', ' Espanyol', s)
        s = re.sub('Real Valladolid', 'Valladolid', s)
        s = re.sub('Bolonia', 'Bologna', s)
        s = re.sub('Lazio Roma|SS Lazio', 'Lazio', s)
        s = re.sub('Spezia Calcio', 'Spezia', s)
        s = re.sub('Inter Milano', 'Inter Milan', s)
        s = re.sub('Udinese Calcio', 'Udinese', s)
        s = re.sub('Inter Milan|Internazionale', 'Inter', s)
        s = re.sub('Napoli', 'Napoles', s)
        s = re.sub('Ferencvarosi', 'Ferencvaros', s)
        s = re.sub('Saint-|Saint ', ' St ', s)
        s = re.sub('Juventus Turin', ' Juventus', s)
        s = re.sub('Utd', 'United', s)
        s = re.sub('Friburgo', 'Freiburg', s)
        s = re.sub('Real Betis', 'Betis', s)
        s = re.sub('Fenerbahçe', 'Fenerbahce', s)
        s = re.sub('Shajtar Donetsk|Shakhtar Donetsk', 'Shaktar Donetsk', s)
        s = re.sub('Feyenoord Rotterdam', 'Feyenoord', s)
        s = re.sub('Sporting Lisboa', 'Sporting', s)
        s = re.sub('Augsburgo', 'Augsburg', s)
        s = re.sub('Colonia|Köln|Cologne U21', 'Cologne', s)
        s = re.sub('Hertha Berlin', 'Hertha', s)
        s = re.sub('Mönchengladbach|M´gladbach', 'Monchengladbach', s)
        s = re.sub('^F |^C ' , '', s)
        s = re.sub('-F |-C ' , '- ', s)
        s = re.sub('Ajax Amsterdam' , 'Ajax', s)
        s = re.sub('Brugge' , 'Brujas', s)
        s = re.sub('PSV Eindhoven' , 'PSV', s) 
        s = re.sub('Red Bull Salzburg' , 'Salzburg', s) 
        
        ###La Liga###
        s = re.sub('Athletic Bilbao|Athletic Club' , 'Athletic', s)
        s = re.sub('Real Mallorca' , 'Mallorca', s)
        
        ### Italia / Serie A ###
        s = re.sub('Atalanta Bergamasca Calcio' , 'Atalanta', s)
        s = re.sub('Hellas Verona' , 'Verona', s)
        s = re.sub('Sassuolo Calcio' , 'Sassuolo', s)
        s = re.sub('Salernitana 1919|Salernitana 19' , 'Salernitana', s)
        
        
        ### Perú / Liga 1 ###
        s = re.sub('Academia Deportiva Cantolao|Academia Cantolao' , 'Cantolao', s)  
        s = re.sub('Tecnica Cajamarca|UTC De Cajamarca' , 'UTC Cajamarca', s)
        s = re.sub('- UTC$' , '- UTC Cajamarca', s) 
        s = re.sub('^UTC -' , 'UTC Cajamarca -', s)
        s = re.sub('Mannucci' , 'Manucci', s) 
        s = re.sub('Ciencianol' , 'Cienciano', s) 
        s = re.sub('Cienciano Cuzco|Sportivo Cienciano|Cienciano Cusco' , 'Cienciano', s)
        s = re.sub('Escuela Municipal Binacional|Em Binacional' , 'Binacional', s)
        s = re.sub('Universitario Deportes' , 'Universitario', s)
        
        ### Inglaterra / Premier League
        s = re.sub('Newcastle United' , 'Newcastle', s)
        s = re.sub('Leeds United' , 'Leeds', s)
        s = re.sub('Leicester City' , 'Leicester', s)
        s = re.sub('Wolverhampton Wanderers|Wolves' , 'Wolverhampton', s)
        s = re.sub('Brighton & Hove Albion' , 'Brighton', s)
        s = re.sub('West Ham United' , 'West Ham', s)
        s = re.sub('1899 Hoffenheim|TSG Hoffenheim' , 'Hoffenheim', s)
        ### Alemania / Bundesliga ####
        s = re.sub('Wolfsburgo' , 'Wolfsburg', s)
        s = re.sub('Hamburgo|Hamburger' , 'Hamburg', s)
        
        
        
        ###Amistosos Internacionales###
        s = re.sub('Arabia Saudi$|Saudi Arabia All Stars|^Arabia Saudi ' , 'Arabia Saudita ', s)
        s = re.sub('Emiratos Arabes Unidos' , 'Emiratos Arabes', s)
        s = re.sub('Tayikistan' , 'Tajikistan', s)
        s = re.sub('Hong Kong, China' , 'Hong Kong', s)
        s = re.sub('Trinidad y Tobago' , 'Trinidad & Tobago', s)
        s = re.sub('Malaysia' , 'Malasia', s)
        s = re.sub('Banglades ' , 'Bangladesh ', s)
        s = re.sub('Myanmar' , 'Birmania', s)
        s = re.sub('Syria' , 'Siria', s)
        s = re.sub('Thailand' , 'Tailandia', s)
        s = re.sub('Bahrain|Bahrein' , 'Barein', s)
        s = re.sub('Palestine' , 'Palestina', s)
        s = re.sub('Curaçao|Curacao' , 'Curasao', s)
        s = re.sub('Kyrgyzstan' , 'Kirguistan', s)
        s = re.sub('Philippines' , 'Filipinas', s)
        s = re.sub('Macedonia Norte|North Macedonia' , 'Macedonia N.', s)
        s = re.sub('Macao' , 'Macau', s)
        s = re.sub('Singapore' , 'Singapur', s)
        s = re.sub('Faroe Islands' , 'Islas Feroe', s)
        s = re.sub('Republica Irlanda' , 'Irlanda', s)
        s = re.sub('EE.\xa0UU.|EE. UU.|USA|EE UU|EEUU' , 'Estados Unidos', s)
        s = re.sub('Chinese Taipei|China Taipei' , 'Taiwan', s)
        s = re.sub('Curazao' , 'Curasao', s)
        s = re.sub('Papua New Guinea' , 'Papua Nueva Guinea', s)
        s = re.sub('Egypt' , 'Egipto', s)
        s = re.sub('South Sudan' , 'Sudan Sur', s)
        s = re.sub('Solomon Islands' , 'Islas Salomon', s)
        s = re.sub('Qatar' , 'Catar', s)
        s = re.sub('Japan' , 'Japon', s)
        s = re.sub('Cape Verde' , 'Cabo Verde', s)
        s = re.sub('Djibouti' , 'Yibuti', s)
        s = re.sub('Camboya' , 'Cambodia', s)
        s = re.sub('Banglades$' , 'Bangladesh', s)
        s = re.sub('San Kitts y Nevis' , 'San Cristobal y Nieves', s) 
        s = re.sub("Cameroon" , 'Camerun', s)
        s = re.sub("Trinidad and Tobago" , 'Trinidad & Tobago', s)
        s = re.sub("Mauritius" , 'Mauricio', s)
        s = re.sub("Kenya" , 'Kenia', s)
        s = re.sub('Democratica Congo|Congo DR|DR Congo|Congo Democratico' , 'Democratica Congo', s)
        s = re.sub("Morocco" , 'Marruecos', s)
        s = re.sub("Republic of Korea" , 'Corea Sur', s)
        s = re.sub("Dominican Republic" , 'Dominicana', s)
        s = re.sub("Poland" , 'Polonia', s)
        s = re.sub("Iraq" , 'Irak', s)
        s = re.sub("Jordania" , 'Jordan', s)
        s = re.sub("Argelia" , 'Algeria', s)
        s = re.sub("Tunisia" , 'Tunez', s)

        
        ### Francia / Ligue 1 ###
        s = re.sub('Saint-Gilloise' , 'Saint Gilloise', s)
        s = re.sub('Stade Rennais|Stade Rennes' , 'Rennes', s)
        s = re.sub('Stade ' , '', s)
        s = re.sub('Lyonnais' , 'Lyon', s)
        s = re.sub('Clermont Foot' , 'Clermont', s)
        s = re.sub('Brestois 29' , 'Brest', s)
        s = re.sub('Marsella' , 'Marseille', s)
        s = re.sub('Nice' , 'Niza', s)
        s = re.sub('Strasbourg Alsace|Estrasburgo' , 'Strasbourg', s)
        
        
        ### Copa del Rey###
        s = re.sub('Real Sporting Gijon' , 'Sporting Gijon', s)
        s = re.sub('Real Oviedo' , 'Oviedo', s)
        s = re.sub('Gimnastic de Tarragona|Gimnastic Tarragona' , 'Gimnastic', s)
        
        ### Copa Libertadores###
        s = re.sub('Cerro Porteno' , 'Cerro Porteño', s)
        s = re.sub('Nacional Asuncion (PAR)' , 'Nacional Asuncion', s)
        s = re.sub('Atletico Mineiro MG|Atletico - MG|Atletico Mineiro - MG|Clube Atletico MG' , 'Atletico Mineiro', s)
        s = re.sub('El Nacional (ECU)' , 'El Nacional', s)
        s = re.sub('Atletico Huracan' , 'Huracan', s)
        s = re.sub('Deportes Magallanes' , 'Magallanes', s)
        s = re.sub('Fortaleza - CE|Fortaleza EC|Fortaleza CE' , 'Fortaleza', s)
        s = re.sub('Catolica Ecuador|Catolica (ECU)|U. Catolica' , 'Catolica', s)
        s = re.sub('Athletico Paranaense|Athletico Paranaense|Athletico' , 'Atletico Paranaense', s)
        s = re.sub('Argentinos Juniors' , 'Argentinos Jrs', s)
        s = re.sub('D Independiente Valle' , 'Independiente Valle', s)
        s = re.sub('Colo - Colo' , 'Colo Colo', s)
        s = re.sub('Nacional Football|Nacional Montevideo' , 'Nacional', s)
        s = re.sub('Internacional P. A.|Internacional RS' , 'Internacional', s)
        s = re.sub('Patronato Parana' , 'Patronato', s)
        s = re.sub('Atletico Nacional S.A|Atletico Nacional Medellin|Atletico Nacional' , 'Atletico Medellin', s)
        s = re.sub('Barcelona Guayaquil' , 'Barcelona', s)
        s = re.sub('Nublense' , 'Ñublense ', s)
        s = re.sub('Deportes Pereira' , 'Pereira', s)
        s = re.sub('Racing Avellaneda' , 'Racing Club', s)
        s = re.sub('Libertad Asuncion' , 'Libertad', s)
        s = re.sub('Liverpool Montevideo' , 'Liverpool', s)
        s = re.sub('Olimpia Asuncion' , 'Olimpia', s)
        s = re.sub('Bolivar La Paz' , 'Bolivar', s)
        
        ###Copa Sudamericana
        s = re.sub('A. Italiano' , 'Audax Italiano', s)
        s = re.sub('Peñarol Montevideo' , 'Peñarol', s)
        s = re.sub('General Caballero Jlm|General Caballero JLM' , 'General Caballero', s)
        s = re.sub('Tacuary Asuncion' , 'Tacuary', s)
        s = re.sub('Liga Quito|L.D.U. Quito' , 'LDU Quito', s)
        s = re.sub('River Plate Montevideo|River Plate - URU' , 'River Plate', s)
        s = re.sub('Guarani Asuncion|Clube Guarani' , 'Guarani', s)
        s = re.sub('Sportivo Ameliano' , 'Ameliano', s)
        s = re.sub('Guabira Montero' , 'Guabira', s)
        s = re.sub('Rionegro Aguilas Doradas|Aguilas Doradas' , 'Rionegro Aguilas', s)
        s = re.sub('Independiente Santa Fe' , 'Santa Fe', s)
        s = re.sub('^Junior -|^Junior Barranquilla -|^Atletico Junior -' , 'Atletico Junior Barranquilla -', s)
        s = re.sub('- Junior$|- Junior Barranquilla$|- Atletico Junior$' , '- Atletico Junior Barranquilla', s)
        s = re.sub('Blooming Santa Cruz' , 'Blooming', s)
        s = re.sub('Atletico Palmaflor Vinto|Atletico Palmaflor' , 'Palmaflor', s)
        s = re.sub('San Lorenzo Almagro' , 'San Lorenzo', s)
        s = re.sub('Goias GO|Goias EC|Goias - GO' , 'Goias', s)
        s = re.sub("Newell´s Old Boys|Newell's Old Boys|Newells Old Boys" , "Newell's", s)
        s = re.sub("Botafogo FR|Botafogo - FR" , 'Botafogo', s)
        s = re.sub("Gimnasia La Plata|Gimnasia y Esgrima La Plata|Gimnasia Y Esgrima La Plata" , 'Gimnasia', s)
        s = re.sub("Estudiantes LP" , 'Estudiantes La Plata', s)
        s = re.sub("America - MG|America MG" , 'America Mineiro', s)
        s = re.sub("Atletico Tigre" , 'Tigre', s)
        s = re.sub("São Paulo" , 'Sao Paulo', s)
        s = re.sub("Red Bull Bragantino" , 'Bragantino', s)
        
        s = re.sub("\((.*?)\)", "",s)
        s = re.sub('-$', '', s)
        s = re.sub('-' , ' - ', s)
        s = re.sub('  ', ' ', s)
        s = re.sub('^ ', '', s)
        s = re.sub(' $', '', s)
    return s

    
def normalize_liga(s):

    if (s=='España / LaLiga') or (s=='La Liga') or (s=='La Liga, España') or (s == 'España - La Liga')or (s == 'Fútbol / LaLiga'):
        s='España / La Liga'
        
    elif s=='España / La Liga':
        s='España / La Liga'
        
    elif (s=='Inglaterra / Premier League') or (s=='Inglaterra Premier League (EPL)')or (s=='Premier League, Inglaterra'):
        s='Inglaterra / Premier League'

    elif (s=='Alemania Bundesliga') or (s=='Bundesliga, Alemania')or (s=='Alemania / Bundesliga') :
        s='Alemania / Bundesliga'
       
    elif (s=='Amistosos Internacionales') or (s=='Amistosos Internacionales, Mundo')or (s=='Fútbol / Internacional') or (s=='Internacional / Amistosos') or (s=='Internacional / Amistosos Internacionales')or (s=='Mundo / Amistosos Internacionales'):
        s='Internacional / Amistosos'
        
    elif (s=='Francia / Ligue 1') or (s=='Francia Ligue 1') or (s=='Ligue 1, Francia'):
        s='Francia / Ligue 1'

    elif (s=='Liga 1 / Liga 1') or (s=='Liga 1 Betsson')or (s=='Peru / Primera Division') or (s=='Peru Liga 1 - Apertura') or (s=='Perú / Liga 1')or (s=='Primera División, Perú') or (s=='Primera División, Perú') or (s=='Perú / Primera División'):
        s='Perú / Liga 1'
        
    elif (s=='Italia - Serie A') or (s=='Italia / Serie A')or (s=='Serie A, Italia'):
        s='Italia / Serie A'

    elif (s=='Champions League') or (s=='Europa / UEFA Champions League')or (s=='Fútbol / Liga de Campeones') or (s=='Internacional de Clubs / UEFA Champions League') or (s=='Liga de Campeones')or (s=='UEFA Champions League, Europa') or (s=='Fútbol / Champions League') or (s=='Clasificación de la Champions League'):
        s='Europa / UEFA Champions League'

    elif (s=='Europa / UEFA Europa League') or (s=='Europa League')or (s=='Fútbol / Liga de Europa') or (s=='Internacional de Clubs / UEFA Europa League') or (s=='Liga de Europa / Partidos')or (s=='UEFA Europa League, Europa') or (s=='Fútbol / Europa League')  or (s=='Clasificación de la Europa League'):
        s='Europa / UEFA Europa League'
        
    elif (s=='Copa del Rey') or (s=='Copa del Rey, España')or (s=='España / Copa del Rey') or (s=='España / Copa Del Rey'):
        s='España / Copa del Rey'

    elif (s=='Americas / Copa Libertadores') or (s=='América / Copa Libertadores')or (s=='Copa Libertadores')or (s=='Copa Libertadores / Clasificación')or (s=='Copa Libertadores / Copa Libertadores')or (s=='Copa Libertadores, Americas')or (s=='Fútbol / Copa Libertadores')or (s=='Internacional de Clubs / Copa Libertadores'):
        s='América / Copa Libertadores'

    elif (s=='Americas / Copa Sudamericana') or (s=='América / Copa Sudamericana')or (s=='Copa Sudamericana')or (s=='Copa Sudamericana / Clasificación')or (s=='Copa Sudamericana / Copa Sudamericana')or (s=='Copa Sudamericana, Americas')or (s=='Fútbol / Copa Sudamericana')or (s=='Internacional de Clubs / Copa Sudamericana'):
        s='América / Copa Sudamericana'
        
    return s

