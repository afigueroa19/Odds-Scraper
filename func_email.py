from datetime import date, datetime, timedelta
import sys
import os
from os.path import join, dirname
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import shutil
from os.path import join, dirname
from dotenv import load_dotenv
from urllib.parse import quote
import time
from os.path import basename
from email.mime.application import MIMEApplication

def enviar_email(mail_to,mail_subject,mail_body,archivo="NINGUNO"):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    username = "onepage@betsoffice.com"
    password = "NOPE."
    mail_from = "onepage@betsoffice.com"

    mimemsg = MIMEMultipart()
    mimemsg['From']=mail_from
    mimemsg['To']=mail_to
    mimemsg['Subject']=mail_subject
    mimemsg.attach(MIMEText(mail_body, 'plain'))
    
    if archivo!="NINGUNO":
        with open(archivo, "rb") as fil:
            part = MIMEApplication(fil.read(),Name=basename(archivo))
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(archivo)
            mimemsg.attach(part)

    connection = smtplib.SMTP(host='smtp.office365.com', port=587)
    connection.starttls()
    connection.login(username,password)
    connection.send_message(mimemsg)
    connection.quit()