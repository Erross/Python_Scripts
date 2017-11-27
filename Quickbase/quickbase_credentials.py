#Script to get credentials from quickbase database with userid and password

import requests
from xml.etree import ElementTree as ET

def get_creds():
    r = requests.get('https://'+resourcename+'.quickbase.com/db/main?a=API_Authenticate&username='+username+'&password='+password+'Mw280381&hours=24')

    tree = ET.fromstring(r.content)

    for child in tree.findall('ticket'):
         Ticket = child.text
    return Ticket

print(get_creds())