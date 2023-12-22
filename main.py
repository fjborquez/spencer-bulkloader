import os
import time
import json

import fast_xbrl_parser as fxp
import argparse

from bson import json_util
from pymongo import MongoClient
from dotenv import load_dotenv

def download_form(input):
    xbrl_dict = fxp.parse(
        input,
        output=['json'], email="fborquez@outlook.com"
    )

    time.sleep(0.33)

    return xbrl_dict['json']

def extract_code(url):
    splited_url = url.split('/')
    return splited_url[-2]

load_dotenv()

MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_SERVER = os.getenv('MONGO_SERVER')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')
CONNECTION_STRING = "mongodb://" + MONGO_USER + ":" + MONGO_PASSWORD + "@" + MONGO_SERVER + "/" + MONGO_DATABASE + "?retryWrites=true&w=majority"

client = MongoClient(CONNECTION_STRING)
collection_name = client['spencer-api']

parser = argparse.ArgumentParser()
parser.add_argument("--cik", help="The company cik number, without zeros")
parser.add_argument("--inputs", help="Array of input forms url to be downloaded")
parser.add_argument("--type", help="Form type, like 10-K or 4")
args = parser.parse_args()
cik = vars(args)['cik']
inputs = vars(args)['inputs']
type = vars(args)['type']
inputs = inputs.split(",")

empresas_result = collection_name.empresas.find({'cik': cik})
empresa = None

if empresas_result.explain().get("executionStats", {}).get("nReturned") >= 1:
    empresa = json.loads(json_util.dumps(empresas_result[0]))
else:
    exit("No existe empresa con el cik ingresado")

for input in inputs:
    json = download_form(input)
    codigo = extract_code(input)
    exists = collection_name.formularios.find({'codigo': codigo})

    if exists.explain().get("executionStats", {}).get("nReturned") == 0:
        payload = {
            "tipo": type,
            "codigo": codigo,
            "empresa_id": empresa['_id']['$oid'],
            "formulario": json,
        }

        collection_name.formularios.insert_one(payload)
    else:
        print(f"El formulario {codigo} ya existe")