import csv
import os
import sys
import logging
import json
from jsonschema import validate
from pathlib import Path
import ConversionExecuter

local_logger:logging.Logger  # Variabile logger

# Schema del file JSON di configurazione
schema = {
        "type":"object",
        "properties":{
            "doc": {"type":"object"},
            "folders":{
                "type":"array",
                "items":{
                    "type":"object",
                    "properties":{
                        "name": {"type": "string"},
                        "input": {"type":"string"},
                        "output": {"type":"string"},
                        "header": {"type":"string"},
                        "extension": {"type":"string"}
                    },
                    "required":["name","input","output","header","extension"]
                }
            }
        },
        "required":["folders"]
    }

def leggiConfigurazione(fileName:str='config.json') -> list:
    filePath = os.path.join(os.getcwd(), fileName)
    with open(filePath, 'r') as file:
        config = json.load(file)
    return config

def validateConfig(config:dict, schema:dict) -> bool:
    try:
        validate(instance=config, schema=schema)
        return True
    except Exception as e:
        local_logger.error("Il file di configurazione non è conforme")
        return False
    

if __name__ == "__main__":
    print(
        "********************************************\n" \
        "**********      CONVERTITORE      **********\n" \
        "********************************************"
    )
    
    logger = ConversionExecuter.ConversionExecuter._startLogger(loggerName="main")
    config = leggiConfigurazione()
    # Verifica validità della configurazione fornita
    if validateConfig(config, schema):
        config = config['folders']
    else:
        sys.exit()

    folders:list = leggiConfigurazione()['folders']
    threads_list:list = list()

    for index, folder in enumerate(folders):
        t = ConversionExecuter.ConversionExecuter(folder)
        t.name = f"Thread-{index}-{folder['name']}"
        t.start()
        threads_list.append(t)
        print(f"Avviato {t.name}")
    '''
    while True:
        stop = input(f"Premi \'q\' per terminare (può richiedere fino a {ConversionExecuter.WAIT_TIME} s)\n> ")
        if stop == "q" or stop == "Q":
            break
    
    for thread in threads_list:
        thread.arresta()
    '''
    for thread in threads_list:
        thread.join() 

    print("Convertitore terminato")
    #sys.exit()