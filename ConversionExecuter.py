import threading
import pickle
import time, sys, os
import logging
from pathlib import Path

# tempo di attesa tra due esecuzioni successive dello script
WAIT_TIME:int = 60 # secondi

class ConversionExecuter(threading.Thread):
    endpoints:dict

    def __init__(self, endPoints:dict):
        super().__init__()
        self.endpoints = endPoints
        self.logger = self._startLogger(threading.current_thread().name)
        self.termina = threading.Event()

    @staticmethod
    def _startLogger(loggerName:str):
        # Configura il logger per il thread corrente
        thread_logger = logging.getLogger(f"{loggerName}-Logger")
        thread_logger.setLevel(logging.INFO)
        
        # Definisci il formato del messaggio di log
        formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
        
        # Aggiungi un gestore per scrivere i messaggi di log su un file
        file_handler = logging.FileHandler("converter.log")
        file_handler.setFormatter(formatter)
        thread_logger.addHandler(file_handler)

        return thread_logger
    
    def cancella_righe(testo:str, rimuovi:list) -> str:
        righe = testo.splitlines()
        # Rimuovi le righe desiderate
        for numero_riga in sorted(rimuovi, reverse=True):
            if numero_riga < 0:  # Se l'indice è negativo, convertilo in un indice positivo
                numero_riga = len(righe) + numero_riga  # Calcola l'indice positivo corrispondente
            del righe[numero_riga]
        
        return ''.join(righe)
    
    def arresta(self):
        self.termina.set()

    def run(self):

        def ottieniNomeFile(filePath):
            nomeCompleto = os.path.basename(filePath)
            nomeSenzaEstensione, _ = os.path.splitext(nomeCompleto)
            return nomeSenzaEstensione
        
        # Memorizza un oggetto python su file
        def salva(object, nomeFile:str):
            with open(f"{nomeFile}.pickle", "wb") as file:
                pickle.dump(object, file)

        ''' Legge un oggetto salvato come file  '''
        def apri(nomeFile:str):
            with open(f"{nomeFile}.pickle", "rb") as file:
                return pickle.load(file)

        '''
        Genera un oggetto set con i nomi dei file presenti nella cartella specificata e con l'estensione richiesta (se indicata).
        '''
        def elenca_file_di_rete(percorso_cartella_rete, estensione:str=None) -> set:
            # Componi il percorso completo alla cartella di rete utilizzando la libreria pathlib
            percorso_cartella = Path(percorso_cartella_rete)
            
            try:
                # Verifica se il percorso e' una directory
                if percorso_cartella.is_dir():
                    # Elenca i file all'interno della cartella di rete
                    if estensione:
                        elenco_file = {file for file in percorso_cartella.iterdir() if file.is_file() and file.suffix == estensione}
                    else:
                        elenco_file = {file for file in percorso_cartella.iterdir() if file.is_file()}
                    return elenco_file
            except FileNotFoundError:
                self.logger.info(f"La cartella di rete {percorso_cartella_rete} non e' stata trovata.")
            except Exception as e:
                self.logger.info(f"Errore generico: {e}")

        self.logger.info("Convertitore avviato")
        while not self.termina.is_set():
            # Carica l'elenco dei file già convertiti.
            # Se presente apre il file con il nome richiesto, in alternativa crea un set vuoto
            try:
                elencoFile_precedente = apri(f"{self.endpoints['name']}")
            except FileNotFoundError:
                elencoFile_precedente = set()

            # elencoFile_precedente = set()

            elencoFile_attuale = elenca_file_di_rete(self.endpoints['input'], self.endpoints['extension'])

            # Se elencoFile_attuale non è stato assegnato significa che la cartella non è raggiungibile
            if elencoFile_attuale is None:
                time.sleep(WAIT_TIME)
                continue

            nuoviFile:set = elencoFile_attuale - elencoFile_precedente
            
            logString = list()

            if nuoviFile:
                for file in nuoviFile:
                    in_filePath = Path(self.endpoints['input']) / file
                    # Leggi il contenuto del file CSV di input
                    with in_filePath.open(mode='r') as file_input:
                        contenuto = file_input.read()

                    contenuto = self.endpoints['header'] + "\n" + contenuto

                    out_fileName = ottieniNomeFile(in_filePath) + ".csv"
                    out_filePath = Path(self.endpoints['output'], out_fileName)
                    # Scrivi il contenuto modificato nel file CSV di output
                    with out_filePath.open(mode='w') as file_output:
                        file_output.write(contenuto)

                    logString.append(out_fileName)
            if len(logString) == 0:
                continue
            elif 1 <= len(logString) <= 20:
                self.logger.info(f"Scritti {len(logString)} file: {logString}")
            else:
                self.logger.info(f"Scritti {len(logString)} file (omesso elenco)")

            elencoFile_precedente = elencoFile_attuale
            salva(elencoFile_attuale, f"{self.endpoints['name']}")
        
            time.sleep(WAIT_TIME)

        print(threading.current_thread().name, " terminato")