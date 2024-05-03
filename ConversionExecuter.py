import threading
import pickle
import time, sys, os
import logging
import pathlib

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
    
    def arresta(self):
        self.termina.set()

    def run(self):

        def ottieniNomeFile(filePath):
            nomeCompleto = os.path.basename(filePath)
            nomeSenzaEstensione, _ = os.path.splitext(nomeCompleto)
            return nomeSenzaEstensione
        
        # Memorizza un oggetto python su file
        def salva(object, nomeFile:str):
            with open(f"{nomeFile}", "wb") as file:
                pickle.dump(object, file)

        ''' Legge un oggetto salvato come file  '''
        def apri(nomeFile:str):
            with open(f"{nomeFile}", "rb") as file:
                return pickle.load(file)

        '''
        Genera un oggetto set con i nomi dei file presenti nella cartella specificata e con l'estensione richiesta (se indicata).
        '''
        def elenca_file_di_rete(percorso_cartella_rete, estensione:str=None) -> set:
            # Componi il percorso completo alla cartella di rete utilizzando la libreria pathlib
            percorso_cartella = pathlib.Path(percorso_cartella_rete)
            
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
            
        '''
        Metodo che restituisce l'elenco dei file presenti in una cartella insieme alla loro data di creazione.
        '''
        @staticmethod
        def elencoFile(folderPath:pathlib.Path, extension:str=None) -> set:
            if not folderPath.is_dir():
                raise NotADirectoryError(f"Il percorso {folderPath} non è una cartella")
            
            elenco_file = set()
            if extension:  # se e' stato specificata l'estensione
                for file in folderPath.glob('*'+extension):
                    if file.is_file:
                        elenco_file.add((file.name, file.stat().st_mtime))
            else:  # estensione non specificata
                for file in folderPath.iterdir():
                    if file.is_file:
                        elenco_file.add((file.name, file.stat().st_mtime))
            return elenco_file
        
        @staticmethod
        def nuoviFile_cartella(inputFolder:pathlib.Path, extension:str, vecchiFile:set) -> set:
            elencoFile_attuale:set = elencoFile(inputFolder, extension)
            nuoviFile:set = elencoFile_attuale - vecchiFile

            # seleziona il file più recente
            ultimoFile = max(elencoFile_attuale, key=lambda x:x[1])
            # aggiungo sempre il file con la data di modifica più recente
            nuoviFile.add(ultimoFile)
            return nuoviFile

        ''' Data una lista, restituisce una lista con i valori unici presenti '''
        def _unique_values(l:list) -> list:
            val_unici = set(l)
            return list(val_unici)

        ''' Cancella i numeri di riga specificati dal testo '''
        def cancella_righe(testo:str, rimuovi:list) -> str:
            # Se la lista ha lunghezza 0 termina subito
            if len(rimuovi) == 0: return testo
            
            righe = testo.splitlines()
            rimuovi = _unique_values(rimuovi)
            if len(righe) - len(rimuovi) < 1:
                raise ValueError("Troppe poche righe")
           
            for i, numero_riga in enumerate(rimuovi):
                if numero_riga < 0:  # Se l'indice e' negativo, convertilo in un indice positivo
                    numero_riga = len(righe) + numero_riga  # Calcola l'indice positivo corrispondente
                    rimuovi[i] = numero_riga  # sovrascrive il valore della riga

            for numero_riga in sorted(rimuovi, reverse=True):
                if 0 <= numero_riga < len(testo):
                    del righe[numero_riga]
            
            return '\n'.join(righe)

        self.logger.info("Convertitore avviato")
        while not self.termina.is_set():
            # Carica l'elenco dei file già convertiti.
            # Se presente apre il file con il nome richiesto, in alternativa crea un set vuoto
            try:
                elencoFile_precedente = apri(f"{self.endpoints['name']}")
            except FileNotFoundError:
                elencoFile_precedente = set()

            # elencoFile_precedente = set()

            nuoviFile:set = nuoviFile_cartella(
                pathlib.Path(self.endpoints['input']),
                self.endpoints['extension'],
                elencoFile_precedente
            )

            # Se elencoFile_attuale non è stato assegnato significa che la cartella non è raggiungibile
            if nuoviFile is None:
                time.sleep(WAIT_TIME)
                continue
            
            logString = list()
            if nuoviFile:
                for file in nuoviFile:
                    in_filePath = pathlib.Path(self.endpoints['input']) / file[0]
                    # Leggi il contenuto del file CSV di input
                    with in_filePath.open(mode='r') as file_input:
                        contenuto = file_input.read()

                    if 'header' in self.endpoints:
                        contenuto = self.endpoints['header'] + "\n" + contenuto
                    if 'delete_rows' in self.endpoints:
                        try:
                            contenuto = cancella_righe(contenuto, self.endpoints['delete_rows'])
                        except ValueError as ve:
                            continue

                    out_fileName = ottieniNomeFile(in_filePath) + ".csv"
                    out_filePath = pathlib.Path(self.endpoints['output'], out_fileName)
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

            elencoFile_precedente = nuoviFile
            salva(nuoviFile, f"{self.endpoints['name']}")
        
            time.sleep(WAIT_TIME)

        print(threading.current_thread().name, " terminato")