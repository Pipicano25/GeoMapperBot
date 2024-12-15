import multiprocessing
from multiprocessing import Pool
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import re
import pandas as pd
from datetime import datetime
import os

# Ruta del archivo Excel
pwd_xlsx = 'C:/Users/anderson.pipicano/Downloads/H3_Pop_200Reg.xlsx'

class GoogleMapsBot:
    def __init__(self, process_id):
        self.process_id = process_id
        self.setup_driver()
        self.url = ''

    def setup_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--log-level=3')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, timeout=2)
        print(f"Bot {self.process_id}: Iniciando navegador...")
        self.driver.get("https://www.google.com/maps")
        time.sleep(2)

    def valida(self, address, url):
        address_parts = address.lower().replace('#', ' ').replace('-', ' ').split()
        address_parts = [part for part in address_parts if part.strip()]
        decoded_url = url.lower()
        url_parts = decoded_url.split('/')
        matches = sum(1 for part in address_parts if any(part in url_word for url_word in url_parts))
        match_percentage = (matches / len(address_parts)) * 100
        return True if match_percentage >= 90 else False

    def search_with_retry(self, search_box, address, ciudad, max_retries=3):
        formatted_address = f"{address}, {ciudad}, Colombia"
        initial_url = self.url
        
        for attempt in range(max_retries):
            try:
                wait_time = 5 + (attempt * 2)
                
                search_box.clear()
                search_box.send_keys(formatted_address)
                search_box.send_keys(Keys.RETURN)
                
                WebDriverWait(self.driver, wait_time).until(
                    lambda driver: (initial_url != driver.current_url) and self.valida(address, driver.current_url)
                )
                return True
                
            except TimeoutException:
                print(f"Bot {self.process_id}: Intento {attempt + 1} fallido para {address}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(1)

    def search_location(self, id_pk, address, ciudad="Popayán"):
        try:
            search_box = self.wait.until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            self.url = self.driver.current_url

            if not self.search_with_retry(search_box, address, ciudad):
                return self.create_error_response(id_pk, address, ciudad, "No se pudo encontrar la dirección después de varios intentos")

            self.url = self.driver.current_url
            print(f"Bot {self.process_id}: {self.url}")

            coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', self.url)

            if coords_match:
                lat, lng = coords_match.groups()
                return {
                    'usuario': id_pk,
                    'direccion': address,
                    'ciudad': ciudad,
                    'latitud': float(lat),
                    'longitud': float(lng),
                    'estado': 'ENCONTRADO',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'bot_id': self.process_id
                }
            else:
                return self.create_error_response(id_pk, address, ciudad, "NO ENCONTRADO")

        except Exception as e:
            return self.create_error_response(id_pk, address, ciudad, str(e))

    def create_error_response(self, id_pk, address, ciudad, error_msg):
        return {
            'usuario': id_pk,
            'direccion': address,
            'ciudad': ciudad,
            'latitud': None,
            'longitud': None,
            'estado': f'ERROR: {error_msg}',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'bot_id': self.process_id
        }

    def process_batch(self, addresses_dict):
        results = []
        print(f"Bot {self.process_id}: Procesando {len(addresses_dict)} direcciones")

        try:
            for id_pk, address in addresses_dict.items():
                print(f"\nBot {self.process_id}: Buscando {address}")
                result = self.search_location(id_pk, address)
                results.append(result)
        finally:
            self.driver.quit()

        return results

def process_worker(args):
    process_id, addresses_dict = args
    bot = GoogleMapsBot(process_id)
    return bot.process_batch(addresses_dict)

def divide_dict(addresses_dict, num_bots, batch_size=30):
    items = list(addresses_dict.items())
    total_items = len(items)
    batches = []
    
    start = 0
    while start < total_items:
        end = start + batch_size
        batch = dict(items[start:end])
        if batch:
            batches.append(batch)
        start = end
        
        if len(batches) >= num_bots:
            break
    
    print(f"Se crearon {len(batches)} lotes de {batch_size} registros cada uno")
    return batches

def process_parallel(direcciones, num_bots=2):
    print(f"Iniciando procesamiento paralelo con {num_bots} bots...")
    
    batches = divide_dict(direcciones, num_bots, batch_size=30)
    actual_num_bots = len(batches)
    
    print(f"Usando {actual_num_bots} bots para procesar {len(direcciones)} direcciones")
    
    process_args = [(i, batch) for i, batch in enumerate(batches)]
    
    with Pool(actual_num_bots) as pool:
        all_results = pool.map(process_worker, process_args)
    
    results = [item for sublist in all_results for item in sublist]
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    df_results = pd.DataFrame(results)
    df_results.to_csv(f'resultados_direcciones_{timestamp}.csv', index=False)
    
    print(f"Procesadas {len(results)} direcciones en total")
    return results

def concatenar_csv():
    directorio_actual = os.getcwd()
    df_lista = []

    for archivo in os.listdir(directorio_actual):
        if archivo.endswith('.csv'):
            ruta_csv = os.path.join(directorio_actual, archivo)
            df = pd.read_csv(ruta_csv)
            df_lista.append(df)

    if df_lista:
        df_completo = pd.concat(df_lista, ignore_index=True)
        archivos_csv = [archivo for archivo in os.listdir(directorio_actual) if archivo.endswith('.csv')]

        for archivo in archivos_csv:
            ruta_archivo = os.path.join(directorio_actual, archivo)
            os.remove(ruta_archivo)

        df_completo.to_csv('archivo_completo.csv', index=False)

def limpiar_csv():
    df = pd.read_csv('archivo_completo.csv')
    df_limpio = df[~df.apply(lambda row: row.astype(str).str.contains('Error', case=False).any(), axis=1)]
    df_limpio.to_csv('archivo_completo_limpio.csv', index=False)

def dejar_consolidado_xlsx():
    directorio_actual = os.getcwd()
    df = pd.read_csv('archivo_completo_limpio.csv', sep=',')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    df.to_excel(f'archivo_completo_limpio_{timestamp}.xlsx', index=False)
    
    archivos_csv = [archivo for archivo in os.listdir(directorio_actual) if archivo.endswith('.csv')]
    for archivo in archivos_csv:
        ruta_archivo = os.path.join(directorio_actual, archivo)
        os.remove(ruta_archivo)

def concatenar_xlsx():
    directorio_actual = os.getcwd()
    df_lista = []

    for archivo in os.listdir(directorio_actual):
        if archivo.endswith('.xlsx'):
            ruta_xlsx = os.path.join(directorio_actual, archivo)
            df = pd.read_excel(ruta_xlsx)
            df_lista.append(df)

    if df_lista:
        df_completo = pd.concat(df_lista, ignore_index=True)
        archivos_xlsx = [archivo for archivo in os.listdir(directorio_actual) if archivo.endswith('.xlsx')]

        for archivo in archivos_xlsx:
            ruta_archivo = os.path.join(directorio_actual, archivo)
            os.remove(ruta_archivo)

        df_completo.to_excel('archivo_completo.xlsx', index=False)

def saber_faltantes():
    df = pd.read_excel(pwd_xlsx)
    df_maps = pd.read_excel("archivo_completo.xlsx")

    df_dos = df[~df['id'].isin(df_maps['usuario'])]
    df_dos.to_excel(pwd_xlsx, index=False)
    num_filas = df_dos.shape[0]
    return False if num_filas == 0 else True

def main():
    df = pd.read_excel(pwd_xlsx)
    
    while not df.empty:
        direcciones = {str(row['id']): row['Direccion'] for index, row in df.iterrows()}
        results = process_parallel(direcciones)
        
        # Procesar resultados
        concatenar_csv()
        limpiar_csv()
        dejar_consolidado_xlsx()
        concatenar_xlsx()
        
        if not saber_faltantes():
            print("Procesamiento completado")
            return False
            
        df = pd.read_excel(pwd_xlsx)
        print("Esperando antes del siguiente lote...")
        # time.sleep(10)
        
    return True

if __name__ == "__main__":
    multiprocessing.freeze_support()
    
    start_time = time.time()
    
    bandera = True
    while bandera is True:
        bandera = main()
        
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"La función tardó {execution_time:.4f} segundos en ejecutarse.")