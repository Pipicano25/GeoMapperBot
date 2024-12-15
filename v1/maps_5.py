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


pwd_xlsx = 'c:/Users/anderson.pipicano/Downloads/H3_Pop_200Reg.xlsx'

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
        self.wait = WebDriverWait(self.driver, timeout=10)
        print(f"Bot {self.process_id}: Iniciando navegador...")
        self.driver.get("https://www.google.com/maps")
        time.sleep(2)

    def search_location(self, id_pk, address, ciudad="Popayán"):
        try:
            # Cada bot tiene su propia URL inicial
            initial_url = self.driver.current_url
            self.url =  self.driver.current_url
            
            search_box = self.wait.until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            search_box.clear()
            
            # Formatear y buscar dirección
            formatted_address = f"{address}, {ciudad}, Colombia"
            search_box.send_keys(formatted_address)
            search_box.send_keys(Keys.RETURN)

            # Esperar a que la página cargue completamente
            def url_changed_and_loaded(driver):
                current_url = driver.current_url
                return (self.url != self.driver.current_url) and ("@" in self.driver.current_url) and (valida(address, self.driver.current_url))

            def valida( address, url):
                """
                Compara la dirección buscada con la URL resultante
                Retorna el porcentaje de coincidencia
                """
                # Limpiar y dividir la dirección original
                address_parts = address.lower().replace('#', ' ').replace('-', ' ').split()
                # Limpiar números y caracteres especiales innecesarios
                address_parts = [part for part in address_parts if part.strip()]
                
                # Decodificar la URL y obtener la parte relevante
                decoded_url = url.lower()
                # Eliminar la parte de coordenadas y parámetros
                url_parts = decoded_url.split('/')[-1].split('@')[0]
                url_parts = url_parts.replace('+', ' ').replace('%20', ' ')
                url_parts = url_parts.split()
                
                # Contar coincidencias
                matches = sum(1 for part in address_parts if any(part in url_word for url_word in url_parts))
                
                # Calcular porcentaje de coincidencia
                match_percentage = (matches / len(address_parts)) * 100
                return True if match_percentage >= 80 else False

            # Espera más robusta
            try:
                WebDriverWait(self.driver, 15).until(url_changed_and_loaded)
                time.sleep(1)  # Pequeña pausa adicional para estabilidad
            except TimeoutException:
                print(f"Bot {self.process_id}: Timeout esperando cambio de URL para {address}")
                return self.create_error_response(id_pk, address, ciudad, "Timeout en carga de URL")

            # Capturar la URL final
            final_url = self.driver.current_url
            self.url =  self.driver.current_url

            print(f"Bot {self.process_id} - URL final: {final_url}")

            # Extraer coordenadas con verificación adicional
            coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', final_url)
            
            if coords_match:
                lat, lng = coords_match.groups()
                # Verificación adicional de coordenadas válidas
                if self.are_coordinates_valid(lat, lng):
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
            
            return self.create_error_response(id_pk, address, ciudad, "No se encontraron coordenadas válidas")

        except Exception as e:
            print(f"Bot {self.process_id} - Error: {str(e)} para dirección {address}")
            return self.create_error_response(id_pk, address, ciudad, str(e))

    def are_coordinates_valid(self, lat, lng):
        """Verifica si las coordenadas están en rangos razonables para Colombia"""
        try:
            lat, lng = float(lat), float(lng)
            # Rango aproximado para Colombia
            return (-4.0 <= lat <= 13.0) and (-79.0 <= lng <= -66.0)
        except ValueError:
            return False

    def create_error_response(self, id_pk, address, ciudad, error_msg):
        """Crea una respuesta de error estandarizada"""
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

        for id_pk, address in addresses_dict.items():
            print(f"Bot {self.process_id}: Buscando {address}")
            result = self.search_location(id_pk, address)
            results.append(result)

        self.driver.quit()
        return results

def process_worker(args):
    """Función worker para el procesamiento paralelo"""
    process_id, addresses_dict = args
    bot = GoogleMapsBot(process_id)
    return bot.process_batch(addresses_dict)


def divide_dict(addresses_dict, num_bots, batch_size=30):
    """
    Divide el diccionario en lotes de 30 registros por bot
    """
    items = list(addresses_dict.items())
    # Calcular cuántos registros procesará cada bot (máximo 30)
    total_batch_size = batch_size * num_bots
    # Tomar solo los primeros 30*num_bots registros
    items_to_process = items[:total_batch_size]
    
    # Dividir los registros entre los bots
    batches = []
    for i in range(num_bots):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size
        bot_batch = dict(items_to_process[start_idx:end_idx])
        if bot_batch:  # Solo añadir si hay registros
            batches.append(bot_batch)
    
    return batches


def main():
    NUM_BOTS = 10  # Número de bots paralelos
    BATCH_SIZE = 30  # Registros por bot
    
    while True:
        # Leer el archivo Excel
        df = pd.read_excel("C:/Users/anderson.pipicano/Downloads/H3_Pop_200Reg.xlsx")
        
        # Si no hay más registros, terminar
        if df.empty:
            print("No hay más registros para procesar")
            return False
            
        # Crear diccionario de direcciones
        direcciones = {str(row['id']): row['Direccion'] for index, row in df.iterrows()}
        
        print(f"\nProcesando lote de máximo {BATCH_SIZE * NUM_BOTS} direcciones...")
        
        # Dividir direcciones entre los bots (máximo 30 por bot)
        batches = divide_dict(direcciones, NUM_BOTS, BATCH_SIZE)
        
        # Si no hay suficientes registros para ningún bot, terminar
        if not batches:
            print("No hay más registros para procesar")
            return False
        
        # Crear argumentos para cada proceso
        process_args = [(i, batch) for i, batch in enumerate(batches)]
        
        # Iniciar pool de procesos
        with Pool(len(batches)) as pool:
            all_results = pool.map(process_worker, process_args)
        
        # Aplanar resultados
        results = [item for sublist in all_results for item in sublist]
        
        # Guardar resultados
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        df_results = pd.DataFrame(results)
        df_results.to_csv(f'resultados_direcciones_{timestamp}.csv', index=False)
        
        # Procesar resultados y verificar faltantes
        concatenar_csv()
        limpiar_csv()
        dejar_consolidado_xlsx()
        concatenar_xlsx()
        
        # Verificar si quedan registros por procesar
        if not saber_faltantes():
            print("Procesamiento completado")
            return False
            
        print("Esperando antes del siguiente lote...")
        time.sleep(10)  # Pausa entre lotes
        
    return True


def concatenar_csv():
    # Obtener el directorio de trabajo actual (donde está ejecutando el script)
    directorio_actual = os.getcwd()

    # Lista para almacenar los DataFrames leídos
    df_lista = []

    # Recorrer todos los archivos en el directorio actual
    for archivo in os.listdir(directorio_actual):
        # Verificar si el archivo es un CSV (comienza con .csv)
        if archivo.endswith('.csv'):
            # Ruta completa al archivo CSV
            ruta_csv = os.path.join(directorio_actual, archivo)
            
            # Leer el archivo CSV y agregarlo a la lista
            df = pd.read_csv(ruta_csv)
            df_lista.append(df)

    # Concatenar todos los DataFrames en uno solo
    df_completo = pd.concat(df_lista, ignore_index=True)
    # Lista de archivos CSV en el directorio
    archivos_csv = [archivo for archivo in os.listdir(directorio_actual) if archivo.endswith('.csv')]

    # Eliminar los archivos CSV anteriores
    for archivo in archivos_csv:
        ruta_archivo = os.path.join(directorio_actual, archivo)
        os.remove(ruta_archivo)  # Eliminar el archivo CSV anterior
    df_completo.to_csv('archivo_completo.csv', index=False)


def limpiar_csv():
    # Leer el archivo CSV
    df = pd.read_csv('archivo_completo.csv')

    # Filtrar las filas que no contengan la palabra "Error" en ninguna columna
    df_limpio = df[~df.apply(lambda row: row.astype(str).str.contains('Error', case=False).any(), axis=1)]

    # Guardar el nuevo DataFrame sin las filas que contienen "Error"
    df_limpio.to_csv('archivo_completo_limpio.csv', index=False)


def dejar_consolidado_xlsx():
    directorio_actual = os.getcwd()
    df = pd.read_csv('archivo_completo_limpio.csv', sep=',')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    df.to_excel(f'archivo_completo_limpio_{timestamp}.xlsx', index=False)
    archivos_csv = [archivo for archivo in os.listdir(directorio_actual) if archivo.endswith('.csv')]

    # Eliminar los archivos CSV anteriores
    for archivo in archivos_csv:
        ruta_archivo = os.path.join(directorio_actual, archivo)
        os.remove(ruta_archivo)


def concatenar_xlsx():
    # Obtener el directorio de trabajo actual (donde está ejecutando el script)
    directorio_actual = os.getcwd()

    # Lista para almacenar los DataFrames leídos
    df_lista = []

    # Recorrer todos los archivos en el directorio actual
    for archivo in os.listdir(directorio_actual):
        # Verificar si el archivo es un archivo Excel (.xlsx)
        if archivo.endswith('.xlsx'):
            # Ruta completa al archivo Excel
            ruta_xlsx = os.path.join(directorio_actual, archivo)
            
            # Leer el archivo Excel y agregarlo a la lista
            df = pd.read_excel(ruta_xlsx)
            df_lista.append(df)

    # Concatenar todos los DataFrames en uno solo
    df_completo = pd.concat(df_lista, ignore_index=True)

    # Lista de archivos Excel en el directorio
    archivos_xlsx = [archivo for archivo in os.listdir(directorio_actual) if archivo.endswith('.xlsx')]

    # Eliminar los archivos Excel anteriores
    for archivo in archivos_xlsx:
        ruta_archivo = os.path.join(directorio_actual, archivo)
        os.remove(ruta_archivo)  # Eliminar el archivo Excel anterior

    # Guardar el DataFrame concatenado en un archivo Excel
    df_completo.to_excel('archivo_completo.xlsx', index=False)


def saber_faltantes():
    df = pd.read_excel(pwd_xlsx)
    df_maps = pd.read_excel("archivo_completo.xlsx")

    df_dos = df[~df['id'].isin(df_maps['usuario'])]
    df_dos.to_excel(pwd_xlsx, index=False)
    num_filas = df_dos.shape[0]
    return False if num_filas == 0 else True


if __name__ == "__main__":
    multiprocessing.freeze_support()  # Necesario para Windows
    
    start_time = time.time()
    
    # bandera = True
    # while bandera is True:
    main()
        # concatenar_csv()
        # limpiar_csv()
        # dejar_consolidado_xlsx()
        # concatenar_xlsx()
        # bandera = saber_faltantes()
        
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"La función tardó {execution_time:.4f} segundos en ejecutarse.")