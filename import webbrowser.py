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
import time
import os


pwd_xlsx = "C:/Users/Pipicano/Downloads/H3_Pop_200Reg - copia.xlsx"

class GoogleMapsBot:

    def __init__(self):
        self.setup_driver()
        self.url = ''
        self.last_lat = None
        self.last_lng = None
        self.same_coords_count = 0
        # Diccionario para almacenar historial de coordenadas
        self.coords_history = {}
    
    def setup_driver(self):
        options = webdriver.ChromeOptions()
        # Configuraciones para modo headless
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')

        # Silenciar logs innecesarios
        options.add_argument('--log-level=3')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, timeout=3)
        print("Iniciando navegador en modo headless...")
        self.driver.get("https://www.google.com/maps")
        time.sleep(2)

    def valida(self, address, url):
        """
        Compara la dirección buscada con la URL resultante
        Retorna el porcentaje de coincidencia
        """

        # Limpiar y dividir la dirección original
        address_parts = address.lower().replace('#', ' ').replace('-',' ').split()
        # Limpiar números y caracteres especiales innecesarios
        address_parts = [part for part in address_parts if part.strip()]

        # Decodificar la URL y obtener la parte relevante
        decoded_url = url.lower()
        # Eliminar la parte de coordenadas y parámetros
        url_parts = decoded_url.split('/')
        # Contar coincidencias
        matches = sum(1 for part in address_parts if any(part in url_word for url_word in url_parts))

        # Calcular porcentaje de coincidencia
        match_percentage = (matches / len(address_parts)) * 100
        return True if match_percentage >= 90 else False
    
    def check_coords(self, address):
        """Verifica si las coordenadas son iguales a las anteriores"""
        coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', self.driver.current_url)
        if coords_match:
            lat, lng = map(float, coords_match.groups())
            coord_key = f"{lat},{lng}"

            # Verificar si estas coordenadas ya se han usado
            if coord_key in self.coords_history:
                # Si las coordenadas ya se usaron para otra dirección
                if address not in self.coords_history[coord_key]:
                    print(f"Coordenadas ya usadas para: {self.coords_history[coord_key]}")
                    self.repeat_count += 1
                    return False

            if self.last_lat == lat and self.last_lng == lng:
                print("Coordenadas repetidas detectadas, reiniciando navegador...")
                # Solo presionar Enter para refrescar la búsqueda
                search_box = self.wait.until(
                    EC.presence_of_element_located((By.ID, "searchboxinput"))
                )
                search_box.send_keys(Keys.RETURN)
                return False
            
            self.last_lat = lat
            self.last_lng = lng
            # Guardar nuevas coordenadas con su dirección
            self.coords_history[coord_key] = {address}
            self.repeat_count = 0

        return True

    def search_location(self, id_pk, address, ciudad="Popayán", max_attempts=10):
        try:
            # Buscar y limpiar campo de búsqueda
            search_box = self.wait.until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            for attempt in range(max_attempts):
                try:
                    search_box.clear()
                    self.url = self.driver.current_url

                    # Formatear y buscar dirección
                    formatted_address = f"{address}, {ciudad}, Colombia"
                    search_box.send_keys(formatted_address)
                    search_box.send_keys(Keys.RETURN)

                    # Esperar a que se actualice la URL
                    # time.sleep(2)
                    # Esperar a que la URL cambie, indicando que la página ha cargado
                    fomateado = address.lower().split()
                    timeout = 3 if attempt < 2 else 5 if attempt < 5 else 10
                    WebDriverWait(self.driver, timeout).until(
                        lambda driver: (self.url != self.driver.current_url) and self.valida( address, self.driver.current_url)
                    )

                    self.url = self.driver.current_url
                    print(self.driver.current_url)

                    # Obtener coordenadas de la URL
                    current_url = self.driver.current_url
                    coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', current_url)

                    if self.check_coords(address):
                        # Si las coordenadas son diferentes, procedemos
                        self.url = self.driver.current_url
                        print(f"Intento {attempt + 1} exitoso: {self.driver.current_url}")
                        break
                    else:
                        print(f"Coordenadas repetidas en intento {attempt + 1}, reintentando...")
                        if attempt == max_attempts - 1:
                            return {
                                'usuario': id_pk,
                                'direccion': address,
                                'ciudad': ciudad,
                                'latitud': None,
                                'longitud': None,
                                'estado': 'ERROR: Coordenadas repetidas después de todos los intentos',
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                except TimeoutException:
                    print(f"Timeout en intento {attempt + 1}")
                    if attempt == max_attempts - 1:
                        raise

            if coords_match:
                lat, lng = coords_match.groups()
                return {
                    'usuario':id_pk,
                    'direccion': address,
                    'ciudad': ciudad,
                    'latitud': float(lat),
                    'longitud': float(lng),
                    'estado': 'ENCONTRADO',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            else:
                return {
                    'usuario':id_pk,
                    'direccion': address,
                    'ciudad': ciudad,
                    'latitud': None,
                    'longitud': None,
                    'estado': 'NO ENCONTRADO',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

        except Exception as e:
            return {
                'usuario':id_pk,
                'direccion': address,
                'ciudad': ciudad,
                'latitud': None,
                'longitud': None,
                'estado': f'ERROR: {str(e)}',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

    def process_addresses(self, addresses):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'resultados_direcciones_{timestamp}.csv'
        results = []
        total = len(addresses)

        print(f"\nProcesando {total} direcciones...")

        for i, address in enumerate(addresses, 1):
            print(f"\nDirección {i}/{total}: {addresses[address]}")
            result = self.search_location(address, addresses[address])

            # time.sleep(1)

            results.append(result)

            # Mostrar resultado en tiempo real
            if result['estado'] == 'ENCONTRADO':

                print(f"✓ Encontrado - Lat: {result['latitud']}, Lng:{result['longitud']}")
            else:
                print(f"✗ {result['estado']}")

        # Guardar resultados
        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nResultados guardados en {output_file}")

        return results

    def close(self):
        self.driver.quit()
        print("Bot cerrado correctamente")


def main():

    df = pd.read_excel(pwd_xlsx)
    direcciones = {}
    contador = 0
    tup = 0
    tam = df.shape[0]
    for index, row in df.iterrows():
        contador += 1
        tup += 1
        direcciones[str(row['id'])] = row['Direccion']
        if contador == 30 or tup == tam:
            # time.sleep(10)
            contador = 0
            bot(direcciones)
            direcciones = {}
            break


def bot(direcciones):
    print("Iniciando búsqueda de direcciones en modo headless...")
    bot = GoogleMapsBot()

    try:
        results = bot.process_addresses(direcciones)

        # Mostrar resumen
        encontradas = sum(1 for r in results if r['estado'] == 'ENCONTRADO')
        print("\nResumen de la búsqueda:")
        print(f"Total de direcciones procesadas: {len(direcciones)}")
        print(f"Direcciones encontradas: {encontradas}")
        print(f"Direcciones no encontradas: {len(direcciones) - encontradas}")

    finally:
        bot.close()


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

    # Medir el tiempo de ejecución
    start_time = time.time()  # Tiempo antes de ejecutar la función

    bandera = True
    while bandera is True:
        main()
        concatenar_csv()
        limpiar_csv()
        dejar_consolidado_xlsx()
        concatenar_xlsx()
        bandera = saber_faltantes()
        bandera = False
    end_time = time.time()  # Tiempo después de ejecutar la función
    execution_time = end_time - start_time  # Calcular la diferencia

    print(f"La función tardó {execution_time:.4f} segundos en ejecutarse.")