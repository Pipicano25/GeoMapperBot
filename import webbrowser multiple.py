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
import multiprocessing
from multiprocessing import Pool
import logging
import sys


def setup_logger():
    # Crear el nombre del archivo con timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'log_proceso_{timestamp}.txt'
    
    # Configurar el logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # Para que siga mostrando en consola
        ]
    )
    return logging.getLogger()


# Crear el logger global
logger = setup_logger()
# pwd_xlsx = "C:/Users/Pipicano/Downloads/H3_Pop_200Reg - copia.xlsx"
pwd_xlsx = "C:/Users/anderson.pipicano/Downloads/Afil_Coord_Google_v02.xlsx"
num_bots = 5


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
        # print("Iniciando navegador en modo headless...")
        logger.info("Iniciando navegador en modo headless...")
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
    
    def evaluar_sugerencia(self, direccion_busqueda, texto_sugerencia):
        """
        Evalúa qué tan relevante es una sugerencia
        Retorna un puntaje: mayor puntaje = mejor coincidencia
        """
        puntaje = 0
        direccion = direccion_busqueda.lower().replace('#', ' ').replace('-', ' ')
        sugerencia = texto_sugerencia.lower()
        
        # Separar en palabras
        palabras_direccion = set(direccion.split())
        palabras_sugerencia = set(sugerencia.split())
        
        # Coincidencias exactas
        coincidencias = palabras_direccion.intersection(palabras_sugerencia)
        puntaje += len(coincidencias) * 10  # 10 puntos por cada palabra que coincide
        
        # Bonus por coincidencia de números
        numeros_direccion = set(word for word in palabras_direccion if any(c.isdigit() for c in word))
        numeros_sugerencia = set(word for word in palabras_sugerencia if any(c.isdigit() for c in word))
        puntaje += len(numeros_direccion.intersection(numeros_sugerencia)) * 15  # 15 puntos extra por números

        # Penalización por palabras muy diferentes
        diferencia = len(palabras_sugerencia) - len(coincidencias)
        puntaje -= diferencia * 5  # -5 puntos por cada palabra extra/faltante
        
        return puntaje
    
    def check_coords(self, address, process_id):
        """Verifica si las coordenadas son iguales a las anteriores"""
        coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', self.driver.current_url)
        if coords_match:
            lat, lng = map(float, coords_match.groups())
            coord_key = f"{lat},{lng}"

            # Verificar si estas coordenadas ya se han usado
            if coord_key in self.coords_history:
                # Si las coordenadas ya se usaron para otra dirección
                if address not in self.coords_history[coord_key]:
                    logger.info(f"Bot {process_id} Coordenadas ya usadas para: {self.coords_history[coord_key]}")
                    # print(f"Bot {process_id} Coordenadas ya usadas para: {self.coords_history[coord_key]}")
                    self.repeat_count += 1
                    return False

            if self.last_lat == lat and self.last_lng == lng:
                logger.info(f"Bot {process_id} Coordenadas repetidas detectadas, reiniciando navegador...")
                # print(f"Bot {process_id} Coordenadas repetidas detectadas, reiniciando navegador...")
                # Solo presionar Enter para refrescar la búsqueda
                search_box = self.wait.until(
                    EC.presence_of_element_located((By.ID, "searchboxinput"))
                )
                for i in range(5):
                    time.sleep(0.01)
                    search_box.send_keys(Keys.RETURN)
                return False
            
            self.last_lat = lat
            self.last_lng = lng
            # Guardar nuevas coordenadas con su dirección
            self.coords_history[coord_key] = {address}
            self.repeat_count = 0

        return True

    def obtener_sugerencias(self):
        """Obtiene las sugerencias del panel de búsqueda de Google Maps"""
        try:
            # XPath para el contenedor de sugerencias
            xpath_base = "/html/body/div[2]/div[3]/div[8]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[1]"
            
            # Esperar a que aparezca el contenedor
            container = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.XPATH, xpath_base))
            )
            
            # Obtener todas las sugerencias (los div hijos)
            sugerencias = container.find_elements(By.XPATH, "./div")
            
            # print(f"Encontradas {len(sugerencias)} sugerencias")
            # for i, sug in enumerate(sugerencias):
                # print(f"Sugerencia {i+1}: {sug.text}")

        except Exception as e:
            # print(f"Error general: {e}")
            return []
        
    def calcular_similitud_direcciones(self, direccion_busqueda, titulo_sugerencia):
        """
        Calcula el porcentaje de similitud entre la dirección buscada y el título de la sugerencia
        """
        # Normalizar textos
        direccion = direccion_busqueda.lower().replace('#', ' ').replace('-', ' ')
        titulo = titulo_sugerencia.lower()
        
        # Convertir a conjuntos de palabras para comparar
        palabras_direccion = set(direccion.split())
        palabras_titulo = set(titulo.split())
        
        # Identificar partes importantes (números y tipos de vía)
        numeros_direccion = set(word for word in palabras_direccion if any(c.isdigit() for c in word))
        numeros_titulo = set(word for word in palabras_titulo if any(c.isdigit() for c in word))
        
        # Tipos de vía comunes
        tipos_via = {'calle', 'carrera', 'avenida', 'cl', 'cra', 'kr', 'av', 'diagonal', 'transversal'}
        
        # Encontrar tipos de vía en cada texto
        tipos_direccion = palabras_direccion.intersection(tipos_via)
        tipos_titulo = palabras_titulo.intersection(tipos_via)
        
        # Calcular puntos
        puntos = 0
        total_puntos_posibles = 0
        
        # Puntos por números (más importantes)
        if numeros_direccion and numeros_titulo:
            coincidencias_numeros = len(numeros_direccion.intersection(numeros_titulo))
            total_numeros = len(numeros_direccion)
            puntos += (coincidencias_numeros / total_numeros) * 60  # 60% del peso total
            total_puntos_posibles += 60
        
        # Puntos por tipos de vía
        if tipos_direccion and tipos_titulo:
            coincidencias_tipos = len(tipos_direccion.intersection(tipos_titulo))
            total_tipos = len(tipos_direccion)
            puntos += (coincidencias_tipos / total_tipos) * 30  # 30% del peso total
            total_puntos_posibles += 30
        
        # Puntos por otras palabras
        otras_palabras_dir = palabras_direccion - numeros_direccion - tipos_direccion
        otras_palabras_tit = palabras_titulo - numeros_titulo - tipos_titulo
        if otras_palabras_dir and otras_palabras_tit:
            coincidencias_otras = len(otras_palabras_dir.intersection(otras_palabras_tit))
            total_otras = len(otras_palabras_dir)
            puntos += (coincidencias_otras / total_otras) * 10  # 10% del peso total
            total_puntos_posibles += 10
        
        # Calcular porcentaje final
        porcentaje = (puntos / total_puntos_posibles * 100) if total_puntos_posibles > 0 else 0
        
        return porcentaje

    # Ejemplo de uso en la evaluación de sugerencias
    def evaluar_sugerencias(self,  direccion_busqueda, sugerencias):
        mejor_puntaje = 0
        mejor_sugerencia = None
        
        for sug in sugerencias:
            similitud = self.calcular_similitud_direcciones(direccion_busqueda, sug['titulo'])
            
            if similitud > mejor_puntaje:
                mejor_puntaje = similitud
                mejor_sugerencia = sug
        
        return mejor_sugerencia

    def extraer_coordenadas_url(self, url):
        # Buscar coordenadas en formato !3d{lat}!4d{lng}
        coord_match = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url)
        
        if coord_match:
            lat, lng = coord_match.groups()
            return float(lat), float(lng)
        
        return None, None
    
    def search_location(self, id_pk, address, process_id, ciudad="Popayán", max_attempts=5):
        try:
            # Buscar y limpiar campo de búsqueda
            search_box = self.wait.until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )

            for attempt in range(max_attempts):
                try:
                    url_sugerida_maps = ''
                    search_box.clear()
                    self.url = self.driver.current_url

                    # Formatear y buscar dirección
                    formatted_address = f"{address}, {ciudad}, Colombia"
                    search_box.send_keys(formatted_address)
                    for i in range(5):
                        time.sleep(0.01)
                        search_box.send_keys(Keys.RETURN)


                    # Esperar a que se actualice la URL
                    # time.sleep(2)
                    # Esperar a que la URL cambie, indicando que la página ha cargado
                    fomateado = address.lower().split()
                    timeout = 0.5 if attempt < 2 else 0.7 if attempt < 5 else 1
                    WebDriverWait(self.driver, timeout).until(
                        lambda driver: (self.url != self.driver.current_url) and self.valida( address, self.driver.current_url)
                    )

                    # print('Sugerencias:')
                    try:
                        # Esperar a que aparezcan las sugerencias
                        sugerencias = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.Nv2PK"))
                        )
                        # print(sugerencias)
                        # Para cada sugerencia, obtener su información
                        resultados = []
                        for sug in sugerencias:
                            try:
                                # Obtener el título (dirección)
                                titulo = sug.find_element(By.CSS_SELECTOR, "div.qBF1Pd").text
                                
                                # Obtener ubicación adicional (barrio, ciudad)
                                ubicacion = sug.find_element(By.CSS_SELECTOR, "div.W4Efsd").text

                                # Propiedades adicionales
                                href = sug.find_element(By.CSS_SELECTOR, "a.hfpxzc").get_attribute("href")
                
                                resultados.append({
                                    'elemento': sug,
                                    'titulo': titulo,
                                    'ubicacion': ubicacion,
                                    'href': href,
                                })
                                
                                # print(f"Sugerencia encontrada:")
                                # print(f"  Título: {titulo}")
                                # print(f"  Ubicación: {ubicacion}")
                                # print(f"  href: {href}")


                            except Exception as e:
                                pass
                                # print(f"Error procesando sugerencia: {e}")

                        logger.info(f"Bot {process_id} {resultados}") 

                        if len(resultados) > 0 :
                            url_sugerida_maps = self.evaluar_sugerencias(address, resultados)['href']

                    except TimeoutException:
                        # print("No se encontraron sugerencias, continuando con búsqueda normal")
                        search_box.send_keys(Keys.RETURN)

                    self.url = self.driver.current_url
                    # print(self.driver.current_url)

                    # Obtener coordenadas de la URL
                    current_url = self.driver.current_url

                    if url_sugerida_maps != '':
                        lat, lng = self.extraer_coordenadas_url(url_sugerida_maps)
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
                        coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', current_url)

                        if self.check_coords(address, process_id):
                            # Si las coordenadas son diferentes, procedemos
                            self.url = self.driver.current_url
                            logger.info(f"Bot {process_id} Intento {attempt + 1} exitoso: {self.driver.current_url}")
                            # print(f"Bot {process_id} Intento {attempt + 1} exitoso: {self.driver.current_url}")
                            break
                        else:
                            logger.info(f"Bot {process_id} Coordenadas repetidas en intento {attempt + 1}, reintentando...")
                            # print(f"Bot {process_id} Coordenadas repetidas en intento {attempt + 1}, reintentando...")
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
                    logger.info(f"Bot {process_id} Timeout en intento {attempt + 1}")
                    # print(f"Bot {process_id} Timeout en intento {attempt + 1}")
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

    def process_addresses(self, addresses, process_id):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'resultados_direcciones_{timestamp}_Bot_{process_id}.csv'
        results = []
        total = len(addresses)
        logger.info(f"\nProcesando {total} direcciones...")
        # print(f"\nProcesando {total} direcciones...")

        for i, address in enumerate(addresses, 1):
            logger.info(f"\n Bot {process_id} Dirección {i}/{total}: {addresses[address]}")
            # print(f"\n Bot {process_id} Dirección {i}/{total}: {addresses[address]}")
            result = self.search_location(address, addresses[address], process_id)

            # time.sleep(1)

            results.append(result)

            # Mostrar resultado en tiempo real
            if result['estado'] == 'ENCONTRADO':
                logger.info(f"✓ Bot {process_id} Encontrado - Lat: {result['latitud']}, Lng:{result['longitud']}")
                # print(f"✓ Bot {process_id} Encontrado - Lat: {result['latitud']}, Lng:{result['longitud']}")
            else:
                logger.info(f"✗  Bot {process_id} {result['estado']}")
                # print(f"✗  Bot {process_id} {result['estado']}")

        # Guardar resultados
        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"\n Bot {process_id} Resultados guardados en {output_file}")
        # print(f"\n Bot {process_id} Resultados guardados en {output_file}")

        return results

    def close(self):
        self.driver.quit()
        logger.info("Bot cerrado correctamente")
        # print("Bot cerrado correctamente")


def divide_dict(addresses_dict):
    """
    Divide el diccionario de direcciones entre el número de bots especificado
    """
    items = list(addresses_dict.items())
    avg = len(items) // num_bots  # Promedio de items por bot
    remainder = len(items) % num_bots  # Items restantes
    result = []
    start = 0
    
    for i in range(num_bots):
        # Si hay restantes, agregar uno más a este lote
        end = start + avg + (1 if remainder > 0 else 0)
        if remainder > 0:
            remainder -= 1
        # Crear diccionario para este bot
        batch = dict(items[start:end])
        if batch:  # Solo añadir si hay registros
            result.append(batch)
        start = end
    
    return result


def process_worker(args):
    """
    Función que ejecutará cada bot
    args: tupla (id_bot, diccionario_direcciones)
    """
    process_id, addresses_dict = args
    bot = GoogleMapsBot()  # Crear nueva instancia del bot
    logger.info(f"Bot {process_id}: Iniciando procesamiento")
    # print(f"Bot {process_id}: Iniciando procesamiento")
    # print(addresses_dict)
    return bot.process_addresses(addresses_dict, process_id)


def process_parallel(direcciones):
    logger.info(f"Iniciando procesamiento paralelo con {num_bots} bots...")
    # print(f"Iniciando procesamiento paralelo con {num_bots} bots...")
    
    # Dividir direcciones entre los bots
    batches = divide_dict(direcciones)
    
    # Crear argumentos para cada proceso
    process_args = [(i, batch) for i, batch in enumerate(batches)]
    
    # Iniciar pool de procesos
    with Pool(len(batches)) as pool:
        async_result = pool.map_async(process_worker, process_args)
        # Obtener todos los resultados
        all_results = async_result.get()
    
    # Aplanar resultados
    results = [item for sublist in all_results for item in sublist]
    return results


def separar_duplicados(archivo_excel):
    """
    Separa los registros duplicados a un nuevo Excel y deja los únicos en el original
    """
    # Leer el archivo Excel
    df = pd.read_excel(archivo_excel)
    
    # Crear un diccionario para almacenar las direcciones por coordenadas
    coord_dict = {}
    duplicados_indices = set()
    
    # Filtrar solo los registros encontrados
    df_encontrados = df[df['estado'] == 'ENCONTRADO']
    
    for idx, row in df_encontrados.iterrows():
        # Crear una clave con las coordenadas
        coord_key = f"{row['latitud']},{row['longitud']}"
        
        if coord_key in coord_dict:
            # Marcar el índice actual y el anterior como duplicados
            duplicados_indices.add(idx)
            duplicados_indices.update(coord_dict[coord_key])
        else:
            coord_dict[coord_key] = {idx}

    # Separar los DataFrames
    df_duplicados = df.loc[list(duplicados_indices)]
    df_unicos = df.drop(index=list(duplicados_indices))

    # Guardar resultados
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Guardar duplicados en nuevo archivo
    archivo_duplicados = f'coordenadas_duplicadas_{timestamp}.xlsx'

    df_duplicados = pd.concat([
        df_duplicados[['usuario', 'direccion']].rename(
                columns={'usuario': 'id', 'direccion': 'Direccion'}
            ), 
            pd.read_excel(pwd_xlsx)
        ], ignore_index=True)
    
    # Sobrescribir archivo original solo con registros únicos
    df_unicos.to_excel(archivo_excel, index=False)
    df_duplicados.to_excel(archivo_duplicados, index=False)
    return True if df_duplicados.shape[0] > 0 else False


def main():

    df = pd.read_excel(pwd_xlsx)
    direcciones = {}
    contador = 0
    tup = 0
    tam = df.shape[0]
    contador_final = 30 * num_bots
    for index, row in df.iterrows():
        contador += 1
        tup += 1
        direcciones[str(row['id'])] = row['Direccion']
        if contador == contador_final or tup == tam:
            # time.sleep(10)
            # bot(direcciones)
            # Procesar con múltiples bots
            results = process_parallel(direcciones)
            concatenar_csv()
            limpiar_csv()
            dejar_consolidado_xlsx()
            concatenar_xlsx()
            contador = 0
            direcciones = {}
    
            if tup > 5000:
                break


def bot(direcciones):
    logger.info("Iniciando búsqueda de direcciones en modo headless...")
    # print("Iniciando búsqueda de direcciones en modo headless...")
    bot = GoogleMapsBot()

    try:
        results = bot.process_addresses(direcciones)

        # Mostrar resumen
        encontradas = sum(1 for r in results if r['estado'] == 'ENCONTRADO')
        logger.info("\nResumen de la búsqueda:")
        logger.info(f"Total de direcciones procesadas: {len(direcciones)}")
        logger.info(f"Direcciones encontradas: {encontradas}")
        logger.info(f"Direcciones no encontradas: {len(direcciones) - encontradas}")
        # print("\nResumen de la búsqueda:")
        # print(f"Total de direcciones procesadas: {len(direcciones)}")
        # print(f"Direcciones encontradas: {encontradas}")
        # print(f"Direcciones no encontradas: {len(direcciones) - encontradas}")

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


def saber_faltantes(faltantes):

    bandera_repetido = True
    df = pd.read_excel(pwd_xlsx)
    df_maps = pd.read_excel("archivo_completo.xlsx")

    df_dos = df[~df['id'].isin(df_maps['usuario'])]
    df_dos.to_excel(pwd_xlsx, index=False)
    num_filas = df_dos.shape[0]

    for i in df_dos['id']:
        faltantes.append(i)
        if i in faltantes:
            bandera_repetido = False

    return False if num_filas == 0 else True, faltantes, bandera_repetido


def incio_bots():
    # Medir el tiempo de ejecución
    start_time = time.time()  # Tiempo antes de ejecutar la función
    duplicados = 0
    bandera = True
    faltantes = []
    bandera_repetido = True

    while bandera is True:
        print('*****************')
        main()
        bandera, faltantes, bandera_repetido = saber_faltantes(faltantes)
        print(bandera, faltantes, bandera_repetido)
        if bandera is False:
            duplicados += 1
            if duplicados < 5 :
                bandera = separar_duplicados('archivo_completo.xlsx')
            else:
                bandera = False
        # bandera = False  if bandera_repetido is False else True

    end_time = time.time()  # Tiempo después de ejecutar la función
    execution_time = end_time - start_time  # Calcular la diferencia
    logger.info(f"La función tardó {execution_time:.4f} segundos en ejecutarse.")


def buscar_duplicados_coordenadas(df, umbral_similitud=70):
    """
    Encuentra registros con coordenadas duplicadas y baja similitud en direcciones
    que estén dentro de una zona específica
    """
    def esta_en_zona(lat, lng):
        # Rangos aproximados de Popayán
        ZONAS = {
            'popayan': {
                'lat_min': 2.4000,
                'lat_max': 2.5000,
                'lng_min': -76.6500,
                'lng_max': -76.5500
            }
            # Puedes añadir más zonas aquí
        }
        
        # Verificar si está en alguna zona
        for zona, rangos in ZONAS.items():
            if (rangos['lat_min'] <= lat <= rangos['lat_max'] and 
                rangos['lng_min'] <= lng <= rangos['lng_max']):
                return True
        return False

    # Filtrar solo registros dentro de la zona
    df_en_zona = df[df.apply(lambda row: esta_en_zona(row['latitud'], row['longitud']), axis=1)]
    
    # Agrupamos por coordenadas
    grupos = df_en_zona.groupby(['latitud', 'longitud'])
    
    # Lista para almacenar registros con baja similitud
    registros_diferentes = []
    
    for (lat, lng), grupo in grupos:
        if len(grupo) > 1:  # Si hay más de una dirección
            direcciones = grupo['direccion'].tolist()
            
            # Comparar primera dirección con el resto
            dir1_parts = set(direcciones[0].lower().replace('#',' ').replace('-',' ').split())
            
            for i in range(1, len(direcciones)):
                dir2_parts = set(direcciones[i].lower().replace('#',' ').replace('-',' ').split())
                coincidencias = len(dir1_parts.intersection(dir2_parts))
                total = max(len(dir1_parts), len(dir2_parts))
                similitud = (coincidencias / total) * 100
                
                if similitud < umbral_similitud:
                    # Agregar solo el registro actual a la lista
                    registro = grupo.iloc[i].to_dict()
                    registro['similitud'] = round(similitud, 2)
                    registro['zona'] = 'Dentro de zona permitida'
                    registros_diferentes.append(registro)
    
    if registros_diferentes:
        df_diferentes = pd.DataFrame(registros_diferentes)
        return df_diferentes
    
    return None

def nuevo_intento():
    df = pd.read_excel('archivo_completo.xlsx')
    duplicados = buscar_duplicados_coordenadas(df)
    if duplicados is not None:
        incio_bots()
        print(f"Se encontraron {len(duplicados)} grupos de direcciones duplicadas")
        duplicados.rename({'direccion':'Direccion', 'usuario': 'id'}).to_excel(pwd_xlsx, index=False)

if __name__ == "__main__":

    multiprocessing.freeze_support()
    incio_bots()
    for i in range(4):  # Ciclo externo
        nuevo_intento()


