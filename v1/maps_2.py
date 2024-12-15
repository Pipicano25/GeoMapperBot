from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import re
import pandas as pd
import logging
from datetime import datetime

class GoogleMapsBot:
    def __init__(self, headless=False):
        self.setup_logging()
        self.setup_driver(headless)
        
    def setup_logging(self):
        # Configurar logging con timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logging.basicConfig(
            filename=f'busqueda_direcciones_{timestamp}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_driver(self, headless):
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, timeout=10)
        self.driver.get("https://www.google.com/maps")
        time.sleep(2)
        
    def search_location(self, address, ciudad="Popayán"):
        try:
            # Encontrar y limpiar el campo de búsqueda
            search_box = self.wait.until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            search_box.clear()
            
            # Formatear la dirección
            formatted_address = f"{address}, {ciudad}, Colombia"
            search_box.send_keys(formatted_address)
            search_box.send_keys(Keys.RETURN)
            
            # Esperar a que se actualice la URL
            time.sleep(3)
            
            # Obtener URL y extraer coordenadas
            current_url = self.driver.current_url
            coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', current_url)
            
            if coords_match:
                lat, lng = coords_match.groups()
                self.logger.info(f"Ubicación encontrada: {formatted_address}")
                
                # Tomar captura de pantalla
                screenshot_name = f"ubicacion_{address.replace(' ', '_').replace('#', '')}.png"
                self.driver.save_screenshot(screenshot_name)
                
                return {
                    'direccion': address,
                    'ciudad': ciudad,
                    'latitud': float(lat),
                    'longitud': float(lng),
                    'estado': 'ENCONTRADO',
                    'captura': screenshot_name
                }
            else:
                self.logger.warning(f"No se encontraron coordenadas para: {formatted_address}")
                return {
                    'direccion': address,
                    'ciudad': ciudad,
                    'latitud': None,
                    'longitud': None,
                    'estado': 'NO ENCONTRADO',
                    'captura': None
                }
                
        except Exception as e:
            self.logger.error(f"Error buscando {address}: {str(e)}")
            return {
                'direccion': address,
                'ciudad': ciudad,
                'latitud': None,
                'longitud': None,
                'estado': f'ERROR: {str(e)}',
                'captura': None
            }
    
    def process_addresses(self, addresses, output_file=None):
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'resultados_direcciones_{timestamp}.csv'
        
        results = []
        total = len(addresses)
        
        for i, address in enumerate(addresses, 1):
            print(f"\nProcesando dirección {i}/{total}: {address}")
            result = self.search_location(address)
            results.append(result)
            
            # Mostrar resultado en tiempo real
            if result['estado'] == 'ENCONTRADO':
                print(f"✓ Encontrado - Lat: {result['latitud']}, Lng: {result['longitud']}")
            else:
                print(f"✗ {result['estado']}")
                
        # Crear DataFrame y guardar resultados
        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False)
        print(f"\nResultados guardados en {output_file}")
        
        return results
    
    def close(self):
        self.driver.quit()
        self.logger.info("Bot cerrado correctamente")

def main():
    # Lista de direcciones a buscar
    direcciones = [
        "calle 6 #10-54",
        "carrera 7 #12-45",
        "calle 8 #15-32",
        "carrera 9 #20-15",
        "calle 4 #8-23",
        "carrera 5 #16-78",
        "calle 10 #25-41",
        "carrera 12 #30-12",
        "calle 15 #18-90",
        "carrera 8 #22-67",
        "calle 6 #10-54, popayan, cauca, colombia",
    ]
    
    print("Iniciando búsqueda de direcciones...")
    bot = GoogleMapsBot(headless=False)
    
    try:
        results = bot.process_addresses(direcciones)
        
        # Mostrar resumen final
        encontradas = sum(1 for r in results if r['estado'] == 'ENCONTRADO')
        print("\nResumen de la búsqueda:")
        print(f"Total de direcciones procesadas: {len(direcciones)}")
        print(f"Direcciones encontradas: {encontradas}")
        print(f"Direcciones no encontradas: {len(direcciones) - encontradas}")
        
    finally:
        bot.close()

if __name__ == "__main__":
    main()