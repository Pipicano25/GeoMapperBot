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


class GoogleMapsBot:
    def __init__(self):
        self.setup_driver()
        
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
        self.wait = WebDriverWait(self.driver, timeout=10)
        print("Iniciando navegador en modo headless...")
        self.driver.get("https://www.google.com/maps")
        time.sleep(2)
        
    def search_location(self, address, ciudad="Popayán"):
        try:
            # Buscar y limpiar campo de búsqueda
            search_box = self.wait.until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            search_box.clear()
            
            # Formatear y buscar dirección
            formatted_address = f"{address}, {ciudad}, Colombia"
            search_box.send_keys(formatted_address)
            search_box.send_keys(Keys.RETURN)
            
            # Esperar a que se actualice la URL
            time.sleep(2.5)
            
            # Obtener coordenadas de la URL
            current_url = self.driver.current_url
            coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', current_url)
            
            if coords_match:
                lat, lng = coords_match.groups()
                return {
                    'direccion': address,
                    'ciudad': ciudad,
                    'latitud': float(lat),
                    'longitud': float(lng),
                    'estado': 'ENCONTRADO',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            else:
                return {
                    'direccion': address,
                    'ciudad': ciudad,
                    'latitud': None,
                    'longitud': None,
                    'estado': 'NO ENCONTRADO',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
        except Exception as e:
            return {
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
            print(f"\nDirección {i}/{total}: {address}")
            result = self.search_location(address)
            
            # time.sleep(1)

            results.append(result)
            
            # Mostrar resultado en tiempo real
            if result['estado'] == 'ENCONTRADO':

                print(f"✓ Encontrado - Lat: {result['latitud']}, Lng: {result['longitud']}")
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
    # Lista de direcciones de ejemplo
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

if __name__ == "__main__":
    main()