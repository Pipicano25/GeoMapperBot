from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import re
import logging

class GoogleMapsBot:
    def __init__(self, headless=False):
        self.setup_logging()
        self.setup_driver(headless)
        
    def setup_logging(self):
        logging.basicConfig(
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
        
    def search_location(self, address):
        try:
            # Encontrar el campo de búsqueda
            search_box = self.wait.until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            search_box.clear()
            
            # Formatear la dirección para mejor búsqueda
            formatted_address = f"{address}, Popayán, Cauca, Colombia"
            search_box.send_keys(formatted_address)
            search_box.send_keys(Keys.RETURN)
            
            # Esperar a que la URL se actualice
            time.sleep(3)
            
            # Obtener la URL actual
            current_url = self.driver.current_url
            
            # Extraer coordenadas
            coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', current_url)
            
            if coords_match:
                lat, lng = coords_match.groups()
                self.logger.info(f"Ubicación encontrada: {formatted_address}")
                self.logger.info(f"Coordenadas: {lat}, {lng}")
                
                # Tomar captura de pantalla
                screenshot_name = "ubicacion_popayan.png"
                self.driver.save_screenshot(screenshot_name)
                self.logger.info(f"Captura guardada como {screenshot_name}")
                
                return float(lat), float(lng)
            else:
                self.logger.warning("No se encontraron coordenadas exactas")
                return None
                
        except Exception as e:
            self.logger.error(f"Error en la búsqueda: {str(e)}")
            return None
            
    def close(self):
        self.driver.quit()
        self.logger.info("Bot cerrado correctamente")

def main():
    direccion = "calle 6 #10-54"
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
        "carrera 8 #22-67"
    ]
    
    bot = GoogleMapsBot(headless=False)
    try:
        coordenadas = bot.search_location(direccion)
        if coordenadas:
            lat, lng = coordenadas
            print("\nResultados de la búsqueda:")
            print(f"Dirección buscada: {direccion}, Popayán")
            print(f"Latitud: {lat}")
            print(f"Longitud: {lng}")
            print("\nSe ha guardado una captura de pantalla como 'ubicacion_popayan.png'")
        else:
            print("\nNo se pudieron encontrar las coordenadas exactas de la dirección.")
            print("Sugerencias:")
            print("- Verifica que la dirección esté correctamente escrita")
            print("- Intenta añadiendo más detalles como el barrio")
            print("- Considera usar un punto de referencia cercano")
    finally:
        bot.close()

if __name__ == "__main__":
    main()