# GeoMapperBot

Bot de automatización que busca direcciones en Google Maps con Selenium y extrae sus coordenadas (latitud/longitud) desde la URL resultante. Lee las direcciones desde un archivo Excel con pandas y soporta ejecución en paralelo con múltiples procesos.

## Tecnologías

- Python 3
- Selenium (Chrome headless)
- pandas
- multiprocessing

## Estructura

| Archivo | Descripción |
|---|---|
| `maps.py` | Lee usuarios desde un Excel |
| `import webbrowser.py` | Bot individual con validación de dirección |
| `import webbrowser multiple.py` | Versión paralela con múltiples bots |
| `v1/` | Iteraciones del bot (`maps.py` a `maps_6.py`) con mejoras progresivas (validación, reintentos, paralelismo) |

## Uso

1. Instala las dependencias (`selenium`, `pandas`) y ChromeDriver.
2. Ajusta la ruta del archivo Excel (`pwd_xlsx`) en el script.
3. Ejecuta el script deseado, por ejemplo:

```bash
python v1/maps_6.py
```

## Licencia

GPL v3 (ver `LICENSE`)
