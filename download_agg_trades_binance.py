import os
import time
from datetime import datetime
import requests
from zipfile import ZipFile
import tempfile
import time
from datetime import datetime

class AggTradesBinanceDownloader:
    """
    Descarga los trades agrupados desde la plataforma de Binance
    y gestiona los archivos en disco para evitar errores de memoria.
    """
    def __init__(self, download_dir=None):
        # Usamos una carpeta temporal del sistema para no llenar el proyecto de basura
        self.download_dir = download_dir or os.path.join(tempfile.gettempdir(), "binance_backtesting")
        os.makedirs(self.download_dir, exist_ok=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}][SISTEMA] Carpeta temporal configurada en: {self.download_dir}")

    def _download_and_extract(self, url, filename_prefix):
        zip_path = os.path.join(self.download_dir, f"{filename_prefix}.zip")
        start_time = time.time()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DESCARGA] Bajando archivo: {url}")
        # Descarga por bloques (streaming) para no saturar la RAM ni la conexión
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024): # Bloques de 1MB
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        # Opcional: imprimir progreso si el archivo es muy grande (>50MB)
                        if total_size > 50*1024*1024:
                             print(f"  > {downloaded/(1024*1024):.1f} MB descargados...", end='\r')

            dl_time = time.time() - start_time
            extract_start = time.time()
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [SISTEMA] Extrayendo ZIP...")
            with ZipFile(zip_path, 'r') as zip_ref:
                csv_name = zip_ref.namelist()[0]
                zip_ref.extractall(self.download_dir)
                csv_path = os.path.join(self.download_dir, csv_name)

            # Eliminar el ZIP inmediatamente para liberar espacio en disco
            os.remove(zip_path)
            extract_time = time.time() - extract_start
            total_time = time.time() - start_time
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] CSV listo en disco: {csv_name} (Descarga: {dl_time:.1f}s, Extraccion: {extract_time:.1f}s, Total: {total_time:.1f}s)")
            return csv_path
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] No se pudo descargar: {url} (Status: {response.status_code})")
            return None

    def download_agg_trades_montly(self, symbol, year, month):
        filename_prefix = f"{symbol}-aggTrades-{year}-{month:02d}"
        url = f"https://data.binance.vision/data/spot/monthly/aggTrades/{symbol}/{filename_prefix}.zip"
        return self._download_and_extract(url, filename_prefix)
    
    def download_agg_trades_daily(self, symbol, year, month, day):
        filename_prefix = f"{symbol}-aggTrades-{year}-{month:02d}-{day:02d}"
        url = f"https://data.binance.vision/data/spot/daily/aggTrades/{symbol}/{filename_prefix}.zip"
        return self._download_and_extract(url, filename_prefix)
