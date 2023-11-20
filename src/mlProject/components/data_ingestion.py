import os
import urllib.request as request
import zipfile
from mlProject import logger
from mlProject.utils.common import get_size
from mlProject.entity.config_entity import DataIngestionConfig
from pathlib import Path


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def download_file(self):
         
        try:
            url1=self.config.source_URL
            print(url1)
            if not os.path.exists(self.config.local_data_file):
                
                filename, headers = request.urlretrieve(
                    url=self.config.source_URL,
                    filename=self.config.local_data_file
                )
                logger.info(f"File downloaded: {filename}\nHeaders: {headers}")
            else:
                logger.info(f"File already exists. Size: {get_size(Path(self.config.local_data_file))}")
        except Exception as e:
            logger.error(f"Error during file download: {e}")

    def extract_zip_file(self):
        try:
            unzip_path = self.config.unzip_dir
            os.makedirs(unzip_path, exist_ok=True)
            with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_path)
            logger.info("ZIP file extraction completed.")
        except zipfile.BadZipFile as bzf:
            logger.error(f"Error extracting ZIP file: {bzf}. The file may be corrupted.")
        except Exception as e:
            logger.error(f"Error during ZIP file extraction: {e}")
