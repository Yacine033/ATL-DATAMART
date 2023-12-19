from minio import Minio
import urllib.request
import pandas as pd
import sys
from urllib.parse import urljoin
from bs4 import BeautifulSoup  
import os
import requests 

def main():
    #grab_data()
    write_data_minio()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    download_folder = r"C:\Users\yacin\OneDrive\Bureau\EPSI 2023-2024\ATELIER ARCHITECTURE DECISIONNEL- DATAMART\ATL-Datamart\data\raw"

    # Créer le fichier téléchargé s'il n'existe pas
    os.makedirs(download_folder, exist_ok=True)

    # Envoyer une requête GET à l'URL
    response = requests.get(url)

    # Vérifier si la requête est acceptée(status code 200)
    if response.status_code == 200:
        # Parser le contenu html de la page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Trouver tous les liens dans la page
        links = soup.find_all('a')

        # boucler sur les liens et télécharger les fichiers désirés
        for link in links:
            href = link.get('href')
            if href and "yellow_tripdata" in href and any(str(year) in href for year in range(2018, 2024)):
                file_url = urljoin(url, href)
                file_name = os.path.join(download_folder, href.split("/")[-1])

                print(f"Downloading {file_name}...")

                # Télécharger le fichier
                file_content = requests.get(file_url).content

                # enregistrer le fichier dans le dossier souhaté
                with open(file_name, 'wb') as file:
                    file.write(file_content)

                print(f"{file_name} downloaded successfully.")

    else:
        print(f"Echec.Page non trouvée. Status code: {response.status_code}")
    
    


def write_data_minio():

    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "data"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)

    else:
        print("Bucket " + bucket + " existe déjà")

    local_directory = './data/raw/'

    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            local_path = os.path.join(root, filename)
            object_name = os.path.relpath(local_path, local_directory)

            client.fput_object(bucket, object_name, local_path)
            print(f"Fichier {object_name} ajouté au bucket {bucket}")


if __name__ == '__main__':
    sys.exit(main())
    