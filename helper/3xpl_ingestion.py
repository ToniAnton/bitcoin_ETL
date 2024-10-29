import requests
import json
import os
import zstandard as zstd
from db.postgres_conn import ingest_tsv
from tqdm import tqdm
import shutil

# Uses the 3xplorer API to get the download links for the dumps

token = '3A0_t3st3xplor3rpub11cb3t4efcd21748a5e'
url = f'https://api.3xpl.com/dumps?from=bitcoin-main&mode=all&token={token}'

def get_dumps():
    response = requests.get(url)
    try:
        data = json.loads(response.text)
      
        # Accessing nested dictionary of dumps
        dumps_dict = data["data"]["dumps"]["bitcoin-main"]
        links = []

        # Iterating over each date key in the dictionary
        for date, details in dumps_dict.items():
            links.append(details["link"])

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    return links

def download_dumps():
    links = get_dumps()
    # Erstelle den Ordner 'dumps' falls er nicht existiert
    if not os.path.exists('dumps'):
        os.makedirs('dumps')

    for link in links:
        # Dateiname aus dem Link extrahieren
        filename = link.split('/')[-1]
        # Vollständiger Pfad zum Speichern der Datei
        file_path = os.path.join('dumps', filename)
        # Hinzufügen des Tokens zum Link
        download_link = link + '?token=3D0_t3st3xplor3rpub11cb3t437b1227878a1'

        # Anfordern der Datei vom Server
        response = requests.get(download_link, stream=True)
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    # Schreibe den Inhalt in eine neue Datei
                    f.write(chunk)
            print(f"Downloaded {filename} to dumps/")
        else:
            print(f"Failed to download {filename}. Status code: {response.status_code}")

def ingest_dumps():
    dumps = os.listdir('dumps')
    count_dumps = len(dumps)

    with tqdm(total=count_dumps) as pbar:
        for dump in dumps:
            # Überprüfe, ob die Datei ein .zst-Archiv ist
            if dump.endswith('.tsv.zst'):
                # Pfad der originalen komprimierten Datei
                zst_path = os.path.join('dumps', dump)
                # Pfad der dekomprimierten Datei
                tsv_path = os.path.splitext(zst_path)[0]  # Entfernt die .zst Endung

                # Öffne die komprimierte Datei und das Ziel für die extrahierte Datei
                with open(zst_path, 'rb') as compressed, open(tsv_path, 'wb') as decompressed:
                    # Erstelle einen Dekompressor
                    dctx = zstd.ZstdDecompressor()
                    # Führe die Dekompression aus und schreibe das Ergebnis
                    dctx.copy_stream(compressed, decompressed)
                ingest_tsv(tsv_path)
                os.remove(tsv_path)

                ingested_path = os.path.join('dumps', 'ingested')
                shutil.move(zst_path, ingested_path)
                pbar.update(1)
        print("Ingestion complete")
        
def main():
    ingest_dumps()

if __name__ == '__main__':
    main()
