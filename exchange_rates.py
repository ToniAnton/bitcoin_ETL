#https://api.coindesk.com/v1/bpi/historical/close.json?currency=btc&start=2011-01-01&end=2024-05-07
#https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip
#https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical?symbol=BTC&convert=USD&time_start=2015-01-01&time_end=2024-05-07

import json
import requests
import datetime

def load_and_show_last_entry(file_path):
    # Datei öffnen und JSON-Daten laden
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Zugriff auf das 'bpi' Dictionary
    bpi_data = data.get('bpi', {})

    # Den letzten Schlüssel im Dictionary 'bpi' finden
    if bpi_data:
        last_date = sorted(bpi_data.keys())[-1]
        return last_date
    else:
        print("Keine Daten im 'bpi' Objekt.")
        return None

def add_new_data_to_json(file_path, new_data):
    try:
        # Laden der existierenden Daten aus der Datei
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Zugriff auf das 'bpi' Dictionary und Hinzufügen der neuen Daten
        if 'bpi' in data:
            data['bpi'].update(new_data)
        else:
            data['bpi'] = new_data

        # Die aktualisierten Daten zurückschreiben
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        
    except FileNotFoundError:
        print(f"Die Datei {file_path} wurde nicht gefunden. Eine neue Datei wird erstellt.")
        # Datei erstellen, wenn sie nicht existiert
        with open(file_path, 'w') as file:
            json.dump({'bpi': new_data}, file, indent=4)
        print("Neue Daten wurden in einer neuen Datei gespeichert.")
    except json.JSONDecodeError:
        print("Fehler beim Parsen der JSON-Daten in der Datei.")
    except Exception as e:
        print(f"Ein unerwarteter Fehler ist aufgetreten: {str(e)}")


def update_exchange_rates():
    # Aktuelles Datum abrufen
    today = datetime.datetime.now()
    
    # Datum im gewünschten Format ausgeben
    datum = today.strftime("%Y-%m-%d")
    last_updated = load_and_show_last_entry("exchange_rates.json")
    url = f"https://rest.coinapi.io/v1/exchangerate/BTC/USD/history?apikey=DD507116-BB63-4054-A463-0956323E41FD&period_id=1DAY&time_start={last_updated}T00:00:00&time_end={datum}T00:00:00"
    response = requests.get(url)
        
    if response.status_code == 200:
        data = response.json()
        new_json = {}
        for entry in data:
            value = round(entry["rate_open"], 2)
            timestamp = entry["time_period_start"][:10]
            new_json[timestamp] = value

        # Dateipfad für die JSON-Datei
        file_path = "exchange_rates.json"
        # Neue Daten zur JSON-Datei hinzufügen
        add_new_data_to_json(file_path, new_json)
    else:
        print(f"Failed to retrieve data: {response.status_code}")
   

def main():
    update_exchange_rates()
    #get_exchange_rates()

if __name__ == "__main__":
    main()
