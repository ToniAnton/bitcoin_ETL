import bitcoin_explorer as bex
import os
import tqdm
import json
from exchange_rates import update_exchange_rates, load_and_show_last_entry
import datetime
import psycopg2
from io import StringIO
import sys
import gc
import pandas as pd
import requests

conn = psycopg2.connect(
    host="localhost",
    database="bitcoin",
    user="postgres",
    password="antondavid139"
)

bitcoin_path = os.path.expanduser("~/.bitcoin")
db = bex.BitcoinDB(bitcoin_path, tx_index=True)

def load_cluster_csv(file_path):
    data_dict = {}
    try:
        # Gesamtanzahl der Zeilen ermitteln
        with open(file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        
        chunksize = 10 ** 6  # Größe der Datenblöcke (anpassbar)
        with tqdm.tqdm(total=total_lines, desc="Laden der Cluster-Daten") as pbar:
            for chunk in pd.read_csv(file_path, header=None, names=["address", "label"], chunksize=chunksize):
                data_dict.update(zip(chunk["address"], chunk["label"]))
                pbar.update(len(chunk))
    except Exception as e:
        print(f"Fehler beim Laden der CSV-Datei: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"CSV-Datei erfolgreich in ein Dictionary geladen. Gesamtanzahl der Einträge: {len(data_dict)}", file=sys.stderr)
    return data_dict

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS events (
            network TEXT,
            block_height INTEGER,
            txid CHAR(64),
            txid_prefix CHAR(7),
            sort_key INTEGER,
            time TIMESTAMPTZ NOT NULL,
            address VARCHAR(64),
            address_prefix CHAR(9),
            currency CHAR(3),
            effect SMALLINT,
            value_btc BIGINT,
            value_usd DECIMAL(16,2),
            cluster_id BIGINT,
            is_change BOOLEAN
        );
        """
        cur.execute(create_table_query)
        
    try:
        cur.execute("""
            SELECT create_hypertable('events', 'block_height', 
                chunk_time_interval => 10000,  -- Etwa 10.000 Blöcke pro Chunk
                if_not_exists => TRUE);
            """)
            
        # Kompression aktivieren
        cur.execute("""
                    ALTER TABLE events SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'cluster_id,address',  
                        timescaledb.compress_orderby = 'time DESC'              
                    );
                """)
            
        cur.execute("""
            SELECT add_compression_policy('events', 
                INTERVAL '1 month',  -- Zeitbasierte Kompression
                if_not_exists => TRUE);
            """)
            
    except Exception as e:
            print(f"Fehler beim Erstellen der TimescaleDB-Konfiguration: {e}")
            
    conn.commit()

def determine_change_address(tx, inputs_info, outputs_info):
    """
    Bestimmt die Change-Adresse in einer Transaktion basierend auf verschiedenen Heuristiken.
    Gibt den Index des Outputs zurück, der die Change-Adresse ist, oder None, wenn keine bestimmt wurde.
    """
    input_labels, input_addresses, input_script_types, input_values = inputs_info
    output_values, output_addresses, output_labels, output_script_types = outputs_info

    num_inputs = len(input_addresses)
    num_outputs = len(output_addresses)
    coinbase = num_inputs == 0

    # Heuristik 0: Coinbase-Transaktion oder nur ein Output
    if num_outputs <= 1 or coinbase:
        return None

    # Heuristik 1: Gleiche Adressen
    for idx, address in enumerate(output_addresses):
        if address in input_addresses and address != "Unknown":
            return idx  # Abbrechen, sobald eine Adresse bestimmt wurde

    # Heuristik 1.1: Gleiche Labels
    for idx, label in enumerate(output_labels):
        if label in input_labels and label != "":
            return idx  # Abbrechen, sobald eine Adresse bestimmt wurde

    #Heuristik 2
    if num_inputs == 1 and num_outputs >= 2:
    # a) Skript-Typ
        input_script_types_set = set(input_script_types)
        if len(input_script_types_set) == 1:
            # Sammle alle Outputs, die den gleichen Skript-Typ haben wie der Input
            matching_outputs = [
                idx for idx, script_type in enumerate(output_script_types)
                if script_type == list(input_script_types_set)[0]
            ]
            # Überprüfe, ob genau ein Output diesen Skript-Typ hat
            if len(matching_outputs) == 1:
                return matching_outputs[0]  # Gebe den Index des Outputs zurück
                
    # Heuristik 3: Mehrere Eingaben, mehrere Ausgaben
    if num_inputs >= 2 and num_outputs >= 2:
        # A) Wenn alle Inputs den gleichen Skript-Typ haben und nur ein Output diesen Skript-Typ hat
        input_script_types_set = set(input_script_types)
        if len(input_script_types_set) == 1:
            # Sammle alle Outputs, die den gleichen Skript-Typ haben wie der Input
            matching_outputs = [
                idx for idx, script_type in enumerate(output_script_types)
                if script_type == list(input_script_types_set)[0]
            ]
   
            # Überprüfe, ob genau ein Output diesen Skript-Typ hat
            if len(matching_outputs) == 1:
                return matching_outputs[0]  # Gebe den Index des Outputs zurück
            
        # B) Iteriere über die Outputs und prüfe, ob ein Output kleiner ist als alle Inputs
        for idx, output_value in enumerate(output_values):
            if all(output_value < input_value for input_value in input_values):
                return idx 
            
        # C) Überprüfe, ob ein Output kleiner ist als der kleinste Input und alle anderen Outputs größer sind als der kleinste Input
        if input_values:
            min_input_value = min(input_values)
            for idx, value in enumerate(output_values):
                if value < min_input_value:
                    # Prüfe, ob alle anderen Outputs größer als der kleinste Input
                    other_outputs = [v for i, v in enumerate(output_values) if i != idx]
                    if all(other_value > min_input_value for other_value in other_outputs):
                        return idx
            
    return None  # Keine Change-Adresse bestimmt

def ingest_blockchain(clusters_csv_path, start_block = 0, stop_block = db.get_block_count()):
    clusters = load_cluster_csv(clusters_csv_path)
    clusters_get = clusters.get  # Cache die get-Methode
    date = load_and_show_last_entry("exchange_rates.json")

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    formatted_yesterday = yesterday.strftime("%Y-%m-%d")

    while str(date) != formatted_yesterday:
        update_exchange_rates()
        date = load_and_show_last_entry("exchange_rates.json")

    with open("exchange_rates.json") as f:
        exchange_rates = json.load(f)

    # Vorberechnung der Umrechnungsfaktoren
    exchange_rate_factors = {date_str: rate / 100000000 for date_str, rate in exchange_rates["bpi"].items()}

    create_table_if_not_exists(conn)
    cur = conn.cursor()

    batch_size = 500000  # Angepasste Batch-Größe
    csv_buffer = StringIO()
    row_count = 0

    csv_header = "network,block_height,txid,txid_prefix,sort_key,time,address,address_prefix,currency,effect,value_btc,value_usd,cluster_id,is_change\n"
    csv_buffer_write = csv_buffer.write  # Cache die write-Methode
    csv_buffer_write(csv_header)

    block_iter = db.get_block_iter_range(connected=True, simplify=True, start=start_block, stop=stop_block)
    total_blocks = stop_block - start_block

    csv_row_template = "{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n"

    with tqdm.tqdm(total=total_blocks, desc="Verarbeitung der Blöcke") as pbar:
        for block_height, block in enumerate(block_iter, start=start_block):
            pbar.update(1)
            txis = block["txdata"]
            sort_key = 1
            timestamp = block["header"]["time"]
            date = datetime.datetime.fromtimestamp(timestamp)
            formatted_date = date.strftime("%Y-%m-%d")
            exchange_rate_factor = exchange_rate_factors.get(formatted_date, 0)

            txid_slice = slice(0, 7)  # Für txid_prefix
            for tx in txis:
                txid = tx["txid"]
                txid_prefix = txid[txid_slice]
                inputs = tx.get("input", [])
                outputs = tx.get("output", [])
                inputs_value = 0
                outputs_value = 0
                coinbase = len(inputs) == 0

                # Sammle Informationen der Inputs
                input_labels = set()
                input_addresses = set()
                input_script_types = set()
                input_values = []

                for i in inputs:
                    addresses = i.get("addresses")
                    address = addresses[0] if addresses else "Unknown"
                   
                    label = clusters_get(address, "")
                    input_labels.add(label)
                    input_addresses.add(address)
                    input_values.append(i.get("value", 0))

                    if address.startswith("bc1q"):
                        script_type = "p2wpkh"
                    elif address.startswith("1"):
                        script_type = "p2pkh"
                    elif address.startswith("3"):
                        script_type = "p2sh"
                    elif address.startswith("bc1p"):
                        script_type = "taproot"
                    else:
                        script_type = "unknown"
                    input_script_types.add(script_type)

                # Verarbeitung der Inputs
                for i in inputs:
                    addresses = i.get("addresses")
                    address = addresses[0] if addresses else "Unknown"
                    value_btc = i.get("value", 0)
                    value_usd = round(value_btc * exchange_rate_factor, 2)
                    label = clusters_get(address, "")
                    address_prefix = address[:9] if address.startswith("bc1") else address[:6]
                    csv_row = csv_row_template.format(
                        "bitcoin-main", block_height, txid, txid_prefix, sort_key,
                        date, address, address_prefix, "BTC", -1, value_btc, value_usd, label, False
                    )
                    csv_buffer_write(csv_row)
                    sort_key += 1
                    row_count += 1
                    inputs_value += value_btc

                # Vorbereitung der Outputs
                output_values = []
                output_addresses = []
                output_labels = []
                output_script_types = []

                for o in outputs:
                    addresses = o.get("addresses")
                    address = addresses[0] if addresses else "Unknown"
                   
                    value_btc = o.get("value", 0)
                    output_values.append(value_btc)
                    output_addresses.append(address)
                    label = clusters_get(address, "")
                    output_labels.append(label)
                    if address.startswith("bc1q"):
                        script_type = "p2wpkh"
                    elif address.startswith("1"):
                        script_type = "p2pkh"
                    elif address.startswith("3"):
                        script_type = "p2sh"
                    elif address.startswith("bc1p"):
                        script_type = "taproot"
                    else:
                        script_type = "unknown"
                    output_script_types.append(script_type)
                    
                # Bestimme die Change-Adresse
                change_idx = determine_change_address(
                    tx,
                    (input_labels, input_addresses, input_script_types, input_values),
                    (output_values, output_addresses, output_labels, output_script_types)
                )

                # Verarbeitung der Outputs
                for idx, o in enumerate(outputs):
                    address = output_addresses[idx]
                    value_btc = output_values[idx]
                    value_usd = round(value_btc * exchange_rate_factor, 2)
                    label = output_labels[idx]
                    address_prefix = address[:9] if address.startswith("bc1") else address[:6]
                    is_change = idx == change_idx
                    csv_row = csv_row_template.format(
                        "bitcoin-main", block_height, txid, txid_prefix, sort_key,
                        date, address, address_prefix, "BTC", 1, value_btc, value_usd, label, is_change
                    )
                    csv_buffer_write(csv_row)
                    sort_key += 1
                    row_count += 1
                    outputs_value += value_btc

                # Verarbeitung der Gebühren
                if not coinbase and inputs_value > outputs_value:
                    fee_value_btc = inputs_value - outputs_value
                    fee_value_usd = round(fee_value_btc * exchange_rate_factor, 2)
                    csv_row = csv_row_template.format(
                        "bitcoin-main", block_height, txid, txid_prefix, sort_key,
                        date, "Fee", "", "BTC", 1, fee_value_btc, fee_value_usd, "", False
                    )
                    csv_buffer_write(csv_row)
                    sort_key += 1
                    row_count += 1

                # Batch-Verarbeitung
                if row_count >= batch_size:
                    csv_buffer.seek(0)
                    cur.copy_expert("COPY events FROM STDIN WITH (FORMAT csv, HEADER true)", csv_buffer)
                    conn.commit()
                    csv_buffer.truncate(0)
                    csv_buffer.seek(0)
                    csv_buffer_write(csv_header)
                    row_count = 0

            # Speicherbereinigung
            if block_height % 1000 == 0:
                gc.collect()

    # Verbleibende Daten kopieren
    if row_count > 0:
        csv_buffer.seek(0)
        cur.copy_expert("COPY events FROM STDIN WITH (FORMAT csv, HEADER true)", csv_buffer)
        conn.commit()
        csv_buffer.close()

    cur.close()
    conn.close()


def test_change_detection(txid):
    response = requests.get(f"http://localhost:8001/transaction/bitcoin/{txid}")
    
    if response.status_code != 200:
        print("Fehler beim Abrufen der Transaktionsdaten:")
        print(response.json())
    else:
        data = response.json()
        transaction = data.get("transaction", {}).get("transaction", {})

        if not transaction:
            print("Transaktion nicht gefunden.")
        else:
            # Extrahieren der Inputs und Outputs
            inputs = transaction.get("inputs", [])
            outputs = transaction.get("outputs", [])

            # Initialisieren der Sets und Listen
            input_labels = set()
            input_addresses = set()
            input_script_types = set()
            input_values = []
            output_values = []
            output_addresses = []
            output_labels = []
            output_script_types = []

            # Extrahieren der Input-Daten
            for inp in inputs:
                label = inp.get("label", "")
                if label:
                    input_labels.add(label)
                address = inp.get("address", "")
                if address:
                    input_addresses.add(address)
                    value = inp.get("value", 0)
                    # Bestimme den script_type basierend auf der Adresse
                    if address.startswith("bc1q"):
                        script_type = "p2wpkh"
                    elif address.startswith("1"):
                        script_type = "p2pkh"
                    elif address.startswith("3"):
                        script_type = "p2sh"
                    elif address.startswith("bc1p"):
                        script_type = "taproot"
                    else:
                        script_type = "unknown"
                    input_script_types.add(script_type)
                    input_values.append(value)

            # Extrahieren der Output-Daten
            for out in outputs:
                value = out.get("value", 0)
                output_values.append(value)
                address = out.get("address", "")
                if address:
                    if address.startswith("bc1q"):
                        script_type = "p2wpkh"
                    elif address.startswith("1"):
                        script_type = "p2pkh"
                    elif address.startswith("3"):
                        script_type = "p2sh"
                    elif address.startswith("bc1p"):
                        script_type = "taproot"
                    else:
                        script_type = "unknown"
                    output_script_types.append(script_type)
                else:
                    output_script_types.append("unknown")
                output_addresses.append(address)
                label = out.get("label", "")
                if label:
                    output_labels.append(label)
                else:
                    output_labels.append("")  # Leeres Label, falls nicht vorhanden
               
            # Aufruf der determine_change_address-Funktion
            change_idx = determine_change_address(
                transaction,
                (input_labels, input_addresses, input_script_types, input_values),
                (output_values, output_addresses, output_labels, output_script_types)
            )

            print(f"Change-Index: {change_idx}")

            # Optional: Markieren der Change-Adresse im output
            if change_idx is not None and 0 <= change_idx < len(outputs):
                outputs[change_idx]["change"] = True
                print(f"Change-Adresse ist: {outputs[change_idx].get('address')}")
            else:
                print("Keine Change-Adresse gefunden.")
