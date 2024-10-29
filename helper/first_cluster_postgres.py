import os
import uuid
import tqdm
import psycopg2
import io
import sys
import gc
import shelve
import numpy as np

class DisjointSet:
    def __init__(self, shelve_path='disjoint_set_data'):
        # Verwenden von shelve für persistente Speicherung
        self.address_to_id = shelve.open(f'{shelve_path}_address_to_id', writeback=False)
        self.id_to_address = shelve.open(f'{shelve_path}_id_to_address', writeback=False)
        self.label = shelve.open(f'{shelve_path}_label', writeback=False)
        self.next_id = len(self.id_to_address)  # Nächste verfügbare Integer-ID

        # Pfade für die memmap-Dateien
        self.parent_file = f'{shelve_path}_parent.dat'
        self.rank_file = f'{shelve_path}_rank.dat'

        # Initialisierung der memmap-Arrays
        if os.path.exists(self.parent_file) and os.path.exists(self.rank_file):
            # Vorhandene Dateien öffnen
            self.parent = np.memmap(self.parent_file, dtype='uint64', mode='r+')
            self.rank = np.memmap(self.rank_file, dtype='uint8', mode='r+')
            self.next_id = self.parent.shape[0]
        else:
            # Arrays werden erst bei Bedarf initialisiert
            self.parent = None
            self.rank = None

    def get_id(self, address):
        if address not in self.address_to_id:
            id = self.next_id
            self.address_to_id[address] = id
            self.id_to_address[str(id)] = address  # Schlüssel als String speichern

            if self.parent is None:
                # Initialisiere die Arrays mit einer Startgröße
                initial_size = 100000  # Startgröße für Arrays
                self.parent = np.memmap(self.parent_file, dtype='uint64', mode='w+', shape=(initial_size,))
                self.rank = np.memmap(self.rank_file, dtype='uint8', mode='w+', shape=(initial_size,))
                self.parent[:] = np.arange(initial_size)  # Elternknoten initialisieren
                self.rank[:] = 0  # Ränge initialisieren
            elif id >= self.parent.shape[0]:
                # Arrays vergrößern
                new_size = self.parent.shape[0] + 100000
                self.parent.flush()
                self.rank.flush()
                # Neue Arrays mit größerer Größe erstellen
                self.parent = np.memmap(self.parent_file, dtype='uint64', mode='r+', shape=(new_size,))
                self.rank = np.memmap(self.rank_file, dtype='uint8', mode='r+', shape=(new_size,))
                # Neue Einträge initialisieren
                self.parent[self.next_id:new_size] = np.arange(self.next_id, new_size)
                self.rank[self.next_id:new_size] = 0

            self.next_id += 1
        else:
            id = self.address_to_id[address]
        return id

    def find(self, x):
        parent = self.parent
        root = x
        while parent[root] != root:
            root = parent[root]
        # Pfadkomprimierung
        while parent[x] != x:
            parent_x = parent[x]
            parent[x] = root
            x = parent_x
        return root

    def union(self, x, y):
        parent = self.parent
        rank = self.rank

        root_x = self.find(x)
        root_y = self.find(y)
        if root_x == root_y:
            return

        rank_x = rank[root_x]
        rank_y = rank[root_y]

        label_x = self.label.get(str(root_x), "")
        label_y = self.label.get(str(root_y), "")

        if rank_x > rank_y:
            parent[root_y] = root_x
            new_label = self.choose_label(label_x, label_y)
            if new_label:
                self.label[str(root_x)] = new_label
            # Speicher freigeben, wenn möglich
            if str(root_y) in self.label:
                del self.label[str(root_y)]
        else:
            parent[root_x] = root_y
            if rank_x == rank_y and rank[root_y] < 255:
                rank[root_y] += 1  # Vermeide Überlauf
            new_label = self.choose_label(label_x, label_y)
            if new_label:
                self.label[str(root_y)] = new_label
            # Speicher freigeben, wenn möglich
            if str(root_x) in self.label:
                del self.label[str(root_x)]  # <-- Hier war der Fehler


    def choose_label(self, label_x, label_y):
        if label_x and not label_y:
            return label_x
        elif label_y and not label_x:
            return label_y
        elif label_x and label_y:
            return label_x  # Priorität für label_x
        else:
            return ""

    def close(self):
        self.address_to_id.close()
        self.id_to_address.close()
        self.label.close()
        if self.parent is not None:
            self.parent.flush()
            self.rank.flush()
            del self.parent
            del self.rank

    def sync(self):
        if self.address_to_id is not None:
            self.address_to_id.sync()
        if self.id_to_address is not None:
            self.id_to_address.sync()
        if self.label is not None:
            self.label.sync()
        if self.parent is not None:
            self.parent.flush()
        if self.rank is not None:
            self.rank.flush()

def process_transactions(ds, transactions):
    for tx in transactions:
        inputs = tx["inputs"]
        unique_addresses = {inp["address"] for inp in inputs}
        addresses = list(unique_addresses)
        if not addresses:
            continue
        first_address = addresses[0]
        id_first = ds.get_id(first_address)

        for address in addresses[1:]:
            id_other = ds.get_id(address)
            ds.union(id_first, id_other)

def load_inputs(from_block, to_block):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="bitcoin",
            user="postgres",
            password="antondavid139"
        )
        cursor = conn.cursor()
        query = f"""
            SELECT DISTINCT e.txid, e.address
            FROM events e
            JOIN (
                SELECT txid, COUNT(DISTINCT address) AS txid_count
                FROM events
                WHERE block_height >= {from_block}
                  AND block_height < {to_block}
                  AND effect = -1
                  AND address != 'the-void'
                GROUP BY txid
            ) tx_count ON e.txid = tx_count.txid
            WHERE e.block_height >= {from_block}
              AND e.block_height < {to_block}
              AND e.effect = -1
              AND e.address != 'the-void'
              AND tx_count.txid_count > 1;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        transactions_dict = {}
        for txid, address in rows:
            if txid not in transactions_dict:
                transactions_dict[txid] = {"txid": txid, "inputs": []}
            transactions_dict[txid]["inputs"].append({"address": address})

        # Umwandlung des Dictionarys in eine Liste
        transactions = list(transactions_dict.values())

        return transactions

    except Exception as e:
        print(f"Error occurred: {e}")
        return None

def cluster(start_block, end_block, batch_size, ds):
    blocks_processed = start_block
    total_blocks = end_block - start_block

    with tqdm.tqdm(total=total_blocks, desc="Clustering Blocks") as pbar:
        while blocks_processed < end_block:
            to_block = min(blocks_processed + batch_size, end_block)
            transactions = load_inputs(blocks_processed, to_block)
            if transactions:
                process_transactions(ds, transactions)
            pbar.update(to_block - blocks_processed)
            blocks_processed = to_block

            # Fortschritt speichern
            if blocks_processed % 10000 == 0:
                progress_file = f"progress_{blocks_processed}.txt"
                with open(progress_file, "w") as f:
                    f.write(f"Verarbeitete Blöcke: {blocks_processed}\n")
                    f.write(f"Anzahl Adressen: {ds.next_id}\n")
                    if ds.parent is not None:
                        mem_usage = (ds.parent.size * ds.parent.dtype.itemsize + ds.rank.size * ds.rank.dtype.itemsize) / (1024 * 1024)
                        f.write(f"Speicherverbrauch: {mem_usage:.2f} MB\n")
                # Speicherbereinigung
                ds.sync()
                gc.collect()

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS address_labels2 (
                address TEXT PRIMARY KEY,
                label TEXT
            );
        """)
        conn.commit()

def create_db_from_ds(ds, conn, batch_size=10000000):
    create_table_if_not_exists(conn)
    total_entries = len(ds.id_to_address)
    processed_entries = 0

    with tqdm.tqdm(total=total_entries, desc="Exporting to PostgreSQL") as pbar:
        cursor = conn.cursor()
        insert_query = "INSERT INTO address_labels2 (address, label) VALUES (%s, %s) ON CONFLICT (address) DO NOTHING"

        batch = []
        for id_str in ds.id_to_address:
            id = int(id_str)
            address = ds.id_to_address[id_str]
            root_id = ds.find(id)
            label = ds.label.get(str(root_id), None)
            if label is None:
                label = "Wallet-" + str(uuid.uuid4())
                ds.label[str(root_id)] = label
            batch.append((address, label))
            processed_entries += 1
            pbar.update(1)

            # Wenn die Batch-Größe erreicht ist, in die Datenbank schreiben
            if len(batch) >= batch_size:
                cursor.executemany(insert_query, batch)
                conn.commit()
                batch.clear()
                # Speicherbereinigung
                ds.sync()
                gc.collect()

        # Restliche Daten in die Datenbank schreiben
        if batch:
            cursor.executemany(insert_query, batch)
            conn.commit()
            batch.clear()
            ds.sync()
            gc.collect()

        cursor.close()
        ds.close()
        gc.collect()
        print(f"Export abgeschlossen.")

def main():
    start_block = 0
    end_block = 857672  # Nach Bedarf anpassen
    batch_size = 1000
    ds = DisjointSet()

    cluster(start_block, end_block, batch_size, ds)

    # Verbindung zur PostgreSQL-Datenbank herstellen
    conn = psycopg2.connect(
        host="localhost",
        database="bitcoin",
        user="postgres",
        password="antondavid139"
    )

    create_db_from_ds(ds, conn, batch_size=100000)

    conn.close()

if __name__ == "__main__":
    main()
