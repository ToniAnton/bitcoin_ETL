import psycopg2
import sys
import os
from psycopg2 import sql
import tqdm

class TqdmFile:
    """
    Ein Wrapper um eine Datei, der tqdm bei jedem Lesevorgang aktualisiert.
    """
    def __init__(self, f, pbar):
        self.f = f
        self.pbar = pbar

    def read(self, size):
        data = self.f.read(size)
        if data:
            self.pbar.update(len(data))
        return data

    def readline(self, size=-1):
        data = self.f.readline(size)
        if data:
            self.pbar.update(len(data))
        return data

    def __getattr__(self, attr):
        return getattr(self.f, attr)

def create_table_if_not_exists(conn):
    """
    Erstellt die Tabelle address_label, falls sie nicht existiert.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS address_label (
        address TEXT NOT NULL,
        label TEXT NOT NULL,
        last_label TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()

def import_csv_to_postgres(csv_file_path, conn):
    """
    Importiert die CSV-Datei in die PostgreSQL-Datenbank mittels COPY,
    mit einer tqdm Fortschrittsanzeige.
    """
    # Gesamtgröße der Datei in Bytes für die Fortschrittsanzeige
    file_size = os.path.getsize(csv_file_path)
    
    with open(csv_file_path, 'rb') as f, conn.cursor() as cur:
        with tqdm.tqdm(total=file_size, unit='B', unit_scale=True, desc='Importiere CSV') as pbar:
            # Wrappen der Datei mit TqdmFile, um den Fortschritt zu aktualisieren
            tqdm_file = TqdmFile(f, pbar)
            copy_sql = """
            COPY address_label(address, label)
            FROM STDIN WITH (FORMAT csv, HEADER false)
            """
            try:
                cur.copy_expert(copy_sql, tqdm_file)
                conn.commit()
                print("Daten erfolgreich importiert.")
            except Exception as e:
                conn.rollback()
                print(f"Fehler beim Importieren der Daten: {e}")
                sys.exit(1)

def create_indexes(conn):
   """
   Erstellt die benötigten Indexes auf der address_label Tabelle.
   """
   index_queries = [
       "CREATE INDEX IF NOT EXISTS idx_address_label_address ON address_label(address);",
       "CREATE INDEX IF NOT EXISTS idx_address_label_label ON address_label(label);",
   ]
   
   with conn.cursor() as cur:
       for query in index_queries:
           try:
               print(f"Erstelle Index: {query}")
               cur.execute(query)
               conn.commit()
           except Exception as e:
               conn.rollback()
               print(f"Fehler beim Erstellen des Index: {e}")
               raise


def main():
    # Pfad zur CSV-Datei
    csv_file_path = 'files/clustering/clusters.csv'  # Passen Sie den Pfad entsprechend an

    # Überprüfen, ob die CSV-Datei existiert
    if not os.path.exists(csv_file_path):
        print(f"CSV-Datei '{csv_file_path}' wurde nicht gefunden.")
        sys.exit(1)

    # Datenbankverbindungsparameter
    db_params = {
        'host': 'localhost',
        'port': '5432',
        'dbname': 'bitcoin',
        'user': 'postgres',
        'password': 'antondavid139'
    }

    try:
        # Verbindung zur PostgreSQL-Datenbank herstellen
        conn = psycopg2.connect(**db_params)
    except Exception as e:
        print(f"Verbindung zur Datenbank fehlgeschlagen: {e}")
        sys.exit(1)

    try:
        # Tabelle erstellen, falls nicht vorhanden
        create_table_if_not_exists(conn)

        # Import der CSV-Datei mit Fortschrittsanzeige
        import_csv_to_postgres(csv_file_path, conn)

        print("Erstelle Indexes...")
        create_indexes(conn)
        print("Indexes erfolgreich erstellt.")

    finally:
        # Verbindung schließen
        conn.close()

if __name__ == "__main__":
    main()
