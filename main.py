from firstIngestion import ingest_blockchain
from create_cluster import run_clusterizer
from db.postgres_conn import create_indexes_events_table
import os
import time
from datetime import timedelta

def format_time(seconds):
   return str(timedelta(seconds=seconds))

def main():
   # Gesamtzeit starten
   total_start_time = time.time()
   
   # Erstelle absoluten Pfad für clusters.csv im aktuellen Verzeichnis
   current_dir = os.path.dirname(os.path.abspath(__file__))
   clusters_csv_path = os.path.join(current_dir, "clusters.csv")
   
   try:
       print("Start Clusterizer")
       cluster_start_time = time.time()
       
       run_clusterizer(clusters_csv_path)
       
       cluster_time = time.time() - cluster_start_time
       print(f"Clusterizing finished - Dauer: {format_time(cluster_time)}")

       print("\nStart ingesting blockchain in db")
       ingest_start_time = time.time()
       
       ingest_blockchain(clusters_csv_path)
       
       ingest_time = time.time() - ingest_start_time
       print(f"Ingesting finished - Dauer: {format_time(ingest_time)}")

       print("\nStart creating indexes")
       index_start_time = time.time()
       
       create_indexes_events_table()
       
       index_time = time.time() - index_start_time
       print(f"Creating indexes finished - Dauer: {format_time(index_time)}")

       # Gesamtzeit berechnen
       total_time = time.time() - total_start_time
       
       print("\n----- Zusammenfassung -----")
       print(f"Clusterizing:     {format_time(cluster_time)}")
       print(f"Blockchain Ingest: {format_time(ingest_time)}")
       print(f"Index Creation:    {format_time(index_time)}")
       print(f"Gesamtzeit:       {format_time(total_time)}")
       
   except Exception as e:
       total_time = time.time() - total_start_time
       print(f"\nProgramm fehlgeschlagen nach {format_time(total_time)}: {e}")
       exit(1)

if __name__ == "__main__":
   main()