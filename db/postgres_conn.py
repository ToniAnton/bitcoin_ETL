from multiprocessing import Pool
from typing import List
import psycopg2


conn_params = {
    "dbname": "bitcoin",
    "user": "postgres",
    "password": "antondavid139",
    "host": "localhost"
}

def create_indexes_events_table():
   conn = psycopg2.connect(**conn_params)
   cur = conn.cursor()
   indexes = [
        "CREATE INDEX IF NOT EXISTS idx_events_address ON events (address)",
        "CREATE INDEX IF NOT EXISTS idx_events_txid ON events (txid)",
        "CREATE INDEX IF NOT EXISTS idx_events_address_prefix ON events (address_prefix)",
        "CREATE INDEX IF NOT EXISTS idx_events_txid_prefix ON events (txid_prefix)", 
        "CREATE INDEX IF NOT EXISTS idx_events_cluster_id ON events (cluster_id)",
        "CREATE INDEX IF NOT EXISTS idx_events_block_height ON events (block_height)",
        "CREATE INDEX IF NOT EXISTS idx_events_cluster_time ON events (cluster_id, time DESC)",
        "CREATE INDEX IF NOT EXISTS idx_events_address_time ON events (address, time DESC)"
   ]
   try:
       for index in indexes:
           print(f"Creating index: {index}")
           cur.execute(index)
           conn.commit()
   except Exception as e:
       print(f"Error creating index: {e}")
       conn.rollback()
   finally:
       cur.close()
       conn.close()

