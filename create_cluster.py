import subprocess
import os
from pathlib import Path

def run_clusterizer(clusters_csv_path: str):
    path = Path("./rust_clusterizer/target/release/bitiodine-rust")
    if not path.exists():
        raise FileNotFoundError(f"Programm nicht gefunden in {path}")

    try:
        # Bestimme Bitcoin-Blocks-Verzeichnis
        home = os.path.expanduser("~")
        default_blocks_dir = os.path.join(home, ".bitcoin/blocks")
        
        # Führe mit Standardparametern aus
        cmd = [
            str(path),
            "--blocks-dir", default_blocks_dir,
            "--output", clusters_csv_path,
            "-v"  # Verbose Output
        ]
        
        print("Starte Clusterizer...")
        
        # Direkte Ausgabe zur Konsole
        process = subprocess.Popen(
            cmd,
            stdout=None,  # Direkt zur Konsole
            stderr=None,  # Direkt zur Konsole
            universal_newlines=True
        )
        
        # Warte auf Beendigung
        process.wait()
        
        # Prüfe Exit-Code
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)

    except subprocess.CalledProcessError as e:
        print(f"Fehler beim Ausführen von dem Clusterizer: {e}")
        raise
    except Exception as e:
        print(f"Unerwarteter Fehler: {e}")
        raise

def main():
    try:
        run_clusterizer()
    except Exception as e:
        print(f"Programm fehlgeschlagen: {e}")
        exit(1)

if __name__ == "__main__":
    main()