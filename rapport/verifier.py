"""Exécute une requête DAX sur le modèle sémantique Ponctualite via la CLI Fabric.

Usage : python rapport/verifier.py <id du modèle> "EVALUATE ROW(\"x\", [Ponctualité])"
Le fichier envoyé est en ASCII pur (accents échappés), sinon la CLI les déforme.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ESPACE = "6501a002-4c79-4ada-aa1e-3b65a5bf1f9b"
FAB = Path(__file__).parent.parent / "fab.ps1"

id_modele, requete = sys.argv[1], sys.argv[2]
with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="ascii") as f:
    json.dump({"queries": [{"query": requete}]}, f, ensure_ascii=True)
sortie = subprocess.run(
    ["powershell", "-NoProfile", "-File", str(FAB), "api", "-A", "powerbi",
     f"groups/{ESPACE}/datasets/{id_modele}/executeQueries", "-X", "post", "-i", f.name, "--output_format", "json"],
    capture_output=True, text=True, encoding="utf-8",
)
resultat = json.loads(sortie.stdout)["result"]["data"][0]
if resultat["status_code"] != 200:
    print(json.dumps(resultat["text"], ensure_ascii=False, indent=1))
    sys.exit(1)
for ligne in resultat["text"]["results"][0]["tables"][0]["rows"]:
    print(json.dumps(ligne, ensure_ascii=False))
