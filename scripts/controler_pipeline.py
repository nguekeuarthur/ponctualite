"""Contrôle la définition d'un pipeline Fabric versionné : il ne doit lancer que des notebooks du dépôt.

Usage : python scripts/controler_pipeline.py <dossier du pipeline>                           contrôle hors ligne
        python scripts/controler_pipeline.py <dossier du pipeline> --espace <id de l'espace>  et contrôle des identifiants

La définition d'un pipeline désigne chaque notebook par son identifiant dans l'espace de travail. Un identifiant faux
ne se voit pas à l'import : le pipeline échoue seulement à l'exécution. Ce script le détecte avant.

- Hors ligne : chaque activité notebook porte le nom d'un dossier `fabric/<nom>.Notebook`, et chaque dépendance
  désigne une activité qui existe.
- Avec `--espace` : l'identifiant de chaque notebook est bien celui du notebook de ce nom dans l'espace de travail.
  La CLI Fabric doit être connectée ; son exécutable est « fab », ou celui que désigne la variable d'environnement FAB.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

FAB = os.environ.get("FAB", "fab")
RACINE = Path(__file__).resolve().parent.parent


def notebooks_de_l_espace(espace):
    sortie = subprocess.run([FAB, "api", f"workspaces/{espace}/items?type=Notebook", "--output_format", "json"],
                            capture_output=True, text=True, encoding="utf-8")
    try:
        reponse = json.loads(sortie.stdout)["result"]["data"][0]
    except (ValueError, KeyError, IndexError):
        sys.exit(f"Réponse illisible de la CLI Fabric : {sortie.stdout[:300]} {sortie.stderr[:300]}")
    if reponse["status_code"] != 200:
        sys.exit(f"Erreur {reponse['status_code']} : {str(reponse.get('text'))[:300]}")
    return {e["displayName"]: e["id"] for e in reponse["text"].get("value", [])}


def main():
    if len(sys.argv) not in (2, 4) or (len(sys.argv) == 4 and sys.argv[2] != "--espace"):
        sys.exit(__doc__)
    dossier = Path(sys.argv[1])
    espace = sys.argv[3] if len(sys.argv) == 4 else None
    activites = json.loads((dossier / "pipeline-content.json").read_text(encoding="utf-8"))["properties"]["activities"]
    noms = [a["name"] for a in activites]
    erreurs = [f"activité en double : {n}" for n in sorted(set(noms)) if noms.count(n) > 1]

    for a in activites:
        for d in a.get("dependsOn", []):
            if d["activity"] not in noms:
                erreurs.append(f"{a['name']} dépend de « {d['activity']} », qui n'existe pas")

    notebooks = [a for a in activites if a["type"] == "TridentNotebook"]
    for a in notebooks:
        if not (RACINE / "fabric" / f"{a['name']}.Notebook").is_dir():
            erreurs.append(f"{a['name']} : aucun dossier fabric/{a['name']}.Notebook dans le dépôt")

    if espace and not erreurs:
        reels = notebooks_de_l_espace(espace)
        for a in notebooks:
            proprietes = a["typeProperties"]
            if proprietes.get("workspaceId") != espace:
                erreurs.append(f"{a['name']} : espace de travail {proprietes.get('workspaceId')} au lieu de {espace}")
            if a["name"] not in reels:
                erreurs.append(f"{a['name']} : notebook absent de l'espace de travail")
            elif proprietes.get("notebookId") != reels[a["name"]]:
                erreurs.append(f"{a['name']} : identifiant {proprietes.get('notebookId')} au lieu de {reels[a['name']]}")

    if erreurs:
        sys.exit(f"{dossier.name} :\n  " + "\n  ".join(erreurs))
    enchainement = " → ".join(noms)
    print(f"{dossier.name} : {len(activites)} activités, dont {len(notebooks)} notebooks ({enchainement})")
    print("Identifiants des notebooks conformes à l'espace de travail" if espace else "Contrôle hors ligne réussi")


if __name__ == "__main__":
    main()
