"""Contrôle la définition d'un pipeline Fabric versionné.

Usage : python scripts/controler_pipeline.py <dossier du pipeline>
        python scripts/controler_pipeline.py <dossier du pipeline> --espace <id de l'espace>
        python scripts/controler_pipeline.py <dossier du pipeline> --deploye <dossier exporté par « fab export »>

La définition d'un pipeline désigne chaque notebook par son identifiant dans l'espace de travail. Un identifiant faux
ne se voit pas à l'import : le pipeline échoue seulement à l'exécution. Ce script le détecte avant.

- Sans option : chaque activité notebook porte le nom d'un dossier `fabric/<nom>.Notebook`, et chaque dépendance
  désigne une activité qui existe.
- `--espace` : l'identifiant de chaque notebook est bien celui du notebook de ce nom dans l'espace de travail.
  La CLI Fabric doit être connectée ; son exécutable est « fab », ou celui que désigne la variable d'environnement FAB.
- `--deploye` : le pipeline déployé, exporté au préalable, est bien celui du dépôt. Le pipeline est importé à la main
  et non par le flux de déploiement, car ses notebooks s'exécutent sous l'identité de qui l'a modifié en dernier.
"""

import argparse
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


def lire(dossier):
    return json.loads((Path(dossier) / "pipeline-content.json").read_text(encoding="utf-8"))


def main():
    arguments = argparse.ArgumentParser(description="Contrôle la définition d'un pipeline Fabric versionné.")
    arguments.add_argument("dossier", help="dossier du pipeline dans le dépôt")
    arguments.add_argument("--espace", help="id de l'espace de travail, pour contrôler les identifiants des notebooks")
    arguments.add_argument("--deploye", help="dossier du pipeline exporté de l'espace de travail, à comparer au dépôt")
    options = arguments.parse_args()

    dossier = Path(options.dossier)
    definition = lire(dossier)
    activites = definition["properties"]["activities"]
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

    if options.espace and not erreurs:
        reels = notebooks_de_l_espace(options.espace)
        for a in notebooks:
            proprietes = a["typeProperties"]
            if proprietes.get("workspaceId") != options.espace:
                erreurs.append(f"{a['name']} : espace de travail {proprietes.get('workspaceId')} au lieu de {options.espace}")
            if a["name"] not in reels:
                erreurs.append(f"{a['name']} : notebook absent de l'espace de travail")
            elif proprietes.get("notebookId") != reels[a["name"]]:
                erreurs.append(f"{a['name']} : identifiant {proprietes.get('notebookId')} au lieu de {reels[a['name']]}")

    if options.deploye and not erreurs and lire(options.deploye) != definition:
        erreurs.append(
            "le pipeline déployé n'est pas celui du dépôt. Le réimporter à la main, sous un compte utilisateur :\n"
            f"    fab import \"<espace>.Workspace/{dossier.name}\" -i {dossier.as_posix()} -f"
        )

    if erreurs:
        sys.exit(f"{dossier.name} :\n  " + "\n  ".join(erreurs))
    print(f"{dossier.name} : {len(activites)} activités, dont {len(notebooks)} notebooks ({' → '.join(noms)})")
    if options.espace:
        print("Identifiants des notebooks conformes à l'espace de travail")
    if options.deploye:
        print("Pipeline déployé identique à celui du dépôt")


if __name__ == "__main__":
    main()
