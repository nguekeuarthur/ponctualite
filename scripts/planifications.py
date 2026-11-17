"""Relève l'état des planifications des notebooks d'un espace de travail Fabric, ou le compare à un relevé précédent.

Usage : python scripts/planifications.py <id de l'espace> releve.json              écrit le relevé
        python scripts/planifications.py <id de l'espace> --comparer releve.json   échoue si une planification a changé

Le déploiement continu réimporte les notebooks : ce script vérifie qu'aucune planification n'a disparu ni été
désactivée au passage. La CLI Fabric doit être connectée ; son exécutable est « fab », ou celui que désigne la
variable d'environnement FAB.
"""

import json
import os
import subprocess
import sys

FAB = os.environ.get("FAB", "fab")


def api(chemin):
    sortie = subprocess.run([FAB, "api", chemin, "--output_format", "json"], capture_output=True, text=True, encoding="utf-8")
    try:
        reponse = json.loads(sortie.stdout)["result"]["data"][0]
    except (ValueError, KeyError, IndexError):
        sys.exit(f"Réponse illisible pour {chemin} : {sortie.stdout[:300]} {sortie.stderr[:300]}")
    if reponse["status_code"] != 200:
        sys.exit(f"Erreur {reponse['status_code']} pour {chemin} : {str(reponse.get('text'))[:300]}")
    return reponse["text"]


def relever(espace):
    """Par notebook, la liste de ses planifications : identifiant, active ou non, rythme."""
    etat = {}
    for element in api(f"workspaces/{espace}/items?type=Notebook").get("value", []):
        planifications = api(f"workspaces/{espace}/items/{element['id']}/jobs/RunNotebook/schedules").get("value", [])
        etat[element["displayName"]] = sorted(
            (
                {
                    "id": p["id"],
                    "active": bool(p.get("enabled")),
                    "etat": (p.get("enabledDetail") or {}).get("enabledEvalState"),
                    "rythme": f"{p['configuration'].get('type')} {p['configuration'].get('times') or p['configuration'].get('interval')}",
                }
                for p in planifications
            ),
            key=lambda p: p["id"],
        )
    return etat


def afficher(etat):
    for notebook in sorted(etat):
        if not etat[notebook]:
            print(f"  {notebook} : aucune planification")
        for p in etat[notebook]:
            print(f"  {notebook} : {p['rythme']}, {'active' if p['active'] else 'INACTIVE'} ({p['etat']})")


def main():
    if len(sys.argv) == 3:
        etat = relever(sys.argv[1])
        with open(sys.argv[2], "w", encoding="utf-8") as f:
            json.dump(etat, f, ensure_ascii=False, indent=2)
        print("Planifications relevées :")
        afficher(etat)
    elif len(sys.argv) == 4 and sys.argv[2] == "--comparer":
        with open(sys.argv[3], encoding="utf-8") as f:
            avant = json.load(f)
        apres = relever(sys.argv[1])
        print("Planifications après le déploiement :")
        afficher(apres)
        ecarts = []
        for notebook, planifications in avant.items():
            restantes = {p["id"]: p for p in apres.get(notebook, [])}
            for p in planifications:
                if p["id"] not in restantes:
                    ecarts.append(f"{notebook} : la planification « {p['rythme']} » a disparu")
                elif p["active"] and not restantes[p["id"]]["active"]:
                    ecarts.append(f"{notebook} : la planification « {p['rythme']} » a été désactivée")
        if ecarts:
            sys.exit("Planifications modifiées par le déploiement :\n" + "\n".join(ecarts))
        print("Aucune planification n'a disparu ni été désactivée.")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
