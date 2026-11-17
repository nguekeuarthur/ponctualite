"""Capture des pages du rapport Power BI en images, pour le README.

Usage : python scripts/capturer_rapport.py <id de l'espace> <id du rapport> <dossier de sortie> <page> [<page> ...]
Exemple : python scripts/capturer_rapport.py <espace> <rapport> docs trajets porte demain jugement meteo

Le rapport déployé est exporté page par page en PDF par l'API Power BI, puis chaque page est convertie en PNG.
L'export direct en image est souvent désactivé au niveau du locataire, alors que l'export en PDF est permis par défaut.

À lancer sur un rapport déployé en mode public (`rapport/generer_rapport.py <id du modèle> --public`), pour qu'aucun
arrêt ni horaire n'apparaisse, puis redéployer le rapport habituel. Relire chaque image avant de la publier.

Prérequis : une session Azure CLI ouverte sur le locataire (`az login`) et les paquets `pypdfium2` et `pillow`.
Le jeton d'accès est demandé à Azure CLI et n'est jamais affiché.
"""

import io
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

LARGEUR = 1600  # largeur des images, en pixels


def jeton():
    sortie = subprocess.run(
        "az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv",
        capture_output=True, text=True, shell=True,
    )
    if not sortie.stdout.strip():
        sys.exit("Pas de jeton : ouvre une session avec « az login ». " + sortie.stderr[:300])
    return sortie.stdout.strip()


def appel(url, acces, corps=None, brut=False):
    donnees = json.dumps(corps).encode() if corps is not None else None
    requete = urllib.request.Request(url, data=donnees, headers={"Authorization": "Bearer " + acces, "Content-Type": "application/json"})
    for _ in range(5):  # une coupure réseau passagère ne doit pas interrompre l'export
        try:
            with urllib.request.urlopen(requete, timeout=120) as reponse:
                contenu = reponse.read()
                return reponse.status, (contenu if brut else (json.loads(contenu) if contenu else {}))
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")[:600]
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            print(f"  coupure réseau ({e}), nouvel essai dans 10 s")
            time.sleep(10)
    return 0, "réseau indisponible"


def exporter_pdf(api, acces, page):
    code, reponse = appel(api + "/ExportTo", acces, {
        "format": "PDF",
        "powerBIReportConfiguration": {"pages": [{"pageName": page}], "settings": {"locale": "fr-FR"}},
    })
    if code not in (200, 202):
        sys.exit(f"{page} : export refusé ({code}) : {reponse}")
    identifiant, etat, debut = reponse["id"], reponse.get("status"), time.time()
    while etat in ("NotStarted", "Running", None) and time.time() - debut < 240:
        time.sleep(5)
        code, reponse = appel(f"{api}/exports/{identifiant}", acces)
        etat = reponse.get("status") if isinstance(reponse, dict) else f"erreur {code}"
    if etat != "Succeeded":
        sys.exit(f"{page} : export non abouti ({etat})")
    code, fichier = appel(f"{api}/exports/{identifiant}/file", acces, brut=True)
    if code != 200:
        sys.exit(f"{page} : téléchargement refusé ({code})")
    if fichier[:2] == b"PK":  # plusieurs fichiers dans une archive
        with zipfile.ZipFile(io.BytesIO(fichier)) as archive:
            fichier = archive.read(next(n for n in archive.namelist() if n.lower().endswith(".pdf")))
    return fichier


def main():
    if len(sys.argv) < 5:
        sys.exit(__doc__)
    import pypdfium2 as pdfium  # importé ici : le reste du dépôt n'en dépend pas

    espace, rapport, dossier, pages = sys.argv[1], sys.argv[2], Path(sys.argv[3]), sys.argv[4:]
    dossier.mkdir(parents=True, exist_ok=True)
    api = f"https://api.powerbi.com/v1.0/myorg/groups/{espace}/reports/{rapport}"
    acces = jeton()
    for page in pages:
        debut = time.time()
        premiere = pdfium.PdfDocument(exporter_pdf(api, acces, page))[0]
        image = premiere.render(scale=LARGEUR / premiere.get_width()).to_pil()
        image.save(dossier / f"{page}.png", optimize=True)
        print(f"{page} : {image.width} x {image.height} px en {time.time() - debut:.0f} s")


if __name__ == "__main__":
    main()
