# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dcef8297-8df2-45d3-a250-d7230356795e",
# META       "default_lakehouse_name": "Ponctualite",
# META       "default_lakehouse_workspace_id": "6501a002-4c79-4ada-aa1e-3b65a5bf1f9b"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Ponctualité · chargement des archives mensuelles
#
# opentransportdata.swiss archive chaque mois les fichiers quotidiens Ist-Daten dans un zip d'environ 1,4 Go
# (`https://archive.opentransportdata.swiss/istdaten/AAAA/ist-daten-v2-AAAA-MM.zip`, format v2 depuis juillet 2025).
#
# Pour chaque mois demandé, ce notebook télécharge le zip dans `Files/brut/archives/`, puis traite chaque jour :
# extraction du CSV, filtrage sur le périmètre de `Files/config/trajets.json`, remplacement de la partition du jour
# dans **bronze.passages**, mise à jour du journal **bronze.chargements**. Un jour déjà présent est **remplacé** :
# relancer ce notebook applique le périmètre courant à tout l'historique.
#
# Avec `RECHARGER_JOURS_EN_LIGNE`, les fichiers quotidiens encore en ligne (le mois en cours) sont aussi rechargés.
# Compter environ 20 à 30 secondes par jour.

# CELL ********************

# Paramètres
MOIS = ["2026-10", "2026-09", "2026-08", "2026-07", "2026-06", "2026-05", "2026-04", "2026-03",
        "2026-02", "2026-01", "2025-12", "2025-11", "2025-10", "2025-09", "2025-08", "2025-07"]
RECHARGER_JOURS_EN_LIGNE = True   # recharge aussi les jours du mois en cours (page du jeu de données)
FORCER = True                     # True : remplace les jours déjà chargés ; False : ne charge que les jours absents
GARDER_ZIP = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import re
import shutil
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

from pyspark.sql import functions as F

ARCHIVE = "https://archive.opentransportdata.swiss/istdaten/{annee}/ist-daten-v2-{mois}.zip"
PAGE_JEU_DE_DONNEES = "https://data.opentransportdata.swiss/en/dataset/ist-daten-v2"
RACINE_LOCALE = "/lakehouse/default/Files"
DOSSIER_BRUT = "brut/istdaten"
DOSSIER_ZIP = "brut/archives"
ENTETES = {"User-Agent": "ponctualite-perso/1.0 (notebook Fabric)"}
MOTIF_JOUR = re.compile(r"(\d{4}-\d{2}-\d{2})_istdaten\.csv$", re.IGNORECASE)  # les archives écrivent « IstDaten »

with open(f"{RACINE_LOCALE}/config/trajets.json", encoding="utf-8") as f:
    CONFIG = json.load(f)
ARRETS = sorted(set(CONFIG["arrets"]))
LIGNES = sorted({(l["operateur"], l["ligne"]) for l in CONFIG["lignes"]})
print(f"Périmètre : {len(ARRETS)} arrêts, {len(LIGNES)} lignes")

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
condition = F.col("HALTESTELLEN_NAME").isin(ARRETS)
for operateur, ligne in LIGNES:
    condition = condition | ((F.col("BETREIBER_ABK") == operateur) & (F.col("LINIEN_TEXT") == ligne))


def jours_deja_charges():
    if not spark.catalog.tableExists("bronze.chargements"):
        return set()
    return {r.jour for r in spark.table("bronze.chargements").select("jour").distinct().collect()}


def telecharger(url, chemin):
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    with urllib.request.urlopen(urllib.request.Request(url, headers=ENTETES), timeout=300) as r, open(chemin, "wb") as f:
        shutil.copyfileobj(r, f, 16 * 1024 * 1024)
    return os.path.getsize(chemin)


def charger_jour(jour, chemin_local, taille):
    """Filtre un CSV quotidien déjà présent dans Files et remplace la partition du jour dans bronze.passages."""
    t0 = time.time()
    relatif = os.path.relpath(chemin_local, RACINE_LOCALE).replace(os.sep, "/")
    brut = spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8").csv(f"Files/{relatif}")
    garde = brut.filter(condition).withColumn("jour", F.lit(jour))
    if spark.catalog.tableExists("bronze.passages"):
        spark.sql(f"DELETE FROM bronze.passages WHERE jour = '{jour}'")
    garde.write.mode("append").format("delta").partitionBy("jour").saveAsTable("bronze.passages")
    n = spark.table("bronze.passages").filter(F.col("jour") == jour).count()
    if spark.catalog.tableExists("bronze.chargements"):
        spark.sql(f"DELETE FROM bronze.chargements WHERE jour = '{jour}'")
    journal = spark.createDataFrame(
        [(jour, n, taille, datetime.now(timezone.utc).replace(tzinfo=None))],
        "jour string, lignes_gardees long, taille_fichier_octets long, charge_le timestamp")
    journal.write.mode("append").format("delta").saveAsTable("bronze.chargements")
    os.remove(chemin_local)
    print(f"  {jour} : {taille / 1e6:.0f} Mo, {n} passages gardés, {time.time() - t0:.0f} s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Archives mensuelles
deja = jours_deja_charges()
for mois in MOIS:
    t_mois = time.time()
    url = ARCHIVE.format(annee=mois[:4], mois=mois)
    chemin_zip = f"{RACINE_LOCALE}/{DOSSIER_ZIP}/ist-daten-v2-{mois}.zip"
    print(f"\n=== {mois} : téléchargement de {url}")
    taille_zip = telecharger(url, chemin_zip)
    print(f"  zip : {taille_zip / 1e9:.2f} Go en {time.time() - t_mois:.0f} s")
    with zipfile.ZipFile(chemin_zip) as z:
        membres = sorted((m for m in z.namelist() if MOTIF_JOUR.search(m)), key=lambda m: MOTIF_JOUR.search(m).group(1))
        print(f"  {len(membres)} jours dans l'archive")
        for membre in membres:
            jour = MOTIF_JOUR.search(membre).group(1)
            if jour in deja and not FORCER:
                continue
            chemin_csv = f"{RACINE_LOCALE}/{DOSSIER_BRUT}/{jour}_istdaten.csv"
            os.makedirs(os.path.dirname(chemin_csv), exist_ok=True)
            with z.open(membre) as src, open(chemin_csv, "wb") as dst:
                shutil.copyfileobj(src, dst, 16 * 1024 * 1024)
            charger_jour(jour, chemin_csv, os.path.getsize(chemin_csv))
            deja.add(jour)
    if not GARDER_ZIP:
        os.remove(chemin_zip)
    print(f"=== {mois} terminé en {(time.time() - t_mois) / 60:.0f} min")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Jours du mois en cours, encore publiés un par un sur la page du jeu de données
if RECHARGER_JOURS_EN_LIGNE:
    with urllib.request.urlopen(urllib.request.Request(PAGE_JEU_DE_DONNEES, headers=ENTETES), timeout=120) as r:
        html = r.read().decode("utf-8", "replace")
    motif = r"https://data\.opentransportdata\.swiss/dataset/[0-9a-f-]+/resource/[0-9a-f-]+/download/(\d{4}-\d{2}-\d{2})_istdaten\.csv"
    fichiers = {m.group(1): m.group(0) for m in re.finditer(motif, html)}
    derniers_mois = {m for m in MOIS}
    a_charger = sorted(j for j in fichiers if j[:7] not in derniers_mois and (FORCER or j not in deja))
    print(f"\n=== Jours en ligne hors archives : {len(a_charger)}")
    for jour in a_charger:
        chemin_csv = f"{RACINE_LOCALE}/{DOSSIER_BRUT}/{jour}_istdaten.csv"
        taille = telecharger(fichiers[jour], chemin_csv)
        charger_jour(jour, chemin_csv, taille)

spark.table("bronze.chargements").agg(
    F.count("*").alias("jours"), F.min("jour").alias("premier"), F.max("jour").alias("dernier"), F.sum("lignes_gardees").alias("passages")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
