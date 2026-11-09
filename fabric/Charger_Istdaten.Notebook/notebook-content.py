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

# # Ponctualité · chargement des données réelles (Ist-Daten)
#
# Chaque jour, opentransportdata.swiss publie un fichier CSV de **tous** les passages
# réalisés en Suisse la veille (environ 2,6 millions de lignes, 300 à 400 Mo).
# Ce notebook :
#
# 1. lit la liste des fichiers quotidiens disponibles sur la page du jeu de données ;
# 2. télécharge chaque jour pas encore chargé dans `Files/brut/istdaten/` ;
# 3. ne garde que le périmètre défini dans `Files/config/trajets.json`
#    (arrêts et lignes suivis) et l'ajoute à la table **bronze.passages**, partitionnée par jour ;
# 4. note le jour dans **bronze.chargements** pour ne jamais le recharger.
#
# Le fichier brut est supprimé après chargement (il reste téléchargeable à tout moment).

# CELL ********************

# Paramètres
JOURS_MAX = 40                 # nombre de jours récents à considérer (la page en liste ~35)
GARDER_FICHIERS_BRUTS = False  # True pour conserver les CSV complets dans Files/brut

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import functions as F

PAGE_JEU_DE_DONNEES = "https://data.opentransportdata.swiss/en/dataset/ist-daten-v2"
RACINE_LOCALE = "/lakehouse/default/Files"
DOSSIER_BRUT = "brut/istdaten"
ENTETES = {"User-Agent": "ponctualite-perso/1.0 (notebook Fabric)"}

with open(f"{RACINE_LOCALE}/config/trajets.json", encoding="utf-8") as f:
    CONFIG = json.load(f)
ARRETS = sorted(set(CONFIG["arrets"]))
LIGNES = sorted({(l["operateur"], l["ligne"]) for l in CONFIG["lignes"]})
print(f"Périmètre : {len(ARRETS)} arrêts, {len(LIGNES)} lignes")

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")


def lire_url(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=ENTETES), timeout=120) as r:
        return r.read()


def telecharger(url, chemin):
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    with urllib.request.urlopen(urllib.request.Request(url, headers=ENTETES), timeout=120) as r, open(chemin, "wb") as f:
        while True:
            bloc = r.read(8 * 1024 * 1024)
            if not bloc:
                break
            f.write(bloc)
    return os.path.getsize(chemin)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fichiers quotidiens disponibles : la page HTML du jeu de données liste un lien par jour
html = lire_url(PAGE_JEU_DE_DONNEES).decode("utf-8", "replace")
MOTIF = r"https://data\.opentransportdata\.swiss/dataset/[0-9a-f-]+/resource/[0-9a-f-]+/download/(\d{4}-\d{2}-\d{2})_istdaten\.csv"
fichiers = {m.group(1): m.group(0) for m in re.finditer(MOTIF, html)}
jours_disponibles = sorted(fichiers)[-JOURS_MAX:]

deja = set()
if spark.catalog.tableExists("bronze.chargements"):
    deja = {r.jour for r in spark.table("bronze.chargements").select("jour").distinct().collect()}
a_charger = [j for j in jours_disponibles if j not in deja]
print(f"{len(jours_disponibles)} jours en ligne ({jours_disponibles[0]} → {jours_disponibles[-1]}), "
      f"{len(deja)} déjà chargés, {len(a_charger)} à charger")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Condition de filtrage : arrêt suivi OU ligne suivie (toutes ses gares)
condition = F.col("HALTESTELLEN_NAME").isin(ARRETS)
for operateur, ligne in LIGNES:
    condition = condition | ((F.col("BETREIBER_ABK") == operateur) & (F.col("LINIEN_TEXT") == ligne))

for jour in a_charger:
    t0 = time.time()
    chemin_local = f"{RACINE_LOCALE}/{DOSSIER_BRUT}/{jour}_istdaten.csv"
    taille = telecharger(fichiers[jour], chemin_local)

    brut = (spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8")
            .csv(f"Files/{DOSSIER_BRUT}/{jour}_istdaten.csv"))
    garde = brut.filter(condition).withColumn("jour", F.lit(jour))

    if spark.catalog.tableExists("bronze.passages"):
        spark.sql(f"DELETE FROM bronze.passages WHERE jour = '{jour}'")
    garde.write.mode("append").format("delta").partitionBy("jour").saveAsTable("bronze.passages")
    n = spark.table("bronze.passages").filter(F.col("jour") == jour).count()

    journal = spark.createDataFrame(
        [(jour, n, taille, datetime.now(timezone.utc).replace(tzinfo=None))],
        "jour string, lignes_gardees long, taille_fichier_octets long, charge_le timestamp")
    journal.write.mode("append").format("delta").saveAsTable("bronze.chargements")

    if not GARDER_FICHIERS_BRUTS:
        os.remove(chemin_local)
    print(f"{jour} : {taille / 1e6:.0f} Mo téléchargés, {n} passages gardés, {time.time() - t0:.0f} s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists("bronze.passages"):
    spark.table("bronze.passages").groupBy("jour").count().orderBy("jour").show(60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
