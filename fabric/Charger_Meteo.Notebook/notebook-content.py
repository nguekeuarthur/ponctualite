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

# # Ponctualité · météo (MétéoSuisse, données ouvertes)
#
# MétéoSuisse publie les mesures de ses stations automatiques (SwissMetNet) sur data.geo.admin.ch :
# un fichier horaire par station et par période (`h_historical_2020-2029`, `h_recent` = année en cours,
# `h_now` = dernières heures). Horodatages en UTC, séparateur `;`.
#
# Les stations à charger sont les clés de `stations_meteo` dans `Files/config/trajets.json`
# (chaque station y est associée aux arrêts qu'elle représente).
#
# - **bronze.meteo_heure** : les fichiers tels quels (tout en texte), remplacés à chaque exécution.
# - **silver.meteo_heure** : une ligne par station et par heure, valeurs typées, heure locale.
# - **gold.meteo_jour** : par station et par jour, les indicateurs utiles aux trajets du matin
#   (températures, précipitations, rafales, neige au sol entre 05:00 et 09:00) et une étiquette « conditions du matin ».
# - **gold.station_meteo** : nom, altitude et coordonnées des stations (fichier de métadonnées encodé en Windows-1252,
#   lu en ISO-8859-1 car Spark ne connaît pas cp1252).

# CELL ********************

FICHIERS = ["h_historical_2020-2029", "h_recent", "h_now"]
FUSEAU = "Europe/Zurich"
HEURE_MATIN_DEBUT, HEURE_MATIN_FIN = 5, 9   # fenêtre « matin » : heures locales 05:00 à 09:59

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import urllib.request

from pyspark.sql import Window
from pyspark.sql import functions as F

URL = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/{station}/ogd-smn_{station}_{fichier}.csv"
URL_STATIONS = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_stations.csv"
RACINE_LOCALE = "/lakehouse/default/Files"
DOSSIER = "brut/meteo"
ENTETES = {"User-Agent": "ponctualite-perso/1.0 (notebook Fabric)"}

with open(f"{RACINE_LOCALE}/config/trajets.json", encoding="utf-8") as f:
    CONFIG = json.load(f)
STATIONS = sorted(CONFIG["stations_meteo"])
print("Stations :", STATIONS)

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
os.makedirs(f"{RACINE_LOCALE}/{DOSSIER}", exist_ok=True)


def telecharger(url, chemin):
    with urllib.request.urlopen(urllib.request.Request(url, headers=ENTETES), timeout=120) as r, open(chemin, "wb") as f:
        f.write(r.read())
    return os.path.getsize(chemin)


def ecrire(df, nom):
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(nom)
    print(f"{nom} : {spark.table(nom).count()} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze : téléchargement et lecture brute
morceaux = []
for station in STATIONS:
    for fichier in FICHIERS:
        nom = f"ogd-smn_{station.lower()}_{fichier}.csv"
        taille = telecharger(URL.format(station=station.lower(), fichier=fichier), f"{RACINE_LOCALE}/{DOSSIER}/{nom}")
        print(f"{nom} : {taille / 1e6:.1f} Mo")
        df = spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8").csv(f"Files/{DOSSIER}/{nom}")
        morceaux.append(df.withColumn("fichier", F.lit(fichier)))

bronze = morceaux[0]
for m in morceaux[1:]:
    bronze = bronze.unionByName(m, allowMissingColumns=True)
ecrire(bronze, "bronze.meteo_heure")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver : une ligne par station et par heure (le fichier le plus récent l'emporte en cas de doublon)
b = spark.table("bronze.meteo_heure")
priorite = F.when(F.col("fichier") == "h_now", 3).when(F.col("fichier") == "h_recent", 2).otherwise(1)
fenetre = Window.partitionBy("station_abbr", "horodatage_utc").orderBy(F.desc("priorite"))


def nombre(colonne):
    return F.col(colonne).cast("double") if colonne in b.columns else F.lit(None).cast("double")


silver = (
    b.withColumn("horodatage_utc", F.to_timestamp("reference_timestamp", "dd.MM.yyyy HH:mm"))
    .withColumn("priorite", priorite)
    .withColumn("rang", F.row_number().over(fenetre))
    .filter("rang = 1")
    .select(
        F.col("station_abbr").alias("station"),
        "horodatage_utc",
        F.from_utc_timestamp("horodatage_utc", FUSEAU).alias("horodatage_local"),
        nombre("tre200h0").alias("temperature"),
        nombre("tre200hn").alias("temperature_min"),
        nombre("tre200hx").alias("temperature_max"),
        nombre("ure200h0").alias("humidite"),
        nombre("rre150h0").alias("precipitation_mm"),
        nombre("fkl010h0").alias("vent_ms"),
        nombre("fkl010h1").alias("rafale_ms"),
        nombre("sre000h0").alias("ensoleillement_min"),
        nombre("pp0qnhh0").alias("pression_qnh"),
        nombre("htoauths").alias("neige_cm"),
    )
    .withColumn("jour", F.to_date("horodatage_local"))
    .withColumn("heure", F.hour("horodatage_local"))
)
ecrire(silver, "silver.meteo_heure")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold : météo du jour, avec un zoom sur le matin
s = spark.table("silver.meteo_heure")
matin = (F.col("heure") >= HEURE_MATIN_DEBUT) & (F.col("heure") <= HEURE_MATIN_FIN)


def si_matin(colonne):
    return F.when(matin, F.col(colonne))


meteo_jour = (
    s.groupBy("station", "jour")
    .agg(
        F.min("temperature_min").alias("temp_min"),
        F.max("temperature_max").alias("temp_max"),
        F.round(F.avg("temperature"), 1).alias("temp_moy"),
        F.round(F.avg(si_matin("temperature")), 1).alias("temp_matin"),
        F.min(si_matin("temperature_min")).alias("temp_min_matin"),
        F.round(F.sum("precipitation_mm"), 1).alias("precip_jour_mm"),
        F.round(F.sum(si_matin("precipitation_mm")), 1).alias("precip_matin_mm"),
        F.max("rafale_ms").alias("rafale_max_ms"),
        F.max(si_matin("rafale_ms")).alias("rafale_max_matin_ms"),
        F.max("neige_cm").alias("neige_cm"),
        F.max(si_matin("neige_cm")).alias("neige_cm_matin"),
        F.sum("ensoleillement_min").alias("ensoleillement_min"),
        F.count("*").alias("heures_mesurees"),
    )
    .withColumn(
        "conditions_matin",
        F.when(F.col("neige_cm_matin") > 0, "neige au sol")
        .when(F.col("temp_min_matin") <= 0, "gel")
        .when(F.col("precip_matin_mm") >= 2, "pluie")
        .when(F.col("rafale_max_matin_ms") >= 15, "vent fort")
        .when(F.col("precip_matin_mm") > 0, "bruine")
        .when(F.col("temp_min_matin").isNull(), None)
        .otherwise("sec"),
    )
)
ecrire(meteo_jour, "gold.meteo_jour")

# Stations : nom, altitude, coordonnées (fichier de métadonnées MétéoSuisse, encodé en cp1252)
chemin_stations = f"{RACINE_LOCALE}/{DOSSIER}/ogd-smn_meta_stations.csv"
telecharger(URL_STATIONS, chemin_stations)
stations = (
    # Spark n'accepte que quelques jeux de caractères (pas cp1252) : ISO-8859-1 suffit pour les accents des noms
    spark.read.option("header", True).option("sep", ";").option("encoding", "ISO-8859-1").csv(f"Files/{DOSSIER}/ogd-smn_meta_stations.csv")
    .filter(F.col("station_abbr").isin(STATIONS))
    .select(
        F.col("station_abbr").alias("station"),
        F.col("station_name").alias("nom"),
        F.col("station_canton").alias("canton"),
        F.col("station_height_masl").cast("int").alias("altitude_m"),
        F.col("station_coordinates_wgs84_lat").cast("double").alias("latitude"),
        F.col("station_coordinates_wgs84_lon").cast("double").alias("longitude"),
    )
)
ecrire(stations, "gold.station_meteo")

meteo_jour.filter(F.col("jour") >= F.date_sub(F.current_date(), 10)).orderBy("station", "jour").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
