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

# # Ponctualité · prédire le risque de retard de demain
#
# Question : « demain matin, quel est le risque que mon trajet arrive avec 3 minutes de retard ou plus (ou soit supprimé) ? »
#
# - **Apprentissage** sur `gold.fait_trajet` (jours ouvrés) : trajet, jour de la semaine, mois, météo du matin
#   (température minimale, précipitations, rafales) et la « forme » récente du trajet (part de retards et retard moyen
#   des 10 derniers jours observés, suppression dans les 5 derniers jours).
# - **Évaluation honnête** : apprentissage sur le passé, test sur les derniers jours (coupure temporelle), comparé à une
#   référence naïve (taux de retard historique du trajet par jour de semaine). Résultats dans `gold.modele_evaluation`
#   et `gold.modele_importance`.
# - **Prévision** : pour le prochain jour ouvré, avec la prévision météo [Open-Meteo](https://open-meteo.com) aux
#   coordonnées de la station associée au départ. Résultat dans `gold.prediction` (une ligne par trajet et par jour cible,
#   conservée pour pouvoir juger les prédictions après coup).
#
# Avec quelques centaines de jours par trajet, le modèle reste simple (gradient boosting peu profond) : il sert à
# hiérarchiser le risque, pas à promettre une minute près.

# CELL ********************

SEUIL_RETARD_MIN = 3
JOURS_TEST = 60              # derniers jours observés réservés à l'évaluation
MIN_JOURS_PAR_TRAJET = 60    # en dessous, le trajet n'est pas prédit
FUSEAU = "Europe/Zurich"
HEURE_MATIN_DEBUT, HEURE_MATIN_FIN = 5, 9

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import urllib.request
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import brier_score_loss, roc_auc_score


def nouveau_modele():
    """Gradient boosting peu profond, puis calibration isotonique pour que « 30 % » veuille dire 30 %."""
    base = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.05, max_iter=150, l2_regularization=1.0,
                                          min_samples_leaf=30, random_state=42)
    return CalibratedClassifierCV(base, method="isotonic", cv=5)

with open("/lakehouse/default/Files/config/trajets.json", encoding="utf-8") as f:
    CONFIG = json.load(f)
STATION_PAR_ARRET = {arret: station for station, arrets in CONFIG.get("stations_meteo", {}).items() for arret in arrets}
MAINTENANT = datetime.now(timezone.utc)

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def ecrire(df, nom, mode="overwrite"):
    (df.write.mode(mode).option("overwriteSchema", "true").format("delta").saveAsTable(nom))
    print(f"{nom} : {spark.table(nom).count()} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Jeu d'apprentissage : un trajet suivi par jour ouvré, avec sa « forme » récente
ft = (
    spark.table("gold.fait_trajet")
    .filter("jour_ouvre = true AND depart_prevu IS NOT NULL")
    .select("jour", "trajet", "arret_depart", "retard_arrivee_min", "supprime", "ponctuel",
            "temp_min_matin", "precip_matin_mm", "rafale_max_matin_ms", "conditions_matin")
    .toPandas()
)
ft["jour"] = pd.to_datetime(ft["jour"])
ft["en_retard"] = ((ft["retard_arrivee_min"] >= SEUIL_RETARD_MIN) | (ft["supprime"] == True)).astype(int)
ft["jour_semaine"] = ft["jour"].dt.dayofweek + 1
ft["mois"] = ft["jour"].dt.month
ft = ft.sort_values(["trajet", "jour"]).reset_index(drop=True)

g = ft.groupby("trajet")
ft["taux_retard_10j"] = g["en_retard"].transform(lambda s: s.shift(1).rolling(10, min_periods=3).mean())
ft["retard_moyen_10j"] = g["retard_arrivee_min"].transform(lambda s: s.shift(1).rolling(10, min_periods=3).mean())
ft["supprime_5j"] = g["supprime"].transform(lambda s: s.shift(1).rolling(5, min_periods=1).max()).fillna(0).astype(int)
ft["taux_retard_historique"] = g["en_retard"].transform(lambda s: s.shift(1).expanding(min_periods=10).mean())

COLONNES_NUM = ["jour_semaine", "mois", "temp_min_matin", "precip_matin_mm", "rafale_max_matin_ms",
                "taux_retard_10j", "retard_moyen_10j", "supprime_5j", "taux_retard_historique"]
trajets_retenus = ft.groupby("trajet").size()
trajets_retenus = sorted(trajets_retenus[trajets_retenus >= MIN_JOURS_PAR_TRAJET].index)
ft = ft[ft["trajet"].isin(trajets_retenus) & ft["taux_retard_10j"].notna()].copy()
print(f"{len(ft)} observations, {len(trajets_retenus)} trajets, taux de retard global {ft['en_retard'].mean():.1%}")


def matrice(df):
    X = df[COLONNES_NUM].copy()
    for t in trajets_retenus:
        X[f"trajet={t}"] = (df["trajet"] == t).astype(int)
    return X

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Évaluation : apprentissage sur le passé, test sur les derniers jours
coupure = sorted(ft["jour"].unique())[-JOURS_TEST]
app, test = ft[ft["jour"] < coupure], ft[ft["jour"] >= coupure]
modele = nouveau_modele()
modele.fit(matrice(app), app["en_retard"])
proba_test = modele.predict_proba(matrice(test))[:, 1]

# Référence naïve : taux de retard du trajet par jour de semaine, appris sur la même période
reference = app.groupby(["trajet", "jour_semaine"])["en_retard"].mean().rename("ref").reset_index()
ref_test = test.merge(reference, on=["trajet", "jour_semaine"], how="left")["ref"].fillna(app["en_retard"].mean()).values

evaluation = {
    "genere_le": MAINTENANT.replace(tzinfo=None), "coupure": pd.Timestamp(coupure).date(),
    "n_apprentissage": int(len(app)), "n_test": int(len(test)), "taux_retard_test": float(test["en_retard"].mean()),
    "auc_modele": float(roc_auc_score(test["en_retard"], proba_test)),
    "auc_reference": float(roc_auc_score(test["en_retard"], ref_test)),
    "brier_modele": float(brier_score_loss(test["en_retard"], proba_test)),
    "brier_reference": float(brier_score_loss(test["en_retard"], ref_test)),
}
print(json.dumps({k: (str(v) if not isinstance(v, (int, float)) else round(v, 3)) for k, v in evaluation.items()}, indent=1, ensure_ascii=False))

importance = permutation_importance(modele, matrice(test), test["en_retard"], scoring="roc_auc", n_repeats=10, random_state=42)
imp = pd.DataFrame({"variable": matrice(test).columns, "importance": importance.importances_mean, "genere_le": evaluation["genere_le"]})
imp = imp.sort_values("importance", ascending=False)
print(imp.head(12).to_string(index=False))

ecrire(spark.createDataFrame(pd.DataFrame([evaluation])), "gold.modele_evaluation")
ecrire(spark.createDataFrame(imp), "gold.modele_importance")

# Modèle final sur toutes les observations
modele_final = nouveau_modele()
modele_final.fit(matrice(ft), ft["en_retard"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prévision météo du prochain jour ouvré (Open-Meteo, sans clé) aux coordonnées de chaque station utilisée
aujourd_hui = datetime.now(ZoneInfo(FUSEAU)).date()
cible = aujourd_hui + timedelta(days=1)
while cible.weekday() >= 5:
    cible += timedelta(days=1)

stations = {r.station: (r.latitude, r.longitude) for r in spark.table("gold.station_meteo").collect()}
previsions = {}
for station, (lat, lon) in stations.items():
    url = ("https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
           "&hourly=temperature_2m,precipitation,wind_gusts_10m&timezone=Europe%2FZurich"
           f"&start_date={cible}&end_date={cible}").format(lat=lat, lon=lon)
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "ponctualite-perso/1.0"}), timeout=60) as r:
        h = json.load(r)["hourly"]
    heures = [int(t[11:13]) for t in h["time"]]
    matin = [i for i, hh in enumerate(heures) if HEURE_MATIN_DEBUT <= hh <= HEURE_MATIN_FIN]
    previsions[station] = {
        "temp_min_matin": min(h["temperature_2m"][i] for i in matin),
        "precip_matin_mm": round(sum(h["precipitation"][i] for i in matin), 1),
        "rafale_max_matin_ms": round(max(h["wind_gusts_10m"][i] for i in matin) / 3.6, 1),
    }
print(f"Jour cible : {cible} ({['lundi','mardi','mercredi','jeudi','vendredi','samedi','dimanche'][cible.weekday()]})")
for s, p in previsions.items():
    print(f"  {s} : {p}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Risque par trajet pour le jour cible
lignes = []
for trajet in trajets_retenus:
    histo = ft[ft["trajet"] == trajet].sort_values("jour")
    derniers = histo.tail(10)
    station = STATION_PAR_ARRET.get(histo["arret_depart"].iloc[-1])
    meteo = previsions.get(station, {})
    x = pd.DataFrame([{
        "trajet": trajet, "jour_semaine": cible.weekday() + 1, "mois": cible.month,
        "temp_min_matin": meteo.get("temp_min_matin"), "precip_matin_mm": meteo.get("precip_matin_mm"),
        "rafale_max_matin_ms": meteo.get("rafale_max_matin_ms"),
        "taux_retard_10j": derniers["en_retard"].mean(), "retard_moyen_10j": derniers["retard_arrivee_min"].mean(),
        "supprime_5j": int(histo.tail(5)["supprime"].max()), "taux_retard_historique": histo["en_retard"].mean(),
    }])
    proba = float(modele_final.predict_proba(matrice(x))[:, 1][0])
    meme_jour = histo[histo["jour_semaine"] == cible.weekday() + 1]["en_retard"].mean()
    lignes.append({
        "jour_cible": cible, "trajet": trajet, "station_meteo": station,
        "probabilite_retard": round(proba, 3),
        "niveau": "élevé" if proba >= 0.25 else "moyen" if proba >= 0.12 else "faible",
        "taux_historique": round(float(histo["en_retard"].mean()), 3),
        "taux_meme_jour_semaine": round(float(meme_jour), 3) if pd.notna(meme_jour) else None,
        "taux_10_derniers_jours": round(float(derniers["en_retard"].mean()), 3),
        "temp_min_prevue": meteo.get("temp_min_matin"), "precip_prevue_mm": meteo.get("precip_matin_mm"),
        "rafale_prevue_ms": meteo.get("rafale_max_matin_ms"),
        "genere_le": MAINTENANT.replace(tzinfo=None),
    })
prediction = pd.DataFrame(lignes).sort_values("probabilite_retard", ascending=False)
print(prediction[["trajet", "probabilite_retard", "niveau", "taux_historique", "taux_10_derniers_jours", "precip_prevue_mm", "temp_min_prevue"]].to_string(index=False))

# Historique des prédictions : une seule ligne par (jour cible, trajet), la plus récente l'emporte
nouvelle = spark.createDataFrame(prediction)
if spark.catalog.tableExists("gold.prediction"):
    ancienne = spark.table("gold.prediction").join(nouvelle.select("jour_cible", "trajet"), ["jour_cible", "trajet"], "left_anti")
    nouvelle = ancienne.unionByName(nouvelle, allowMissingColumns=True)
ecrire(nouvelle, "gold.prediction")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
