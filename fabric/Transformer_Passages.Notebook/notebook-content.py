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

# # Ponctualité · bronze → silver → gold
#
# - **Silver** `silver.passage` : les passages du périmètre avec de vrais types (dates, booléens),
#   des noms de colonnes en français et les retards calculés en minutes.
# - **Gold** : un modèle en étoile pour Power BI :
#   `dim_date`, `dim_ligne`, `dim_arret`, `fait_passage` (un passage à un arrêt)
#   et `fait_trajet` (un trajet suivi par jour : « mon train de 07:55 », avec son retard réel à l'arrivée).
# - **Itinéraires porte à porte** `gold.fait_itineraire` et `gold.fait_correspondance` : les trajets suivis enchaînés,
#   correspondances comprises. Une correspondance manquée décale tout le reste du voyage.
# - **Libellés publics** `gold.dim_trajet` et `gold.dim_itineraire` : pour chaque trajet et chaque itinéraire, un
#   libellé sans arrêt ni horaire (clé `nom_public` de la configuration), qui permet de montrer le rapport sans
#   dévoiler le périmètre.
# - **Prévisions jugées** `gold.prediction_jugee` : chaque prévision du notebook `Predire_Retards` rapprochée de ce
#   qui s'est réellement passé, pour juger le modèle au fil des semaines.
# - **Recette** `gold.qualite` : les tables gold sont contrôlées avant d'être publiées. Clés de dimension uniques,
#   faits rattachés à leurs dimensions, grain respecté, fraîcheur, volume.
# - **Modèle sémantique** : en fin de notebook, le modèle Direct Lake est rafraîchi pour que le rapport lise les
#   tables du jour, seulement si la recette n'a relevé aucun échec bloquant.
#
# Définition de la ponctualité : comme aux CFF, un passage est ponctuel si le retard à l'arrivée est **inférieur à 3 minutes**.
# Un train supprimé n'est jamais ponctuel.

# CELL ********************

SEUIL_PONCTUALITE_MIN = 3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import functools
import json
import traceback

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

with open("/lakehouse/default/Files/config/trajets.json", encoding="utf-8") as f:
    CONFIG = json.load(f)


def ecrire(df, nom):
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(nom)
    print(f"{nom} : {spark.table(nom).count()} lignes")


def journal(etape, message="OK"):
    """Trace d'une étape facultative dans Files/journal. Ces étapes n'arrêtent jamais la chaîne quotidienne :
    en cas d'erreur, la trace complète est écrite ici pour pouvoir la lire après coup."""
    try:
        import datetime
        import os

        os.makedirs("/lakehouse/default/Files/journal", exist_ok=True)
        with open(f"/lakehouse/default/Files/journal/{etape}.txt", "w", encoding="utf-8") as f:
            f.write(f"{datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M:%S} UTC\n{message}\n")
    except Exception as e:
        print(f"Journal non écrit pour {etape} : {e}")


def horodatage(colonne):
    # Heures prévues « JJ.MM.AAAA HH:MM », heures réelles « JJ.MM.AAAA HH:MM:SS », vide si inconnu
    c = F.when(F.col(colonne) == "", None).otherwise(F.col(colonne))
    return F.coalesce(F.to_timestamp(c, "dd.MM.yyyy HH:mm:ss"), F.to_timestamp(c, "dd.MM.yyyy HH:mm"))


def minutes(reel, prevu):
    return (F.unix_timestamp(reel) - F.unix_timestamp(prevu)) / 60.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver
bronze = spark.table("bronze.passages")

passage = (
    bronze.select(
        F.to_date("jour").alias("jour"),
        F.col("FAHRT_BEZEICHNER").alias("course_id"),
        F.col("BETREIBER_ABK").alias("operateur"),
        F.col("BETREIBER_NAME").alias("operateur_nom"),
        F.col("PRODUKT_ID").alias("produit"),
        F.col("VERKEHRSMITTEL_TEXT").alias("type_transport"),
        F.col("LINIEN_TEXT").alias("ligne"),
        F.col("HALTESTELLEN_NAME").alias("arret"),
        F.col("BPUIC").alias("bpuic"),
        F.col("SLOID").alias("sloid"),
        horodatage("ANKUNFTSZEIT").alias("arrivee_prevue"),
        horodatage("AN_PROGNOSE").alias("arrivee_reelle"),
        F.col("AN_PROGNOSE_STATUS").alias("arrivee_statut"),
        horodatage("ABFAHRTSZEIT").alias("depart_prevu"),
        horodatage("AB_PROGNOSE").alias("depart_reel"),
        F.col("AB_PROGNOSE_STATUS").alias("depart_statut"),
        (F.col("FAELLT_AUS_TF") == "true").alias("supprime"),
        (F.col("ZUSATZFAHRT_TF") == "true").alias("supplementaire"),
        (F.col("DURCHFAHRT_TF") == "true").alias("passage_sans_arret"),
    )
    .dropDuplicates(["jour", "course_id", "arret", "arrivee_prevue", "depart_prevu"])
    .withColumn("retard_arrivee_min", F.round(minutes("arrivee_reelle", "arrivee_prevue"), 1))
    .withColumn("retard_depart_min", F.round(minutes("depart_reel", "depart_prevu"), 1))
    .withColumn("retard_fiable", F.col("arrivee_statut") == "REAL")
    .withColumn(
        "ponctuel",
        F.when(F.col("supprime"), F.lit(False))
        .when(F.col("retard_arrivee_min").isNull(), F.lit(None).cast("boolean"))
        .otherwise(F.col("retard_arrivee_min") < SEUIL_PONCTUALITE_MIN),
    )
    .withColumn(
        "ordre_arret",
        F.row_number().over(
            Window.partitionBy("jour", "course_id").orderBy(F.coalesce("depart_prevu", "arrivee_prevue"))
        ),
    )
)
ecrire(passage, "silver.passage")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold : dimensions
s = spark.table("silver.passage")

NOMS_JOURS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
NOMS_MOIS = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août",
             "septembre", "octobre", "novembre", "décembre"]
bornes = s.agg(F.min("jour").alias("debut"), F.max("jour").alias("fin")).first()
dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{bornes.debut}'), to_date('{bornes.fin}'), interval 1 day)) AS jour")
    .withColumn("annee", F.year("jour"))
    .withColumn("mois", F.month("jour"))
    .withColumn("nom_mois", F.element_at(F.array(*[F.lit(m) for m in NOMS_MOIS]), F.month("jour")))
    .withColumn("semaine_iso", F.weekofyear("jour"))
    .withColumn("jour_semaine", ((F.dayofweek("jour") + 5) % 7) + 1)  # 1 = lundi … 7 = dimanche
    .withColumn("nom_jour", F.element_at(F.array(*[F.lit(j) for j in NOMS_JOURS]), F.col("jour_semaine")))
    .withColumn("jour_ouvre", F.col("jour_semaine") <= 5)
    .withColumn("debut_mois", F.trunc("jour", "month"))
)
ecrire(dim_date, "gold.dim_date")

dim_ligne = (
    s.groupBy("operateur", "ligne")
    .agg(F.first("operateur_nom").alias("operateur_nom"), F.first("type_transport").alias("type_transport"),
         F.first("produit").alias("produit"), F.count("*").alias("nb_passages"))
    .withColumn("ligne_cle", F.concat_ws("|", "operateur", "ligne"))
    .withColumn("libelle", F.concat_ws(" ", "type_transport", "ligne"))
)
ecrire(dim_ligne, "gold.dim_ligne")

# Une ligne par arrêt, sans tenir compte de la casse ni des espaces : c'est ainsi que Power BI compare les textes.
# Deux graphies du même nom seraient deux lignes pour Spark, mais un doublon de clé qui casse la relation du modèle.
dim_arret = (
    s.groupBy(F.lower(F.trim(F.regexp_replace("arret", r"\s+", " "))).alias("cle_arret"))
    .agg(F.min("arret").alias("arret"), F.first("bpuic").alias("bpuic"), F.countDistinct("ligne").alias("nb_lignes"),
         F.count("*").alias("nb_passages"))
    .drop("cle_arret")
)
ecrire(dim_arret, "gold.dim_arret")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold : faits
fait_passage = (
    s.withColumn("ligne_cle", F.concat_ws("|", "operateur", "ligne"))
    .withColumn("heure_prevue", F.date_format(F.coalesce("arrivee_prevue", "depart_prevu"), "HH:mm"))
    .withColumn("heure_du_jour", F.hour(F.coalesce("arrivee_prevue", "depart_prevu")))
)
ecrire(fait_passage, "gold.fait_passage")

# Un enregistrement par jour et par trajet suivi : la course prise au départ, et son arrivée réelle.
# La course retenue est la PREMIÈRE de la ligne qui part de l'arrêt à l'heure habituelle ou dans les minutes
# qui suivent (tolérance : clé « tolerance_min », 10 min par défaut) : un horaire de vacances décalé de quelques
# minutes (par exemple 07:24 au lieu de 07:19) reste ainsi « mon bus ».
import re


def plus_minutes(hhmm, minutes):
    h, m = map(int, hhmm.split(":"))
    total = h * 60 + m + minutes
    return f"{total // 60:02d}:{total % 60:02d}"


morceaux = []
for t in CONFIG["trajets"]:
    if not re.fullmatch(r"\d{2}:\d{2}", t["heure_depart"]):
        print(f"Trajet « {t['nom']} » ignoré : heure de départ à confirmer")
        continue
    debut, fin = t["heure_depart"], plus_minutes(t["heure_depart"], t.get("tolerance_min", 10))
    premier_du_jour = Window.partitionBy("jour").orderBy("depart_prevu")
    depart = (
        s.filter((F.col("operateur") == t["operateur"]) & (F.col("ligne") == t["ligne"]) & (F.col("arret") == t["depart"])
                 & (F.date_format("depart_prevu", "HH:mm") >= debut) & (F.date_format("depart_prevu", "HH:mm") <= fin))
        .withColumn("rang", F.row_number().over(premier_du_jour))
        .filter("rang = 1")
        .select("jour", "course_id", "operateur", "ligne", "arret", "depart_prevu", "depart_reel", "depart_statut",
                F.col("retard_depart_min"), F.col("supprime").alias("supprime_au_depart"))
        .withColumnRenamed("arret", "arret_depart")
    )
    arrivee = (
        s.filter(F.col("arret") == t["arrivee"])
        .select("jour", "course_id", F.col("arret").alias("arret_arrivee"), "arrivee_prevue", "arrivee_reelle",
                "arrivee_statut", "retard_arrivee_min", "retard_fiable", "ponctuel",
                F.col("supprime").alias("supprime_a_l_arrivee"))
    )
    # Objectif d'arrivée (ex. « 09:00 » = début du travail) : le trajet est réussi si l'arrivée réelle le précède
    objectif = t.get("objectif_arrivee")
    morceaux.append(
        depart.join(arrivee, ["jour", "course_id"], "left")
        .withColumn("trajet", F.lit(t["nom"]))
        .withColumn("jours_prevus", F.lit(t["jours"]))
        .withColumn("ligne_cle", F.concat_ws("|", "operateur", "ligne"))
        .withColumn("objectif_arrivee", F.lit(objectif))
        .withColumn(
            "arrive_avant_objectif",
            F.when(F.lit(objectif).isNull() | F.col("arrivee_reelle").isNull(), F.lit(None).cast("boolean"))
            .otherwise(F.date_format("arrivee_reelle", "HH:mm:ss") < F.lit(f"{objectif}:00")),
        )
    )

fait_trajet = morceaux[0]
for m in morceaux[1:]:
    fait_trajet = fait_trajet.unionByName(m)
fait_trajet = (
    fait_trajet.withColumn("supprime", F.col("supprime_au_depart") | F.coalesce("supprime_a_l_arrivee", F.lit(False)))
    .withColumn("ponctuel", F.when(F.col("supprime"), F.lit(False)).otherwise(F.col("ponctuel")))
    .join(dim_date.select("jour", "nom_jour", "jour_ouvre"), "jour", "left")
)

# Météo du matin à la station associée à l'arrêt de départ (si le notebook Charger_Meteo a déjà tourné)
COLONNES_METEO = ["temp_matin", "temp_min_matin", "precip_matin_mm", "precip_jour_mm", "rafale_max_matin_ms",
                  "neige_cm_matin", "conditions_matin"]
if spark.catalog.tableExists("gold.meteo_jour") and CONFIG.get("stations_meteo"):
    correspondance = spark.createDataFrame(
        [(arret, station) for station, arrets in CONFIG["stations_meteo"].items() for arret in arrets],
        "arret_depart string, station_meteo string",
    )
    meteo = spark.table("gold.meteo_jour").select("station", "jour", *COLONNES_METEO).withColumnRenamed("station", "station_meteo")
    fait_trajet = fait_trajet.join(correspondance, "arret_depart", "left").join(meteo, ["station_meteo", "jour"], "left")
else:
    fait_trajet = fait_trajet.withColumn("station_meteo", F.lit(None).cast("string"))
    for c in COLONNES_METEO:
        fait_trajet = fait_trajet.withColumn(c, F.lit(None).cast("string" if c == "conditions_matin" else "double"))
ecrire(fait_trajet, "gold.fait_trajet")

# Dimension des trajets suivis : le nom configuré (clé des faits) et son libellé public, sans arrêt ni horaire.
# Le rapport affiche l'un ou l'autre selon son mode ; sans « nom_public », le libellé est un simple numéro.
dim_trajet = spark.createDataFrame(
    [(t["nom"], t.get("nom_public") or f"Trajet {i}", i) for i, t in enumerate(CONFIG["trajets"], 1)],
    "trajet string, libelle_public string, ordre int",
)
ecrire(dim_trajet, "gold.dim_trajet")

fait_trajet.orderBy("trajet", "jour").select(
    "jour", "nom_jour", "trajet", "depart_prevu", "arrivee_prevue", "arrivee_reelle", "retard_arrivee_min", "ponctuel", "supprime"
).show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold : prévisions jugées. Chaque prévision de `gold.prediction` est rapprochée du trajet réel du même jour.
# Un retard réel est une arrivée à SEUIL_PONCTUALITE_MIN minutes ou plus, ou une suppression : la cible du modèle.
# Une prévision n'est jugée que si le trajet a été mesuré ce jour-là ; celle de demain attend son tour.
# L'étape ne fait jamais échouer la transformation : sans prévision, ou si le rapprochement échoue, elle est ignorée.
try:
    if spark.catalog.tableExists("gold.prediction"):
        reel = spark.table("gold.fait_trajet").select(
            F.col("jour").alias("jour_cible"), "trajet",
            F.col("retard_arrivee_min").alias("retard_reel_min"), F.coalesce("supprime", F.lit(False)).alias("supprime"),
        )
        alerte = F.col("niveau") == "élevé"
        issue = F.col("en_retard").cast("double")
        prediction_jugee = (
            spark.table("gold.prediction")
            .select("jour_cible", "trajet", "probabilite_retard", "niveau", "taux_historique", "genere_le")
            .join(reel, ["jour_cible", "trajet"], "inner")
            .where(F.col("supprime") | F.col("retard_reel_min").isNotNull())
            .withColumn("en_retard", F.col("supprime") | (F.col("retard_reel_min") >= SEUIL_PONCTUALITE_MIN))
            .withColumn("niveau_ordre", F.when(F.col("niveau") == "faible", 1).when(F.col("niveau") == "moyen", 2).otherwise(3))
            .withColumn(
                "resultat",
                F.when(alerte & F.col("en_retard"), "alerte justifiée")
                .when(alerte, "fausse alerte")
                .when(F.col("en_retard"), "retard non annoncé")
                .otherwise("calme confirmé"),
            )
            # Écarts quadratiques : leur moyenne est le score de Brier du modèle, et celui de la référence naïve
            .withColumn("ecart_carre", F.pow(F.col("probabilite_retard") - issue, 2))
            .withColumn("ecart_carre_reference", F.pow(F.col("taux_historique") - issue, 2))
        )
        ecrire(prediction_jugee, "gold.prediction_jugee")
        journal("prediction_jugee")
    else:
        print("gold.prediction absente : aucune prévision à juger")
        journal("prediction_jugee", "gold.prediction absente")
except Exception as e:
    print(f"Prévisions jugées non mises à jour : {type(e).__name__} : {e}")
    journal("prediction_jugee", traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold : itinéraires porte à porte. Un itinéraire enchaîne plusieurs trajets suivis (clé « itineraires » de la
# configuration) ; jusque-là, chaque étape était mesurée seule, à son heure habituelle. Le voyageur est simulé
# jour après jour, avec trois courses par étape :
# - la course PRÉVUE : l'horaire idéal. La course habituelle de la première étape, puis à chaque correspondance la
#   première course de la ligne habituelle qui part au moins « correspondance_min » minutes après l'arrivée prévue ;
# - la course VISÉE : celle qu'il compte prendre en arrivant. C'est la course prévue tant que tout se passe comme
#   prévu ; après un écart, c'est la course qui arrive le plus tôt, toutes lignes confondues ;
# - la course PRISE : la course visée si elle n'est pas supprimée et encore à quai quand il arrive réellement, délai
#   de correspondance compris ; sinon le premier véhicule qui relie les deux arrêts, quelle que soit sa ligne.
# Une correspondance est manquée quand la course prise n'est pas la course visée : chaque correspondance est donc
# jugée pour elle-même, sans compter deux fois les conséquences d'un premier raté.
# Hypothèses : un véhicule ne part pas avant son heure prévue ; le voyageur ne prend pas une course plus tôt que
# prévu ; sans heure réelle mesurée, l'heure prévue est utilisée et le jour est marqué « mesure incomplète ».
# L'étape ne fait jamais échouer la transformation : sans itinéraire, ou en cas d'erreur, elle est ignorée.
MESUREE = ["REAL", "GESCHAETZT"]
COLONNES_COURSE = ["course_id", "ligne", "depart_prevu", "depart_eff", "depart_mesure",
                   "arrivee_prevue", "arrivee_eff", "arrivee_mesure", "supprime"]


def heure_effective(reelle, statut, prevue):
    """Heure réelle si elle a été mesurée, heure prévue sinon."""
    return F.when(F.col(statut).isin(MESUREE) & F.col(reelle).isNotNull(), F.col(reelle)).otherwise(F.col(prevue))


def courses_entre(passages, t, toutes_lignes=False):
    """Courses qui desservent l'arrêt de départ de l'étape puis son arrêt d'arrivée : celles de sa ligne, ou toutes."""
    au_depart = (F.col("arret") == t["depart"]) & F.col("depart_prevu").isNotNull() & ~F.coalesce("passage_sans_arret", F.lit(False))
    if not toutes_lignes:
        au_depart = au_depart & (F.col("operateur") == t["operateur"]) & (F.col("ligne") == t["ligne"])
    depart = passages.filter(au_depart).select(
        "jour", "course_id", "ligne", "depart_prevu",
        heure_effective("depart_reel", "depart_statut", "depart_prevu").alias("depart_eff"),
        F.coalesce(F.col("depart_statut").isin(MESUREE) & F.col("depart_reel").isNotNull(), F.lit(False)).alias("depart_mesure"),
        F.coalesce("supprime", F.lit(False)).alias("supprime_depart"))
    arrivee = passages.filter((F.col("arret") == t["arrivee"]) & F.col("arrivee_prevue").isNotNull()).select(
        "jour", "course_id", "arrivee_prevue",
        heure_effective("arrivee_reelle", "arrivee_statut", "arrivee_prevue").alias("arrivee_eff"),
        F.coalesce(F.col("arrivee_statut").isin(MESUREE) & F.col("arrivee_reelle").isNotNull(), F.lit(False)).alias("arrivee_mesure"),
        F.coalesce("supprime", F.lit(False)).alias("supprime_arrivee"))
    return (
        depart.join(arrivee, ["jour", "course_id"])
        .filter(F.col("arrivee_prevue") > F.col("depart_prevu"))  # l'arrêt d'arrivée vient après : le bon sens de la ligne
        .dropDuplicates(["jour", "course_id"])
        .withColumn("supprime", F.col("supprime_depart") | F.col("supprime_arrivee"))
        .select("jour", *COLONNES_COURSE)
    )


def avec_prefixe(df, prefixe):
    return df.select("jour", *[F.col(c).alias(f"{prefixe}_{c}") for c in df.columns if c != "jour"])


def premiere_du_jour(df, *ordre):
    rang = F.row_number().over(Window.partitionBy("jour").orderBy(*ordre))
    return df.withColumn("_rang", rang).filter("_rang = 1").drop("_rang")


def apres(colonne, delai_min):
    return F.col(colonne) + F.expr(f"INTERVAL {int(delai_min)} MINUTES")


def ecart_min(fin, debut):
    return F.round((F.unix_timestamp(fin) - F.unix_timestamp(debut)) / 60.0, 1)


def differente(a, b):
    """Vrai si les deux courses diffèrent, ou si l'une des deux manque."""
    return F.coalesce(F.col(a) != F.col(b), F.lit(True))


try:
    definitions = CONFIG.get("itineraires", [])
    if not definitions:
        print("Aucun itinéraire dans la configuration : étape ignorée")
        journal("itineraires", "Aucun itinéraire dans la configuration")
    else:
        TRAJET_PAR_NOM = {t["nom"]: t for t in CONFIG["trajets"]}
        arrets_utiles = sorted({a for it in definitions for nom in it["etapes"]
                                for a in (TRAJET_PAR_NOM[nom]["depart"], TRAJET_PAR_NOM[nom]["arrivee"])})
        passages_utiles = s.filter(F.col("arret").isin(arrets_utiles)).select(
            "jour", "course_id", "operateur", "ligne", "arret", "depart_prevu", "depart_reel", "depart_statut",
            "arrivee_prevue", "arrivee_reelle", "arrivee_statut", "supprime", "passage_sans_arret").cache()
        itineraires, correspondances, libelles = [], [], []
        for it in definitions:
            etapes = [TRAJET_PAR_NOM[nom] for nom in it["etapes"]]
            n = len(etapes)
            nom_public = it.get("nom_public") or f"Itinéraire {len(libelles) + 1}"
            libelles.append((it["nom"], nom_public, len(libelles) + 1))
            delais = it.get("correspondance_min", 3)
            delais = list(delais) if isinstance(delais, list) else [delais] * (n - 1)
            habituelles = [courses_entre(passages_utiles, t) for t in etapes]
            toutes = [courses_entre(passages_utiles, t, toutes_lignes=True) for t in etapes]

            # 1. Courses prévues (p) : l'horaire idéal, sur les lignes habituelles
            debut, fin = etapes[0]["heure_depart"], plus_minutes(etapes[0]["heure_depart"], etapes[0].get("tolerance_min", 10))
            heure = F.date_format("depart_prevu", "HH:mm")
            voyage = avec_prefixe(premiere_du_jour(habituelles[0].filter((heure >= debut) & (heure <= fin)), "depart_prevu"), "p1")
            for k in range(2, n + 1):
                suite = voyage.join(avec_prefixe(habituelles[k - 1], f"p{k}"), "jour").filter(
                    F.col(f"p{k}_depart_prevu") >= apres(f"p{k - 1}_arrivee_prevue", delais[k - 2]))
                voyage = premiere_du_jour(suite, f"p{k}_depart_prevu")

            for k in range(1, n + 1):
                # 2. Course visée (q) : la course prévue, ou après un écart celle qui arrive le plus tôt, toutes lignes
                if k == 1:
                    voyage = voyage.select("*", *[F.col(f"p1_{c}").alias(f"q1_{c}") for c in COLONNES_COURSE])
                else:
                    repli = premiere_du_jour(
                        voyage.select("jour", f"r{k - 1}_arrivee_prevue").join(avec_prefixe(toutes[k - 1], "a"), "jour")
                        .filter(F.col("a_depart_prevu") >= apres(f"r{k - 1}_arrivee_prevue", delais[k - 2])),
                        "a_arrivee_prevue", "a_depart_prevu",
                    ).drop(f"r{k - 1}_arrivee_prevue")
                    voyage = voyage.join(repli, "jour", "left")
                    sans_ecart = ~differente(f"r{k - 1}_course_id", f"p{k - 1}_course_id")
                    voyage = voyage.select(
                        *[c for c in voyage.columns if not c.startswith("a_")],
                        *[F.when(sans_ecart, F.col(f"p{k}_{c}")).otherwise(F.col(f"a_{c}")).alias(f"q{k}_{c}") for c in COLONNES_COURSE],
                    )

                # 3. Course prise (r) : la course visée si elle est prenable, sinon le premier véhicule qui arrive
                prenable = (F.col(f"r{k}_depart_prevu") >= F.col(f"q{k}_depart_prevu")) & ~F.col(f"r{k}_supprime")
                if k > 1:
                    a_quai = F.greatest(f"r{k}_depart_eff", f"r{k}_depart_prevu")
                    prenable = prenable & (a_quai >= apres(f"r{k - 1}_arrivee_eff", delais[k - 2]))
                est_visee = (F.col(f"r{k}_course_id") == F.col(f"q{k}_course_id")).cast("int")
                prise = premiere_du_jour(
                    voyage.join(avec_prefixe(toutes[k - 1], f"r{k}"), "jour").filter(prenable),
                    est_visee.desc(), f"r{k}_arrivee_prevue", f"r{k}_depart_prevu",
                )
                voyage = voyage.join(prise.select("jour", *[c for c in prise.columns if c.startswith(f"r{k}_")]), "jour", "left")

            manquee = [differente(f"r{k}_course_id", f"q{k}_course_id") for k in range(1, n + 1)]
            suppression = functools.reduce(
                lambda a, b: a | b, [manquee[k - 1] & F.coalesce(F.col(f"q{k}_supprime"), F.lit(False)) for k in range(1, n + 1)])
            mesure = functools.reduce(
                lambda a, b: a & b,
                [F.coalesce(F.col(f"r{k}_arrivee_mesure"), F.lit(False)) for k in range(1, n + 1)]
                + [F.coalesce(F.col(f"q{k}_depart_mesure"), F.lit(False)) for k in range(2, n + 1)],
            )
            objectif = it.get("objectif_arrivee")
            itineraires.append(
                voyage.select(
                    "jour",
                    F.lit(it["nom"]).alias("itineraire"),
                    F.lit(n).alias("nb_etapes"),
                    F.col("p1_depart_prevu").alias("depart_prevu"),
                    F.col(f"p{n}_arrivee_prevue").alias("arrivee_prevue"),
                    F.col(f"r{n}_arrivee_eff").alias("arrivee_reelle"),
                    F.concat_ws(" > ", *[F.col(f"r{k}_ligne") for k in range(1, n + 1)]).alias("lignes_prises"),
                    sum((m.cast("int") for m in manquee[1:]), F.lit(0)).alias("correspondances_manquees"),
                    F.when(F.col(f"r{n}_course_id").isNull(), "sans solution")
                    .when(suppression, "course supprimée")
                    .when(functools.reduce(lambda a, b: a | b, manquee), "correspondance manquée")
                    .otherwise("comme prévu").alias("statut"),
                    mesure.alias("mesure_complete"),
                )
                .withColumn("retard_min", ecart_min("arrivee_reelle", "arrivee_prevue"))
                .withColumn("duree_prevue_min", ecart_min("arrivee_prevue", "depart_prevu"))
                .withColumn("duree_reelle_min", ecart_min("arrivee_reelle", "depart_prevu"))
                .withColumn("objectif_arrivee", F.lit(objectif))
                .withColumn(
                    "arrive_avant_objectif",
                    F.when(F.lit(objectif).isNull() | F.col("arrivee_reelle").isNull(), F.lit(None).cast("boolean"))
                    .otherwise(F.date_format("arrivee_reelle", "HH:mm:ss") < F.lit(f"{objectif}:00")),
                )
            )

            # Une ligne par correspondance et par jour : la marge réelle entre l'arrivée et le départ de la course visée
            for k in range(2, n + 1):
                depart_vise = F.greatest(f"q{k}_depart_eff", f"q{k}_depart_prevu")
                tenue = ~manquee[k - 1]
                correspondances.append(
                    voyage.select(
                        "jour",
                        F.lit(it["nom"]).alias("itineraire"),
                        F.lit(k - 1).alias("rang"),
                        F.lit(f"{etapes[k - 2]['ligne']} → {etapes[k - 1]['ligne']}").alias("correspondance"),
                        F.lit(f"{nom_public.split(' : ')[0]}, correspondance {k - 1}").alias("correspondance_public"),
                        F.lit(etapes[k - 1]["depart"]).alias("arret"),
                        F.lit(int(delais[k - 2])).alias("minimum_min"),
                        F.col(f"r{k - 1}_arrivee_eff").alias("arrivee_reelle_amont"),
                        F.col(f"q{k}_depart_prevu").alias("depart_prevu"),
                        depart_vise.alias("depart_reel"),
                        ecart_min(f"q{k}_depart_prevu", f"r{k - 1}_arrivee_prevue").alias("marge_prevue_min"),
                        ecart_min(depart_vise, f"r{k - 1}_arrivee_eff").alias("marge_reelle_min"),
                        tenue.alias("tenue"),
                        F.when(tenue, F.lit(None).cast("string"))
                        .when(F.coalesce(F.col(f"q{k}_supprime"), F.lit(False)), "course supprimée")
                        .otherwise("arrivée trop tardive").alias("cause"),
                        F.col(f"r{k}_ligne").alias("ligne_prise"),
                        ecart_min(f"r{k}_depart_prevu", f"q{k}_depart_prevu").alias("attente_supplementaire_min"),
                        (F.coalesce(F.col(f"r{k - 1}_arrivee_mesure"), F.lit(False))
                         & F.coalesce(F.col(f"q{k}_depart_mesure"), F.lit(False))).alias("mesure_complete"),
                    ).withColumn("marge_reelle_arrondie", F.floor("marge_reelle_min").cast("int"))
                )

        ecrire(spark.createDataFrame(libelles, "itineraire string, libelle_public string, ordre int"), "gold.dim_itineraire")
        jours = dim_date.select("jour", "nom_jour", "jour_ouvre")
        ecrire(functools.reduce(lambda a, b: a.unionByName(b), itineraires).join(jours, "jour", "left"), "gold.fait_itineraire")
        if correspondances:
            ecrire(functools.reduce(lambda a, b: a.unionByName(b), correspondances).join(jours, "jour", "left"), "gold.fait_correspondance")
        passages_utiles.unpersist()
        bilan = (spark.table("gold.fait_itineraire").filter("jour_ouvre").groupBy("itineraire", "statut").count()
                 .orderBy("itineraire", "statut").collect())
        journal("itineraires", "\n".join(f"{b['itineraire']} | {b['statut']} | {b['count']}" for b in bilan))
except Exception as e:
    print(f"Itinéraires non mis à jour : {type(e).__name__} : {e}")
    journal("itineraires", traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Recette : les tables gold sont contrôlées avant d'être publiées dans le modèle sémantique.
# - Contrôles bloquants : ceux dont l'échec casse ou fausse le rapport. Clé de dimension en double au sens de Power BI
#   (qui compare les textes sans la casse), fait sans dimension, grain non respecté, table vide. En cas d'échec, le
#   modèle n'est pas rafraîchi et le notebook se termine en erreur.
# - Alertes : fraîcheur, volume, jours manquants, heures réelles, trajets non trouvés. Elles sont consignées sans
#   rien bloquer : un fichier source en retard n'est pas une faute de la transformation.
# Chaque exécution ajoute ses résultats à `gold.qualite`, qui garde l'historique.
# Exercice d'alerte : un fichier `Files/config/simuler_echec_recette` dans le lakehouse fait échouer la prochaine
# recette, pour vérifier que le blocage fonctionne ; il est retiré aussitôt.
import os
from datetime import datetime, timedelta
from statistics import median
from zoneinfo import ZoneInfo

EXECUTION = datetime.now(ZoneInfo("Europe/Zurich")).replace(tzinfo=None, microsecond=0)
controles = []


def cle(colonne):
    """La clé telle que Power BI la compare : sans tenir compte de la casse ni des espaces superflus."""
    return F.lower(F.trim(F.regexp_replace(F.col(colonne).cast("string"), r"\s+", " ")))


def controler(nom, famille, gravite, calcul):
    """Exécute un contrôle et le consigne. Une erreur pendant le calcul compte comme un échec, avec sa cause."""
    try:
        reussi, valeur, seuil, detail = calcul()
    except Exception as e:
        reussi, valeur, seuil, detail = False, None, "", f"{type(e).__name__} : {e}"
    controles.append((nom, famille, gravite, "réussi" if reussi else "échoué",
                      None if valeur is None else float(valeur), seuil, str(detail)[:500]))


def unicite(table, colonne):
    def calcul():
        doublons = spark.table(table).groupBy(cle(colonne).alias("k")).count().filter("count > 1")
        n = doublons.count()
        return n == 0, n, "aucune clé en double", ", ".join(str(r["k"]) for r in doublons.limit(3).collect())
    return calcul


def integrite(fait, colonne_fait, dimension, colonne_dimension):
    def calcul():
        connues = spark.table(dimension).select(cle(colonne_dimension).alias("k")).distinct()
        orphelines = (spark.table(fait).filter(F.col(colonne_fait).isNotNull())
                      .select(cle(colonne_fait).alias("k")).distinct().join(connues, "k", "left_anti"))
        n = orphelines.count()
        return n == 0, n, "aucune valeur sans dimension", ", ".join(str(r["k"]) for r in orphelines.limit(3).collect())
    return calcul


def grain(table, colonnes):
    def calcul():
        n = spark.table(table).groupBy(*colonnes).count().filter("count > 1").count()
        return n == 0, n, "une seule ligne par " + " et ".join(colonnes), ""
    return calcul


def existe(table):
    return spark.catalog.tableExists(table)


# Contrôles bloquants
OBLIGATOIRES = ["gold.dim_date", "gold.dim_ligne", "gold.dim_arret", "gold.dim_trajet", "gold.fait_passage", "gold.fait_trajet"]
controler("Tables obligatoires présentes et non vides", "complétude", "bloquant", lambda: (
    lambda vides: (not vides, len(vides), "aucune table vide", ", ".join(vides)))(
        [x for x in OBLIGATOIRES if not existe(x) or spark.table(x).limit(1).count() == 0]))

for table, colonne in [("gold.dim_date", "jour"), ("gold.dim_ligne", "ligne_cle"), ("gold.dim_arret", "arret"),
                       ("gold.dim_trajet", "trajet"), ("gold.dim_itineraire", "itineraire")]:
    if existe(table):
        controler(f"Clé unique : {table.split('.')[1]}", "unicité", "bloquant", unicite(table, colonne))

for fait, colonne_fait, dimension, colonne_dimension in [
    ("gold.fait_passage", "jour", "gold.dim_date", "jour"),
    ("gold.fait_passage", "ligne_cle", "gold.dim_ligne", "ligne_cle"),
    ("gold.fait_passage", "arret", "gold.dim_arret", "arret"),
    ("gold.fait_trajet", "jour", "gold.dim_date", "jour"),
    ("gold.fait_trajet", "trajet", "gold.dim_trajet", "trajet"),
    ("gold.prediction", "trajet", "gold.dim_trajet", "trajet"),
    ("gold.prediction_jugee", "trajet", "gold.dim_trajet", "trajet"),
    ("gold.fait_itineraire", "itineraire", "gold.dim_itineraire", "itineraire"),
    ("gold.fait_correspondance", "itineraire", "gold.dim_itineraire", "itineraire"),
]:
    if existe(fait) and existe(dimension):
        controler(f"Rattachement : {fait.split('.')[1]}.{colonne_fait}", "intégrité", "bloquant",
                  integrite(fait, colonne_fait, dimension, colonne_dimension))

for table, colonnes in [("gold.fait_trajet", ["jour", "trajet"]), ("gold.fait_itineraire", ["jour", "itineraire"]),
                        ("gold.prediction", ["jour_cible", "trajet"])]:
    if existe(table):
        controler(f"Grain : {table.split('.')[1]}", "grain", "bloquant", grain(table, colonnes))

# Alertes : elles portent sur le dernier jour chargé et sur les semaines qui le précèdent
passages = spark.table("gold.fait_passage")
par_jour = {l["jour"]: l["n"] for l in passages.groupBy("jour").agg(F.count("*").alias("n")).collect()}
dernier = max(par_jour)
veille = EXECUTION.date() - timedelta(days=1)


def fraicheur():
    retard = (veille - dernier).days
    return retard <= 0, retard, "données de la veille", f"dernier jour chargé : {dernier:%d.%m.%Y}"


def volume():
    semblables = [n for jour, n in par_jour.items()
                  if 0 < (dernier - jour).days <= 28 and (jour.weekday() < 5) == (dernier.weekday() < 5)]
    if not semblables:
        return True, None, "entre 50 % et 150 % de la médiane", "pas assez d'historique"
    rapport = par_jour[dernier] / median(semblables)
    return 0.5 <= rapport <= 1.5, round(rapport, 2), "entre 50 % et 150 % de la médiane des 4 dernières semaines", \
        f"{par_jour[dernier]} passages, médiane {int(median(semblables))}"


def jours_manquants():
    absents = [dernier - timedelta(days=i) for i in range(35) if dernier - timedelta(days=i) not in par_jour]
    return not absents, len(absents), "aucun jour manquant sur 35 jours", ", ".join(f"{j:%d.%m.%Y}" for j in absents[:8])


def heures_reelles():
    part = (passages.filter((F.col("jour") == F.lit(dernier)) & F.col("arrivee_prevue").isNotNull())
            .agg(F.avg((F.col("arrivee_statut") == "REAL").cast("double")).alias("p")).first()["p"])
    return part is not None and part >= 0.5, None if part is None else round(part, 3), "au moins 50 % d'heures réelles mesurées", ""


def trajets_trouves():
    jour = dernier
    while jour.weekday() >= 5:
        jour -= timedelta(days=1)
    prevus = [x["nom"] for x in CONFIG["trajets"] if re.fullmatch(r"\d{2}:\d{2}", x["heure_depart"])
              and (x.get("jours") != "ven" or jour.weekday() == 4)]
    trouves = {l["trajet"] for l in spark.table("gold.fait_trajet").filter(F.col("jour") == F.lit(jour)).select("trajet").collect()}
    absents = [x for x in prevus if x not in trouves]
    return not absents, len(absents), "tous les trajets suivis trouvés", f"{jour:%d.%m.%Y} : " + ", ".join(absents) if absents else ""


controler("Fraîcheur : données de la veille chargées", "fraîcheur", "alerte", fraicheur)
controler("Volume du dernier jour", "volume", "alerte", volume)
controler("Jours manquants sur les 35 derniers jours", "complétude", "alerte", jours_manquants)
controler("Heures réelles mesurées le dernier jour", "mesure", "alerte", heures_reelles)
controler("Trajets suivis trouvés le dernier jour ouvré", "complétude", "alerte", trajets_trouves)

# Exercice d'alerte : un fichier déposé dans le lakehouse fait échouer la recette, une fois
DRAPEAU = "/lakehouse/default/Files/config/simuler_echec_recette"
if os.path.exists(DRAPEAU):
    os.remove(DRAPEAU)
    controles.append(("Exercice d'alerte : échec simulé", "exercice", "bloquant", "échoué", None, "",
                      "fichier Files/config/simuler_echec_recette présent"))

bloquants = [c for c in controles if c[2] == "bloquant" and c[3] == "échoué"]
alertes = [c for c in controles if c[2] == "alerte" and c[3] == "échoué"]
RECETTE_BLOQUEE = bool(bloquants)

recette = (
    spark.createDataFrame(controles, "controle string, famille string, gravite string, statut string, valeur double, seuil string, detail string")
    .withColumn("execution", F.lit(EXECUTION).cast("timestamp"))
    .withColumn("jour_controle", F.lit(dernier).cast("date"))
    .withColumn("derniere_execution", F.lit(True))
)
if existe("gold.qualite"):
    recette = spark.table("gold.qualite").withColumn("derniere_execution", F.lit(False)).unionByName(recette, allowMissingColumns=True)
ecrire(recette, "gold.qualite")

verdict = "BLOQUÉE" if RECETTE_BLOQUEE else "validée"
bilan = [f"Recette {verdict} : {len(controles)} contrôles, {len(bloquants)} échec(s) bloquant(s), {len(alertes)} alerte(s)"]
bilan += [f"{c[2]} | {c[0]} | {c[6]}" for c in bloquants + alertes]
print("\n".join(bilan))
journal("qualite", "\n".join(bilan))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dernière étape : publier. Le modèle sémantique Direct Lake relit les tables gold (« recadrage ») : c'est ce
# rafraîchissement qui rend les données du jour visibles dans le rapport.
# - Recette validée : toutes les tables sont publiées.
# - Recette bloquée : seule la table de la recette est publiée, pour que le rapport montre l'échec ; les autres
#   tables restent sur leur dernière version validée, et le notebook se termine en erreur.
# Le rafraîchissement lui-même ne fait jamais échouer la transformation : si le modèle n'est pas déployé, il est ignoré.
MODELE_SEMANTIQUE = "Ponctualite"

try:
    import sempy.fabric as fabric

    if RECETTE_BLOQUEE:
        demande = fabric.refresh_dataset(dataset=MODELE_SEMANTIQUE, refresh_type="full", objects=[{"table": "qualite"}])
        print(f"Modèle sémantique {MODELE_SEMANTIQUE} : seule la table de la recette est publiée ({demande})")
    else:
        demande = fabric.refresh_dataset(dataset=MODELE_SEMANTIQUE, refresh_type="full")
        print(f"Modèle sémantique {MODELE_SEMANTIQUE} : rafraîchissement demandé ({demande})")
except Exception as e:  # modèle absent, droits manquants ou service indisponible
    print(f"Modèle sémantique {MODELE_SEMANTIQUE} non rafraîchi : {type(e).__name__} : {e}")

if RECETTE_BLOQUEE:
    raise RuntimeError("Recette de qualité en échec, tables non publiées : " + " ; ".join(c[0] for c in bloquants))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
