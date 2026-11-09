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

import json

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

dim_arret = (
    s.groupBy("arret")
    .agg(F.first("bpuic").alias("bpuic"), F.countDistinct("ligne").alias("nb_lignes"), F.count("*").alias("nb_passages"))
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

# Un enregistrement par jour et par trajet suivi : la course prise au départ, et son arrivée réelle
morceaux = []
for t in CONFIG["trajets"]:
    depart = (
        s.filter((F.col("operateur") == t["operateur"]) & (F.col("ligne") == t["ligne"])
                 & (F.col("arret") == t["depart"]) & (F.date_format("depart_prevu", "HH:mm") == t["heure_depart"]))
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
ecrire(fait_trajet, "gold.fait_trajet")

fait_trajet.orderBy("trajet", "jour").select(
    "jour", "nom_jour", "trajet", "depart_prevu", "arrivee_prevue", "arrivee_reelle", "retard_arrivee_min", "ponctuel", "supprime"
).show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
