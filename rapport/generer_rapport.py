"""Génère le rapport Power BI « Ponctualité » au format PBIR (un dossier par page et par visuel).

Usage : python rapport/generer_rapport.py <id du modèle sémantique Ponctualite> [--public] [--sortie=<dossier>]

Mode public (--public) : le rapport affiche les libellés génériques des trajets et des itinéraires (colonnes
« libelle_public » des dimensions) et laisse de côté les heures de passage. Les données sont les mêmes, seul
l'affichage change : c'est ce mode qui sert aux captures et aux démonstrations. --sortie écrit le rapport dans un
autre dossier que celui du dépôt, pour ne pas toucher à la version versionnée.
Puis  : .\\ponctualite\\fab.ps1 import "Ponctualite.Workspace/Ponctualité.Report" -i .\\ponctualite\\rapport\\Ponctualite.Report -f
"""

import json
import shutil
import sys
from pathlib import Path

ICI = Path(__file__).parent
DOSSIER = ICI / "Ponctualite.Report"
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"
ESPACE = "Ponctualite"
MODELE = "Ponctualite"
LARGEUR, HAUTEUR, MARGE = 1280, 720, 20
PUBLIC = "--public" in sys.argv


def ecrire(chemin, contenu):
    chemin.parent.mkdir(parents=True, exist_ok=True)
    chemin.write_text(json.dumps(contenu, ensure_ascii=False, indent=2), encoding="utf-8")


def mesure(table, nom):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": table}}, "Property": nom}}, f"{table}.{nom}", nom


def colonne(table, nom):
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": nom}}, f"{table}.{nom}", nom


def litteral(valeur):
    return {"expr": {"Literal": {"Value": valeur}}}


def filtre_egal(table, nom, valeur):
    """Filtre de page : la colonne vaut la valeur donnée (littéral : true, 'texte', 3L…). Visible dans le volet Filtres."""
    return {
        "name": f"filtre_{table}_{nom}",
        "field": {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": nom}},
        "type": "Categorical",
        "filter": {
            "Version": 2,
            "From": [{"Name": "t", "Entity": table, "Type": 0}],
            "Where": [{"Condition": {"In": {
                "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": nom}}],
                "Values": [[{"Literal": {"Value": valeur}}]],
            }}}],
        },
        "howCreated": "User",
    }


def fond_degrade(champ, minimum="0.5D", maximum="1D", couleur_min="#D64550", couleur_max="#1AAB40"):
    """Mise en forme conditionnelle : fond des cellules du rouge (min) au vert (max) selon la mesure."""
    expr, ref, _ = champ
    return {
        "values": [
            {
                "properties": {
                    "backColor": {
                        "solid": {
                            "color": {
                                "expr": {
                                    "FillRule": {
                                        "Input": expr,
                                        "FillRule": {
                                            "linearGradient2": {
                                                "min": {"color": {"Literal": {"Value": f"'{couleur_min}'"}}},
                                                "max": {"color": {"Literal": {"Value": f"'{couleur_max}'"}}},
                                                "nullColoringStrategy": {"strategy": {"Literal": {"Value": "'asZero'"}}},
                                            }
                                        },
                                    }
                                }
                            }
                        }
                    }
                },
                # Les deux clés sont nécessaires : le joker applique la règle cellule par cellule
                "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}], "metadata": ref},
            }
        ]
    }


class Page:
    def __init__(self, nom, titre, filtres=None):
        self.nom, self.titre, self.visuels, self.filtres = nom, titre, [], filtres or []

    def visuel(self, type_visuel, x, y, l, h, roles, titre=None, objets=None, tri=None):
        etat = {}
        for role, champs in roles.items():
            etat[role] = {
                "projections": [
                    {"field": f, "queryRef": ref, "nativeQueryRef": natif, **({"active": True} if role == "Category" else {})}
                    for f, ref, natif in champs
                ]
            }
        requete = {"queryState": etat}
        if tri:
            champ, direction = tri
            requete["sortDefinition"] = {"sort": [{"field": champ[0], "direction": direction}], "isDefaultSort": True}
        visuel = {"visualType": type_visuel, "query": requete, "drillFilterOtherVisuals": True}
        if objets:
            visuel["objects"] = objets
        if titre:
            visuel["visualContainerObjects"] = {
                "title": [{"properties": {"show": litteral("true"), "text": litteral(f"'{titre}'")}}]
            }
        nom = f"{self.nom}_{len(self.visuels):02d}"
        self.visuels.append(
            {
                "$schema": f"{SCHEMA}/visualContainer/1.5.0/schema.json",
                "name": nom,
                "position": {"x": x, "y": y, "z": len(self.visuels), "width": l, "height": h, "tabOrder": len(self.visuels)},
                "visual": visuel,
            }
        )

    def segment(self, x, y, l, h, champ, titre=None, mode="Dropdown"):
        self.visuel(
            "slicer", x, y, l, h, {"Values": [champ]}, titre,
            {"data": [{"properties": {"mode": litteral(f"'{mode}'")}}],
             "header": [{"properties": {"show": litteral("false")}}]},
        )

    def cartes(self, y, champs, x=MARGE, largeur_totale=LARGEUR - 2 * MARGE, hauteur=100):
        largeur = (largeur_totale - 10 * (len(champs) - 1)) // len(champs)
        for i, c in enumerate(champs):
            self.visuel("card", x + i * (largeur + 10), y, largeur, hauteur, {"Values": [c]})

    def enregistrer(self, racine):
        page = {
            "$schema": f"{SCHEMA}/page/1.3.0/schema.json",
            "name": self.nom,
            "displayName": self.titre,
            "displayOption": "FitToPage",
            "height": HAUTEUR,
            "width": LARGEUR,
        }
        if self.filtres:
            page["filterConfig"] = {"filters": self.filtres}
        ecrire(racine / self.nom / "page.json", page)
        for v in self.visuels:
            ecrire(racine / self.nom / "visuals" / v["name"] / "visual.json", v)


def pages():
    # Libellés des trajets, des itinéraires et des correspondances : le nom configuré, ou le libellé public
    trajet = colonne("dim_trajet", "libelle_public" if PUBLIC else "trajet")
    itineraire = colonne("dim_itineraire", "libelle_public" if PUBLIC else "itineraire")
    correspondance = colonne("fait_correspondance", "correspondance_public" if PUBLIC else "correspondance")

    # Page 1 : mes trajets suivis
    trajets = Page("trajets", "Mes trajets")
    trajets.segment(MARGE, MARGE, 295, 60, trajet, "Trajet")
    trajets.segment(335, MARGE, 295, 60, colonne("dim_date", "nom_jour"), "Jour de la semaine")
    trajets.segment(650, MARGE, 295, 60, colonne("dim_date", "nom_mois"), "Mois")
    trajets.segment(965, MARGE, 295, 60, colonne("dim_date", "jour_ouvre"), "Jour ouvré")
    trajets.cartes(
        100,
        [
            mesure("fait_trajet", "Ponctualité"),
            mesure("fait_trajet", "Arrivées avant l'objectif"),
            mesure("fait_trajet", "Retard moyen (min)"),
            mesure("fait_trajet", "Retard max (min)"),
            mesure("fait_trajet", "Jours à plus de 3 min"),
            mesure("fait_trajet", "Suppressions"),
            mesure("fait_trajet", "Minutes perdues"),
        ],
    )
    trajets.visuel(
        "clusteredColumnChart", MARGE, 220, 820, 230,
        {"Category": [colonne("dim_date", "jour")], "Series": [trajet],
         "Y": [mesure("fait_trajet", "Retard moyen (min)")]},
        "Retard à l'arrivée, jour par jour",
    )
    trajets.visuel(
        "clusteredBarChart", 860, 220, 400, 230,
        {"Category": [trajet], "Y": [mesure("fait_trajet", "Ponctualité")]},
        "Ponctualité par trajet",
    )
    trajets.visuel(
        "clusteredColumnChart", MARGE, 465, 500, 235,
        {"Category": [colonne("dim_date", "nom_jour")], "Series": [trajet],
         "Y": [mesure("fait_trajet", "Ponctualité")]},
        "Ponctualité selon le jour de la semaine",
    )
    trajets.visuel(
        "tableEx", 540, 465, 720, 235,
        {"Values": [
            colonne("fait_trajet", "jour"), colonne("fait_trajet", "nom_jour"), trajet,
            *([] if PUBLIC else [colonne("fait_trajet", "arrivee_prevue"), colonne("fait_trajet", "arrivee_reelle")]),
            mesure("fait_trajet", "Retard moyen (min)"), mesure("fait_trajet", "Suppressions"),
        ]},
        "Les pires jours",
        tri=(mesure("fait_trajet", "Retard moyen (min)"), "Descending"),
    )

    # Page 2 : les itinéraires porte à porte, correspondances comprises
    # Filtres de page : jours ouvrés, et seulement les jours où toutes les heures utiles ont été mesurées.
    porte = Page("porte", "Porte à porte", filtres=[
        filtre_egal("dim_date", "jour_ouvre", "true"),
        filtre_egal("fait_itineraire", "mesure_complete", "true"),
        filtre_egal("fait_correspondance", "mesure_complete", "true"),
    ])
    porte.segment(MARGE, MARGE, 400, 60, colonne("dim_date", "annee"), "Année")
    porte.segment(440, MARGE, 400, 60, colonne("dim_date", "nom_mois"), "Mois")
    porte.segment(860, MARGE, 400, 60, colonne("fait_itineraire", "statut"), "Déroulement du voyage")
    porte.visuel(
        "tableEx", MARGE, 100, 1240, 125,
        {"Values": [
            itineraire, mesure("fait_itineraire", "Voyages"),
            mesure("fait_itineraire", "Part des voyages comme prévu"), mesure("fait_itineraire", "Taux de correspondance manquée"),
            mesure("fait_itineraire", "Arrivées avant l'objectif, porte à porte"), mesure("fait_itineraire", "Voyages arrivés après l'objectif"),
            mesure("fait_itineraire", "Retard porte à porte moyen (min)"), mesure("fait_itineraire", "Retard porte à porte max (min)"),
            mesure("fait_itineraire", "Durée prévue moyenne (min)"), mesure("fait_itineraire", "Durée réelle moyenne (min)"),
        ]},
        "Bilan de chaque itinéraire, toutes étapes enchaînées",
    )
    porte.visuel(
        "clusteredBarChart", MARGE, 240, 600, 220,
        {"Category": [colonne("fait_itineraire", "statut")], "Series": [itineraire],
         "Y": [mesure("fait_itineraire", "Voyages")]},
        "Comment se passent les voyages",
    )
    porte.visuel(
        "lineChart", 640, 240, 620, 220,
        {"Category": [colonne("dim_date", "debut_mois")], "Series": [itineraire],
         "Y": [mesure("fait_itineraire", "Taux de correspondance manquée")]},
        "Voyages avec une correspondance manquée, mois après mois",
    )
    porte.visuel(
        "tableEx", MARGE, 475, 600, 225,
        {"Values": [
            correspondance, mesure("fait_correspondance", "Correspondances"),
            mesure("fait_correspondance", "Part des correspondances tenues"), mesure("fait_correspondance", "Marge prévue (min)"),
            mesure("fait_correspondance", "Minimum nécessaire (min)"), mesure("fait_correspondance", "Marge réelle moyenne (min)"),
            mesure("fait_correspondance", "Attente supplémentaire médiane (min)"),
        ]},
        "Chaque correspondance : la marge prévue, le minimum nécessaire et la marge réelle",
    )
    porte.visuel(
        "clusteredColumnChart", 640, 475, 620, 225,
        {"Category": [colonne("fait_correspondance", "marge_reelle_arrondie")], "Series": [correspondance],
         "Y": [mesure("fait_correspondance", "Correspondances")]},
        "Marge réelle à chaque correspondance, en minutes",
    )

    # Page 3 : lignes et gares (tous les passages du périmètre)
    lignes = Page("lignes", "Lignes et gares")
    lignes.segment(MARGE, MARGE, 400, 60, colonne("dim_ligne", "libelle"), "Ligne")
    lignes.segment(440, MARGE, 400, 60, colonne("dim_arret", "arret"), "Gare ou arrêt")
    lignes.segment(860, MARGE, 400, 60, colonne("dim_date", "jour_ouvre"), "Jour ouvré")
    lignes.cartes(
        100,
        [
            mesure("fait_passage", "Passages"),
            mesure("fait_passage", "Ponctualité des passages"),
            mesure("fait_passage", "Retard moyen des passages (min)"),
            mesure("fait_passage", "Retard P90 (min)"),
            mesure("fait_passage", "Passages supprimés"),
        ],
    )
    lignes.visuel(
        "clusteredBarChart", MARGE, 220, 500, 480,
        {"Category": [colonne("dim_ligne", "libelle")], "Y": [mesure("fait_passage", "Ponctualité des passages")]},
        "Ponctualité par ligne",
        tri=(mesure("fait_passage", "Ponctualité des passages"), "Descending"),
    )
    lignes.visuel(
        "lineChart", 540, 220, 720, 230,
        {"Category": [colonne("fait_passage", "heure_du_jour")], "Y": [mesure("fait_passage", "Retard moyen des passages (min)")]},
        "Retard moyen selon l'heure de la journée",
    )
    lignes.visuel(
        "pivotTable", 540, 465, 720, 235,
        {"Rows": [colonne("dim_ligne", "libelle")], "Columns": [colonne("dim_date", "nom_jour")],
         "Values": [mesure("fait_passage", "Ponctualité des passages")]},
        "Ponctualité par ligne et jour de la semaine",
        objets=fond_degrade(mesure("fait_passage", "Ponctualité des passages")),
    )

    # Page 4 : quelle alternative prendre
    alternatives = Page("alternatives", "Alternatives")
    alternatives.segment(MARGE, MARGE, 400, 60, colonne("dim_arret", "arret"), "Gare d'arrivée")
    alternatives.segment(440, MARGE, 400, 60, colonne("dim_ligne", "libelle"), "Ligne")
    alternatives.segment(860, MARGE, 400, 60, colonne("dim_date", "jour_ouvre"), "Jour ouvré")
    alternatives.visuel(
        "pivotTable", MARGE, 100, 1240, 300,
        {"Rows": [colonne("dim_ligne", "libelle")], "Columns": [colonne("fait_passage", "heure_du_jour")],
         "Values": [mesure("fait_passage", "Ponctualité des passages")]},
        "Ponctualité à l'arrivée par ligne et heure : les cases vertes sont les trains à prendre",
        objets=fond_degrade(mesure("fait_passage", "Ponctualité des passages")),
    )
    alternatives.visuel(
        "tableEx", MARGE, 415, 1240, 285,
        {"Values": [
            colonne("dim_ligne", "libelle"), colonne("fait_passage", "heure_prevue"),
            mesure("fait_passage", "Passages"), mesure("fait_passage", "Ponctualité des passages"),
            mesure("fait_passage", "Retard moyen des passages (min)"), mesure("fait_passage", "Retard P90 par horaire (min)"),
            mesure("fait_passage", "Passages supprimés"),
        ]},
        "Chaque horaire à l'arrivée, avec sa fiabilité (le retard P90 s'affiche une fois la gare choisie)",
        tri=(colonne("fait_passage", "heure_prevue"), "Ascending"),
    )
    # Page 5 : la météo du matin explique-t-elle les retards ?
    meteo = Page("meteo", "Météo")
    meteo.segment(MARGE, MARGE, 400, 60, trajet, "Trajet")
    meteo.segment(440, MARGE, 400, 60, colonne("fait_trajet", "conditions_matin"), "Conditions du matin")
    meteo.segment(860, MARGE, 400, 60, colonne("dim_date", "jour_ouvre"), "Jour ouvré")
    meteo.cartes(
        100,
        [
            mesure("fait_trajet", "Ponctualité"),
            mesure("fait_trajet", "Retard moyen (min)"),
            mesure("fait_trajet", "Jours de gel"),
            mesure("fait_trajet", "Température du matin (°C)"),
            mesure("fait_trajet", "Précipitations du matin (mm)"),
        ],
    )
    meteo.visuel(
        "clusteredBarChart", MARGE, 220, 500, 230,
        {"Category": [colonne("fait_trajet", "conditions_matin")], "Y": [mesure("fait_trajet", "Ponctualité")]},
        "Ponctualité selon les conditions du matin",
        tri=(mesure("fait_trajet", "Ponctualité"), "Descending"),
    )
    meteo.visuel(
        "clusteredColumnChart", 540, 220, 720, 230,
        {"Category": [colonne("fait_trajet", "conditions_matin")], "Series": [trajet],
         "Y": [mesure("fait_trajet", "Retard moyen (min)")]},
        "Retard moyen selon les conditions, par trajet",
    )
    meteo.visuel(
        "scatterChart", MARGE, 465, 620, 235,
        {"Category": [colonne("fait_trajet", "jour")], "X": [mesure("fait_trajet", "Température min. du matin (°C)")],
         "Y": [mesure("fait_trajet", "Retard moyen (min)")], "Series": [trajet]},
        "Chaque jour : température minimale du matin et retard à l'arrivée",
    )
    meteo.visuel(
        "scatterChart", 660, 465, 600, 235,
        {"Category": [colonne("fait_trajet", "jour")], "X": [mesure("fait_trajet", "Précipitations du matin (mm)")],
         "Y": [mesure("fait_trajet", "Retard moyen (min)")], "Series": [trajet]},
        "Chaque jour : précipitations du matin et retard à l'arrivée",
    )
    # Page 6 : le risque de demain, et ce que vaut le modèle
    # Filtre de page : seule la prévision la plus récente (celle du prochain trajet). L'historique reste accessible
    # en modifiant ce filtre dans le volet Filtres. Le segment en liste affiche donc le jour prédit en clair.
    demain = Page("demain", "Demain", filtres=[filtre_egal("prediction", "derniere_prevision", "true")])
    demain.segment(MARGE, MARGE, 400, 60, colonne("prediction", "jour_cible"), "Jour prédit", mode="Basic")
    demain.cartes(
        100,
        [
            mesure("prediction", "Risque de retard"),
            mesure("prediction", "Risque maximal"),
            mesure("prediction", "Taux historique du trajet"),
            mesure("modele_evaluation", "AUC du modèle"),
            mesure("modele_evaluation", "AUC de la référence"),
            mesure("modele_evaluation", "Jours de test"),
        ],
    )
    demain.visuel(
        "tableEx", MARGE, 220, 820, 240,
        {"Values": [
            colonne("prediction", "jour_cible"), trajet, colonne("prediction", "niveau"),
            mesure("prediction", "Risque de retard"), colonne("prediction", "taux_historique"),
            colonne("prediction", "taux_10_derniers_jours"), colonne("prediction", "temp_min_prevue"),
            colonne("prediction", "precip_prevue_mm"), colonne("prediction", "rafale_prevue_ms"),
        ]},
        "Risque de retard par trajet, avec la météo prévue du matin",
        objets=fond_degrade(mesure("prediction", "Risque de retard"), couleur_min="#1AAB40", couleur_max="#D64550"),
        tri=(mesure("prediction", "Risque de retard"), "Descending"),
    )
    demain.visuel(
        "clusteredBarChart", 860, 220, 400, 240,
        {"Category": [trajet], "Y": [mesure("prediction", "Risque de retard")]},
        "Risque par trajet",
        tri=(mesure("prediction", "Risque de retard"), "Descending"),
    )
    demain.visuel(
        "clusteredBarChart", MARGE, 475, 620, 225,
        {"Category": [colonne("modele_importance", "variable")], "Y": [mesure("modele_importance", "Importance de la variable")]},
        "Ce qui pèse dans la prédiction (importance par permutation)",
        tri=(mesure("modele_importance", "Importance de la variable"), "Descending"),
    )
    demain.visuel(
        "tableEx", 660, 475, 600, 225,
        {"Values": [
            colonne("modele_evaluation", "coupure"), colonne("modele_evaluation", "n_apprentissage"),
            colonne("modele_evaluation", "n_test"), colonne("modele_evaluation", "taux_retard_test"),
            colonne("modele_evaluation", "auc_modele"), colonne("modele_evaluation", "auc_reference"),
            colonne("modele_evaluation", "brier_modele"), colonne("modele_evaluation", "brier_reference"),
        ]},
        "Évaluation : apprentissage sur le passé, test sur les derniers jours",
    )
    # Page 7 : les prévisions passées face à ce qui s'est réellement produit
    jugement = Page("jugement", "Prévisions contre réalité")
    jugement.segment(MARGE, MARGE, 400, 60, trajet, "Trajet")
    jugement.segment(440, MARGE, 400, 60, colonne("prediction_jugee", "niveau"), "Niveau annoncé")
    jugement.segment(860, MARGE, 400, 60, colonne("prediction_jugee", "resultat"), "Résultat")
    jugement.cartes(
        100,
        [
            mesure("prediction_jugee", "Prévisions jugées"),
            mesure("prediction_jugee", "Taux de retard réel"),
            mesure("prediction_jugee", "Risque moyen annoncé"),
            mesure("prediction_jugee", "Alertes"),
            mesure("prediction_jugee", "Précision des alertes"),
            mesure("prediction_jugee", "Retards annoncés"),
            mesure("prediction_jugee", "Brier du modèle"),
            mesure("prediction_jugee", "Brier de la référence"),
        ],
    )
    jugement.visuel(
        "clusteredColumnChart", MARGE, 220, 500, 230,
        {"Category": [colonne("prediction_jugee", "niveau")],
         "Y": [mesure("prediction_jugee", "Risque moyen annoncé"), mesure("prediction_jugee", "Taux de retard réel")]},
        "Calibration : risque annoncé et retards réels, par niveau",
    )
    jugement.visuel(
        "lineChart", 540, 220, 720, 230,
        {"Category": [colonne("prediction_jugee", "jour_cible")],
         "Y": [mesure("prediction_jugee", "Risque moyen annoncé"), mesure("prediction_jugee", "Taux de retard réel")]},
        "Jour après jour : risque annoncé et retards réels",
    )
    jugement.visuel(
        "clusteredBarChart", MARGE, 465, 500, 235,
        {"Category": [colonne("prediction_jugee", "resultat")], "Y": [mesure("prediction_jugee", "Prévisions jugées")]},
        "Ce que chaque prévision est devenue",
        tri=(mesure("prediction_jugee", "Prévisions jugées"), "Descending"),
    )
    jugement.visuel(
        "tableEx", 540, 465, 720, 235,
        {"Values": [
            colonne("prediction_jugee", "jour_cible"), trajet, colonne("prediction_jugee", "niveau"),
            colonne("prediction_jugee", "resultat"), colonne("prediction_jugee", "probabilite_retard"),
            colonne("prediction_jugee", "retard_reel_min"), colonne("prediction_jugee", "supprime"),
        ]},
        "Chaque prévision et son issue",
        tri=(colonne("prediction_jugee", "jour_cible"), "Descending"),
    )
    return [trajets, porte, lignes, alternatives, meteo, demain, jugement]


def main():
    arguments = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(arguments) != 1:
        sys.exit(__doc__)
    id_modele = arguments[0]
    sortie = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--sortie=")), None)
    dossier = Path(sortie) / DOSSIER.name if sortie else DOSSIER
    if dossier.exists():
        shutil.rmtree(dossier)
    definition = dossier / "definition"

    ecrire(
        dossier / ".platform",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "Report", "displayName": "Ponctualité"},
            "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
        },
    )
    ecrire(
        dossier / "definition.pbir",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {
                "byConnection": {
                    "connectionString": f"Data Source=\"powerbi://api.powerbi.com/v1.0/myorg/{ESPACE}\";"
                    f"initial catalog={MODELE};integrated security=ClaimsToken;semanticmodelid={id_modele}"
                }
            },
        },
    )
    ecrire(definition / "version.json", {"$schema": f"{SCHEMA}/versionMetadata/1.0.0/schema.json", "version": "2.0.0"})
    ecrire(
        definition / "report.json",
        {
            "$schema": f"{SCHEMA}/report/1.2.0/schema.json",
            "themeCollection": {"baseTheme": {"name": "CY24SU10", "reportVersionAtImport": "5.61", "type": "SharedResources"}},
            "layoutOptimization": "None",
            "resourcePackages": [
                {"name": "SharedResources", "type": "SharedResources",
                 "items": [{"name": "CY24SU10", "path": "BaseThemes/CY24SU10.json", "type": "BaseTheme"}]}
            ],
            "settings": {
                "useStylableVisualContainerHeader": True,
                "defaultDrillFilterOtherVisuals": True,
                "allowChangeFilterTypes": True,
                "useEnhancedTooltips": True,
                "useDefaultAggregateDisplayName": True,
            },
        },
    )
    theme = dossier / "StaticResources" / "SharedResources" / "BaseThemes" / "CY24SU10.json"
    theme.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(ICI / "theme" / "CY24SU10.json", theme)

    liste = pages()
    ecrire(
        definition / "pages" / "pages.json",
        {"$schema": f"{SCHEMA}/pagesMetadata/1.0.0/schema.json", "pageOrder": [p.nom for p in liste], "activePageName": liste[0].nom},
    )
    for p in liste:
        p.enregistrer(definition / "pages")
    mode = " (mode public)" if PUBLIC else ""
    print(f"Rapport généré{mode} : {sum(len(p.visuels) for p in liste)} visuels sur {len(liste)} pages")


if __name__ == "__main__":
    main()
