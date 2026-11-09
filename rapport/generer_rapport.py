"""Génère le rapport Power BI « Ponctualité » au format PBIR (un dossier par page et par visuel).

Usage : python rapport/generer_rapport.py <id du modèle sémantique Ponctualite>
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


def ecrire(chemin, contenu):
    chemin.parent.mkdir(parents=True, exist_ok=True)
    chemin.write_text(json.dumps(contenu, ensure_ascii=False, indent=2), encoding="utf-8")


def mesure(table, nom):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": table}}, "Property": nom}}, f"{table}.{nom}", nom


def colonne(table, nom):
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": nom}}, f"{table}.{nom}", nom


def litteral(valeur):
    return {"expr": {"Literal": {"Value": valeur}}}


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
    def __init__(self, nom, titre):
        self.nom, self.titre, self.visuels = nom, titre, []

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

    def segment(self, x, y, l, h, champ, titre=None):
        self.visuel(
            "slicer", x, y, l, h, {"Values": [champ]}, titre,
            {"data": [{"properties": {"mode": litteral("'Dropdown'")}}],
             "header": [{"properties": {"show": litteral("false")}}]},
        )

    def cartes(self, y, champs, x=MARGE, largeur_totale=LARGEUR - 2 * MARGE, hauteur=100):
        largeur = (largeur_totale - 10 * (len(champs) - 1)) // len(champs)
        for i, c in enumerate(champs):
            self.visuel("card", x + i * (largeur + 10), y, largeur, hauteur, {"Values": [c]})

    def enregistrer(self, racine):
        ecrire(
            racine / self.nom / "page.json",
            {
                "$schema": f"{SCHEMA}/page/1.3.0/schema.json",
                "name": self.nom,
                "displayName": self.titre,
                "displayOption": "FitToPage",
                "height": HAUTEUR,
                "width": LARGEUR,
            },
        )
        for v in self.visuels:
            ecrire(racine / self.nom / "visuals" / v["name"] / "visual.json", v)


def pages():
    # Page 1 : mes trajets suivis
    trajets = Page("trajets", "Mes trajets")
    trajets.segment(MARGE, MARGE, 400, 60, colonne("fait_trajet", "trajet"), "Trajet")
    trajets.segment(440, MARGE, 400, 60, colonne("dim_date", "nom_jour"), "Jour de la semaine")
    trajets.segment(860, MARGE, 400, 60, colonne("dim_date", "nom_mois"), "Mois")
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
        {"Category": [colonne("dim_date", "jour")], "Series": [colonne("fait_trajet", "trajet")],
         "Y": [mesure("fait_trajet", "Retard moyen (min)")]},
        "Retard à l'arrivée, jour par jour",
    )
    trajets.visuel(
        "clusteredBarChart", 860, 220, 400, 230,
        {"Category": [colonne("fait_trajet", "trajet")], "Y": [mesure("fait_trajet", "Ponctualité")]},
        "Ponctualité par trajet",
    )
    trajets.visuel(
        "clusteredColumnChart", MARGE, 465, 500, 235,
        {"Category": [colonne("dim_date", "nom_jour")], "Series": [colonne("fait_trajet", "trajet")],
         "Y": [mesure("fait_trajet", "Ponctualité")]},
        "Ponctualité selon le jour de la semaine",
    )
    trajets.visuel(
        "tableEx", 540, 465, 720, 235,
        {"Values": [
            colonne("fait_trajet", "jour"), colonne("fait_trajet", "nom_jour"), colonne("fait_trajet", "trajet"),
            colonne("fait_trajet", "arrivee_prevue"), colonne("fait_trajet", "arrivee_reelle"),
            mesure("fait_trajet", "Retard moyen (min)"), mesure("fait_trajet", "Suppressions"),
        ]},
        "Les pires jours",
        tri=(mesure("fait_trajet", "Retard moyen (min)"), "Descending"),
    )

    # Page 2 : lignes et gares (tous les passages du périmètre)
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

    # Page 3 : quelle alternative prendre
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
            mesure("fait_passage", "Retard moyen des passages (min)"), mesure("fait_passage", "Retard P90 (min)"),
            mesure("fait_passage", "Passages supprimés"),
        ]},
        "Chaque horaire à l'arrivée, avec sa fiabilité",
        tri=(colonne("fait_passage", "heure_prevue"), "Ascending"),
    )
    return [trajets, lignes, alternatives]


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    id_modele = sys.argv[1]
    if DOSSIER.exists():
        shutil.rmtree(DOSSIER)
    definition = DOSSIER / "definition"

    ecrire(
        DOSSIER / ".platform",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "Report", "displayName": "Ponctualité"},
            "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
        },
    )
    ecrire(
        DOSSIER / "definition.pbir",
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
    theme = DOSSIER / "StaticResources" / "SharedResources" / "BaseThemes" / "CY24SU10.json"
    theme.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(ICI / "theme" / "CY24SU10.json", theme)

    liste = pages()
    ecrire(
        definition / "pages" / "pages.json",
        {"$schema": f"{SCHEMA}/pagesMetadata/1.0.0/schema.json", "pageOrder": [p.nom for p in liste], "activePageName": liste[0].nom},
    )
    for p in liste:
        p.enregistrer(definition / "pages")
    print(f"Rapport généré : {sum(len(p.visuels) for p in liste)} visuels sur {len(liste)} pages")


if __name__ == "__main__":
    main()
