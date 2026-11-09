"""Génère la définition du tableau de bord temps réel (Real-Time Dashboard) « Ponctualité en direct ».

Usage : python temps_reel/generer_tableau_de_bord.py
Puis  : .\\ponctualite\\fab.ps1 import "Ponctualite.Workspace/Ponctualite_TR.KQLDashboard" -i .\\ponctualite\\temps_reel\\Ponctualite_TR.KQLDashboard -f

Les identifiants sont dérivés des noms (uuid5) : réimporter met à jour le tableau de bord au lieu d'en créer un autre.
Aucun arrêt personnel n'est écrit ici : les requêtes regroupent par ce qu'elles trouvent dans les données.
"""

import json
import uuid
from pathlib import Path

ICI = Path(__file__).parent
DOSSIER = ICI / "Ponctualite_TR.KQLDashboard"
ESPACE = "6501a002-4c79-4ada-aa1e-3b65a5bf1f9b"
BASE_KQL_ID = "51835a78-0070-41a0-915a-900ed173fb7d"
KUSTO_URI = "https://trd-bp2z7a3rjmcazqk89v.z6.kusto.fabric.microsoft.com"
FUSEAU = "Europe/Zurich"
LIGNES_SUIVIES = "'IR90', 'RE33', 'IC1', 'R1', 'R2', '28', '360', '362', 'm1'"


def ident(nom):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ponctualite-tr/{nom}"))


SOURCE = ident("source")
PAGE_DIRECT, PAGE_ANTICIPATION = ident("page-direct"), ident("page-anticipation")

# Dernier relevé complet : toutes les lignes relevées dans les 2 minutes qui suivent le début du dernier passage
DERNIER_RELEVE = (
    "let dernier = toscalar(departs | summarize max(releve));\n"
    "let instantane = departs | where releve > dernier - 2m;\n"
)
HEURE = f"format_datetime(datetime_utc_to_local({{}}, '{FUSEAU}'), 'HH:mm')"

REQUETES = {
    "dernier_releve": DERNIER_RELEVE + f"print ['Dernier relevé'] = format_datetime(datetime_utc_to_local(dernier, '{FUSEAU}'), 'HH:mm:ss')",
    "departs_annonces": DERNIER_RELEVE + "instantane | where depart_prevu >= now() - 2m | summarize ['Départs annoncés'] = count()",
    "retard_moyen": DERNIER_RELEVE + "instantane | where depart_prevu >= now() - 2m | summarize ['Retard moyen annoncé (min)'] = round(avg(retard_min), 1)",
    "perturbes": DERNIER_RELEVE + "instantane | where depart_prevu >= now() - 2m | summarize ['Annulés ou non desservis'] = countif(annule or non_desservi)",
    "prochains_departs": DERNIER_RELEVE + (
        "instantane\n"
        "| where depart_prevu >= now() - 2m\n"
        f"| project ['Arrêt'] = arret, ['Prévu'] = {HEURE.format('depart_prevu')}, ['Estimé'] = {HEURE.format('coalesce(depart_estime, depart_prevu)')},\n"
        "    ['Retard (min)'] = retard_min, Ligne = strcat(mode, ' ', ligne), Destination = destination,\n"
        "    Quai = coalesce(quai_estime, quai_prevu), ['Perturbé'] = annule or non_desservi, tri = depart_prevu\n"
        "| order by ['Arrêt'] asc, tri asc\n"
        "| project-away tri"
    ),
    "retard_par_ligne": DERNIER_RELEVE + (
        "instantane\n"
        "| where depart_prevu >= now() - 2m and isnotnull(retard_min)\n"
        "| summarize ['Retard moyen (min)'] = round(avg(retard_min), 1) by Ligne = strcat(mode, ' ', ligne)\n"
        "| order by ['Retard moyen (min)'] desc"
    ),
    "retard_dans_le_temps": (
        "departs\n"
        "| where releve between (_startTime .. _endTime)\n"
        f"| where ligne in ({LIGNES_SUIVIES}) and isnotnull(retard_min)\n"
        "| summarize ['Retard moyen (min)'] = round(avg(retard_min), 1) by bin(releve, 5m), Ligne = ligne\n"
        "| order by releve asc"
    ),
    "perturbations": (
        "departs\n"
        "| where releve between (_startTime .. _endTime)\n"
        "| where annule or non_desservi\n"
        "| summarize arg_max(releve, *) by course_ref, arret\n"
        f"| project ['Vu à'] = format_datetime(datetime_utc_to_local(releve, '{FUSEAU}'), 'HH:mm'), ['Arrêt'] = arret,\n"
        f"    ['Prévu'] = {HEURE.format('depart_prevu')}, Ligne = strcat(mode, ' ', ligne), Destination = destination,\n"
        "    Motif = iff(annule, 'Course annulée', 'Arrêt non desservi')\n"
        "| order by ['Vu à'] desc"
    ),
    # Anticipation : pour chaque départ observé jusqu'au bout (dernier relevé à 6 min ou moins du départ),
    # on compare le retard annoncé X minutes avant avec le retard « final » (dernier relevé).
    "anticipation": (
        "let fin = now() - 5m;\n"
        "let courses = departs\n"
        "| where releve between (_startTime .. _endTime) and depart_prevu < fin and isnotnull(retard_min)\n"
        "| extend cle = strcat(course_ref, '|', arret), minutes_avant = datetime_diff('second', depart_prevu, releve) / 60.0\n"
        "| where minutes_avant between (0 .. 90);\n"
        "let final = courses | summarize (derniere_minute, retard_final) = arg_min(minutes_avant, retard_min) by cle | where derniere_minute <= 6;\n"
        "courses\n"
        "| join kind=inner final on cle\n"
        "| extend ['Minutes avant le départ'] = bin(minutes_avant, 10)\n"
        "| summarize ['Départs observés'] = dcount(cle), ['Écart moyen avec le retard final (min)'] = round(avg(abs(retard_min - retard_final)), 2),\n"
        "    en_retard = dcountif(cle, retard_final >= 3), annonces = dcountif(cle, retard_final >= 3 and retard_min >= 3)\n"
        "    by ['Minutes avant le départ']\n"
        "| extend ['Retards déjà annoncés (%)'] = round(100.0 * annonces / en_retard, 0)\n"
        "| project-away en_retard, annonces\n"
        "| order by ['Minutes avant le départ'] asc"
    ),
    "departs_observes": (
        "let fin = now() - 5m;\n"
        "departs\n"
        "| where releve between (_startTime .. _endTime) and depart_prevu < fin and isnotnull(retard_min)\n"
        "| extend cle = strcat(course_ref, '|', arret), minutes_avant = datetime_diff('second', depart_prevu, releve) / 60.0\n"
        "| where minutes_avant between (0 .. 90)\n"
        "| summarize (derniere_minute, retard_final) = arg_min(minutes_avant, retard_min) by cle\n"
        "| where derniere_minute <= 6\n"
        "| summarize ['Départs observés jusqu\\'au bout'] = count(), ['dont en retard de 3 min ou plus'] = countif(retard_final >= 3)"
    ),
    "retards_annonces": (
        "let fin = now() - 5m;\n"
        "departs\n"
        "| where releve between (_startTime .. _endTime) and depart_prevu < fin and isnotnull(retard_min)\n"
        "| extend minutes_avant = round(datetime_diff('second', depart_prevu, releve) / 60.0, 1)\n"
        "| where minutes_avant between (0 .. 90)\n"
        "| summarize (min_avant, retard_final) = arg_min(minutes_avant, retard_min), (max_avant, retard_premier) = arg_max(minutes_avant, retard_min), releves = count()\n"
        "    by arret, mode, ligne, destination, depart_prevu\n"
        "| where min_avant <= 6 and retard_final >= 3\n"
        f"| project ['Prévu'] = {HEURE.format('depart_prevu')}, ['Arrêt'] = arret, Ligne = strcat(mode, ' ', ligne), Destination = destination,\n"
        "    ['Retard final (min)'] = retard_final, ['Annoncé au 1er relevé (min)'] = retard_premier, ['1er relevé (min avant)'] = max_avant, ['Relevés'] = releves\n"
        "| order by ['Retard final (min)'] desc"
    ),
}


def requete(nom):
    texte = REQUETES[nom]
    variables = [v for v in ("_startTime", "_endTime") if v in texte]
    return {"id": ident(f"requete-{nom}"), "dataSource": {"kind": "inline", "dataSourceId": SOURCE}, "text": texte, "usedVariables": variables}


def tuile(nom, titre, type_visuel, page, x, y, l, h, options=None):
    return {
        "id": ident(f"tuile-{nom}"),
        "title": titre,
        "visualType": type_visuel,
        "pageId": page,
        "layout": {"x": x, "y": y, "width": l, "height": h},
        "queryRef": {"kind": "query", "queryId": ident(f"requete-{nom}")},
        "visualOptions": options or {},
    }


CARTE = {"multiStat__textSize": "auto", "multiStat__valueColumn": None, "colorRulesDisabled": False, "colorStyle": "light", "colorRules": []}


def regle_couleur(nom, colonne, operateur, valeur, couleur, icone=None):
    return {
        "id": ident(f"regle-{nom}"), "ruleType": "colorByCondition", "applyToColumn": colonne, "hideText": False,
        "applyTo": "cells", "conditions": [{"operator": operateur, "column": colonne, "values": [valeur]}],
        "chainingOperator": "and", "colorStyle": "bold", "color": couleur, "tag": "", "icon": icone, "ruleName": "", "visualType": "table",
    }


def graphique(x_col, y_cols, series=None, legende=True):
    return {
        "multipleYAxes": {"base": {"id": "-1", "label": "", "columns": [], "yAxisScale": "linear", "yAxisMaximumValue": None,
                                   "yAxisMinimumValue": None, "horizontalLines": []}, "additional": [], "showMultiplePanels": False},
        "seriesColors": {}, "hideLegend": not legende, "legendLocation": "bottom", "xColumnTitle": "", "xColumn": x_col,
        "yColumns": y_cols, "seriesColumns": series, "xAxisScale": "linear", "verticalLine": "", "crossFilter": [],
        "crossFilterDisabled": False, "drillthroughDisabled": False, "forceAxisTicks": False, "drillthrough": [],
        "selectedDataOnLoad": {"all": True, "limit": 10},
    }


def definition():
    tuiles = [
        tuile("dernier_releve", "Dernier relevé", "card", PAGE_DIRECT, 0, 0, 6, 3, CARTE),
        tuile("departs_annonces", "Départs annoncés à mes arrêts", "card", PAGE_DIRECT, 6, 0, 6, 3, CARTE),
        tuile("retard_moyen", "Retard moyen annoncé", "card", PAGE_DIRECT, 12, 0, 6, 3, CARTE),
        tuile("perturbes", "Annulés ou non desservis", "card", PAGE_DIRECT, 18, 0, 6, 3, CARTE),
        tuile("prochains_departs", "Prochains départs (dernier relevé)", "table", PAGE_DIRECT, 0, 3, 16, 10, {
            "table__enableRenderLinks": False, "colorRulesDisabled": False, "colorStyle": "light", "crossFilterDisabled": False,
            "drillthroughDisabled": False, "crossFilter": [], "drillthrough": [], "table__renderLinks": [],
            "colorRules": [
                regle_couleur("retard", "Retard (min)", ">=", "3", "red", "warning"),
                regle_couleur("avance", "Retard (min)", "<", "3", "green"),
                regle_couleur("perturbe", "Perturbé", "==", "true", "red", "critical"),
            ],
        }),
        tuile("retard_par_ligne", "Retard moyen annoncé par ligne", "bar", PAGE_DIRECT, 16, 3, 8, 10, graphique("Ligne", ["Retard moyen (min)"], None, False)),
        tuile("retard_dans_le_temps", "Retard annoncé au fil des relevés, lignes suivies", "line", PAGE_DIRECT, 0, 13, 16, 8,
              graphique("releve", ["Retard moyen (min)"], ["Ligne"])),
        tuile("perturbations", "Perturbations vues sur la période", "table", PAGE_DIRECT, 16, 13, 8, 8, {
            "table__enableRenderLinks": False, "colorRulesDisabled": False, "colorStyle": "light", "crossFilterDisabled": False,
            "drillthroughDisabled": False, "crossFilter": [], "drillthrough": [], "table__renderLinks": [], "colorRules": [],
        }),
        tuile("anticipation", "À combien de minutes du départ le retard est-il déjà annoncé ?", "column", PAGE_ANTICIPATION, 0, 0, 16, 9,
              graphique("Minutes avant le départ", ["Retards déjà annoncés (%)"], None, False)),
        tuile("departs_observes", "Départs observés jusqu'au bout", "multistat", PAGE_ANTICIPATION, 16, 0, 8, 9, {
            "multiStat__textSize": "auto", "multiStat__valueColumn": None, "colorRulesDisabled": False, "colorStyle": "light",
            "multiStat__displayOrientation": "vertical", "multiStat__labelColumn": None, "multiStat__slot": {"width": 1, "height": 2},
            "colorRules": [],
        }),
        tuile("retards_annonces", "Les retards de la période : quand ont-ils été annoncés ?", "table", PAGE_ANTICIPATION, 0, 9, 24, 10, {
            "table__enableRenderLinks": False, "colorRulesDisabled": False, "colorStyle": "light", "crossFilterDisabled": False,
            "drillthroughDisabled": False, "crossFilter": [], "drillthrough": [], "table__renderLinks": [], "colorRules": [],
        }),
    ]
    # Même structure que les tableaux de bord publiés par Microsoft dans fabric-toolbox (schéma 60) :
    # pas de clé « title » (le nom vient de l'élément Fabric), source de type kusto-trident.
    return {
        "schema_version": "60",
        "autoRefresh": {"enabled": True, "defaultInterval": "5m"},
        "baseQueries": [],
        "dataSources": [{
            "id": SOURCE, "name": "Ponctualite_TR", "kind": "kusto-trident", "scopeId": "kusto-trident",
            "clusterUri": KUSTO_URI, "database": BASE_KQL_ID, "workspace": ESPACE,
        }],
        "pages": [{"id": PAGE_DIRECT, "name": "En direct"}, {"id": PAGE_ANTICIPATION, "name": "Anticipation"}],
        "parameters": [{
            "kind": "duration", "id": ident("parametre-periode"), "displayName": "Période", "description": "",
            "beginVariableName": "_startTime", "endVariableName": "_endTime",
            "defaultValue": {"kind": "dynamic", "count": 12, "unit": "hours"}, "showOnPages": {"kind": "all"},
        }],
        "queries": [requete(n) for n in REQUETES],
        "tiles": tuiles,
    }


def main():
    DOSSIER.mkdir(parents=True, exist_ok=True)
    (DOSSIER / ".platform").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "KQLDashboard", "displayName": "Ponctualite_TR",
                     "description": "Tableau de bord temps réel : prochains départs, retards annoncés, perturbations, anticipation"},
        "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    d = definition()
    (DOSSIER / "RealTimeDashboard.json").write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Tableau de bord généré : {len(d['tiles'])} tuiles, {len(d['queries'])} requêtes, {len(d['pages'])} pages")


if __name__ == "__main__":
    main()
