# Ponctualité : quel train prendre pour ne jamais être en retard

L'application CFF me dit que **mon train a du retard**. Ce projet me dit **lequel prendre pour ne pas en avoir** :
il mesure la fiabilité réelle de mes trajets quotidiens sur plusieurs mois, cherche les schémas
(jour de la semaine, heure, saison, météo) et finira par prédire le risque de retard avant le départ.

Projet personnel construit sur Microsoft Fabric (essai), avec les données ouvertes des transports publics suisses.

## Les données

| Source | Contenu | Usage |
|---|---|---|
| [Ist-Daten v2](https://data.opentransportdata.swiss/en/dataset/ist-daten-v2) (opentransportdata.swiss) | Tous les passages réalisés en Suisse la veille : horaire prévu, horaire réel, suppression, pour chaque arrêt de chaque course (~2,6 millions de lignes par jour) | Historique : les 35 derniers jours en ligne, les mois précédents dans les [archives](https://archive.opentransportdata.swiss/actual_data_archive.htm) |
| OJP 2.0 (API) | Départs en temps réel à un arrêt | Flux temps réel (à venir) |
| MétéoSuisse (données ouvertes) | Observations horaires | Corrélation retard ↔ météo (à venir) |

Les Ist-Daten couvrent les trains, bus, trams et le métro m1 lausannois ; le m2 n'y figure pas.

## Architecture

```text
opentransportdata.swiss (CSV quotidien, 300 à 400 Mo)
        │  notebook Charger_Istdaten : téléchargement, filtrage sur mes arrêts et lignes
        ▼
Lakehouse Ponctualite
   bronze.passages        passages bruts du périmètre, partitionnés par jour (chargement idempotent)
   bronze.chargements     journal : un jour n'est jamais chargé deux fois
        │  notebook Transformer_Passages
        ▼
   silver.passage         types corrects, colonnes en français, retards en minutes, drapeau « ponctuel » (< 3 min, comme aux CFF)
   gold.dim_date / dim_ligne / dim_arret
   gold.fait_passage      un passage à un arrêt
   gold.fait_trajet       un trajet suivi par jour : « mon train de 07:55 », avec son retard réel à l'arrivée
        │
        ▼
Power BI (Direct Lake) : fiabilité par trajet, par jour de semaine, par heure ; meilleure alternative

API OJP 2.0 (prochains départs, temps réel)
        │  notebook Python Collecter_Temps_Reel, toutes les 5 minutes
        ▼
Eventhouse Ponctualite_TR, table departs : un instantané par relevé et par départ annoncé
        → tableau de bord temps réel (KQL) et, plus tard, « combien de minutes avant le départ
          sait-on qu'un train sera en retard ? »
```

## Premiers résultats (01.10 → 08.11.2026, jours ouvrés)

| Trajet suivi | Jours | Ponctuel (< 3 min) | Retard moyen à l'arrivée | Pire jour | Supprimé |
|---|---|---|---|---|---|
| Bus de rabattement vers la gare | 22 | 86 % | +0,9 min | +3,3 min | 0 |
| Train du matin n° 1 (IR) | 26 | 92 % | −0,2 min | +5,6 min | 0 |
| Train du matin n° 2 (R) | 26 | 80 % | +2,1 min | +5,8 min | 1 |

Le train régional est le maillon faible : un jour sur cinq, il arrive avec plus de 3 minutes de retard.

## Confidentialité

Le périmètre (arrêts, lignes, trajets suivis) est décrit dans `config/trajets.json`, **qui n'est pas publié** :
il contient mes arrêts de domicile et mes horaires. `config/trajets.exemple.json` montre le format ;
copie-le en `trajets.json` et mets tes propres trajets. La clé OJP est dans `.env` (non publié), copiée dans le
lakehouse sous `Files/config/ojp.env` ; elle n'apparaît ni dans le code, ni dans les sorties des scripts.

## Lancer le projet

La CLI Fabric passe par `fab.ps1`, qui utilise un profil Azure isolé (tenant personnel).

```powershell
# 1. Envoyer le périmètre dans le lakehouse
.\ponctualite\fab.ps1 cp .\ponctualite\config\trajets.json "Ponctualite.Workspace/Ponctualite.Lakehouse/Files/config/trajets.json" -f

# 2. Déployer les notebooks
.\ponctualite\fab.ps1 import "Ponctualite.Workspace/Charger_Istdaten.Notebook" -i .\ponctualite\fabric\Charger_Istdaten.Notebook --format .py -f
.\ponctualite\fab.ps1 import "Ponctualite.Workspace/Transformer_Passages.Notebook" -i .\ponctualite\fabric\Transformer_Passages.Notebook --format .py -f

# 3. Charger l'historique (asynchrone, ~2 min par jour de données), puis transformer
.\ponctualite\fab.ps1 job start "Ponctualite.Workspace/Charger_Istdaten.Notebook"
.\ponctualite\fab.ps1 job run "Ponctualite.Workspace/Transformer_Passages.Notebook"
```

La capacité d'essai n'exécute qu'un travail Spark à la fois : attendre la fin du chargement avant de transformer.

```powershell
# 4. Temps réel : tester la clé OJP, envoyer la clé dans le lakehouse, déployer et planifier le collecteur
python -I .\ponctualite\scripts\tester_ojp.py 8501118 "Renens VD"
.\ponctualite\fab.ps1 cp .\ponctualite\.env "Ponctualite.Workspace/Ponctualite.Lakehouse/Files/config/ojp.env" -f
.\ponctualite\fab.ps1 import "Ponctualite.Workspace/Collecter_Temps_Reel.Notebook" -i .\ponctualite\fabric\Collecter_Temps_Reel.Notebook --format .py -f
.\ponctualite\fab.ps1 job run-sch "Ponctualite.Workspace/Collecter_Temps_Reel.Notebook" --type cron --interval 5 --start 2026-11-09T10:00:00 --end 2026-12-28T23:59:00 --enable
```

Neuf arrêts relevés toutes les 5 minutes = ~2'600 requêtes par jour, sous la limite gratuite de l'API (20'000 par jour).

```powershell
# 5. Power BI : modèle sémantique Direct Lake (TMDL) puis rapport PBIR généré par script
.\ponctualite\fab.ps1 import "Ponctualite.Workspace/Ponctualite.SemanticModel" -i .\ponctualite\rapport\Ponctualite.SemanticModel -f
.\ponctualite\fab.ps1 get "Ponctualite.Workspace/Ponctualite.SemanticModel" -q id
python .\ponctualite\rapport\generer_rapport.py <id du modèle>
.\ponctualite\fab.ps1 import "Ponctualite.Workspace/Ponctualité.Report" -i .\ponctualite\rapport\Ponctualite.Report -f
```

Après la création de nouvelles tables dans le lakehouse, forcer la synchronisation du point de terminaison SQL
(`fab api -X post "workspaces/<espace>/sqlEndpoints/<id>/refreshMetadata?preview=true" -i "{}"`), sinon Direct Lake
ne voit pas les tables. Dans le rapport, la mise en forme conditionnelle des matrices exige un sélecteur à deux clés
(`dataViewWildcard` + `metadata`), sans quoi elle est ignorée en silence.

## Le rapport

Trois pages, en Direct Lake sur le modèle gold :

1. **Mes trajets** : ponctualité, retard moyen et maximal, jours à plus de 3 minutes, suppressions, minutes perdues ;
   retard jour par jour, ponctualité selon le jour de la semaine, liste des pires jours.
2. **Lignes et gares** : les 869'000 passages du périmètre ; ponctualité par ligne, retard selon l'heure de la journée,
   matrice ligne × jour de la semaine colorée du rouge au vert.
3. **Alternatives** : pour une gare d'arrivée, la ponctualité de chaque ligne heure par heure, puis chaque horaire avec
   sa fiabilité : de quoi choisir le train à prendre plutôt que celui qu'on a l'habitude de prendre.

## Arborescence

```text
ponctualite/
├── config/
│   ├── trajets.exemple.json      format du périmètre (publié)
│   └── trajets.json              mon périmètre (jamais publié)
├── fabric/
│   ├── Charger_Istdaten.Notebook/       bronze (Spark)
│   ├── Transformer_Passages.Notebook/   silver et gold (Spark)
│   └── Collecter_Temps_Reel.Notebook/   relevé OJP → Eventhouse (Python)
├── rapport/
│   ├── Ponctualite.SemanticModel/     modèle Direct Lake (TMDL) : tables, relations, mesures
│   ├── generer_rapport.py             génère le rapport PBIR (3 pages, 29 visuels)
│   ├── verifier.py                    exécute une requête DAX sur le modèle
│   └── theme/                         thème Power BI de base
├── scripts/
│   └── tester_ojp.py                  prochains départs d'un arrêt, en ligne de commande
├── .env                           clé OJP (jamais publiée)
├── fab.ps1                        CLI Fabric sur le tenant du projet
└── README.md
```
