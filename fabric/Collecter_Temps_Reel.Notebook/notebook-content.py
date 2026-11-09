# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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

# # Ponctualité · relevé temps réel
#
# Notebook Python (sans Spark, pour démarrer vite) planifié toutes les quelques minutes.
# À chaque exécution, il interroge l'API **OJP 2.0** pour les prochains départs à chaque arrêt suivi
# (`Files/config/trajets.json`, clé dans `Files/config/ojp.env`) et ajoute un **instantané** à la table
# `departs` de l'Eventhouse `Ponctualite_TR`.
#
# Comme chaque passage est relevé plusieurs fois avant son départ, on pourra répondre à une question que
# l'historique seul ne permet pas : **combien de minutes avant le départ sait-on qu'un train sera en retard ?**

# CELL ********************

import json
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import notebookutils

POINT_D_ACCES_OJP = "https://api.opentransportdata.swiss/ojp20"
KUSTO_URI = "https://trd-bp2z7a3rjmcazqk89v.z6.kusto.fabric.microsoft.com"
BASE_KQL = "Ponctualite_TR"
TABLE = "departs"
NB_RESULTATS = 20
RACINE = "/lakehouse/default/Files"
NS = {"ojp": "http://www.vdv.de/ojp", "siri": "http://www.siri.org.uk/siri"}

with open(f"{RACINE}/config/trajets.json", encoding="utf-8") as f:
    CONFIG = json.load(f)
CLE = next(l.split("=", 1)[1].strip().strip('"') for l in open(f"{RACINE}/config/ojp.env", encoding="utf-8")
           if l.startswith("OJP_CLE="))
ARRETS = CONFIG["arrets_temps_reel"]
print(f"{len(ARRETS)} arrêts à relever")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def requete_ojp(bpuic, nom):
    maintenant = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<OJP xmlns="http://www.vdv.de/ojp" xmlns:siri="http://www.siri.org.uk/siri" version="2.0">
  <OJPRequest>
    <siri:ServiceRequest>
      <siri:ServiceRequestContext><siri:Language>fr</siri:Language></siri:ServiceRequestContext>
      <siri:RequestTimestamp>{maintenant}</siri:RequestTimestamp>
      <siri:RequestorRef>ponctualite-perso</siri:RequestorRef>
      <OJPStopEventRequest>
        <siri:RequestTimestamp>{maintenant}</siri:RequestTimestamp>
        <siri:MessageIdentifier>{uuid.uuid4()}</siri:MessageIdentifier>
        <Location>
          <PlaceRef>
            <siri:StopPointRef>{bpuic}</siri:StopPointRef>
            <Name><Text>{nom}</Text></Name>
          </PlaceRef>
        </Location>
        <Params>
          <NumberOfResults>{NB_RESULTATS}</NumberOfResults>
          <StopEventType>departure</StopEventType>
          <IncludePreviousCalls>false</IncludePreviousCalls>
          <IncludeOnwardCalls>false</IncludeOnwardCalls>
          <UseRealtimeData>full</UseRealtimeData>
        </Params>
      </OJPStopEventRequest>
    </siri:ServiceRequest>
  </OJPRequest>
</OJP>"""


def texte(elem, chemin):
    if elem is None:
        return ""
    trouve = elem.find(chemin, NS)
    return (trouve.text or "").strip() if trouve is not None else ""


def iso(valeur):
    return datetime.fromisoformat(valeur.replace("Z", "+00:00")).astimezone(timezone.utc) if valeur else None


def relever(arret):
    """Interroge OJP pour un arrêt et renvoie une liste de dictionnaires (un par départ annoncé)."""
    req = urllib.request.Request(
        POINT_D_ACCES_OJP, data=requete_ojp(arret["bpuic"], arret["nom"]).encode("utf-8"), method="POST",
        headers={"Authorization": f"Bearer {CLE}", "Content-Type": "application/xml",
                 "Accept": "application/xml", "User-Agent": "ponctualite-perso/1.0"})
    with urllib.request.urlopen(req, timeout=60) as rep:
        racine = ET.fromstring(rep.read())
    releve = datetime.now(timezone.utc)
    lignes = []
    for ev in racine.findall(".//ojp:StopEvent", NS):
        appel = ev.find("ojp:ThisCall/ojp:CallAtStop", NS)
        service = ev.find("ojp:Service", NS)
        prevu = iso(texte(appel, "ojp:ServiceDeparture/ojp:TimetabledTime"))
        estime = iso(texte(appel, "ojp:ServiceDeparture/ojp:EstimatedTime"))
        lignes.append({
            "releve": releve,
            "arret": arret["nom"],
            "bpuic": arret["bpuic"],
            "operateur": texte(service, "siri:OperatorRef"),
            "mode": texte(service, "ojp:Mode/ojp:ShortName/ojp:Text") or texte(service, "ojp:Mode/ojp:PtMode"),
            "ligne": texte(service, "ojp:PublishedServiceName/ojp:Text") or texte(service, "ojp:PublishedLineName/ojp:Text"),
            "destination": texte(service, "ojp:DestinationText/ojp:Text"),
            "course_ref": texte(service, "ojp:JourneyRef"),
            "depart_prevu": prevu,
            "depart_estime": estime,
            "retard_min": round((estime - prevu).total_seconds() / 60, 2) if prevu and estime else None,
            "quai_prevu": texte(appel, "ojp:PlannedQuay/ojp:Text"),
            "quai_estime": texte(appel, "ojp:EstimatedQuay/ojp:Text"),
            "annule": texte(service, "ojp:Cancelled") == "true",
            "non_desservi": appel is not None and appel.find("ojp:NotServicedStop", NS) is not None,
        })
    return lignes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

SCHEMA = [("releve", "datetime"), ("arret", "string"), ("bpuic", "string"), ("operateur", "string"),
          ("mode", "string"), ("ligne", "string"), ("destination", "string"), ("course_ref", "string"),
          ("depart_prevu", "datetime"), ("depart_estime", "datetime"), ("retard_min", "real"),
          ("quai_prevu", "string"), ("quai_estime", "string"), ("annule", "bool"), ("non_desservi", "bool")]


def commande_kusto(csl):
    jeton = notebookutils.credentials.getToken("kusto")
    req = urllib.request.Request(
        f"{KUSTO_URI}/v1/rest/mgmt", data=json.dumps({"db": BASE_KQL, "csl": csl}).encode("utf-8"), method="POST",
        headers={"Authorization": f"Bearer {jeton}", "Content-Type": "application/json; charset=utf-8",
                 "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as rep:
        return json.loads(rep.read())


def litteral(valeur, type_kql):
    if valeur is None or valeur == "":
        return f"{type_kql}(null)" if type_kql != "string" else '""'
    if type_kql == "datetime":
        return f"datetime({valeur.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z)"
    if type_kql == "bool":
        return "true" if valeur else "false"
    if type_kql == "real":
        return f"real({valeur})"
    return json.dumps(str(valeur), ensure_ascii=False)


def ingerer(lignes):
    colonnes = ", ".join(f"{nom}:{t}" for nom, t in SCHEMA)
    valeurs = ",\n".join(", ".join(litteral(l[nom], t) for nom, t in SCHEMA) for l in lignes)
    return commande_kusto(f".set-or-append {TABLE} <| datatable({colonnes}) [\n{valeurs}\n]")


# La table est créée (ou complétée) si besoin
commande_kusto(f".create-merge table {TABLE} ({', '.join(f'{n}:{t}' for n, t in SCHEMA)})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

toutes = []
for arret in ARRETS:
    try:
        lignes = relever(arret)
        toutes.extend(lignes)
        print(f"{arret['nom']}: {len(lignes)} départs")
    except Exception as e:  # un arrêt en erreur ne doit pas bloquer les autres
        print(f"{arret['nom']}: ERREUR {e}")

if toutes:
    ingerer(toutes)
    print(f"{len(toutes)} lignes ajoutées à {BASE_KQL}.{TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
