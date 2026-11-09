"""Teste l'API OJP 2.0 : prochains départs en temps réel à un arrêt.

Usage : python -I scripts/tester_ojp.py [BPUIC] [nom]
La clé est lue dans ponctualite/.env (OJP_CLE=...) et n'est jamais affichée.
"""
import sys
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

POINT_D_ACCES = "https://api.opentransportdata.swiss/ojp20"
NS = {"ojp": "http://www.vdv.de/ojp", "siri": "http://www.siri.org.uk/siri"}


def lire_cle():
    env = Path(__file__).resolve().parent.parent / ".env"
    for ligne in env.read_text(encoding="utf-8").splitlines():
        if ligne.startswith("OJP_CLE="):
            cle = ligne.split("=", 1)[1].strip().strip('"')
            if cle:
                return cle
    sys.exit("Clé absente : mets OJP_CLE=... dans ponctualite/.env")


def requete_departs(bpuic, nom, nombre=12):
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
          <DepArrTime>{maintenant}</DepArrTime>
        </Location>
        <Params>
          <NumberOfResults>{nombre}</NumberOfResults>
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
    trouve = elem.find(chemin, NS)
    return trouve.text if trouve is not None and trouve.text else ""


def heure(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone().strftime("%H:%M:%S") if iso else "--:--"


def retard(prevu, estime):
    if not prevu or not estime:
        return ""
    d = datetime.fromisoformat(estime.replace("Z", "+00:00")) - datetime.fromisoformat(prevu.replace("Z", "+00:00"))
    return f"{d.total_seconds() / 60:+.1f} min"


def main():
    bpuic = sys.argv[1] if len(sys.argv) > 1 else "8501118"
    nom = sys.argv[2] if len(sys.argv) > 2 else "Renens VD"
    corps = requete_departs(bpuic, nom).encode("utf-8")
    req = urllib.request.Request(
        POINT_D_ACCES, data=corps, method="POST",
        headers={"Authorization": f"Bearer {lire_cle()}", "Content-Type": "application/xml",
                 "Accept": "application/xml", "User-Agent": "ponctualite-perso/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as rep:
            print(f"HTTP {rep.status}, quota restant : {rep.headers.get('X-RateLimit-Remaining', '?')}")
            xml = rep.read()
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code} : {e.read()[:500].decode('utf-8', 'replace')}")

    racine = ET.fromstring(xml)
    evenements = racine.findall(".//ojp:StopEvent", NS)
    print(f"Départs à {nom} ({bpuic}) : {len(evenements)} résultats")
    for ev in evenements:
        appel = ev.find("ojp:ThisCall/ojp:CallAtStop", NS)
        prevu = texte(appel, "ojp:ServiceDeparture/ojp:TimetabledTime")
        estime = texte(appel, "ojp:ServiceDeparture/ojp:EstimatedTime")
        quai = texte(appel, "ojp:EstimatedQuay/ojp:Text") or texte(appel, "ojp:PlannedQuay/ojp:Text")
        service = ev.find("ojp:Service", NS)
        ligne = texte(service, "ojp:PublishedServiceName/ojp:Text") or texte(service, "ojp:PublishedLineName/ojp:Text")
        mode = texte(service, "ojp:Mode/ojp:ShortName/ojp:Text") or texte(service, "ojp:Mode/ojp:PtMode")
        destination = texte(service, "ojp:DestinationText/ojp:Text")
        annule = texte(service, "ojp:Cancelled") == "true" or appel.find("ojp:NotServicedStop", NS) is not None
        print(f"  {heure(prevu)} → réel {heure(estime)} {retard(prevu, estime):>9}  {mode} {ligne:<6} {destination:<28} quai {quai}{'  ANNULÉ' if annule else ''}")

    if not evenements:
        print(xml[:1500].decode("utf-8", "replace"))


if __name__ == "__main__":
    main()
