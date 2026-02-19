#!/usr/bin/env python3
"""
Fleet-Map updater (GitHub Actions safe)
Priority rules (with CPF handling):
- Level 3 (DC fast): Charging > Available > Occupied   (single-port behavior)
- Level 2 CPF* (single-port): Charging > Available > Occupied
- Other Level 2 / multi-port: Available > Charging > Occupied

Inputs:
- Secrets: CP_USERNAME, CP_PASSWORD
Outputs (repo root / $GITHUB_WORKSPACE):
- stations_per_station_slim.csv
- status_latest_slim.csv (with station_label and port counts)
"""

import os, re, time, shutil, tempfile
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

import requests
import pandas as pd

# ===== Credentials from GitHub Secrets =====
USERNAME = os.getenv("CP_USERNAME")
PASSWORD = os.getenv("CP_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError("Missing CP_USERNAME/CP_PASSWORD environment variables. Set these in GitHub → Settings → Secrets → Actions.")

# ===== SOAP endpoint / paths =====
ENDPOINT = "https://webservices.chargepoint.com/webservices/chargepoint/services/5.1"
OUTPUT_DIR = os.getenv("GITHUB_WORKSPACE", os.getcwd())

STATUS_OUT = os.path.join(OUTPUT_DIR, "status_latest_slim.csv")
STATIONS_CACHE = os.path.join(OUTPUT_DIR, "stations_per_station_slim.csv")
LOG_FILE = os.path.join(OUTPUT_DIR, "chargepoint_refresh.log")

# ===== Search footprint (adjust if needed) =====
LAT, LON, RADIUS_MILES, STATE_FILTER = 40.7128, -74.0060, 100, "NY"

# ===== Engine settings =====
PAGE_SIZE, STATUS_CONCURRENCY = 500, 20
HTTP_TIMEOUT, HTTP_RETRIES = 60, 3

# ===== Output schema =====
FINAL_COLS = [
    "stationName","Address","City","State","postalCode","Lat","Long",
    "Charger type","Charger type (legend)","stationModel",
    "StationNetworkStatus","LastPortStatus","faultReason",
    "station_label","ports_total","ports_charging","ports_occupied","ports_available",
    "StatusTimestamp","_loaded_at_utc",
]

def log(msg):
    ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{ts} {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")
    except Exception:  # in Actions, logs via print are enough
        pass

_session = requests.Session()

def build_envelope(body_xml:str)->bytes:
    return f"""
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:dictionary:com.chargepoint.webservices">
  <soapenv:Header xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
    <wsse:Security soapenv:mustUnderstand="1">
      <wsse:UsernameToken>
        <wsse:Username>{USERNAME}</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{PASSWORD}</wsse:Password>
      </wsse:UsernameToken>
    </wsse:Security>
  </soapenv:Header>
  <soapenv:Body>{body_xml}</soapenv:Body>
</soapenv:Envelope>
""".strip().encode("utf-8")

def post_soap(body_xml:str)->bytes:
    headers = {"Content-Type": "text/xml; charset=UTF-8"}
    last_err=None
    for i in range(HTTP_RETRIES):
        try:
            r=_session.post(ENDPOINT,data=build_envelope(body_xml),headers=headers,timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            b=r.content
            if b and (b.find(b"<Fault")!=-1 or b.find(b":Fault")!=-1): raise RuntimeError("SOAP Fault returned")
            return b
        except Exception as e:
            last_err=e; log(f"HTTP/SOAP error (try {i+1}/{HTTP_RETRIES}): {e}"); time.sleep(2**i)
    raise last_err

def strip_tag(tag:str)->str: return tag.split("}")[-1] if "}" in tag else tag

# ---------------- Parsers ----------------
def parse_stations(xml_bytes:bytes):
    root=ET.fromstring(xml_bytes); body=root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None: return []
    rows=[]
    for st in body.iter():
        if strip_tag(st.tag)=="stationData":
            rec={}
            for child in st:
                name=strip_tag(child.tag)
                if name=="Port": continue
                rec[name]=(child.text or "").strip()
            ports=[p for p in st if strip_tag(p.tag)=="Port"]
            if ports:
                for p in ports:
                    row=rec.copy()
                    row["portNumber"]=(p.findtext("./portNumber") or "").strip()
                    lat=p.findtext("./Geo/Lat"); lon=p.findtext("./Geo/Long")
                    row["Lat"]=float(lat) if lat else None
                    row["Long"]=float(lon) if lon else None
                    rows.append(row)
            else:
                rows.append(rec)
    return rows

def parse_status(xml_bytes:bytes):
    root=ET.fromstring(xml_bytes); body=root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None: return []
    out=[]
    for st in body.iter():
        if strip_tag(st.tag)=="stationData":
            sid=(st.findtext("./stationID") or "").strip()
            station_net=(st.findtext("./networkStatus") or "").strip()
            for p in st.findall("./Port"):
                out.append({
                    "stationID":sid,
                    "portNumber":(p.findtext("./portNumber") or "").strip(),
                    "PortStatus":(p.findtext("./Status") or "").strip(),
                    "faultReason":(p.findtext("./faultReason") or "").strip(),
                    "StatusTimestamp":(p.findtext("./TimeStamp") or "").strip(),
                    "StationNetworkStatus":station_net,
                })
    return out

def parse_load(xml_bytes:bytes):
    root=ET.fromstring(xml_bytes); body=root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None: return []
    out=[]
    for resp in body.iter():
        if strip_tag(resp.tag)=="getLoadResponse":
            for sd in resp.findall(".//stationData"):
                sid=(sd.findtext("./stationID") or "").strip()
                ports=sd.findall("./Port")
                totals={"ports_total":0,"ports_charging":0,"ports_occupied":0,"ports_available":0}
                for p in ports:
                    totals["ports_total"]+=1
                    port_load_txt=(p.findtext("./portLoad") or "0").strip()
                    try: port_load=float(port_load_txt)
                    except: port_load=0.0
                    session_id=(p.findtext("./sessionID") or "").strip()
                    if port_load>0: totals["ports_charging"]+=1
                    elif session_id and session_id!="0": totals["ports_occupied"]+=1
                    else: totals["ports_available"]+=1
                out.append({"stationID":sid, **totals})
    return out

# --------------- Fetchers ---------------
def fetch_stations_full_per_station()->pd.DataFrame:
    log("Fetching stations via getStations…")
    all_rows=[]; start=0
    while True:
        body=f"""
<urn:getStations>
  <searchQuery>
    <geo><latitude>{LAT}</latitude><longitude>{LON}</longitude><distance>{RADIUS_MILES}</distance></geo>
    <state>{STATE_FILTER}</state>
    <startRecord>{start}</startRecord>
    <maxRecords>{PAGE_SIZE}</maxRecords>
  </searchQuery>
</urn:getStations>""".strip()
        xml=post_soap(body); page=parse_stations(xml)
        if not page: break
        all_rows.extend(page); start+=PAGE_SIZE
    df_ports=pd.DataFrame(all_rows)
    if df_ports.empty: log("WARNING: getStations returned no rows"); return df_ports
    if "stationID" not in df_ports.columns: df_ports["stationID"]=""
    df_ports["stationID"]=df_ports["stationID"].astype(str)
    ensure=["stationID","stationName","stationModel","Address","City","State","postalCode","sgName","sgname","Lat","Long"]
    for c in ensure:
        if c not in df_ports.columns: df_ports[c]=None

    def first_non_null(series):
        for v in series:
            if pd.notnull(v) and v!="": return v
        return None

    grouped=df_ports.groupby("stationID", as_index=False).agg({
        "stationName":first_non_null,"stationModel":first_non_null,"Address":first_non_null,
        "City":first_non_null,"State":first_non_null,"postalCode":first_non_null,
        "sgName":first_non_null,"sgname":first_non_null,"Lat":first_non_null,"Long":first_non_null,
    })

    def classify(model:str)->str:
        m=(model or "").upper()
        l3=["CPE250","CPE200","EXPRESS","EXPRESS 200","EXPRESS 250","DCFC","TRITIUM","PK350","ABB","BTC","RTM","HPC"]
        l2=["CT4020","CT4025","CT4000","CT4010","CT4011","CT500","CT600","CT-4000","CPF25","CPF50","CPF32","CT4010-HD2","CT2000","WALLBOX"]
        if "GW" in m and not any(x in m for x in l2+l3): return "Gateway (Not a Charger)"
        if any(x in m for x in l3) or "LEVEL 3" in m or "DC FAST" in m or "FAST" in m: return "Level 3"
        if any(x in m for x in l2) or "LEVEL 2" in m or " L2" in m: return "Level 2"
        return "Unknown"

    grouped["Charger type"]=grouped["stationModel"].apply(classify)

    def legend(base, sg):
        sg_u=(sg or "")
        is_public=bool(re.search(r"\bPublic Stations\b", sg_u, flags=re.IGNORECASE))
        is_solar =bool(re.search(r"\bSolar Stations\b",  sg_u, flags=re.IGNORECASE))
        label=base
        if base and base.startswith("Gateway"): return None
        if is_public: label += " - Public Stations"
        if is_solar:  label += " - Solar"
        return label

    sgcol="sgName" if "sgName" in grouped.columns else ("sgname" if "sgname" in grouped.columns else None)
    sg_series=grouped[sgcol].astype(str) if sgcol else pd.Series([""]*len(grouped), index=grouped.index)
    grouped["Charger type (legend)"]=[legend(bt, sg) for bt, sg in zip(grouped["Charger type"], sg_series)]
    return grouped.drop_duplicates(subset=["stationID"], keep="first")

def fetch_status_for_station(sid:str):
    body=f"""
<urn:getStationStatus>
  <searchQuery><stationID>{sid}</stationID></searchQuery>
</urn:getStationStatus>""".strip()
    try: xml=post_soap(body); return parse_status(xml)
    except Exception as e: log(f"Status fetch failed for {sid}: {e}"); return []

def fetch_all_statuses(station_ids):
    log(f"Fetching status for {len(station_ids)} stations (concurrency={STATUS_CONCURRENCY})…")
    rows=[]
    with ThreadPoolExecutor(max_workers=STATUS_CONCURRENCY) as ex:
        futs={ex.submit(fetch_status_for_station, sid): sid for sid in station_ids}
        for fut in as_completed(futs):
            try: rows.extend(fut.result())
            except Exception as e: log(f"Error in status future: {e}")
    df=pd.DataFrame(rows)
    if df.empty: return df
    df["StatusTimestamp_dt"]=pd.to_datetime(df["StatusTimestamp"], errors="coerce", utc=True)
    df=df.sort_values(["stationID","StatusTimestamp_dt"])
    agg=df.groupby("stationID", as_index=False).agg({
        "StatusTimestamp_dt":"last","StationNetworkStatus":"last","PortStatus":"last","faultReason":"last"
    }).rename(columns={"StatusTimestamp_dt":"StatusTimestamp_dt_latest","PortStatus":"LastPortStatus"})
    agg["StatusTimestamp"]=agg["StatusTimestamp_dt_latest"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return agg

def fetch_load_for_station(sid:str):
    body=f"""
<urn:getLoad>
  <searchQuery><stationID>{sid}</stationID></searchQuery>
</urn:getLoad>""".strip()
    try: xml=post_soap(body); return parse_load(xml)
    except Exception as e: log(f"Load fetch failed for {sid}: {e}"); return []

def fetch_all_loads(station_ids):
    log(f"Fetching load for {len(station_ids)} stations (concurrency={STATUS_CONCURRENCY})…")
    rows=[]
    with ThreadPoolExecutor(max_workers=STATUS_CONCURRENCY) as ex:
        futs={ex.submit(fetch_load_for_station, sid): sid for sid in station_ids}
        for fut in as_completed(futs):
            try: rows.extend(fut.result())
            except Exception as e: log(f"Error in load future: {e}")
    return pd.DataFrame(rows)

# --------------- IO helpers ---------------
def atomic_write_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="cp_", suffix=".csv", dir=os.path.dirname(path)); os.close(fd)
    try: df.to_csv(tmp, index=False, encoding="utf-8"); shutil.move(tmp, path)
    finally:
        try:
            if os.path.exists(tmp): os.remove(tmp)
        except Exception: pass

def slim_output(merged:pd.DataFrame)->pd.DataFrame:
    out=merged.copy(); out["_loaded_at_utc"]=datetime.now(timezone.utc).isoformat()
    for c in FINAL_COLS:
        if c not in out.columns: out[c]=None
    if "Lat" in out.columns and "Long" in out.columns:
        out=out[pd.notnull(out["Lat"]) & pd.notnull(out["Long"])]
    return out[FINAL_COLS]

# --------------- Main ---------------
def main():
    log(f"Working dir: {OUTPUT_DIR}")
    stations_df=fetch_stations_full_per_station()
    if stations_df is None or stations_df.empty:
        log("ERROR: No station metadata retrieved. Aborting."); return

    
    ids=stations_df["stationID"].dropna().unique().tolist()

    status_df=fetch_all_statuses(ids)
    load_df=fetch_all_loads(ids)

    merged=stations_df
    if not status_df.empty: merged=merged.merge(status_df, on=["stationID"], how="left")
    if load_df is not None and not load_df.empty: merged=merged.merge(load_df, on=["stationID"], how="left")

    for c in ["ports_total","ports_charging","ports_occupied","ports_available"]:
        if c not in merged.columns: merged[c]=0

    # Priority by level with CPF single-port special case
    def compute_label(row):
        charger_type = (row.get("Charger type") or "").upper()
        model = (row.get("stationModel") or "").upper()
        avail = int(row.get("ports_available", 0) or 0)
        chg   = int(row.get("ports_charging", 0) or 0)
        occ   = int(row.get("ports_occupied", 0) or 0)

        is_dc  = ("LEVEL 3" in charger_type) or ("DC" in charger_type) or ("FAST" in charger_type)
        is_cpf = model.startswith("CPF")  # CPF25/32/50... = single-port Level 2

        if is_dc or is_cpf:
            # Single-port behavior: Charging > Available > Occupied
            if chg   > 0: return "Charging"
            if avail > 0: return "Available"
            if occ   > 0: return "Occupied"
            return "Available"
        else:
            # Multi-port Level 2: Available > Charging > Occupied
            if avail > 0: return "Available"
            if chg   > 0: return "Charging"
            if occ   > 0: return "Occupied"
            return "Available"

    merged["station_label"] = merged.apply(compute_label, axis=1)

    # Fault override (normalize fields to string safely)
    def norm(x):
        if x is None: return ""
        try:
            import pandas as _pd
            if _pd.isna(x): return ""
        except Exception:
            pass
        return str(x)

    fr  = merged.get("faultReason", pd.Series([""]*len(merged))).map(norm).str.strip().str.upper()
    lps = merged.get("LastPortStatus", pd.Series([""]*len(merged))).map(norm).str.strip().str.upper()
    net = merged.get("StationNetworkStatus", pd.Series([""]*len(merged))).map(norm).str.strip().str.upper()

    fault_mask = (
        lps.isin({"FAULTED","UNAVAILABLE","OUTOFORDER","MAINTENANCE"}) |
        net.isin({"UNAVAILABLE","OFFLINE"}) |
        ((~fr.isin({"", "NONE", "N/A", "NULL"})) & lps.isin({"FAULTED","UNAVAILABLE","OUTOFORDER","MAINTENANCE"}))
    )
    merged.loc[fault_mask, "station_label"] = "Faulted"

    final_df=slim_output(merged); atomic_write_csv(final_df, STATUS_OUT)
    log(f"Wrote {STATUS_OUT} with {len(final_df):,} rows")

if __name__=="__main__":
    main()
