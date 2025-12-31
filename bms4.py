import json
import os
import random
import time
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import cloudscraper

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 4
API_TIMEOUT = 12
HARD_TIMEOUT = 15

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = os.path.join("advance", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
LOG_FILE      = f"{LOG_DIR}/bms{SHARD_ID}.log"

# =====================================================
# LOGGING
# =====================================================
def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# =====================================================
# HELPERS
# =====================================================
def calc_occupancy(sold, total):
    return round((sold / total) * 100, 2) if total else 0.0

# =====================================================
# HARD TIMEOUT
# =====================================================
class TimeoutError(Exception):
    pass

def hard_timeout(seconds):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            result = {}
            error = {}

            def target():
                try:
                    result["value"] = fn(*args, **kwargs)
                except Exception as e:
                    error["err"] = e

            t = threading.Thread(target=target, daemon=True)
            t.start()
            t.join(seconds)

            if t.is_alive():
                raise TimeoutError("Hard timeout hit")
            if "err" in error:
                raise error["err"]

            return result.get("value")
        return wrapper
    return decorator

# =====================================================
# USER AGENTS / IDENTITY
# =====================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

thread_local = threading.local()

class Identity:
    def __init__(self):
        self.ua = random.choice(USER_AGENTS)
        self.ip = ".".join(str(random.randint(20, 230)) for _ in range(4))
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )

    def headers(self):
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Origin": "https://in.bookmyshow.com",
            "Referer": "https://in.bookmyshow.com/",
            "X-Forwarded-For": self.ip,
        }

def get_identity():
    if not hasattr(thread_local, "identity"):
        thread_local.identity = Identity()
        log("🧠 New identity created")
    return thread_local.identity

def reset_identity():
    if hasattr(thread_local, "identity"):
        del thread_local.identity
    log("🔄 Identity reset")

# =====================================================
# FETCH API
# =====================================================
@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(venue_code):
    ident = get_identity()
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)
    if not r.text.strip().startswith("{"):
        raise RuntimeError("Blocked / HTML")
    return r.json()

# =====================================================
# PARSER
# =====================================================
def parse_payload(data):
    out = []

    sd = data.get("ShowDetails", [])
    if not sd:
        return out

    venue = sd[0].get("Venues", {})
    venue_name = venue.get("VenueName", "")
    venue_add  = venue.get("VenueAdd", "")
    chain      = venue.get("VenueCompName", "Unknown")

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim  = ch.get("EventDimension", "").strip() or "UNKNOWN"
            lang = ch.get("EventLanguage", "").strip() or "UNKNOWN"

            for sh in ch.get("ShowTimes", []):
                if sh.get("ShowDateCode") != DATE_CODE:
                    continue

                total = sold = avail = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free  = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += free
                    sold  += seats - free
                    gross += (seats - free) * price

                out.append({
                    "movie": title,
                    "venue": venue_name,
                    "address": venue_add,
                    "language": lang,
                    "dimension": dim,
                    "chain": chain,
                    "time": sh.get("ShowTime", ""),
                    "audi": sh.get("Attributes", "") or "",
                    "session_id": str(sh.get("SessionId", "")),
                    "totalSeats": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2)
                })

    return out

# =====================================================
# DEDUPE
# =====================================================
def dedupe(rows):
    seen = set()
    out = []
    for r in rows:
        key = (r["venue"], r["time"], r["session_id"], r["audi"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    log("🚀 SCRIPT STARTED")

    with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
        venues = json.load(f)

    all_rows = []

    for i, vcode in enumerate(venues, 1):
        log(f"[{i}/{len(venues)}] {vcode}")
        try:
            raw = fetch_api_raw(vcode)
            rows = parse_payload(raw)
            for r in rows:
                r["city"] = venues[vcode].get("City", "Unknown")
                r["state"] = venues[vcode].get("State", "Unknown")
                r["source"] = "BMS"
                r["date"] = DATE_CODE
            all_rows.extend(rows)
        except Exception as e:
            reset_identity()
            log(f"❌ {vcode} | {type(e).__name__}")
        time.sleep(random.uniform(0.35, 0.7))

    detailed = dedupe(all_rows)

    # =====================================================
    # SUMMARY
    # =====================================================
    summary = {}

    for r in detailed:
        movie = r["movie"]
        city  = r["city"]
        state = r["state"]
        venue = r["venue"]
        lang  = r["language"]
        dim   = r["dimension"]

        total = r["totalSeats"]
        sold  = r["sold"]
        gross = r["gross"]
        occ   = calc_occupancy(sold, total)

        if movie not in summary:
            summary[movie] = {
                "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                "venues": set(), "cities": set(),
                "fastfilling": 0, "housefull": 0,
                "details": {},
                "Language_details": {},
                "Format_details": {}
            }

        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)

        if occ >= 98: m["housefull"] += 1
        elif occ >= 50: m["fastfilling"] += 1

        # -------- CITY --------
        ck = (city, state)
        if ck not in m["details"]:
            m["details"][ck] = {
                "city": city, "state": state,
                "venues": set(), "shows": 0,
                "gross": 0.0, "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }
        d = m["details"][ck]
        d["venues"].add(venue)
        d["shows"] += 1
        d["gross"] += gross
        d["sold"] += sold
        d["totalSeats"] += total
        if occ >= 98: d["housefull"] += 1
        elif occ >= 50: d["fastfilling"] += 1

        # -------- LANGUAGE --------
        if lang not in m["Language_details"]:
            m["Language_details"][lang] = {
                "language": lang,
                "venues": set(), "shows": 0,
                "gross": 0.0, "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }
        L = m["Language_details"][lang]
        L["venues"].add(venue)
        L["shows"] += 1
        L["gross"] += gross
        L["sold"] += sold
        L["totalSeats"] += total
        if occ >= 98: L["housefull"] += 1
        elif occ >= 50: L["fastfilling"] += 1

        # -------- FORMAT --------
        if dim not in m["Format_details"]:
            m["Format_details"][dim] = {
                "dimension": dim,
                "venues": set(), "shows": 0,
                "gross": 0.0, "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }
        F = m["Format_details"][dim]
        F["venues"].add(venue)
        F["shows"] += 1
        F["gross"] += gross
        F["sold"] += sold
        F["totalSeats"] += total
        if occ >= 98: F["housefull"] += 1
        elif occ >= 50: F["fastfilling"] += 1

    # =====================================================
    # FINAL SUMMARY
    # =====================================================
    final_summary = {}

    for movie, m in summary.items():
        final_summary[movie] = {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": calc_occupancy(m["sold"], m["totalSeats"]),
            "City_details": [],
            "Language_details": [],
            "Format_details": []
        }

        for d in m["details"].values():
            final_summary[movie]["City_details"].append({
                "city": d["city"],
                "state": d["state"],
                "venues": len(d["venues"]),
                "shows": d["shows"],
                "gross": round(d["gross"], 2),
                "sold": d["sold"],
                "totalSeats": d["totalSeats"],
                "fastfilling": d["fastfilling"],
                "housefull": d["housefull"],
                "occupancy": calc_occupancy(d["sold"], d["totalSeats"])
            })

        for l in m["Language_details"].values():
            final_summary[movie]["Language_details"].append({
                "language": l["language"],
                "venues": len(l["venues"]),
                "shows": l["shows"],
                "gross": round(l["gross"], 2),
                "sold": l["sold"],
                "totalSeats": l["totalSeats"],
                "fastfilling": l["fastfilling"],
                "housefull": l["housefull"],
                "occupancy": calc_occupancy(l["sold"], l["totalSeats"])
            })

        for f in m["Format_details"].values():
            final_summary[movie]["Format_details"].append({
                "dimension": f["dimension"],
                "venues": len(f["venues"]),
                "shows": f["shows"],
                "gross": round(f["gross"], 2),
                "sold": f["sold"],
                "totalSeats": f["totalSeats"],
                "fastfilling": f["fastfilling"],
                "housefull": f["housefull"],
                "occupancy": calc_occupancy(f["sold"], f["totalSeats"])
            })

    # =====================================================
    # SAVE
    # =====================================================
    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")
