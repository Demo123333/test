import cloudscraper
import json
import os
import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- Random Generators -----------------

# Example User-Agent pool
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.1 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.1 (Windows NT 11.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}",
    # Chrome on Mac
    "Mozilla/5.1 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.38",
    # Safari on Mac
    "Mozilla/5.1 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{safari_ver} Safari/605.1.16",
]


def get_random_user_agent():
    template = random.choice(USER_AGENTS)
    return template.format(
        version=f"{random.randint(70,120)}.0.{random.randint(1000,5000)}.{random.randint(0,150)}",
        minor=random.randint(12, 15),
        safari_ver=f"{random.randint(13,17)}.0.{random.randint(1,3)}",
    )


def get_random_ip():
    return ".".join(str(random.randint(1, 255)) for _ in range(4))


def get_headers():
    random_ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://in.bookmyshow.com",
        "Referer": "https://in.bookmyshow.com/",
        "X-Forwarded-For": random_ip,
        "Client-IP": random_ip,
    }


headers = get_headers()

# ---------------- Fetch Functions -----------------
def fetch_city_data(city_slug):
    scraper = cloudscraper.create_scraper()
    headers = get_headers()

    homepage_url = f"https://in.bookmyshow.com/explore/home/{city_slug}"
    homepage_response = scraper.get(homepage_url, headers=headers)
    if homepage_response.status_code != 200:
        return None

    json_url = "https://in.bookmyshow.com/serv/getData?cmd=QUICKBOOK&type=MT"
    json_response = scraper.get(json_url, headers=headers)
    if json_response.status_code != 200:
        return None

    try:
        return json.loads(json_response.text)
    except:
        return None

# ---------------- Extraction -----------------
def extract_movies(data):
    result = {}
    movies = data.get('moviesData', {}).get('BookMyShow', {}).get('arrEvents', [])
    for movie in movies:
        title = movie.get('EventTitle')
        child_events = movie.get('ChildEvents', [])
        if not child_events:
            continue
        # yahan se nikal lo default event code
        default_event_code = movie.get("EventCode")
        first_variant = child_events[0]
        main_poster = f"https://in.bmscdn.com/events/moviecard/{first_variant.get('EventImageCode')}.jpg"
        main_genres = first_variant.get("Genre", [])
        main_rating = first_variant.get("EventCensor")
        main_duration = first_variant.get("Duration")
        main_event_date = first_variant.get("EventDate")
        main_is_new = first_variant.get("isNewEvent")

        if title not in result:
            result[title] = {
                "Title": title,
                "Poster": main_poster,
                "Genres": main_genres,
                "Rating": main_rating,
                "Duration": main_duration,
                "EventDate": main_event_date,
                "isNewEvent": main_is_new,
                "DefaultEventCode": default_event_code,   # <-- add this
                "Variants": []
            }

        existing_event_codes = {v["EventCode"] for v in result[title]["Variants"]}
        for variant in child_events:
            code = variant.get("EventCode")
            if code not in existing_event_codes:
                variant_info = {
                    "VariantName": variant.get("EventName"),
                    "EventCode": code,
                    "Language": variant.get("EventLanguage"),
                    "Format": variant.get("EventDimension")
                }
                result[title]["Variants"].append(variant_info)
    return result


def extract_venues(data):
    venues = {}
    try:
        raw_venues = data["cinemas"]["BookMyShow"]["aiVN"]["venues"]
    except KeyError:
        return venues

    for v in raw_venues:
        code = v.get("VenueCode")
        if code not in venues:
            venues[code] = {
                "VenueCode": code,
                "VenueName": v.get("VenueName"),
                "VenueAddress": v.get("VenueAddress"),
                "City": v.get("City"),
                "State": v.get("State"),
                "RegionCode": v.get("RegionCode"),
                "SubRegionCode": v.get("SubRegionCode"),
                "Latitude": v.get("VenueLatitude"),
                "Longitude": v.get("VenueLongitude"),
                "AvailableFormats": v.get("availableEventFormats")
            }
    return venues

# ---------------- State Management -----------------
def safe_load(filename):
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                data = f.read().strip()
                if not data:
                    return set()
                return set(json.loads(data))
        except:
            print(f"[!] Warning: {filename} corrupted, resetting.")
            return set()
    return set()

def safe_save(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(sorted(list(data)), f, indent=2, ensure_ascii=False)

# ---------------- Worker -----------------
lock = threading.Lock()
consecutive_errors = 0

def process_city(city_slug, all_movies, all_venues, fetched, failed):
    global consecutive_errors
    if city_slug in fetched:
        print(f"⏩ Skipping {city_slug}, already fetched.")
        return

    data = fetch_city_data(city_slug)
    if not data:
        with lock:
            consecutive_errors += 1
            failed.add(city_slug)
            safe_save(failed, "citiesfailed.json")
            print(f"[❌ Failed] {city_slug}. Error streak = {consecutive_errors}")
        return

    movies = extract_movies(data)
    venues = extract_venues(data)

    with lock:
        consecutive_errors = 0
        all_movies[city_slug] = movies
        for code, venue in venues.items():
            if code not in all_venues:
                all_venues[code] = venue
        fetched.add(city_slug)
        if city_slug in failed:
            failed.remove(city_slug)   # success → remove from failed
        safe_save(fetched, "citiesfetched.json")
        safe_save(failed, "citiesfailed.json")
        print(f"✅ {city_slug}: {len(movies)} movies, {len(venues)} venues")

    time.sleep(random.uniform(1, 2))  # jitter delay

# ---------------- Main -----------------
if __name__ == "__main__":
    all_movies = {}
    all_venues = {}

    with open("citiesbms.json", "r", encoding="utf-8") as f:
        city_slugs = [city["RegionSlug"] for city in json.load(f)]

    fetched = safe_load("citiesfetched.json")
    failed = safe_load("citiesfailed.json")

    # Retry failed cities first
    cities_to_fetch = list(failed | set(city_slugs))

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(process_city, slug, all_movies, all_venues, fetched, failed): slug for slug in cities_to_fetch}

        for future in as_completed(futures):
            if consecutive_errors >= 10:
                print("❌ 10 consecutive errors. Stopping all workers...")
                executor.shutdown(wait=False, cancel_futures=True)
                break

    # Save venues (as before)
    os.makedirs("output", exist_ok=True)
    with open("output/venues.json", "w", encoding="utf-8") as f:
        json.dump(all_venues, f, indent=2, ensure_ascii=False)

    # ---------------- Flatten and count cities per movie -----------------
# ---------------- Flatten and count cities per movie & variant -----------------
movies_flat = {}
movie_city_count = {}
variant_city_count = {}

for city, movies in all_movies.items():
    for movie_name, details in movies.items():
        if movie_name not in movies_flat:
            movies_flat[movie_name] = {
                "Title": details.get("Title"),
                "Poster": details.get("Poster"),
                "Genres": details.get("Genres", []),
                "Rating": details.get("Rating"),
                "Duration": details.get("Duration"),
                "EventDate": details.get("EventDate"),
                "isNewEvent": details.get("isNewEvent"),
                "DefaultEventCode": details.get("DefaultEventCode"),
                "Variants": []
            }
            movie_city_count[movie_name] = set()
        
        # Track cities for this movie
        movie_city_count[movie_name].add(city)

        # Track cities for each variant
        for variant in details.get("Variants", []):
            code = variant["EventCode"]
            if code not in variant_city_count:
                variant_city_count[code] = set()
            variant_city_count[code].add(city)

            # Add variant details if not already present
            existing_codes = {v["EventCode"] for v in movies_flat[movie_name]["Variants"]}
            if code not in existing_codes:
                movies_flat[movie_name]["Variants"].append(variant)

# Prepare the final sorted list of movies with city count included
sorted_movies = sorted(
    movies_flat.values(),
    key=lambda x: len(movie_city_count[x["Title"]]),
    reverse=True
)

# Add CityCount for movie and variants
final_movies = []
for movie in sorted_movies:
    movie_name = movie["Title"]
    default_code = movie.get("DefaultEventCode")  # <-- har movie ka alag default

    # Add city count to each variant
    variants_with_count = []
    for variant in movie["Variants"]:
        code = variant["EventCode"]
        variants_with_count.append({
            **variant,
            "CityCount": len(variant_city_count.get(code, []))
        })

    # Custom sort: default variant first, then by CityCount desc
    variants_sorted = sorted(
        variants_with_count,
        key=lambda v: (0 if v["EventCode"] == default_code else 1, -v["CityCount"])
    )

    new_movie = {
        "Title": movie["Title"],
        "Poster": movie["Poster"],
        "Genres": movie["Genres"],
        "Rating": movie["Rating"],
        "Duration": movie["Duration"],
        "EventDate": movie["EventDate"],
        "isNewEvent": movie["isNewEvent"],
        "CityCount": len(movie_city_count[movie_name]),
        "DefaultEventCode": default_code,   # <-- output me bhi rakh sakte ho
        "Variants": variants_sorted
    }
    final_movies.append(new_movie)

# Save sorted movies to JSON
with open("output/movies.json", "w", encoding="utf-8") as f:
    json.dump(final_movies, f, indent=2, ensure_ascii=False)

print(f"🎉 Finished. Saved {len(final_movies)} unique movies with city count & variants sorted by city coverage.")