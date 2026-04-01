"""
Populate city, state, country in telecom_sites using reverse geocoding.
Uses a grid approach: divide the coordinate space into cells, reverse geocode
each cell center, then assign sites to their nearest cell.
"""

import psycopg2
import math
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

DB = "postgresql://postgres:ROOT@localhost:5432/telecom_cch"

geolocator = Nominatim(user_agent="telecom_cch_geo_populator", timeout=10)

def reverse_geocode(lat, lng):
    """Reverse geocode a lat/lng to city, state, country."""
    try:
        location = geolocator.reverse(f"{lat},{lng}", language="en", exactly_one=True)
        if location and location.raw.get("address"):
            addr = location.raw["address"]
            city = (addr.get("city") or addr.get("town") or addr.get("village")
                    or addr.get("suburb") or addr.get("county") or "")
            state = addr.get("state") or ""
            country = addr.get("country") or ""
            return city, state, country
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"  Geocoding error for ({lat},{lng}): {e}")
    return "", "", ""


def main():
    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Get all distinct sites with lat/lng
    cur.execute("SELECT DISTINCT site_id, latitude, longitude FROM telecom_sites WHERE latitude IS NOT NULL")
    sites = cur.fetchall()
    print(f"Found {len(sites)} unique sites")

    # Build grid: ~0.01 degree cells (~1km)
    GRID_SIZE = 0.01
    grid_cache = {}  # (grid_lat, grid_lng) -> (city, state, country)

    # Group sites by grid cell
    site_grid = {}  # site_id -> (grid_lat, grid_lng)
    grid_sites = {}  # (grid_lat, grid_lng) -> [site_ids]

    for site_id, lat, lng in sites:
        glat = round(math.floor(float(lat) / GRID_SIZE) * GRID_SIZE + GRID_SIZE / 2, 5)
        glng = round(math.floor(float(lng) / GRID_SIZE) * GRID_SIZE + GRID_SIZE / 2, 5)
        key = (glat, glng)
        site_grid[site_id] = key
        grid_sites.setdefault(key, []).append(site_id)

    print(f"Grid cells to geocode: {len(grid_sites)} (instead of {len(sites)} individual calls)")

    # Reverse geocode each grid cell center
    for i, (key, site_list) in enumerate(grid_sites.items()):
        glat, glng = key
        city, state, country = reverse_geocode(glat, glng)
        grid_cache[key] = (city, state, country)
        print(f"  [{i+1}/{len(grid_sites)}] ({glat},{glng}) -> {city}, {state}, {country} ({len(site_list)} sites)")
        time.sleep(1.1)  # Nominatim rate limit: 1 req/sec

    # Update database
    print("\nUpdating database...")
    updated = 0
    for site_id, key in site_grid.items():
        city, state, country = grid_cache.get(key, ("", "", ""))
        if city or state or country:
            cur.execute(
                "UPDATE telecom_sites SET city = %s, state = %s, country = %s WHERE site_id = %s",
                (city, state, country, site_id)
            )
            updated += 1

    conn.commit()
    print(f"Updated {updated} sites")

    # Verify
    cur.execute("SELECT city, state, country, COUNT(DISTINCT site_id) FROM telecom_sites WHERE city != '' GROUP BY city, state, country ORDER BY COUNT(DISTINCT site_id) DESC")
    print("\nResults:")
    for row in cur.fetchall():
        print(f"  {row[0]}, {row[1]}, {row[2]} — {row[3]} sites")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
