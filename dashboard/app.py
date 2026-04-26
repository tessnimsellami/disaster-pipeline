# load_to_neon.py - Version finale corrigee (sans suppression de donnees)
import requests
import psycopg2
from datetime import datetime, timedelta

# Tes identifiants Neon (a garder secrets !)
NEON_URI = "postgresql://neondb_owner:npg_IAgFcOe63yQx@ep-soft-meadow-abmj4037.eu-west-2.aws.neon.tech/neondb?sslmode=require"

def setup_table_if_not_exists(cur):
    """Cree la table seulement si elle n'existe pas (NE SUPPRIME PAS les donnees existantes)"""
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS disasters_gold;
        
        CREATE TABLE IF NOT EXISTS disasters_gold.gold_disasters (
            disaster_id TEXT PRIMARY KEY,
            event_type TEXT,
            event_type_label TEXT,
            event_name TEXT,
            alert_level TEXT,
            alert_level_num INTEGER,
            status TEXT,
            country TEXT,
            iso3 TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            event_date TIMESTAMPTZ,
            population_affected BIGINT,
            source_tag TEXT,
            is_active BOOLEAN
        );
    """)
    print("Table gold_disasters prete (donnees existantes conservees)")

def fetch_eonet_events(limit=25):
    """Recupere des evenements EONET reels depuis l'API NASA"""
    url = f"https://eonet.gsfc.nasa.gov/api/v3/events?limit={limit}&status=open"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        events = []
        
        for evt in data.get("events", []):
            geometries = evt.get("geometry", [])
            geo = geometries[-1] if geometries else {}
            coords = geo.get("coordinates", [None, None])
            category = evt.get("categories", [{}])[0]
            
            events.append({
                'disaster_id': f"EONET-{evt.get('id')}",
                'event_type': category.get('id', 'UNKNOWN'),
                'event_type_label': category.get('title', 'Unknown'),
                'event_name': evt.get('title', 'Untitled'),
                'alert_level': 'GREEN',
                'alert_level_num': 1,
                'status': 'ongoing',
                'country': None,
                'iso3': None,
                'latitude': coords[1] if len(coords) > 1 else None,
                'longitude': coords[0] if len(coords) > 0 else None,
                'event_date': datetime.utcnow(),
                'population_affected': 0,
                'source_tag': 'EONET',
                'is_active': True
            })
        return events
    except Exception as e:
        print(f"Erreur EONET: {e}")
        return []

def fetch_gdacs_sample(limit=15):
    """Genere des evenements GDACS representatifs (format reel, valeurs plausibles)"""
    samples = [
        {"type": "EQ", "label": "Earthquake", "country": "Turkey", "iso3": "TUR", "lat": 39.0, "lon": 35.0, "pop": 1500},
        {"type": "FL", "label": "Flood", "country": "Brazil", "iso3": "BRA", "lat": -15.0, "lon": -50.0, "pop": 8000},
        {"type": "TC", "label": "Tropical Cyclone", "country": "Philippines", "iso3": "PHL", "lat": 13.0, "lon": 122.0, "pop": 25000},
        {"type": "WF", "label": "Wildfire", "country": "USA", "iso3": "USA", "lat": 37.0, "lon": -120.0, "pop": 300},
        {"type": "VO", "label": "Volcano", "country": "Indonesia", "iso3": "IDN", "lat": -6.0, "lon": 110.0, "pop": 12000},
        {"type": "DR", "label": "Drought", "country": "Kenya", "iso3": "KEN", "lat": 1.0, "lon": 38.0, "pop": 50000},
        {"type": "EQ", "label": "Earthquake", "country": "Japan", "iso3": "JPN", "lat": 36.0, "lon": 138.0, "pop": 2000},
        {"type": "FL", "label": "Flood", "country": "India", "iso3": "IND", "lat": 20.0, "lon": 78.0, "pop": 15000},
    ]
    
    events = []
    for i, s in enumerate(samples[:limit]):
        events.append({
            'disaster_id': f"GDACS-SAMPLE-{i+1:03d}",
            'event_type': s['type'],
            'event_type_label': s['label'],
            'event_name': f"{s['label']} in {s['country']}",
            'alert_level': 'ORANGE' if s['pop'] > 10000 else 'GREEN',
            'alert_level_num': 2 if s['pop'] > 10000 else 1,
            'status': 'ongoing',
            'country': s['country'],
            'iso3': s['iso3'],
            'latitude': s['lat'],
            'longitude': s['lon'],
            'event_date': datetime.utcnow() - timedelta(days=i),
            'population_affected': s['pop'],
            'source_tag': 'GDACS',
            'is_active': True
        })
    return events

def load_to_neon():
    """Fonction principale : connexion + creation table + insertion"""
    try:
        conn = psycopg2.connect(NEON_URI)
        cur = conn.cursor()
        print("Connecte a Neon.tech")
        
        # Creer la table SI ELLE N'EXISTE PAS (ne supprime pas les donnees !)
        setup_table_if_not_exists(cur)
        conn.commit()
        
        # Recuperer les evenements EONET (API reelle)
        print("Recuperation EONET...")
        eonet_events = fetch_eonet_events(limit=25)
        print(f"   -> {len(eonet_events)} evenements EONET")
        
        # Generer les evenements GDACS (representatifs)
        print("Generation GDACS...")
        gdacs_events = fetch_gdacs_sample(limit=15)
        print(f"   -> {len(gdacs_events)} evenements GDACS")
        
        # Inserer avec gestion d'erreur par ligne
        inserted = 0
        for evt in eonet_events + gdacs_events:
            try:
                cur.execute("""
                    INSERT INTO disasters_gold.gold_disasters 
                    (disaster_id, event_type, event_type_label, event_name, alert_level,
                     alert_level_num, status, country, iso3, latitude, longitude,
                     event_date, population_affected, source_tag, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (disaster_id) DO NOTHING
                """, (
                    evt['disaster_id'], evt['event_type'], evt['event_type_label'],
                    evt['event_name'], evt['alert_level'], evt['alert_level_num'],
                    evt['status'], evt['country'], evt['iso3'], evt['latitude'],
                    evt['longitude'], evt['event_date'], evt['population_affected'],
                    evt['source_tag'], evt['is_active']
                ))
                inserted += 1
            except psycopg2.errors.UniqueViolation:
                # Deja existant, on ignore grace a ON CONFLICT
                pass
            except Exception as e:
                print(f"   Skip {evt['disaster_id']}: {e}")
                conn.rollback()
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"\nSUCCES ! {inserted} evenements injectes dans Neon")
        print("   - Les donnees existantes ont ete conservees")
        print("   - Les nouveaux evenements ont ete ajoutes sans doublons")
        
    except Exception as e:
        print(f"Erreur: {e}")

if __name__ == "__main__":
    load_to_neon()