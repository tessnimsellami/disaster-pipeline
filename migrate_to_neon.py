# migrate_to_neon.py - Migration simple des 498 evenements vers Neon
import psycopg2
import subprocess
import sys

# Identifiants
NEON_URI = "postgresql://neondb_owner:npg_IAgFcOe63yQx@ep-soft-meadow-abmj4037.eu-west-2.aws.neon.tech/neondb?sslmode=require"
LOCAL_CMD = ["docker", "compose", "exec", "-T", "postgres", "pg_dump", "-U", "pipeline", "-d", "disasters", "--table=disasters_gold.gold_disasters", "--data-only", "--column-inserts"]

def migrate():
    print("🔄 Migration des 498 evenements vers Neon...")
    
    # 1. Export depuis PostgreSQL local
    print("   → Export depuis Docker...")
    result = subprocess.run(LOCAL_CMD, capture_output=True, text=True, cwd=".")
    if result.returncode != 0:
        print(f"   ❌ Erreur export: {result.stderr}")
        return
    
    sql = result.stdout.replace("INSERT INTO gold_disasters", "INSERT INTO disasters_gold.gold_disasters ON CONFLICT (disaster_id) DO NOTHING")
    
    # 2. Import vers Neon
    print("   → Import vers Neon...")
    try:
        with psycopg2.connect(NEON_URI) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS disasters_gold;")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS disasters_gold.gold_disasters (
                        disaster_id TEXT PRIMARY KEY, event_type TEXT, event_type_label TEXT,
                        event_name TEXT, alert_level TEXT, alert_level_num INTEGER, status TEXT,
                        country TEXT, iso3 TEXT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
                        event_date TIMESTAMPTZ, population_affected BIGINT, source_tag TEXT, is_active BOOLEAN
                    );
                """)
                conn.commit()
                
                # Executer les inserts
                for line in sql.split(';'):
                    if line.strip().startswith("INSERT"):
                        try:
                            cur.execute(line + ";")
                        except:
                            pass  # Ignorer doublons
                conn.commit()
        
        # 3. Verifier
        with psycopg2.connect(NEON_URI) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM disasters_gold.gold_disasters;")
                total = cur.fetchone()[0]
        
        print(f"✅ SUCCES ! {total} evenements dans Neon")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    migrate()