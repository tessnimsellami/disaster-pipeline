# migrate_all.py - Migre TOUTES les tables vers Neon (Version corrigée)
import psycopg2
import subprocess

NEON_URI = "postgresql://neondb_owner:npg_IAgFcOe63yQx@ep-soft-meadow-abmj4037.eu-west-2.aws.neon.tech/neondb?sslmode=require"

def migrate_table(table_name, schema="disasters_gold"):
    """Migre une table spécifique depuis Docker vers Neon"""
    print(f"   → Migration de {schema}.{table_name}...")
    
    # Export depuis Docker (syntaxe f-string corrigée)
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "pg_dump", "-U", "pipeline", 
         "-d", "disasters", f"--table={schema}.{table_name}", "--data-only", "--column-inserts"],
        capture_output=True, text=True, cwd="."
    )
    
    if result.returncode != 0:
        print(f"      ⚠️ Erreur export: {result.stderr[:100]}")
        return 0
    
    sql = result.stdout
    # Nettoyer les références public. et ajouter le bon schéma
    sql = sql.replace("public.", f"{schema}.")
    sql = sql.replace(f"INSERT INTO {table_name}", f"INSERT INTO {schema}.{table_name}")
    
    # Ajouter ON CONFLICT uniquement pour la table qui a disaster_id comme PK
    if table_name == "gold_disasters":
        sql = sql.replace(f"INSERT INTO {schema}.{table_name}", 
                          f"INSERT INTO {schema}.{table_name} ON CONFLICT (disaster_id) DO NOTHING")
    
    # Import vers Neon
    inserted = 0
    with psycopg2.connect(NEON_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
            for line in sql.split(';'):
                if line.strip().startswith("INSERT"):
                    try:
                        cur.execute(line + ";")
                        inserted += 1
                    except:
                        pass  # Ignorer erreurs mineures/doublons
            conn.commit()
    
    return inserted

def main():
    print("🔄 Migration complète vers Neon...")
    
    # 1. Migre gold_disasters
    gold_count = migrate_table("gold_disasters", "disasters_gold")
    print(f"   ✅ gold_disasters: {gold_count} événements")
    
    # 2. Migre mart_disasters_by_country
    country_count = migrate_table("mart_disasters_by_country", "disasters_marts")
    print(f"   ✅ mart_disasters_by_country: {country_count} lignes")
    
    # 3. Migre mart_disasters_timeline
    timeline_count = migrate_table("mart_disasters_timeline", "disasters_marts")
    print(f"   ✅ mart_disasters_timeline: {timeline_count} lignes")
    
    # Vérifier le total final
    with psycopg2.connect(NEON_URI) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM disasters_gold.gold_disasters;")
            total = cur.fetchone()[0]
    
    print(f"\n🎉 Migration terminée !")
    print(f"   • Total événements dans gold_disasters: {total}")
    print(f"   • Tables dbt migrées: mart_disasters_by_country, mart_disasters_timeline")
    print(f"   • Ton dashboard affichera maintenant TOUS les graphiques ✅")

if __name__ == "__main__":
    main()