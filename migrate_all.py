# migrate_all.py - Migration avec remplissage population_affected
import psycopg2
import psycopg2.extras

LOCAL_DB = {"host": "localhost", "port": 5432, "dbname": "disasters", "user": "pipeline", "password": "pipeline123"}
NEON_DB = {"host": "ep-soft-meadow-abmj4037.eu-west-2.aws.neon.tech", "port": 5432, "dbname": "neondb", "user": "neondb_owner", "password": "npg_IAgFcOe63yQx", "sslmode": "require"}

# Valeurs réalistes par type de catastrophe
POP_DEFAULTS = {
    "Earthquake": 2500, "Earthquakes": 2500,
    "Flood": 5000, "Floods": 5000,
    "Tropical Cyclone": 10000,
    "Drought": 15000,
    "Volcano": 3000, "Volcanoes": 3000,
    "Wildfire": 800, "Wildfires": 800,
    "Tsunami": 8000,
    "Severe Storms": 3500,
    "Landslides": 500,
    "Dust and Haze": 2000,
    "Sea and Lake Ice": 1000,
}
DEFAULT_POP = 0

def get_pg_type(col):
    dtype = col[1].upper()
    max_len = col[2]
    prec, scale = col[3], col[4]

    if dtype in ('CHARACTER VARYING', 'VARCHAR'):
        return f"VARCHAR({max_len})" if max_len else "TEXT"
    if dtype == 'CHARACTER':
        return f"CHAR({max_len})" if max_len else "CHAR(1)"
    if dtype == 'TIMESTAMP WITHOUT TIME ZONE': return "TIMESTAMP"
    if dtype == 'TIMESTAMP WITH TIME ZONE': return "TIMESTAMPTZ"
    if dtype in ('NUMERIC', 'DECIMAL'):
        return f"NUMERIC({prec},{scale})" if prec else "NUMERIC"
    return dtype

def migrate():
    print("🔄 Migration 100% Python vers Neon (avec population réaliste)...\n")
    conn_src = conn_dst = None
    try:
        conn_src = psycopg2.connect(**LOCAL_DB)
        conn_dst = psycopg2.connect(**NEON_DB)
        cur_src = conn_src.cursor()
        cur_dst = conn_dst.cursor()

        tables = [
            ("gold_disasters", "disasters_gold", "disaster_id"),
            ("mart_disasters_by_country", "disasters_marts", None),
            ("mart_disasters_timeline", "disasters_marts", None)
        ]

        for tbl, schema, pk_col in tables:
            print(f"📦 Migration {schema}.{tbl}")

            cur_src.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s", (schema, tbl))
            if cur_src.fetchone()[0] == 0:
                print("   ⚠️ Table non trouvée, ignorée.\n"); continue

            cur_src.execute(f"SELECT COUNT(*) FROM {schema}.{tbl}")
            count = cur_src.fetchone()[0]
            print(f"   📊 Lignes locales: {count}")
            if count == 0:
                print("   ⏭️ Vide, ignoré.\n"); continue

            # Récupérer la structure
            cur_src.execute("""
                SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position
            """, (schema, tbl))
            cols = cur_src.fetchall()
            col_names = [c[0] for c in cols]
            col_names_quoted = [f'"{c}"' for c in col_names]
            col_defs = [f'"{c[0]}" {get_pg_type(c)}' for c in cols]

            # Créer la table
            col_defs_str = ', '.join(col_defs)
            if pk_col and pk_col in col_names:
                col_defs_str += f", PRIMARY KEY ({pk_col})"

            cur_dst.execute(f"DROP TABLE IF EXISTS {schema}.{tbl} CASCADE")
            cur_dst.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur_dst.execute(f"CREATE TABLE {schema}.{tbl} ({col_defs_str})")
            conn_dst.commit()
            print("   🛠️ Table créée")

            # Vérifier si population_affected existe
            has_pop = "population_affected" in col_names
            has_type = "event_type_label" in col_names

            # Migration des données
            cur_src.execute(f"SELECT {', '.join(col_names_quoted)} FROM {schema}.{tbl}")
            placeholders = ', '.join(['%s'] * len(cols))
            insert_sql = f"INSERT INTO {schema}.{tbl} ({', '.join(col_names_quoted)}) VALUES ({placeholders})"
            if pk_col:
                insert_sql += f" ON CONFLICT ({pk_col}) DO NOTHING"

            inserted = 0
            updated_pop = 0
            batch = []
            
            for row in cur_src:
                row_list = list(row)
                
                # 🔥 REMPLISSAGE INTELLIGENT de population_affected
                if has_pop and tbl == "gold_disasters":
                    pop_idx = col_names.index("population_affected")
                    current_pop = row_list[pop_idx]
                    
                    # Si NULL ou 0, on génère une valeur réaliste
                    if current_pop is None or current_pop == 0:
                        if has_type:
                            type_idx = col_names.index("event_type_label")
                            event_type = row_list[type_idx]
                            estimated = POP_DEFAULTS.get(event_type, DEFAULT_POP)
                        else:
                            estimated = DEFAULT_POP
                        row_list[pop_idx] = estimated
                        updated_pop += 1
                
                batch.append(tuple(row_list))
                
                if len(batch) >= 500:
                    try:
                        psycopg2.extras.execute_batch(cur_dst, insert_sql, batch)
                        inserted += len(batch)
                        conn_dst.commit()
                    except Exception as e:
                        conn_dst.rollback()
                        for r in batch:
                            try:
                                cur_dst.execute(insert_sql, r)
                                inserted += 1
                            except: pass
                        conn_dst.commit()
                    batch = []

            if batch:
                try:
                    psycopg2.extras.execute_batch(cur_dst, insert_sql, batch)
                    inserted += len(batch)
                except:
                    conn_dst.rollback()
                    for r in batch:
                        try: cur_dst.execute(insert_sql, r); inserted += 1
                        except: pass
                conn_dst.commit()

            pop_msg = f" (dont {updated_pop} populations estimées)" if updated_pop > 0 else ""
            print(f"   ✅ Succès: {inserted} lignes migrées{pop_msg}\n")

        # Vérification finale
        cur_dst.execute("SELECT COUNT(*) FROM disasters_gold.gold_disasters")
        total = cur_dst.fetchone()[0]
        
        cur_dst.execute("SELECT SUM(population_affected) FROM disasters_gold.gold_disasters")
        total_pop = cur_dst.fetchone()[0] or 0
        
        print(f"🎉 Migration terminée !")
        print(f"   • Total événements: {total}")
        print(f"   • Population totale affectée: {int(total_pop):,}")

    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn_src: conn_src.close()
        if conn_dst: conn_dst.close()

if __name__ == "__main__":
    migrate()