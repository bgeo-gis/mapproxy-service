"""
Copyright © 2025 by BGEO. All rights reserved.
The program is free software: you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.
"""
import yaml
import psycopg2
import os
from pathlib import Path
import datetime
import time
import json
from make_conf_v2 import make_config_v2

def seed(config_path: str, generated_config_path: str, file_name: str, coverage: dict | None = None):

    # Get the configuration files
    seed_yaml_path = os.path.join(config_path, "temp")
    Path(os.path.join(config_path, "temp")).mkdir(parents=True, exist_ok=True)

    # Temp files (seed yaml and last seed time)
    seed_yaml_file = os.path.join(seed_yaml_path, f"{file_name}_seed.yaml")
    last_seed_file = os.path.join(seed_yaml_path, f"{file_name}_last_seed.time")

    # Generated seed config file
    base_config_file = os.path.join(generated_config_path, f"{file_name}.yaml")

    # Base seed config file
    user_config_file = os.path.join(config_path, f"{file_name}.yaml")
    with open(user_config_file, "r") as f:
            config: dict = yaml.safe_load(f)

    db_schema = config["data_db_schema"]
    # grid_name = "main"

    print("Replica DB: ", config["db_url"])
    print("Main DB: ", config["db_url_remote"])

    # Connect to main database for tilecluster data
    conn = psycopg2.connect(config["db_url"])
    cursor = conn.cursor()

    # Connect to remote database for selector operations
    remote_conn = psycopg2.connect(config["db_url_remote"])
    remote_cursor = remote_conn.cursor()

    cursor.execute(f'SELECT tilecluster_id, expl_id, sector_id, state FROM {config["tiling_db_table"]}')
    tilecluster_data = cursor.fetchall()

    for tilecluster_id, expl, sector, state in tilecluster_data:
        grid_name = f"{tilecluster_id}_grid"
        print(f"Seeding {tilecluster_id}...")

        # Update selector tables in remote database
        remote_cursor.execute(
            f"DELETE FROM {db_schema}.selector_expl WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_expl (expl_id, cur_user) VALUES ({expl}, current_user);"

            f"DELETE FROM {db_schema}.selector_sector WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_sector (sector_id, cur_user) VALUES ({sector}, current_user);"

            f"DELETE FROM {db_schema}.selector_state WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_state (state_id, cur_user) VALUES ({state}, current_user);"
        )
        remote_conn.commit()

        # Refresh materialized views in remote database after selector updates
        materialized_views = config["materialized_views"]
        print("Materialized views: ", materialized_views)
        for view in materialized_views:
            print("Refreshing materialized view: ", view)
            remote_cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
            print(f"{view} materialized view refreshed")
        remote_conn.commit()

        output = {
            "seeds": {
                "seed_prog": {
                    "caches": [f"{tilecluster_id}_cache"],
                    "refresh_before": {
                        "minutes": 0
                    },
                    "grids": [grid_name],
                }
            },
        }

        if coverage is not None:
            output["coverages"] = {
                "main_coverage": {
                    **coverage
                }
            }
            output["seeds"]["seed_prog"]["coverages"] = ["main_coverage"]

        with open(seed_yaml_file, "w") as f:
            yaml.dump(output, f)

        print("base_config_file:  ", base_config_file)
        print("seed_yaml_file:  ", seed_yaml_file)
        os.system(f"mapproxy-seed -f {base_config_file} -s {seed_yaml_file} -c 4 --seed seed_prog > /logs/mapproxy_seed_all.log 2>&1")

    # Delete and recreate the last_seed_file
    if os.path.exists(last_seed_file):
        os.remove(last_seed_file)
    with open(last_seed_file, "w"):
        pass

    # Close database connections
    cursor.close()
    conn.close()
    remote_cursor.close()
    remote_conn.close()


def seed_update(config_path: str, generated_config_path: str, file_name: str, coverage: dict | None = None):
    # Get current time
    seed_update_start_time = datetime.datetime.now()
    # Get the configuration files
    seed_yaml_path = os.path.join(config_path, "temp")
    Path(os.path.join(config_path, "temp")).mkdir(parents=True, exist_ok=True)

    # Temp files (seed yaml and last seed time)
    seed_yaml_file = os.path.join(seed_yaml_path, f"{file_name}_seed.yaml")

    geojson_file_path = os.path.join(seed_yaml_path, f"{file_name}_geometry.geojson")

    last_seed_file = os.path.join(seed_yaml_path, f"{file_name}_last_seed.time")

    print("Last seed file: ", last_seed_file)

    if not os.path.exists(last_seed_file):
        print(f"Last seed file `{last_seed_file}` does not exist, please do a full seed before updating")
        exit(1)

    last_seed = datetime.datetime.fromtimestamp(os.path.getmtime(last_seed_file))
    print("Last seed time: ", last_seed)

    # Base seed config file
    user_config_file = os.path.join(config_path, f"{file_name}.yaml")
    with open(user_config_file, "r") as f:
            config: dict = yaml.safe_load(f)

    db_schema = config["data_db_schema"]

    print("Replica DB: ", config["db_url"])
    print("Main DB: ", config["db_url_remote"])

    # Connect to main database for tilecluster data and feature boundary queries
    conn = psycopg2.connect(config["db_url"])
    cursor = conn.cursor()
    print("Replica DB connected")

    # Connect to remote database for materialized views and logging
    remote_conn = psycopg2.connect(config["db_url_remote"])
    remote_cursor = remote_conn.cursor()
    print("Main DB connected")

    # Refresh materialized views in remote database
    materialized_views = config["materialized_views"]
    print("Materialized views: ", materialized_views)
    for view in materialized_views:
        print("Refreshing materialized view: ", view)
        remote_cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
        print(f"{view} materialized view refreshed")

    # Refresh tilecluster materialized view
    remote_cursor.execute(f"REFRESH MATERIALIZED VIEW {config['tiling_db_table']}")
    print(f"{config['tiling_db_table']} materialized view refreshed")

    remote_conn.commit()

    # Generate new yaml config file
    make_config_v2(config_path, generated_config_path, file_name)
    print("Configuración regenerada correctamente (make_config_v2)")

    # Get config file
    base_config_file = os.path.join(generated_config_path, f"{file_name}.yaml")


    cursor.execute(f'SELECT tilecluster_id, expl_id, sector_id, state FROM {config["tiling_db_table"]}')
    tilecluster_data = cursor.fetchall()

    for tilecluster_id, expl, sector, state in tilecluster_data:
        print("Tilecluster ID: ", tilecluster_id)
        # Get coverage for the tilecluster from the database
        update_tables= config["update_tables"]
        crs = config["crs"]
        feature_json = {
            "client": {"device": 4, "infoType": 1, "lang": "ES"},
            "form": {},
            "feature": {"update_tables": update_tables},
            "data": {"type": "time", "lastSeed": f"{str(last_seed)}", "extra": f"expl_id = '{expl}' AND sector_id = '{sector}' AND state = '{state}'"}
        }
        feature_argument = json.dumps(feature_json)
        sql_query = f'SELECT {db_schema}.gw_fct_getfeatureboundary($${feature_argument}$$)'
        print("SQL: ", sql_query)
        cursor.execute(sql_query)
        result = cursor.fetchone()
        print("RESULT 0", result[0])
        geo_json = result[0]

        if geo_json['coordinates']:
            with open(geojson_file_path, 'w') as f:
                json.dump(geo_json, f, ensure_ascii=False)

            # Log the start of the re-tiling process in remote database
            start_time = datetime.datetime.now()
            process_id = f"seed_update_{seed_update_start_time}"
            remote_cursor.execute(
                f"INSERT INTO {config['log_table']} (process_id, tilecluster_id, project_id, start_time, geometry) "
                f"VALUES (%s, %s, %s, %s, ST_GeomFromGeoJSON(%s))",
                (process_id, tilecluster_id, file_name, start_time, json.dumps(geo_json))
            )
            remote_conn.commit()

            grid_name = f"{tilecluster_id}_grid"
            print(f"Seeding {tilecluster_id}...")
            output = {
                "seeds": {
                    "seed_prog": {
                        "caches": [f"{tilecluster_id}_cache"],
                        "refresh_before": {
                            "minutes": 0
                        },
                        "grids": [grid_name],
                    }
                },
            }

            if geo_json is not None:
                output["coverages"] = {
                    "main_coverage": {
                        "clip": True,
                        "srs": crs,
                        "datasource": geojson_file_path,
                    }
                }
                output["seeds"]["seed_prog"]["coverages"] = ["main_coverage"]

            with open(seed_yaml_file, "w") as f:
                yaml.dump(output, f)

            print("base_config_file:  ", base_config_file)
            print("seed_yaml_file:  ", seed_yaml_file)
            os.system(f"mapproxy-seed -f {base_config_file} -s {seed_yaml_file} -c 4 --seed seed_prog > /logs/mapproxy_seed_update.log 2>&1")

            # Log the end of the re-tiling process in remote database
            end_time = datetime.datetime.now()
            remote_cursor.execute(
                f"UPDATE {config['log_table']} SET end_time = %s WHERE tilecluster_id = %s AND start_time = %s",
                (end_time, tilecluster_id, start_time)
            )
            remote_conn.commit()

            print("seeded ", tilecluster_id)
            # Delete and recreate the last_seed_file
            if os.path.exists(last_seed_file):
                os.remove(last_seed_file)
            with open(last_seed_file, "w"):
                pass

        # Delete the geojson file to refresh it in the next iteration
        if os.path.exists(geojson_file_path):
            os.remove(geojson_file_path)

    # Close database connections
    cursor.close()
    conn.close()
    remote_cursor.close()
    remote_conn.close()

def seed_muni(config_path: str, generated_config_path: str, file_name: str, coverage: dict | None = None):

    # Get the configuration files
    seed_yaml_path = os.path.join(config_path, "temp")
    Path(os.path.join(config_path, "temp")).mkdir(parents=True, exist_ok=True)

    # Temp files (seed yaml and last seed time)
    seed_yaml_file = os.path.join(seed_yaml_path, f"{file_name}_seed.yaml")
    last_seed_file = os.path.join(seed_yaml_path, f"{file_name}_last_seed.time")

    # Generated seed config file
    base_config_file = os.path.join(generated_config_path, f"{file_name}.yaml")

    # Base seed config file
    user_config_file = os.path.join(config_path, f"{file_name}.yaml")
    with open(user_config_file, "r") as f:
            config: dict = yaml.safe_load(f)

    db_schema = config["data_db_schema"]

    print("Replica DB: ", config["db_url"])
    print("Main DB: ", config["db_url_remote"])

    # Connect to main database for tilecluster data
    conn = psycopg2.connect(config["db_url"])
    cursor = conn.cursor()

    # Connect to remote database for selector operations
    remote_conn = psycopg2.connect(config["db_url_remote"])
    remote_cursor = remote_conn.cursor()

    cursor.execute(f'SELECT tilecluster_id, muni_id, state FROM {config["tiling_db_table"]}')
    tilecluster_data = cursor.fetchall()

    for tilecluster_id, muni, state in tilecluster_data:
        grid_name = f"{tilecluster_id}_grid"
        print(f"Seeding {tilecluster_id}...")

        # Update selector tables in remote database
        remote_cursor.execute(
            f"DELETE FROM {db_schema}.selector_municipality WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_municipality (muni_id, cur_user) VALUES ({muni}, current_user);"

            f"DELETE FROM {db_schema}.selector_state WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_state (state_id, cur_user) VALUES ({state}, current_user);"
        )
        remote_conn.commit()

        # Refresh materialized views in remote database after selector updates
        materialized_views = config["materialized_views"]
        print("Materialized views: ", materialized_views)
        for view in materialized_views:
            print("Refreshing materialized view: ", view)
            remote_cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
            print(f"{view} materialized view refreshed")
        remote_conn.commit()

        output = {
            "seeds": {
                "seed_prog": {
                    "caches": [f"{tilecluster_id}_cache"],
                    "refresh_before": {
                        "minutes": 0
                    },
                    "grids": [grid_name],
                }
            },
        }

        if coverage is not None:
            output["coverages"] = {
                "main_coverage": {
                    **coverage
                }
            }
            output["seeds"]["seed_prog"]["coverages"] = ["main_coverage"]

        with open(seed_yaml_file, "w") as f:
            yaml.dump(output, f)

        print("base_config_file:  ", base_config_file)
        print("seed_yaml_file:  ", seed_yaml_file)
        os.system(f"mapproxy-seed -f {base_config_file} -s {seed_yaml_file} -c 4 --seed seed_prog > /logs/mapproxy_seed_all.log 2>&1")

    # Delete and recreate the last_seed_file
    if os.path.exists(last_seed_file):
        os.remove(last_seed_file)
    with open(last_seed_file, "w"):
        pass

    # Close database connections
    cursor.close()
    conn.close()
    remote_cursor.close()
    remote_conn.close()


def seed_update_muni(config_path: str, generated_config_path: str, file_name: str, coverage: dict | None = None):
    # Get current time
    seed_update_start_time = datetime.datetime.now()
    # Get the configuration files
    seed_yaml_path = os.path.join(config_path, "temp")
    Path(os.path.join(config_path, "temp")).mkdir(parents=True, exist_ok=True)

    # Temp files (seed yaml and last seed time)
    seed_yaml_file = os.path.join(seed_yaml_path, f"{file_name}_seed.yaml")

    geojson_file_path = os.path.join(seed_yaml_path, f"{file_name}_geometry.geojson")

    last_seed_file = os.path.join(seed_yaml_path, f"{file_name}_last_seed.time")

    print("Last seed file: ", last_seed_file)

    if not os.path.exists(last_seed_file):
        print(f"Last seed file `{last_seed_file}` does not exist, please do a full seed before updating")
        exit(1)

    last_seed = datetime.datetime.fromtimestamp(os.path.getmtime(last_seed_file))
    print("Last seed time: ", last_seed)

    # Generated seed config file
    base_config_file = os.path.join(generated_config_path, f"{file_name}.yaml")

    # Base seed config file
    user_config_file = os.path.join(config_path, f"{file_name}.yaml")
    with open(user_config_file, "r") as f:
            config: dict = yaml.safe_load(f)

    db_schema = config["data_db_schema"]

    print("Replica DB: ", config["db_url"])
    print("Main DB: ", config["db_url_remote"])

    # Connect to main database for tilecluster data and feature boundary queries
    conn = psycopg2.connect(config["db_url"])
    cursor = conn.cursor()
    print("Replica DB connected")

    # Connect to remote database for materialized views and logging
    remote_conn = psycopg2.connect(config["db_url_remote"])
    remote_cursor = remote_conn.cursor()
    print("Main DB connected")

    # Refresh materialized views in remote database
    materialized_views = config["materialized_views"]
    print("Materialized views: ", materialized_views)
    for view in materialized_views:
        print("Refreshing materialized view: ", view)
        remote_cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
        print(f"{view} materialized view refreshed")
    remote_conn.commit()

    cursor.execute(f'SELECT tilecluster_id, muni_id, state FROM {config["tiling_db_table"]}')
    tilecluster_data = cursor.fetchall()

    for tilecluster_id, muni, state in tilecluster_data:
        print("Tilecluster ID: ", tilecluster_id)
        # Get coverage for the tilecluster from the database
        update_tables= config["update_tables"]
        crs = config["crs"]
        feature_json = {
            "client": {"device": 4, "infoType": 1, "lang": "ES"},
            "form": {},
            "feature": {"update_tables": update_tables},
            "data": {"type": "time", "lastSeed": f"{str(last_seed)}", "extra": f"muni_id = '{muni}' AND state = '{state}'"}
        }
        feature_argument = json.dumps(feature_json)
        sql_query = f'SELECT {db_schema}.gw_fct_getfeatureboundary($${feature_argument}$$)'
        print("SQL: ", sql_query)
        cursor.execute(sql_query)
        result = cursor.fetchone()
        print("RESULT 0", result[0])
        geo_json = result[0]

        if geo_json['coordinates']:
            with open(geojson_file_path, 'w') as f:
                json.dump(geo_json, f, ensure_ascii=False)

            # Log the start of the re-tiling process in remote database
            start_time = datetime.datetime.now()
            process_id = f"seed_update_{seed_update_start_time}"
            remote_cursor.execute(
                f"INSERT INTO {config['log_table']} (process_id, tilecluster_id, project_id, start_time, geometry) "
                f"VALUES (%s, %s, %s, %s, ST_GeomFromGeoJSON(%s))",
                (process_id, tilecluster_id, file_name, start_time, json.dumps(geo_json))
            )
            remote_conn.commit()

            grid_name = f"{tilecluster_id}_grid"
            print(f"Seeding {tilecluster_id}...")
            output = {
                "seeds": {
                    "seed_prog": {
                        "caches": [f"{tilecluster_id}_cache"],
                        "refresh_before": {
                            "minutes": 0
                        },
                        "grids": [grid_name],
                    }
                },
            }

            if geo_json is not None:
                output["coverages"] = {
                    "main_coverage": {
                        "clip": True,
                        "srs": crs,
                        "datasource": geojson_file_path,
                    }
                }
                output["seeds"]["seed_prog"]["coverages"] = ["main_coverage"]

            with open(seed_yaml_file, "w") as f:
                yaml.dump(output, f)

            print("base_config_file:  ", base_config_file)
            print("seed_yaml_file:  ", seed_yaml_file)
            os.system(f"mapproxy-seed -f {base_config_file} -s {seed_yaml_file} -c 4 --seed seed_prog > /logs/mapproxy_seed_update.log 2>&1")

            # Log the end of the re-tiling process in remote database
            end_time = datetime.datetime.now()
            remote_cursor.execute(
                f"UPDATE {config['log_table']} SET end_time = %s WHERE tilecluster_id = %s AND start_time = %s",
                (end_time, tilecluster_id, start_time)
            )
            remote_conn.commit()

            print("seeded ", tilecluster_id)
            # Delete and recreate the last_seed_file
            if os.path.exists(last_seed_file):
                os.remove(last_seed_file)
            with open(last_seed_file, "w"):
                pass

        # Delete the geojson file to refresh it in the next iteration
        if os.path.exists(geojson_file_path):
            os.remove(geojson_file_path)

    # Close database connections
    cursor.close()
    conn.close()
    remote_cursor.close()
    remote_conn.close()