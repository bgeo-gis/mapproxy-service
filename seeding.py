"""
Copyright © 2025 by BGEO. All rights reserved.
The program is free software: you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.
"""
from typing import Any, Callable
import yaml
import psycopg2
import os
from dataclasses import dataclass

@dataclass
class MapZone:
    table: str
    tab: str
    column: str

MAP_ZONES: dict[str, MapZone] = {
    "E": MapZone(
        "selector_expl",
        "tab_exploitation",
        "expl_id"
    ),
    "S": MapZone(
        "selector_sector",
        "tab_sector",
        "sector_id"
    ),
    "M": MapZone(
        "selector_municipality",
        "tab_municipality",
        "muni_id"
    ),
    "T": MapZone(
        "selector_state",
        "tab_network_state",
        "state"
    ),
}

def seed(
    config: dict,
    local_conn: psycopg2.extensions.connection,
    remote_conn: psycopg2.extensions.connection,
    generated_config_path: str,
    temp_folder: str,
    file_name: str,
    coverage: dict | Callable[[str, list[tuple[MapZone, str]], Any], dict | None] | None = None
):
    local_cursor = local_conn.cursor()
    remote_cursor = remote_conn.cursor()

    # Temp files (seed yaml and last seed time)
    seed_yaml_file = os.path.join(temp_folder, f"{file_name}_seed.yaml")

    # Generated seed config file
    base_config_file = os.path.join(generated_config_path, f"{file_name}.yaml")

    db_schema = config["data_db_schema"]

    local_cursor.execute(f'SELECT tilecluster_id FROM {config["tileclusters_table"]}')
    tilecluster_ids = local_cursor.fetchall()

    for tilecluster_id, in tilecluster_ids:
        mapzones: list[tuple[MapZone, str]] = []
        for part in tilecluster_id.split("-"):
            mapzone_name_id = part[0]
            mapzone_id = part[1:]

            if mapzone_name_id not in MAP_ZONES:
                raise ValueError(f"Invalid mapzone name: {mapzone_name_id}")

            mapzone = MAP_ZONES[mapzone_name_id]
            mapzones.append((mapzone, mapzone_id))

        coverage_dict = {}
        if coverage is None:
            coverage_dict = {
                "clip": True,
                "srs": config["crs"],
                "datasource": config["db_url"],
                "where": f"SELECT ST_Buffer(geom, 0) FROM {config['tileclusters_table']} WHERE tilecluster_id = '{tilecluster_id}'"
            }
        elif isinstance(coverage, dict):
            coverage_dict = coverage
        elif callable(coverage):
            result = coverage(tilecluster_id, mapzones, remote_conn)
            if result is None:
                print(f"No coverage found for tilecluster {tilecluster_id}, skipping seeding")
                continue

            coverage_dict = result
        else:
            raise ValueError("Coverage must be a dict or a callable function that returns a dict")

        for mapzone, mapzone_id in mapzones:
            remote_cursor.execute(f"DELETE FROM {db_schema}.{mapzone.table} WHERE cur_user = current_user;")

        for mapzone, mapzone_id in mapzones:
            # IMPORTANT: Set `value` to `True`
            remote_cursor.execute(
                f'''SELECT {db_schema}.gw_fct_setselectors($${{
                    "client":{{
                        "device": 5, "lang": "es_ES", "tiled": "False", "infoType": 1, "epsg": 25831
                    }}, 
                    "form":{{}}, "feature":{{}}, "data":{{
                        "filterFields":{{}}, "pageInfo":{{}}, "selectorType": "selector_basic", "tabName": "{mapzone.tab}", "addSchema": "NULL", "id": "{mapzone_id}", "isAlone": "False", "disableParent": "False", "value": "True"
                    }}
                }}$$);'''
            )
            result = remote_cursor.fetchone()
            if (
                result is None or
                result[0] is None or
                result[0]["status"] != "Accepted"
            ):
                raise ValueError(f"Error setting selector for {mapzone.tab} with id {mapzone_id}: {result}")

        remote_conn.commit()

        print(f"Seeding {tilecluster_id}...")
        grid_name = f"{tilecluster_id}_grid"

        # Refresh materialized views in remote database after selector updates
        materialized_views = config["materialized_views"]
        for view in materialized_views:
            print("Refreshing materialized view: ", view)
            remote_cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")

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
            "coverages": {
                "main_coverage": coverage_dict
            }
        }


        # If callback return None, we skip the seeding so this is not harmfull
        output["seeds"]["seed_prog"]["coverages"] = ["main_coverage"]

        with open(seed_yaml_file, "w") as f:
            yaml.dump(output, f)

        print("base_config_file:  ", base_config_file)
        print("seed_yaml_file:  ", seed_yaml_file)
        os.system(f"mapproxy-seed -f {base_config_file} -s {seed_yaml_file} -c {os.cpu_count()} --seed seed_prog > /logs/mapproxy_seed.log 2>&1")

