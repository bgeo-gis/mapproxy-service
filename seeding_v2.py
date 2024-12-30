"""
Copyright © 2024 by BGEO. All rights reserved.
The program is free software: you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.
"""
import yaml
import psycopg2
import os
from pathlib import Path

def seed(config_path: str, generated_config_path: str, file_name: str, coverage: dict | None = None):
    seed_yaml_path = os.path.join(config_path, "temp")
    Path(os.path.join(config_path, "temp")).mkdir(parents=True, exist_ok=True)

    seed_yaml_file = os.path.join(seed_yaml_path, f"{file_name}_seed.yaml")

    base_config_file = os.path.join(generated_config_path, f"{file_name}.yaml")
    user_config_file = os.path.join(config_path, f"{file_name}.yaml")

    with open(user_config_file, "r") as f:
        config: dict = yaml.safe_load(f)

    db_schema = config["data_db_schema"]
    grid_name = "main"

    conn = psycopg2.connect(config["db_url"])
    cursor = conn.cursor()

    cursor.execute(f'SELECT tilecluster_id, muni_id, expl_id, sector_id, state FROM {config["tiling_db_table"]}')
    tilecluster_data = cursor.fetchall()

    for tilecluster_id, muni, expl, sector, state in tilecluster_data:
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

        if coverage is not None:
            output["coverages"] = {
                "main_coverage": {
                    **coverage
                }
            }
            output["seeds"]["seed_prog"]["coverages"] = ["main_coverage"]


        with open(seed_yaml_file, "w") as f:
            yaml.dump(output, f)

        cursor.execute(
            f"DELETE FROM {db_schema}.selector_municipality WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_municipality (muni_id, cur_user) VALUES ({muni}, current_user);"

            f"DELETE FROM {db_schema}.selector_expl WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_expl (expl_id, cur_user) VALUES ({expl}, current_user);"

            f"DELETE FROM {db_schema}.selector_sector WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_sector (sector_id, cur_user) VALUES ({sector}, current_user);"

            f"DELETE FROM {db_schema}.selector_state WHERE cur_user = current_user;"
            f"INSERT INTO {db_schema}.selector_state (state_id, cur_user) VALUES ({state}, current_user);"
        )
        conn.commit()

        os.system(f"mapproxy-seed -f {base_config_file} -s {seed_yaml_file} -c 4 --seed seed_prog")
