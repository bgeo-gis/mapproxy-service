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

def make_config(config_path: str, generated_config_path: str, file_name: str):
    with open(os.path.join(config_path, f"{file_name}.yaml"), "r") as f:
        config: dict = yaml.safe_load(f)

    grid_name = "main"

    conn = psycopg2.connect(config["db_url"])
    cursor = conn.cursor()

    output = {
        "services": {
            "demo": None,
            "wmts": None,
            # "wms": {
            #     "srs": ["EPSG:25831"],
            # },
        },
        "layers": [],
        "caches": {},
        "sources": {},
        "grids": {
            grid_name: {
                **config["grid"],
            }
        },
        "globals": {
            "cache": {
                "base_dir": f'/srv/qwc_service/mapproxy/tiles/{file_name}'
            }
        }
    }

    cursor.execute(f'SELECT tilecluster_id FROM {config["tiling_db_table"]}')
    tilecluster_data = cursor.fetchall()

    for tilecluster_id, in tilecluster_data:
        print(tilecluster_id)
        output["sources"][f"{tilecluster_id}_source"] = {
            "type": "wms",
            "seed_only": True,
            "req": {
                "transparent": True,
                **config["sources"]["inventory_source"],
            },
            "coverage": {
                "clip": False,
                "srs": "EPSG:31982",
                "datasource": config["db_url"],
                "where": f"SELECT ST_Buffer(geom, 0) FROM {config['tiling_db_table']} WHERE tilecluster_id = '{tilecluster_id}'",
            },
            "wms_opts": {
                "featureinfo": True,
            }
        }

        output["caches"][f"{tilecluster_id}_cache"] = {
            "disable_storage": False,
            "sources": [f"{tilecluster_id}_source"],
            "grids": [grid_name],
        }

        output["layers"].append({
            "name": tilecluster_id,
            "title": tilecluster_id,
            "tile_sources": [f"{tilecluster_id}_cache"],
        })

    Path(generated_config_path).mkdir(parents=True, exist_ok=True)
    with open(os.path.join(generated_config_path, f"{file_name}.yaml"), "w") as f:
        f.write(yaml.dump(output, default_flow_style=False, sort_keys=False))

