"""
Copyright © 2025 by BGEO. All rights reserved.
The program is free software: you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.
"""

import yaml
import os
from pathlib import Path


def make_config(config: dict, remote_conn, generated_config_path: str, geom_path: str, file_name: str):
    remote_cursor = remote_conn.cursor()
    generated_config_file = os.path.join(generated_config_path, f"{file_name}.yaml")

    grid_name = "main_grid"

    output = {
        "services": {
            "demo": None,
            "wmts": {
                "restful_template": "/tiles/{Layer}/{TileMatrixSet}/{TileMatrix}/{TileCol}/{TileRow}.{Format}"
            },
        },
        "layers": [],
        "caches": {},
        "sources": {},
        "grids": {
            grid_name: {
                "name": grid_name,
                "srs": config["grid"]["srs"],
                "origin": config["grid"]["origin"],
                "res": list(config["res"]), # Without the list it generats weird stuff
                "bbox": config["grid"]["bbox"],
            }
        },
        "globals": {
            "cache": {
                "base_dir": f'/srv/qwc_service/mapproxy/tiles/{file_name}'
            }
        }
    }

    remote_cursor.execute(f'SELECT tilecluster_id FROM {config["tileclusters_table"]}')
    tilecluster_data = remote_cursor.fetchall()

    additional_source = config["sources"].get("additional_source", None)
    additional_schema = config.get("additional_schema", None)

    if bool(additional_source) != bool(additional_schema):
        raise ValueError("Both 'additional_sources' and 'additional_schema' must be provided or neither.")

    for tilecluster_id, in tilecluster_data:
        print(tilecluster_id)

        source = config["sources"]["inventory_source"]
        if additional_source:
            is_additional_schema = False
            for part in tilecluster_id.split("-"):
                mapzone_name_id = part[0]
                mapzone_id = part[1:]

                if mapzone_name_id == "N" and int(mapzone_id) == 2:
                    is_additional_schema = True
                    break

            if is_additional_schema:
                source = additional_source

        output["sources"][f"{tilecluster_id}_source"] = {
            "type": "wms",
            "seed_only": True,
            "req": {
                "transparent": True,
                **source,
            },
            "coverage": {
                "srs": config["crs"],
                "datasource": os.path.join(geom_path, f'{tilecluster_id}.wkt'),
            },
            "wms_opts": {
                "featureinfo": True,
            }
        }
        output["caches"][f"{tilecluster_id}_cache"] = {
            "cache": {
                "type": "file",
                "use_grid_names": True,
            },
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
    with open(generated_config_file, "w") as f:
        f.write(yaml.dump(output, default_flow_style=False, sort_keys=False))
