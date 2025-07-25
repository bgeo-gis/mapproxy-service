"""
Copyright © 2025 by BGEO. All rights reserved.
The program is free software: you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.
"""

import yaml
import os
from pathlib import Path

def parse_bbox(bbox_str):
    bbox_str = bbox_str.replace('BOX(', '').replace(')', '')
    min_coords, max_coords = bbox_str.split(',')
    min_x, min_y = map(float, min_coords.split())
    max_x, max_y = map(float, max_coords.split())
    return [min_x, min_y, max_x, max_y]

def get_bbox_from_db(tilecluster_id, local_conn, config):
    cur = local_conn.cursor()
    query = f"SELECT ST_Extent(geom) FROM {config['tileclusters_table']} WHERE tilecluster_id = %s"
    cur.execute(query, (tilecluster_id,))
    bbox_str = cur.fetchone()[0]
    bbox = parse_bbox(bbox_str)
    return bbox


def make_config(config: dict, local_conn, generated_config_path: str, file_name: str):
    local_cursor = local_conn.cursor()
    generated_config_file = os.path.join(generated_config_path, f"{file_name}.yaml")

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
        "grids": {},
        "globals": {
            "cache": {
                "base_dir": f'/srv/qwc_service/mapproxy/tiles/{file_name}'
            }
        }
    }

    local_cursor.execute(f'SELECT tilecluster_id FROM {config["tileclusters_table"]}')
    tilecluster_data = local_cursor.fetchall()

    for tilecluster_id, in tilecluster_data:
        print(tilecluster_id)
        grid_name = f"{tilecluster_id}_grid"
        bbox = get_bbox_from_db(tilecluster_id, local_conn, config)

        # Give extra space to ensure new elements fit in the cache file-structure
        bbox[0] -= 50_000
        bbox[1] -= 50_000
        bbox[2] += 50_000
        bbox[3] += 50_000

        output["sources"][f"{tilecluster_id}_source"] = {
            "type": "wms",
            "seed_only": True,
            "req": {
                "transparent": True,
                **config["sources"]["inventory_source"],
            },
            "coverage": {
                "srs": config["crs"],
                "datasource": config["db_url"],
                "where": f"SELECT ST_Buffer(geom, 0) FROM {config['tileclusters_table']} WHERE tilecluster_id = '{tilecluster_id}'",
            },
            "wms_opts": {
                "featureinfo": True,
            }
        }
        output["grids"][grid_name] = {
            "srs": config["grid"]["srs"],
            "origin": config["grid"]["origin"],
            "res": list(config["res"]), # Without the list it generats weird stuff
            "bbox": bbox,
        }
        output["caches"][f"{tilecluster_id}_cache"] = {
            "disable_storage": False,
            # "link_single_color_images": "hardlink",
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
