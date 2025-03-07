import yaml
import psycopg2
import os
from pathlib import Path

def parse_bbox(bbox_str):
    bbox_str = bbox_str.replace('BOX(', '').replace(')', '')
    min_coords, max_coords = bbox_str.split(',')
    min_x, min_y = map(float, min_coords.split())
    max_x, max_y = map(float, max_coords.split())
    return [min_x, min_y, max_x, max_y]

def get_bbox_from_db(tilecluster_id, db_url, config):
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    query = f"SELECT ST_Extent(geom) FROM {config['tiling_db_table']} WHERE tilecluster_id = %s"
    cur.execute(query, (tilecluster_id,))
    bbox_str = cur.fetchone()[0]
    cur.close()
    conn.close()
    bbox = parse_bbox(bbox_str)
    return bbox


def make_config_v2(config_path: str, generated_config_path: str, file_name: str):
    with open(os.path.join(config_path, f"{file_name}.yaml"), "r") as f:
        config: dict = yaml.safe_load(f)

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
        "grids": {},
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
        grid_name = f"{tilecluster_id}_grid"
        bbox = get_bbox_from_db(tilecluster_id, config["db_url"], config)
        output["sources"][f"{tilecluster_id}_source"] = {
            "type": "wms",
            "seed_only": True,
            "req": {
                "transparent": True,
                **config["sources"]["inventory_source"],
            },
            "coverage": {
                "clip": True,
                "srs": "EPSG:31982",
                "datasource": config["db_url"],
                "where": f"SELECT ST_Buffer(geom, 0) FROM {config['tiling_db_table']} WHERE tilecluster_id = '{tilecluster_id}'",
            },
            "wms_opts": {
                "featureinfo": True,
            }
        }
        output["grids"][grid_name] = {
            "srs": config["grid"]["srs"],
            "origin": config["grid"]["origin"],
            "res": [
                52.9166666667,  # 0      200000.00000000
                26.4583333333,  # 1      100000.00000000
                13.2291666667,  # 2       50000.00000000
                5.2916666667,   # 3       20000.00000000
                2.6458333333,   # 4       10000.00000000
                1.3229166667,   # 5        5000.00000000
                0.6614583333,   # 6        2500.00000000
                0.3307291667,   # 7        1250.00000000
                0.1653645833,   # 8         625.00000000
                0.0826822917,   # 9         312.50000000
                0.0413411458,   # 10         156.25000000
            ],
            "bbox": bbox,
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