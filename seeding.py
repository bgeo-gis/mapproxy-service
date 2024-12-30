"""
Copyright © 2024 by BGEO. All rights reserved.
The program is free software: you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.
"""

import os
import psycopg2
import datetime
import json
import argparse

# Command line arguments
parser = argparse.ArgumentParser(prog='Seeding Program')
parser.add_argument("config_file")
parser.add_argument(
    '-m', '--mode',
    choices=["full", "update", "exploitation", "basemap", "update_elements", "update_timestamp"],
    required=True,
)
parser.add_argument(
    '-f', '--feature',
    help="JSON string for the 'feature' argument",
    required=False,
)
args = parser.parse_args()
mode = args.mode
config_file_path = args.config_file

if args.feature:
    try:
        # Parse the JSON feature argument from the command line
        feature_data = json.loads(args.feature)
    except json.JSONDecodeError:
        print("Invalid JSON for the 'feature' argument.")
        exit(1)

# Get configuration
config = None
with open(config_file_path) as config_file:
    config = json.load(config_file)
    # print(config)

last_seed_file_path = config["last_seed_file_path"]
base_config_path = config["base_config_path"]
db_schema = config["db_schema"]
schema_tiled = config["schema_tiled"]
update_tables = config["update_tables"]
max_level = config["max_level"]
seed_yaml_file_path = '/tmp/temp_seed.yaml'
geojson_file_path='/tmp/geometry.geojson'
geo_json = None

# Connect to the database
db_url = config["db_url"]
pg_service = db_url.split("=")[-1]

conn = psycopg2.connect(db_url)
cursor = conn.cursor()

# Check last seed time (only when updating)
last_seed = None
if mode == "update" or mode == "update_timestamp":
    if not os.path.exists(last_seed_file_path):
        print(f"Last seed file `{last_seed_file_path}` does not exist, please do a full seed before updating")
        exit(1)

    last_seed = datetime.datetime.fromtimestamp(os.path.getmtime(last_seed_file_path))

# Recreate the last seed file to indicate the time of the newest seed
if os.path.exists(last_seed_file_path):
    os.remove(last_seed_file_path)

if os.path.exists(geojson_file_path):
    os.remove(geojson_file_path)

with open(last_seed_file_path, 'w'):
    pass

# Seed basemap (only when doing a full seed)
if config.get("basemap") and mode == "full" or mode == "basemap":

    # Generate the seeding config file
    basemap_file_str = (
         "seeds:\n"
         "  seed_prog:\n"
        f"    caches: [{config['basemap']['cache_name']}]\n"
         "    refresh_before:\n"
         "      minutes: 0\n"
         "    levels:\n"
        f"      to: {max_level}\n"
    )
    with open(seed_yaml_file_path, 'w') as seed_yaml_file:
        seed_yaml_file.write(basemap_file_str)

    # Activate all exploitations in selector
    cursor.execute((
        f'SELECT {db_schema}.gw_fct_setselectors($${{'
         '    "client": {"device": 5, "lang": "es_ES", "cur_user": "mapproxy", "tiled": "True", "infoType": 1, "epsg": 25831}, '
         '    "form":{}, '
         '    "feature":{}, '
         '    "data":{"filterFields":{}, "pageInfo":{}, "selectorType": "None", "tabName": "tab_exploitation", "addSchema": "NULL", "checkAll": "True"}'
         '}$$);'
    ))
    conn.commit()

    # Execute seed command with the generated config
    os.system(f"mapproxy-seed -f {base_config_path} -s {seed_yaml_file_path} -c 4 --seed seed_prog")

# Seed diferent exploitations separately
if config.get("exploitations") and mode in {"exploitation", "update", "full", "update_elements", "update_timestamp"}:
    for expl_config in config["exploitations"]:

        # Generate temporal seed config file
        expl_id = expl_config["expl_id"]
        cache_name = expl_config["cache_name"]

        expl_geom_query = f"SELECT ST_Buffer(the_geom, 300) FROM {schema_tiled}.{db_schema}_t_exploitation WHERE expl_id='{expl_id}'"

        coverage_where_str = ""
        if mode == "update":
            cursor.execute((f'REFRESH MATERIALIZED VIEW {schema_tiled}.{db_schema}_t_node'))
            conn.commit()
            print("Node materialized view refreshed----------")
            coverage_where_list = []
            for table in update_tables:
                print("TABLE: ", table)
                coverage_where_list.append((
                    f"SELECT ST_Buffer(the_geom, 1) "
                    f"FROM {schema_tiled}.{table} "
                    f"WHERE lastupdate > '{str(last_seed)}'::timestamp AND ST_Intersects(the_geom, ({expl_geom_query}))"
                ))
                print("LAST_SEED: ",(str(last_seed)))
            coverage_where_str = " UNION ".join(coverage_where_list)

        elif mode == "update_elements":
            cursor.execute((f'SELECT refresh_materialized_views()'))
            conn.commit()
            print("Materialized views refreshed----------")
            feature_json = {
                "client": {"device": 4, "infoType": 1, "lang": "ES"},
                "form": {},
                "feature": feature_data,
                "data": {"type": "feature"}
            }
            feature_argument = json.dumps(feature_json)
            sql_query = f'SELECT {db_schema}.gw_fct_getfeatureboundary($${feature_argument}$$)'
            print("SQL: ", sql_query)
            cursor.execute(sql_query)
            result = cursor.fetchone()
            print("RESULT 0", result[0])
            geo_json = result[0]

            with open(geojson_file_path, 'w') as f:
                json.dump(geo_json, f, ensure_ascii=False)

        elif mode == "update_timestamp":
            cursor.execute((f'SELECT refresh_materialized_views()'))
            conn.commit()
            print("Materialized views refreshed----------")
            tables = [table.split('_')[-1] for table in update_tables]
            feature_json = {
                "client": {"device": 4, "infoType": 1, "lang": "ES"},
                "form": {},
                "feature": {"update_tables": tables},
                "data": {"type": "time", "lastSeed": f"{str(last_seed)}"}
            }
            feature_argument = json.dumps(feature_json)
            sql_query = f'SELECT {db_schema}.gw_fct_getfeatureboundary($${feature_argument}$$)'
            print("SQL: ", sql_query)
            cursor.execute(sql_query)
            result = cursor.fetchone()
            print("RESULT 0", result[0])
            geo_json = result[0]

            with open(geojson_file_path, 'w') as f:
                json.dump(geo_json, f, ensure_ascii=False)
        else:
            coverage_where_str = expl_geom_query

        if mode == "exploitation":
             expl_file_str = (
             'seeds:\n'
             '  seed_prog:\n'
            f'    caches: [{cache_name}]\n'
             '    coverages: [seed_cov]\n'
             '    refresh_before:\n'
             '      minutes: 0\n'
             '    levels:\n'
            f'      to: {max_level}\n'
             'coverages:\n'
             '  seed_cov:\n'
             '    srs: "EPSG:25831"\n'
            f'    datasource: {db_url}\n'
            f'    where: {coverage_where_str}\n'
        )
        else:
            expl_file_str = (
                 'seeds:\n'
                 '  seed_prog:\n'
                f'    caches: [{cache_name}]\n'
                 '    coverages: [seed_cov]\n'
                 '    refresh_before:\n'
                 '      minutes: 0\n'
                 '    levels:\n'
                f'      to: {max_level}\n'
                 'coverages:\n'
                 '  seed_cov:\n'
                 '    srs: "EPSG:25831"\n'
                f'    datasource: /tmp/geometry.geojson\n'
        )

        print(expl_file_str)
        with open(seed_yaml_file_path, 'w') as seed_yaml_file:
            seed_yaml_file.write(expl_file_str)

        # Select only the current exploitation in the selector
        cursor.execute((
            f'SELECT {db_schema}.gw_fct_setselectors($${{'
             '    "client": {"device": 5, "lang": "es_ES", "cur_user": "mapproxy", "tiled": "True", "infoType": 1, "epsg": 25831}, '
             '    "form":{}, '
             '    "feature":{}, '
             '    "data":{"filterFields":{}, "pageInfo":{}, "selectorType": "None", "tabName": "tab_exploitation", "addSchema": "NULL", "checkAll": "False"}'
             '}$$);'
        ))
        cursor.execute((
            f'SELECT {db_schema}.gw_fct_setselectors($${{'
             '    "client":{"device": 5, "lang": "es_ES", "cur_user": "mapproxy", "tiled": "True", "infoType": 1, "epsg": 25831}, '
             '    "form":{}, '
             '    "feature":{}, '
             '    "data":{'
             '        "filterFields":{}, "pageInfo":{}, "selectorType": "selector_basic", "tabName": "tab_exploitation", "addSchema": "NULL", '
            f'        "id": "{expl_id}", '
             '        "isAlone": "False", "disableParent": "False", "value": "True"'
             '    }'
             '}$$);'
        ))
        conn.commit()

        # Execute seed command with the generated config
        os.system(f"mapproxy-seed -f {base_config_path} -s {seed_yaml_file_path} -c 4 --seed seed_prog")

# Remove temporal seed config file
# if os.path.exists(seed_yaml_file_path):
#     os.remove(seed_yaml_file_path)

