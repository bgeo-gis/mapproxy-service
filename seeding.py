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
    choices=["full", "update"],
    required=True,
)
args = parser.parse_args()
mode = args.mode
config_file_path = args.config_file

# Get configuration
config = None
with open(config_file_path) as config_file:
    config = json.load(config_file)
    # print(config)

last_seed_file_path = config["last_seed_file_path"]
base_config_path = config["base_config_path"]
db_schema = config["db_schema"]
update_tables = config["update_tables"]
max_level = config["max_level"]
seed_yaml_file_path = '/tmp/temp_seed.yaml'

# Connect to the database
db_url = config["db_url"]
pg_service = db_url.split("=")[-1]

conn = psycopg2.connect(db_url)
cursor = conn.cursor()

# Check last seed time (only when updating)
last_seed = None
if mode == "update":
    if not os.path.exists(last_seed_file_path):
        print(f"Last seed file `{last_seed_file_path}` does not exist, please do a full seed before updating")
        exit(1)
    
    last_seed = datetime.datetime.fromtimestamp(os.path.getmtime(last_seed_file_path))

# Recreate the last seed file to indicate the time of the newest seed
if os.path.exists(last_seed_file_path):
    os.remove(last_seed_file_path)

with open(last_seed_file_path, 'w'):
    pass

# Seed basemap (only when doing a full seed)
if config.get("basemap") and mode == "full":

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
    os.system(f"mapproxy-seed -f {base_config_path} -s {seed_yaml_file_path} --seed seed_prog")

# Seed diferent exploitations separately
if config.get("exploitations"):
    for expl_config in config["exploitations"]:

        # Generate temporal seed config file
        expl_id = expl_config["expl_id"]
        cache_name = expl_config["cache_name"]

        expl_geom_query = f"SELECT ST_Buffer(the_geom, 300) FROM {db_schema}.exploitation WHERE expl_id='{expl_id}'"

        coverage_where_str = ""
        if mode == "update":
            coverage_where_list = []
            for table in update_tables:
                coverage_where_list.append((
                    f"SELECT ST_Buffer(the_geom, 1) "
                    f"FROM {db_schema}.{table} "
                    f"WHERE lastupdate > '{str(last_seed)}'::timestamp AND ST_Intersects(the_geom, ({expl_geom_query}))"
                ))
            coverage_where_str = " UNION ".join(coverage_where_list)
        else:
            coverage_where_str = expl_geom_query
            
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
            f'    datasource: "PG: service={pg_service}"\n'
            f'    where: {coverage_where_str}\n'
        )
        
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
        os.system(f"mapproxy-seed -f {base_config_path} -s {seed_yaml_file_path} --seed seed_prog")

# Remove temporal seed config file
os.remove(seed_yaml_file_path)
