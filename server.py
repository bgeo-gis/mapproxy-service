"""
Copyright © 2025 by BGEO. All rights reserved.
The program is free software: you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.
"""

from flask import Flask, request, Response
from flask_jwt_extended import jwt_required
from qwc_services_core.auth import auth_manager
from qwc_services_core.runtime_config import RuntimeConfig
from qwc_services_core.tenant_handler import TenantHandler

from mapproxy.multiapp import make_wsgi_app

import yaml
import json
import time
import os
import psycopg2
import datetime
from pathlib import Path

from make_conf import make_config
from seeding import seed, MapZone, MAP_ZONES

user_config_path = '/srv/qwc_service/mapproxy/config/'
generated_config_path = os.path.join(user_config_path, 'config-out')

temp_folder = os.path.join(user_config_path, "temp")
Path(temp_folder).mkdir(parents=True, exist_ok=True)

def get_mapproxy_app():
    return make_wsgi_app(generated_config_path, allow_listing=True, debug=False)

mapproxy_app = get_mapproxy_app()

app = Flask(__name__)
tenant_handler = TenantHandler(app.logger)

# jwt = auth_manager(app)

def get_user_config(config_name: str) -> dict:
    user_config_file = os.path.join(user_config_path, f"{config_name}.yaml")
    if not os.path.exists(user_config_file):
        raise FileNotFoundError(f"User config file {user_config_file} does not exist")

    with open(user_config_file, "r") as f:
        return yaml.safe_load(f)

def create_db_connections(config: dict) -> tuple:
    try:
        local_conn = psycopg2.connect(config["db_url"])
        remote_conn = psycopg2.connect(config["db_url_remote"])
        return local_conn, remote_conn
    except Exception as e:
        raise ConnectionError(f"Could not connect to local database: {e}")

def _set_selectors(config: dict, remote_conn) -> None:
    remote_cursor = remote_conn.cursor()

    # Unselect all selectors
    for mapzone in MAP_ZONES.values():
        remote_cursor.execute(f"DELETE FROM {config['data_db_schema']}.{mapzone.table} WHERE cur_user = current_user;")

    for selector in config.get("selectors", []):
        key, value = next(iter(selector.items()))
        mapzone = MAP_ZONES.get(key)
        if mapzone is None:
            raise ValueError(f"Unknown mapzone_id: {key}")

        print(f"Processing mapzone: {mapzone.tab}, column: {mapzone.column}, value: {value}")

        if value == True:
            query = {
                "client":{"device": 5, "lang": "es_ES", "tiled": "False", "infoType": 1},
                "form":{}, "feature":{},
                "data":{
                    "filterFields":{}, "pageInfo":{}, "selectorType": "selector_basic", "tabName": mapzone.tab, "addSchema": "NULL", "checkAll": "True"
                }
            }
            query_str = f"SELECT {config['data_db_schema']}.gw_fct_setselectors($${json.dumps(query)}$$)"
            print("Executing query:", query_str)
            remote_cursor.execute(query_str)

            result = remote_cursor.fetchone()
            print("Result of setselectors:", result)
        elif isinstance(value, list):
            for item in value:
                query = {
                    "client":{"device": 5, "lang": "es_ES", "tiled": "False", "infoType": 1},
                    "form":{}, "feature":{},
                    "data":{
                        "filterFields":{}, "pageInfo":{}, "selectorType": "selector_basic", "tabName": mapzone.tab, "addSchema": "NULL", "id": str(item), "isAlone": "False", "disableParent": "False", "value": "True"
                    }
                }
                query_str = f"SELECT {config['data_db_schema']}.gw_fct_setselectors($${json.dumps(query)}$$)"
                print("Executing query:", query_str)
                remote_cursor.execute(query_str)
                result = remote_cursor.fetchone()
                print("Result of setselectors:", result)

    remote_conn.commit()

# Refresh the tileclusters materialized view, and check if it has been updated (aka, diferent rows)
def refresh_tileclusters(config: dict, remote_conn, must_be_equal: bool) -> None:
    remote_cursor = remote_conn.cursor()

    # Get the current tileclusters
    remote_cursor.execute(f'SELECT tilecluster_id FROM {config["tileclusters_table"]}')
    current_tileclusters = remote_cursor.fetchall()

    _set_selectors(config, remote_conn)

    # Refresh parent materialized view
    materialized_views = config["materialized_views"]
    for view in materialized_views:
        print("Refreshing materialized view: ", view)
        remote_cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")

    remote_cursor.execute(f"REFRESH MATERIALIZED VIEW {config['tileclusters_table']}")
    remote_conn.commit()

    remote_cursor.execute(f"SELECT tilecluster_id FROM {config['tileclusters_table']}")
    new_tileclusters = remote_cursor.fetchall()

    if must_be_equal and set(current_tileclusters) != set(new_tileclusters):
        raise ValueError("PANIC: Tileclusters have changed after refresh, please check the database")


@app.route('/seeding/refresh_tileclusters')
# @jwt_required()
def refresh_tileclusters_():
    config = request.args.get("config")
    if config is None:
        return Response("Config not provided", 400)

    try:
        user_config = get_user_config(config)
        local_conn, remote_conn = create_db_connections(user_config)

        refresh_tileclusters(user_config, remote_conn, must_be_equal=True)

        return Response(f"Refreshed {config} tileclusters", 200)
    except Exception as e:
        return Response(f"Error refreshing tileclusters: {e}", 500)

@app.route('/seeding/set_selectors')
# @jwt_required()
def set_selectors():
    config = request.args.get("config")
    if config is None:
        return Response("Config not provided", 400)

    try:
        user_config = get_user_config(config)
        local_conn, remote_conn = create_db_connections(user_config)

        _set_selectors(user_config, remote_conn)

        return Response(f"Selectors set for {config}", 200)
    except Exception as e:
        return Response(f"Error setting selectors: {e}", 500)

@app.route('/seeding/seed/all')
# @jwt_required()
def seed_all():
    global mapproxy_app

    file_name = request.args.get("config")
    if file_name is None:
        return Response("Config not provided", 400)

    try:
        start_time = time.perf_counter()

        config = get_user_config(file_name)
        local_conn, remote_conn = create_db_connections(config)
        remote_cursor = remote_conn.cursor()

        # Insert initial seed time into database
        remote_cursor.execute(f"""INSERT INTO {config['tiling_db_schema']}.last_seed_time (id, last_seed)
                                  VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET last_seed = %s""",
                              (file_name, datetime.datetime.now(), datetime.datetime.now()))
        remote_conn.commit()

        refresh_tileclusters(config, remote_conn, must_be_equal=False)
        make_config(config, local_conn, generated_config_path, file_name)
        seed(config, local_conn, remote_conn, generated_config_path, temp_folder, file_name)

        mapproxy_app = get_mapproxy_app()

        return Response(f"Config {file_name} seeded. Time taken: {time.perf_counter() - start_time}", 200)
    except Exception as e:
        return Response(f"Error seeding config: {e}", 500)


@app.route('/seeding/seed/update')
# @jwt_required()
def seed_update_time():
    global mapproxy_app

    file_name = request.args.get("config")
    if file_name is None:
        return Response("Config not provided", 400)

    try:
        start_time = time.perf_counter()
        seed_update_start_time = datetime.datetime.now()

        config = get_user_config(file_name)
        local_conn, remote_conn = create_db_connections(config)
        remote_cursor = remote_conn.cursor()

        # Get last seed time from database
        remote_cursor.execute(f"""SELECT last_seed
                                  FROM {config['tiling_db_schema']}.last_seed_time
                                  WHERE id = '{file_name}'""")
        result = remote_cursor.fetchone()
        if result is None:
            raise ValueError(f"Last seed time does not exist in the db, please do a full seed before updating")
        assert len(result) == 1, "Expected one result from last_seed_time query"
        last_seed_time = result[0]
        print(f"Last seed time:", last_seed_time)

        def make_coverage(tilecluster_id: str, mapzones: list[tuple[MapZone, str]], remote_conn) -> dict | None:
            geojson_file_path = os.path.join(temp_folder, f"{file_name}_geom_{tilecluster_id}.geojson")

            extra_str = " AND ".join(f"{mz.column}='{id}'" for mz, id in mapzones)
            print("Extra string for SQL:", extra_str)

            feature_json = {
                "client": {"device": 4, "infoType": 1, "lang": "ES"},
                "form": {},
                "feature": {"update_tables": config["update_tables"]},
                "data": {"type": "time", "lastSeed": f"{str(last_seed_time)}", "extra": extra_str}
            }
            remote_cursor.execute(f'SELECT {config["data_db_schema"]}.gw_fct_getfeatureboundary($${json.dumps(feature_json)}$$)')
            result = remote_cursor.fetchone()
            if result is None:
                return None

            geojson = result[0]
            if geojson['coordinates']:
                with open(geojson_file_path, 'w') as f:
                    json.dump(geojson, f, ensure_ascii=False)

                # Log the start of the re-tiling process in remote database
                start_time = datetime.datetime.now()
                process_id = f"seed_update_{seed_update_start_time}"
                remote_cursor.execute(
                    f"INSERT INTO {config['tiling_db_schema']}.logs (process_id, tilecluster_id, project_id, start_time, geometry) "
                    f"VALUES (%s, %s, %s, %s, ST_GeomFromGeoJSON(%s))",
                    (process_id, tilecluster_id, file_name, start_time, json.dumps(geojson))
                )
                remote_conn.commit()
            else:
                return None

            return {
                # "clip": True,
                "srs": config["crs"],
                "datasource": geojson_file_path,
            }

        refresh_tileclusters(config, remote_conn, must_be_equal=True)
        seed(
            config,
            local_conn,
            remote_conn,
            generated_config_path,
            temp_folder,
            file_name,
            make_coverage
        )

        # 
        mapproxy_app = get_mapproxy_app()

        remote_cursor.execute(
            f"UPDATE {config['tiling_db_schema']}.last_seed_time "
            f"SET last_seed = %s WHERE id = %s",
            (datetime.datetime.now(), file_name)
        )
        remote_conn.commit()

        return Response(f"Config {file_name} seeded. Time taken: {time.perf_counter() - start_time}", 200)
    except Exception as e:
        return Response(f"Error seeding config: {e}", 500)


@app.route('/seeding/seed/feature')
# @jwt_required()
def seed_feature():
    theme = request.args.get("theme")
    valve_id = request.args.get("valveId")
    if theme is None:
        return Response("Theme is not provided", 400)

    if valve_id is None:
        return Response("Element not provided", 400)

    tenant = tenant_handler.tenant()
    print("tenant ->", tenant)
    config_handler = RuntimeConfig("giswater", app.logger)
    giswater_config = config_handler.tenant_config(tenant)
    config_name: str = giswater_config.get("themes").get(theme).get("tile_config") # type: ignore

    user_config_file = os.path.join(user_config_path, f"{config_name}.yaml")
    with open(user_config_file, "r") as f:
        config: dict = yaml.safe_load(f)

    db_url = config["db_url"]
    db_schema = config["data_db_schema"]

    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    print("Materialized views refreshed----------")
    feature_json = {
        "client": {"device": 4, "infoType": 1, "lang": "ES"},
        "form": {},
        "feature": {
            "node": [int(valve_id)],
        },
        "data": {"type": "feature"}
    }
    feature_argument = json.dumps(feature_json)
    sql_query = f'SELECT {db_schema}.gw_fct_getfeatureboundary($${feature_argument}$$)'

    print("SQL:", sql_query)

    cursor.execute(sql_query)
    result = cursor.fetchone()
    if result is None:
        return Response("Element not found", 404)

    print("RESULT 0", result)
    geo_json = result[0]

    geojson_file_path = os.path.join(user_config_path, f"{config_name}_feature.geojson")
    with open(geojson_file_path, 'w') as f:
        json.dump(geo_json, f, ensure_ascii=False)

    try:
        start_time = time.perf_counter()

        feature = {
            "srs": "EPSG:31982",
            "datasource": geojson_file_path
        }
        seed(user_config_path, generated_config_path, config_name, feature)

        return Response(f"Element {feature} seeded. Time taken: {time.perf_counter() - start_time}", 200)
    except Exception as e:
        print(e)
        return Response(f"Error seeding element: {e}", 500)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
# @jwt_required()
def call_wsgi(path):
    # Convert Flask's request to WSGI environ
    environ = request.environ
    response_body = []

    # Call the WSGI app and collect the response
    def start_response(status, headers, exc_info=None):
        nonlocal response_body
        response_body.append((status, headers))

    result = mapproxy_app(environ, start_response)

    # Extract the status and headers
    status, headers = response_body[0]
    response = Response(result, status=status)

    for header in headers:
        response.headers.add_header(*header)

    return response

# app = DispatcherMiddleware(mapproxy_app, {"/seeding": flask_app})
# app = mapproxy_app

