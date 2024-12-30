"""
Copyright © 2023 by BGEO. All rights reserved.
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

from make_conf import make_config
from seeding_v2 import seed

user_config_path = '/srv/qwc_service/mapproxy/config/'
generated_config_path = os.path.join(user_config_path, 'config-out')

mapproxy_app = make_wsgi_app(generated_config_path, allow_listing=True, debug=False)

app = Flask(__name__)
tenant_handler = TenantHandler(app.logger)
jwt = auth_manager(app)

@app.route('/seeding/generate_config')
@jwt_required()
def generate_config():
    config = request.args.get("config")
    if config is None:
        return Response("Config not provided", 400)

    try:
        make_config(user_config_path, generated_config_path, config)
        return Response(f"Config {config} generated", 200)
    except Exception as e:
        return Response(f"Error generating config: {e}", 500)

@app.route('/seeding/seed/all')
@jwt_required()
def seed_all():
    file_name = request.args.get("config")
    if file_name is None:
        return Response("Config not provided", 400)

    try:
        start_time = time.perf_counter()

        seed(user_config_path, generated_config_path, file_name)

        return Response(f"Config {file_name} seeded. Time taken: {time.perf_counter() - start_time}", 200)
    except Exception as e:
        return Response(f"Error seeding config: {e}", 500)

@app.route('/seeding/seed/feature')
@jwt_required()
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
            "srs": "EPSG:25831",
            "datasource": geojson_file_path
        }
        seed(user_config_path, generated_config_path, config_name, feature)

        return Response(f"Element {feature} seeded. Time taken: {time.perf_counter() - start_time}", 200)
    except Exception as e:
        print(e)
        return Response(f"Error seeding element: {e}", 500)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
@jwt_required()
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

