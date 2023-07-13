from flask import Flask, request, make_response
from qwc_services_core.auth import optional_auth, auth_manager
from qwc_services_core.runtime_config import RuntimeConfig
from qwc_services_core.tenant_handler import TenantHandler

from mapproxy.multiapp import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

import subprocess
import html
import json

app = make_wsgi_app('/srv/qwc_service/mapproxy/config', allow_listing=True, debug=False)

flask_app = Flask(__name__)
tenant_handler = TenantHandler(flask_app.logger)
jwt = auth_manager(flask_app)

@flask_app.route('/update')
@jwt_required()
def seeding():
    tenant = tenant_handler.tenant()
    print("tenant ->", tenant)
    config_handler = RuntimeConfig("giswater", flask_app.logger)
    config = config_handler.tenant_config(tenant)

    args = request.get_json(force=True) if request.is_json else request.args
    theme = args.get("theme")
    
    tile_update_config_file = config.get("themes").get(theme).get("tile_update_config_file")
    print(tile_update_config_file)

    if tile_update_config_file:
        print("Executing...")
        cmd = [
            "python3",
            "/srv/qwc_service/seeding.py",
            f"/srv/qwc_service/mapproxy/config/{tile_update_config_file}",
            "-m", "update"
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        code = 200 if result.returncode == 0 else 500

        stdout = result.stdout.decode()
        stderr = result.stderr.decode()

        print("Finished executing :)")

        response_msg = (
            f"INFO:\n"
            f"{stdout}\n\n"
            f"ERRORS:\n"
            f"{stderr}"
        )
        print(response_msg)

        response = make_response(response_msg, code)
        response.mimetype = "text/plain"
        return response
    else:
        error_str = f"Theme `{theme}` does not have a tiling script setup"
        return error_str, 400

app = DispatcherMiddleware(app, {"/seeding": flask_app})