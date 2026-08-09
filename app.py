from __future__ import annotations

import os
from flask import Flask, send_from_directory

from streaming_site import streaming_bp

app = Flask(__name__)

app.secret_key = os.environ.get("APP_SECRET", "dev-secret-key")

app.register_blueprint(streaming_bp)


@app.route("/robots.txt")
def robots_txt():
    return send_from_directory(app.static_folder, "robots.txt")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), threaded=True)
