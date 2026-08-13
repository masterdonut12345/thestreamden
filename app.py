from __future__ import annotations

import os
from flask import Flask, send_from_directory

from streaming_site import streaming_bp
from stream_api import api_bp

app = Flask(__name__)

app.secret_key = os.environ.get("APP_SECRET", "dev-secret-key")

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Strict",
    SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "0") == "1",
    PERMANENT_SESSION_LIFETIME=60 * 60 * 8,
)

app.register_blueprint(streaming_bp)
app.register_blueprint(api_bp)


@app.route("/robots.txt")
def robots_txt():
    return send_from_directory(app.static_folder, "robots.txt")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), threaded=True)
