from __future__ import annotations

import os

from flask import Flask, session

from streaming_site import streaming_bp

app = Flask(__name__)

APP_SECRET = os.environ.get("APP_SECRET", "dev-secret-key")
SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "0").lower() in ("1", "true", "yes")
SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")

app.secret_key = APP_SECRET
app.config.update(
    SESSION_COOKIE_SECURE=SESSION_COOKIE_SECURE,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=SESSION_COOKIE_SAMESITE,
)

app.register_blueprint(streaming_bp)


# -----------------------------
# CSRF helpers
# -----------------------------


def generate_csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = os.urandom(32).hex()
        session["csrf_token"] = token
    return token


@app.context_processor
def inject_csrf():
    return {"csrf_token": generate_csrf_token()}


@app.before_request
def before_any_request():
    generate_csrf_token()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
