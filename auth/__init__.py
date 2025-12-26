"""店舗アプリ向けログイン画面"""
__author__ = "豊原みどり"
__version__ = "1.0.00 20251216"

from flask import Blueprint

auth_bp = Blueprint(
    "auth",
    __name__,
    template_folder="templates",
    static_folder="static"
)

from . import routes
