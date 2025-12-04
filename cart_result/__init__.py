"""滞留カゴ車台数実績一覧（店運で使用 Excelダウンロード）"""
__author__ = "豊原みどり"
__vertion__ = "1.0.00 20251204"

from flask import Blueprint

cart_result_bp = Blueprint(
    "cart_result", __name__, 
    template_folder="templates",
    static_folder="static",
    static_url_path="/cart_result/static"
    )

from . import app
