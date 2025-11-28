from flask import Blueprint

cart_result_bp = Blueprint(
    "cart_result", __name__, 
    template_folder="templates",
    static_folder="static",
    static_url_path="/cart_result/static"
    )

from . import app
