import sys, os
# C:\flask_apps をPythonのモジュールパスに追加(commonの関数を使用するため)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # C:\flask_apps
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from flask import Flask

# サブアプリをインポート(Blueprint を読み込む)
from autosupply_web import autosupply_bp  # autosupply_web/__init__.py
from cart_stay_register import cart_bp  # cart_stay_register/__init__.py
from cart_result import cart_result_bp  # cart_result/__init__.py

from hacfl import hacfl_bp

# メインFlaskサーバ
app = Flask(__name__)

#セッションキー作成
app.secret_key = "secret_key_12345"

# Blueprint登録（URLプレフィックスごとに分ける）
app.register_blueprint(autosupply_bp, url_prefix='/flask/autosupply_web')
app.register_blueprint(cart_bp, url_prefix="/flask/cart_stay_register")
app.register_blueprint(cart_result_bp, url_prefix="/flask/cart_result")

app.register_blueprint(hacfl_bp, url_prefix="/hacfl")

# DBG
@app.route("/__debug_static_main__")
def debug_static_main():
    from flask import jsonify
    return jsonify({
        "app_static_folder": app.static_folder,
        "app_static_url_path": app.static_url_path
    })

if __name__ == "__main__":
    #app.run(host="0.0.0.0", port=5000, debug=True)
    # use_reloader=False を追加しました
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
