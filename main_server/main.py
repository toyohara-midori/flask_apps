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

class PrefixMiddleware(object):
    #　以下は本番用？
    # def __init__(self, app, prefix=''):
    #     self.app = app
    #     self.prefix = prefix

    # def __call__(self, environ, start_response):
    #     # Flaskに「私のルートURLは /flask です」と強制的に教える
    #     environ['SCRIPT_NAME'] = self.prefix
    #     return self.app(environ, start_response)

    # 以下はデバッグ用？
    def __init__(self, app, prefix=''):
        self.app = app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        # 1. リンク生成用： Flaskに「私のルートURLは /flask です」と教える
        environ['SCRIPT_NAME'] = self.prefix
        
        # 2. 【進化ポイント】 IISのマネをする機能
        # もしリクエストの先頭に /flask が付いていたら、それを剥ぎ取る！
        path_info = environ.get('PATH_INFO', '')
        if path_info.startswith(self.prefix):
            environ['PATH_INFO'] = path_info[len(self.prefix):]

        return self.app(environ, start_response)

# メインFlaskサーバ
app = Flask(__name__)

#セッションキー作成
app.secret_key = "secret_key_12345"

app.wsgi_app = PrefixMiddleware(app.wsgi_app, prefix='/flask')

# Blueprint登録（URLプレフィックスごとに分ける）
app.register_blueprint(autosupply_bp, url_prefix='/autosupply_web')
app.register_blueprint(cart_bp, url_prefix="/cart_stay_register")
app.register_blueprint(cart_result_bp, url_prefix="/cart_result")

app.register_blueprint(hacfl_bp, url_prefix="/hacfl")

# DBG
@app.route("/__debug_static_main__")
def debug_static_main():
    from flask import jsonify
    return jsonify({
        "app_static_folder": app.static_folder,
        "app_static_url_path": app.static_url_path
    })

@app.route('/')
def root_check():
    return "<h1>Flask Server is Alive!</h1>"

# ----------------------------------------
if __name__ == "__main__":
    #app.run(host="0.0.0.0", port=5000, debug=True)
    # use_reloader=False を追加しました
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
