import os
import importlib
from flask import Blueprint

# 1. 自分自身の情報を定義
__author__ = "Fujiname"
__version__ = "1.0.0"
__description__ = "モジュールのバージョン一覧を表示するツール"

# 2. ブループリント作成
tools_bp = Blueprint('tools', __name__)

@tools_bp.route('/modules')
def module_list():
    # 無視するフォルダ (自分自身 'tools' も無視リストに入れます)
    IGNORE_DIRS = ['venv', 'env', '__pycache__', '.git', '.vs', 'static', 'templates', 'common', 'tools']
    
    # 自分のいるフォルダ(tools)の「一つ上」を見に行く
    current_dir = os.path.dirname(__file__)
    base_dir = os.path.dirname(current_dir) # これで flask_apps フォルダになる
    
    all_items = os.listdir(base_dir)
    modules_info = []

    for item in all_items:
        # フォルダチェック & 無視リストチェック
        item_path = os.path.join(base_dir, item)
        if item in IGNORE_DIRS or not os.path.isdir(item_path):
            continue
        
        # __init__.py があるかチェック
        if not os.path.exists(os.path.join(item_path, '__init__.py')):
            continue

        try:
            # モジュールを動的に読み込む
            mod = importlib.import_module(item)
            
            info = {
                "name": item,
                "author": getattr(mod, '__author__', '未記入'),
                "version": getattr(mod, '__version__', '-'),
                "desc": getattr(mod, '__doc__', '説明なし')
            }
            modules_info.append(info)
        except Exception as e:
            modules_info.append({"name": item, "author": "エラー", "version": str(e), "desc": ""})

    # 表示用HTML
    html = "<h1>インストール済みモジュール一覧</h1>"
    html += "<p>現在稼働中のサブシステム情報です。</p>"
    html += "<table border='1' cellpadding='5' style='border-collapse:collapse; width:80%;'>"
    html += "<tr style='background:#eee'><th>フォルダ名</th><th>バージョン</th><th>担当者</th><th>説明</th></tr>"
    
    for m in modules_info:
        html += f"<tr><td><b>{m['name']}</b></td><td>{m['version']}</td><td>{m['author']}</td><td>{m['desc']}</td></tr>"
    
    html += "</table>"
    return html