from flask import Blueprint, Flask, request, jsonify, render_template, send_from_directory
import pyodbc
import datetime
import os

# Blueprint定義
app = Blueprint('flyer_app', __name__, template_folder='templates', static_folder='static')

# =====================================
# 手動CORS設定（flask_corsなし）
# =====================================
@app.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response

# =====================================
# SQL Server接続設定
# =====================================
DB_CONFIG = {
    "server": "SQLS08-14",
    "database": "JSNDWH-b",
    "username": "sqlsadmin",
    "password": "Jason3080",  # ← 実際のパスワードを入れる
}

def get_connection():
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)

# =====================================
# ルート: チラシエディタ画面表示
# =====================================
@app.route('/')
def index():
    return render_template('flyer_editor.html')


# =====================================
# DB接続テスト
# =====================================
@app.route('/test_db')
def test_db():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 flyer_id, prtitle FROM DBA.flyer_header")
        row = cursor.fetchone()
        conn.close()
        if row:
            return f"✅ DB接続OK! サンプルデータ: {row.prtitle}"
        else:
            return "✅ DB接続OK! でもデータはまだない。"
    except Exception as e:
        return f"❌ 接続エラー: {e}"


# =====================================
# チラシヘッダ登録API
# =====================================
@app.route('/add_flyer', methods=['POST'])
def add_flyer():
    data = request.get_json()
    title = data.get('prtitle')
    start = data.get('prdt_s')
    end = data.get('prdt_e')
    user = data.get('upnm', 'system')

    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """
        INSERT INTO DBA.flyer_header (prtitle, prdt_s, prdt_e, rgdt, updt, upnm)
        VALUES (?, ?, ?, GETDATE(), GETDATE(), ?)
        """
        cursor.execute(sql, (title, start, end, user))
        conn.commit()
        return jsonify({"status": "ok", "message": "flyer_header 登録完了"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
    finally:
        conn.close()


# =====================================
# チラシ明細登録API
# =====================================
@app.route('/add_item', methods=['POST'])
def add_item():
    data = request.get_json()

    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """
        INSERT INTO DBA.flyer_item (
            flyer_id, page_no, mnam_k, hnam_k, kika_k, retn, retn_intax,
            each_flag, row_pos, col_pos, rowspan, colspan, image_path,
            remarks, disp_flag, del_flag, upnm, rgdt, updt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
        """
        cursor.execute(sql, (
            data.get('flyer_id'),
            data.get('page_no', 1),
            data.get('mnam_k'),
            data.get('hnam_k'),
            data.get('kika_k'),
            data.get('retn'),
            data.get('retn_intax'),
            data.get('each_flag', 0),
            data.get('row_pos'),
            data.get('col_pos'),
            data.get('rowspan', 1),
            data.get('colspan', 1),
            data.get('image_path'),
            data.get('remarks'),
            data.get('disp_flag', 1),
            data.get('del_flag', 0),
            data.get('upnm', 'system')
        ))
        conn.commit()
        return jsonify({"status": "ok", "message": "flyer_item 登録完了"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
    finally:
        conn.close()


# =====================================
# チラシ一覧取得API
# =====================================
@app.route('/flyer_list')
def flyer_list():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT flyer_id, prtitle, prdt_s, prdt_e FROM DBA.flyer_header ORDER BY flyer_id DESC")
    rows = cursor.fetchall()
    conn.close()
    result = [
        {"flyer_id": r.flyer_id, "title": r.prtitle, "start": str(r.prdt_s), "end": str(r.prdt_e)}
        for r in rows
    ]
    return jsonify(result)


# =====================================
# 画像フォルダの公開
# =====================================
@app.route('/img/<path:filename>')
def serve_image(filename):
    base_path = r'C:\flask_apps\flyer_web\img'
    return send_from_directory(base_path, filename)


if __name__ == "__main__":
    from flask import Flask
    demo_app = Flask(__name__)
    demo_app.register_blueprint(app, url_prefix="/")
    demo_app.run(host="0.0.0.0", port=5000, debug=True)

