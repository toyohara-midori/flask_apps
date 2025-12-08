from flask import Blueprint, render_template, request, redirect, url_for, session, Response, jsonify, render_template_string
from common.db_check_util import is_db_available, MAINTENANCE_HTML

# ロジックのインポート
from common.hacfl_db_logic import (
    parse_and_insert_work, 
    get_work_data_checked, 
    migrate_work_to_main,
    insert_single_record,
    get_store_name_by_cd,
    get_product_info_by_cd,
    check_time_and_get_config
)

hacfl_bp = Blueprint(
    'hacfl', 
    __name__, 
    template_folder='templates'
)

# ---------------------------------------------------
# ★共通処理: リクエストのたびにDBチェック
# ---------------------------------------------------
@hacfl_bp.before_request
def check_db_status():
    if not request.endpoint:
        return
    
    # 静的ファイルは除外 (CSSなどが読み込めなくなるのを防ぐ)
    if 'static' in request.endpoint:
        return

    # DBチェック実行
    if not is_db_available():
        # ★ファイルを作らず、文字列をそのままHTMLとして返す
        # 503 (Service Unavailable) のステータスコードも一緒に返すと親切です
        return render_template_string(MAINTENANCE_HTML), 503


# ---------------------------------------------------
# 1. テンプレートCSVダウンロード機能
# ---------------------------------------------------
@hacfl_bp.route('/download_template')
def download_template():
    # 5項目用のCSVデータ (ヘッダー + サンプル1行)
    csv_data = [
        "店舗CD,商品CD,発注数,発注日(任意),納品日(任意)",
        "111,12345678,10,,"
    ]
    # 改行コードで結合
    csv_string = "\r\n".join(csv_data)

    # レスポンス作成 (Excel用にShift-JISでエンコード)
    response = Response(
        csv_string.encode("cp932"), 
        mimetype="text/csv"
    )
    # ファイル名指定
    response.headers["Content-Disposition"] = "attachment; filename=hacfl_template.csv"
    
    return response


# ---------------------------------------------------
# 2. トップ画面 (単発登録 & CSVアップロード)
# ---------------------------------------------------
@hacfl_bp.route('/', methods=['GET', 'POST'])
def index():
    msg = ""
    error = ""
    
    # セッションクリア (初期化)
    if request.method == 'GET':
        session.pop('hacfl_batch_id', None)
        session.pop('hacfl_mode', None)

    if request.method == 'POST':
        # フォームからモードとアクションタイプを取得
        mode = request.form.get('mode') # normal or morning
        action_type = request.form.get('action_type') # single or csv
        
        # モードをセッションに保存 (Confirm画面で使うため)
        session['hacfl_mode'] = mode

        # --- A. 単発登録の場合 ---
        if action_type == 'single':
            form_data = {
                'cucd': request.form.get('cucd'),
                'cocd': request.form.get('cocd'),
                'odsu': request.form.get('odsu'),
                'oddt': request.form.get('oddt'),
                'dldt': request.form.get('dldt')
            }
            success, message = insert_single_record(mode, form_data)
            if success:
                msg = message
            else:
                error = message
        
        # --- B. CSVアップロードの場合 ---
        elif action_type == 'csv':
            if 'csv_file' not in request.files:
                error = "ファイルが送信されていません。"
            else:
                file = request.files['csv_file']
                # 引数に mode を追加
                success, message, batch_id = parse_and_insert_work(file, mode)
                
                if success:
                    session['hacfl_batch_id'] = batch_id
                    return redirect(url_for('hacfl.confirm'))
                else:
                    error = message

    return render_template('hacfl/index.html', msg=msg, error=error)

# ===================================================
#  非同期通信用API (JavaScriptから呼ばれる)
# ===================================================
@hacfl_bp.route('/api/get_store_name')
def api_get_store_name():
    cucd = request.args.get('cucd')
    name = get_store_name_by_cd(cucd)
    if name:
        return jsonify({'found': True, 'name': name})
    else:
        return jsonify({'found': False})

@hacfl_bp.route('/api/get_product_info')
def api_get_product_info():
    cocd = request.args.get('cocd')
    info = get_product_info_by_cd(cocd)
    if info:
        return jsonify({'found': True, 'info': info})
    else:
        return jsonify({'found': False})

# ---------------------------------------------------
# 3. 確認画面 (CSVの中身とエラーを表示)
# ---------------------------------------------------
@hacfl_bp.route('/confirm', methods=['GET', 'POST'])
def confirm():
    batch_id = session.get('hacfl_batch_id')
    mode = session.get('hacfl_mode') # モードも取得
    
    if not batch_id or not mode:
        return redirect(url_for('hacfl.index'))

    msg = ""
    error = ""

    # ★追加: モード設定を取得して、画面表示用の名前(例: "当日朝締め")を取り出す
    _, _, config = check_time_and_get_config(mode)
    mode_name = config['name'] if config else "不明なモード"

    # --- 登録ボタンが押された場合 (POST) ---
    if request.method == 'POST':
        # 引数に mode を追加 (テーブル振分けのため)
        success, message, count = migrate_work_to_main(batch_id, mode)
        
        if success:
            session.pop('hacfl_batch_id', None)
            # 件数をセッションに一時保存
            session['hacfl_reg_count'] = count
            return redirect(url_for('hacfl.complete'))
        else:
            error = message

    # --- 画面表示 (GET) ---
    # ★変更: 第2引数に mode を渡す
    has_error, data_list = get_work_data_checked(batch_id, mode)
    return render_template(
        'hacfl/confirm.html', 
        data_list=data_list, 
        has_error=has_error, 
        error_msg=error,
        mode_name=mode_name  # ★追加: 画面にモード名を渡す
    )


# ---------------------------------------------------
# 4. 完了画面
# ---------------------------------------------------
@hacfl_bp.route('/complete')
def complete():
    # セッションから件数を取得して表示
    count = session.pop('hacfl_reg_count', 0)
    return render_template('hacfl/complete.html', count=count)