from flask import Blueprint, render_template, request, redirect, url_for
from common.db_check_util import is_db_available, MAINTENANCE_HTML
from common.auth_util import get_remote_user
from common.ad_tool import is_user_in_group
from common.log_util import write_op_log
# ① 住所（どこから？）
from common.hacfl_db_logic import (

    # ② 持ち物リスト（なにを？）
    parse_and_insert_work,      # 作業データを解釈して保存する機能
    get_work_data_checked,      # チェック済みの作業データを取得する機能
    migrate_work_to_main,       # データをメインの場所に移動させる機能
    insert_single_record,       # 1件だけデータを登録する機能
    check_time_and_get_config   # 時間を確認して設定を取得する機能
)

hacfl_bp = Blueprint(
    'hacfl',
    __name__,
    template_folder='templates'
)
# ---------------------------------------------------
# ★共通処理: リクエストごとのチェック (DB & 認証)
# ---------------------------------------------------
@hacfl_bp.before_request
def before_request_handler():
    # 静的ファイル等はチェック対象外
    if not request.endpoint or 'static' in request.endpoint:
        return

    # 1. DB接続チェック
    if not is_db_available():
        return render_template_string(MAINTENANCE_HTML), 503

    # 2. ユーザー特定
    user = get_remote_user(request)
    if not user:
        abort(401)

    # 3. ADグループ認証
    allowed_groups = [
        'Domain Admins',
        'G-商品部ディストリビューター',
        'G-商品部バイヤー'
    ]
    
    has_permission = False
    try:
        for group in allowed_groups:
            if is_user_in_group(user, group):
                has_permission = True
                break
    except Exception as e:
        print(f"[AD Auth Error] {e}")
        abort(403, description="認証サーバーへの接続に失敗しました。")

    if not has_permission:
        print(f"[Access Denied] User: {user}, Module: hacfl")
        abort(403, description="この機能を利用する権限がありません。")


# ---------------------------------------------------
# 1. テンプレートCSVダウンロード機能
# ---------------------------------------------------
@hacfl_bp.route('/download_template')
def download_template():
    csv_data = [
        "店舗CD,商品CD,発注数,発注日(任意),納品日(任意)",
        "111,12345678,10,,"
    ]
    csv_string = "\r\n".join(csv_data)

    response = Response(
        csv_string.encode("cp932"), 
        mimetype="text/csv"
    )
    response.headers["Content-Disposition"] = "attachment; filename=hacfl_template.csv"
    
    return response


# ---------------------------------------------------
# 2. トップ画面 (単発登録 & CSVアップロード)
# ---------------------------------------------------
@hacfl_bp.route('/', methods=['GET', 'POST'])
def index():
    msg = ""
    error = ""
    current_mode = None  # 初期状態は未選択
    
    current_user = get_remote_user(request)

    # --- GET時の処理 ---
    if request.method == 'GET':
        session.pop('hacfl_batch_id', None)
        session.pop('hacfl_mode', None)

    # --- POST時の処理 ---
    if request.method == 'POST':
        current_mode = request.form.get('mode')
        action_type = request.form.get('action_type')
        
        session['hacfl_mode'] = current_mode

        # --- A. 単発登録 ---
        if action_type == 'single':
            form_data = {
                'cucd': request.form.get('cucd'),
                'cocd': request.form.get('cocd'),
                'odsu': request.form.get('odsu'),
                'oddt': request.form.get('oddt'),
                'dldt': request.form.get('dldt')
            }
            success, message = insert_single_record(current_mode, form_data)
            
            if success:
                msg = message
                write_op_log(
                    user_id=current_user,
                    module='hacfl',
                    action='SINGLE_INSERT',
                    msg=f"成功: {message}"
                )
            else:
                error = message
        
        # --- B. CSVアップロード ---
        elif action_type == 'csv':
            if 'csv_file' not in request.files:
                error = "ファイルが送信されていません。"
            else:
                file = request.files['csv_file']
                success, message, batch_id = parse_and_insert_work(file, current_mode)
                
                if success:
                    session['hacfl_batch_id'] = batch_id
                    
                    write_op_log(
                        user_id=current_user,
                        module='hacfl',
                        action='CSV_UPLOAD',
                        msg=f"BatchID: {batch_id} アップロード完了"
                    )
                    
                    return redirect(url_for('hacfl.confirm'))
                else:
                    error = message

    return render_template(
        'hacfl/index.html', 
        msg=msg, 
        error=error,
        mode=current_mode,
        current_user=current_user
    )


# ===================================================
#  非同期通信用API
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

@hacfl_bp.route('/api/check_mode_time')
def api_check_mode_time():
    mode = request.args.get('mode')
    is_ok, message, _ = check_time_and_get_config(mode)
    return jsonify({
        'valid': is_ok,
        'message': message
    })


# ---------------------------------------------------
# 3. 確認画面
# ---------------------------------------------------
@hacfl_bp.route('/confirm', methods=['GET', 'POST'])
def confirm():
    batch_id = session.get('hacfl_batch_id')
    mode = session.get('hacfl_mode')
    current_user = get_remote_user(request)
    
    if not batch_id or not mode:
        return redirect(url_for('hacfl.index'))

    msg = ""
    error = ""

    # モード名取得
    _, _, config = check_time_and_get_config(mode)
    mode_name = config['name'] if config else "不明なモード"

    if request.method == 'POST':
        success, message, count = migrate_work_to_main(batch_id, mode)
        
        if success:
            write_op_log(
                user_id=current_user,
                module='hacfl',
                action='CSV_REGIST',
                msg=f"一括登録完了: {count}件 (BatchID: {batch_id}, Mode: {mode})"
            )
            
            session.pop('hacfl_batch_id', None)
            session['hacfl_reg_count'] = count
            session['hacfl_reg_mode'] = mode  # ★モードを保存
            return redirect(url_for('hacfl.complete'))
        else:
            error = message

    has_error, data_list = get_work_data_checked(batch_id, mode)
    
    return render_template(
        'hacfl/confirm.html', 
        data_list=data_list, 
        has_error=has_error, 
        error_msg=error,
        mode_name=mode_name,
        mode=mode
    )  # ★ここのカッコ閉じが抜けていた可能性が高いです


# ---------------------------------------------------
# 4. 完了画面
# ---------------------------------------------------
@hacfl_bp.route('/complete')
def complete():
    # セッションから件数とモードを取得
    count = session.pop('hacfl_reg_count', 0)
    mode = session.pop('hacfl_reg_mode', 'normal') 
    
    # モード設定を取得
    config = MODE_CONFIG.get(mode)
    mode_name = config['name'] if config else "通常予約"
    
    # 色を決定
    theme_color = "#dc3545" if mode == 'morning' else "#007bff"

    return render_template(
        'hacfl/complete.html', 
        count=count, 
        mode_name=mode_name,
        theme_color=theme_color
    )