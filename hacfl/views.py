from flask import render_template, request, redirect, url_for, session, Response, jsonify, flash
from common.auth_util import get_remote_user
from common.logger import write_log

# Blueprint本体をインポート (名前はプロジェクトに合わせて調整してください)
from . import hacfl_bp

# DBロジックのインポート
from common.hacfl_db_logic import (
    parse_and_insert_work,      # 作業テーブルへの取込
    get_work_data_checked,      # 確認画面用データ取得
    migrate_work_to_main,       # 本番テーブルへの反映
    insert_single_record,       # 単発登録
    check_time_and_get_config,  # モード・時間チェック
    get_store_name_by_cd,       # (API用) 店舗名取得
    get_product_info_by_cd,     # (API用) 商品情報取得
    MODE_CONFIG                 # モード定義辞書
)

# ---------------------------------------------------
# 1. テンプレートCSVダウンロード機能
# ---------------------------------------------------
@hacfl_bp.route('/download_template')
def download_template():
    # ★修正: ヘッダー行を削除し、データのみにする
    # ユーザーが列の意味を忘れないよう、HTML側に「列順序」の説明があるためこれでOKです
    csv_data = [
        "111,12345678,10"
    ]
    csv_string = "\r\n".join(csv_data)

    response = Response(
        csv_string.encode("cp932"), 
        mimetype="text/csv"
    )
    # ファイル名も template より sample の方が実態に近いかもしれません
    response.headers["Content-Disposition"] = "attachment; filename=hacfl_sample.csv"
    return response
# ---------------------------------------------------
# 2. トップ画面 (単発登録 & CSVアップロード)
# ---------------------------------------------------
@hacfl_bp.route('/', methods=['GET', 'POST'])
def index():
    # ★ここでの初期化が重要です
    msg = ""
    error = ""
    current_mode = None

    # 認証済みユーザーを取得 (common.auth_util)
    current_user = get_remote_user(request)

    # --- GET時の処理: セッション初期化 ---
    if request.method == 'GET':
        # BatchIDは消してOK（CSVの途中経過はリセット）
        session.pop('hacfl_batch_id', None)
        # セッションに前回のモードが残っていればそれを使う
        current_mode = session.get('hacfl_mode')

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
            # ★DBロジックへユーザーIDを渡す
            success, message = insert_single_record(current_mode, form_data, user_id=current_user)
            
            if success:
                write_log(
                    module_name='hacfl',
                    user_id=current_user,
                    action_type='SINGLE_INSERT',
                    message=f"成功: {message}"
                )
                
                flash(message, 'success')  # メッセージを一時保存
                return redirect(url_for('hacfl.index'))  # 自分自身へリダイレクト
            else:
                error = message
        
        # --- B. CSVアップロード ---
        elif action_type == 'csv':
            if 'csv_file' not in request.files:
                error = "ファイルが送信されていません。"
            else:
                file = request.files['csv_file']
                
                # ★追加: 画面入力の日付を取得 (チェックボックスONの場合のみ)
                csv_oddt = request.form.get('csv_oddt') if request.form.get('chk_oddt') else None
                csv_dldt = request.form.get('csv_dldt') if request.form.get('chk_dldt') else None

                # 朝締めモードなら強制的に発注日はNoneにする
                if current_mode == 'morning':
                    csv_oddt = None

                # ★DBロジックへ渡す
                success, message, batch_id = parse_and_insert_work(
                    file, 
                    current_mode, 
                    user_id=current_user,
                    fixed_oddt=csv_oddt,
                    fixed_dldt=csv_dldt
                )
                
                if success:
                    session['hacfl_batch_id'] = batch_id
                    
                    write_log(
                        module_name='hacfl',
                        user_id=current_user,
                        action_type='CSV_UPLOAD',
                        message=f"BatchID: {batch_id} アップロード完了"
                    )
                    return redirect(url_for('hacfl.confirm'))
                else:
                    error = message

    return render_template(
        'hacfl/index.html', 
        msg=msg,   # ここで変数を渡しているため、上で初期化が必要です
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

    # --- 登録実行 (POST) ---
    if request.method == 'POST':
        # ★本登録実行
        success, message, count = migrate_work_to_main(batch_id, mode, user_id=current_user)
        
        if success:
            write_log(
                module_name='hacfl',
                user_id=current_user,
                action_type='CSV_REGIST',
                message=f"一括登録完了: {count}件 (BatchID: {batch_id}, Mode: {mode})"
            )
            
            session.pop('hacfl_batch_id', None)
            session['hacfl_reg_count'] = count
            session['hacfl_reg_mode'] = mode
            return redirect(url_for('hacfl.complete'))
        else:
            error = message

    # --- データ表示 (GET/Error時) ---
    # ★変更: バリデーション関数に mode を渡す
    has_error, data_list = get_work_data_checked(batch_id, mode)
    
    return render_template(
        'hacfl/confirm.html', 
        data_list=data_list, 
        has_error=has_error, 
        error_msg=error,
        mode_name=mode_name,
        mode=mode
    )

# ---------------------------------------------------
# 4. 完了画面
# ---------------------------------------------------
@hacfl_bp.route('/complete')
def complete():
    count = session.pop('hacfl_reg_count', 0)
    mode = session.pop('hacfl_reg_mode', 'normal') 
    
    config = MODE_CONFIG.get(mode)
    mode_name = config['name'] if config else "通常予約"
    
    theme_color = "#dc3545" if mode == 'morning' else "#007bff"

    return render_template(
        'hacfl/complete.html', 
        count=count, 
        mode_name=mode_name,
        theme_color=theme_color
    )