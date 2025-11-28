from flask import Blueprint, render_template, request, redirect, url_for, session, Response

# ロジックのインポート
from common.hacfl_db_logic import (
    parse_and_insert_work, 
    get_work_data_checked, 
    migrate_work_to_main
)

hacfl_bp = Blueprint(
    'hacfl', 
    __name__, 
    template_folder='templates'
)

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
# 2. アップロード画面 (TOP)
# ---------------------------------------------------
@hacfl_bp.route('/', methods=['GET', 'POST'])
def index():
    msg = ""
    error = ""
    
    # 画面を開いたときはセッション(前の情報)をクリアしておく
    if request.method == 'GET':
        session.pop('hacfl_batch_id', None)

    if request.method == 'POST':
        # ファイルチェック
        if 'csv_file' not in request.files:
            error = "ファイルが送信されていません。"
        else:
            file = request.files['csv_file']
            
            # ロジック呼び出し (CSV読込 -> ワーク登録)
            success, message, batch_id = parse_and_insert_work(file)
            
            if success:
                # 成功したら batch_id をセッションに保存して確認画面へ
                session['hacfl_batch_id'] = batch_id
                return redirect(url_for('hacfl.confirm'))
            else:
                # 失敗 (サイズオーバー、拡張子エラーなど)
                error = message

    return render_template('hacfl/index.html', msg=msg, error=error)


# ---------------------------------------------------
# 3. 確認画面 (CSVの中身とエラーを表示)
# ---------------------------------------------------
@hacfl_bp.route('/confirm', methods=['GET', 'POST'])
def confirm():
    batch_id = session.get('hacfl_batch_id')
    if not batch_id:
        return redirect(url_for('hacfl.index'))

    msg = ""
    error = ""

    # --- 登録ボタンが押された場合 (POST) ---
    if request.method == 'POST':
        # ★変更: 戻り値で count も受け取る
        success, message, count = migrate_work_to_main(batch_id)
        
        if success:
            session.pop('hacfl_batch_id', None)
            
            # ★追加: 完了画面で表示するために件数をセッションに一時保存
            session['hacfl_reg_count'] = count
            
            return redirect(url_for('hacfl.complete'))
        else:
            error = message

    # (GET時の処理はそのまま)
    has_error, data_list = get_work_data_checked(batch_id)
    
    return render_template(
        'hacfl/confirm.html', 
        data_list=data_list, 
        has_error=has_error,
        error_msg=error
    )



# ---------------------------------------------------
# 4. 完了画面
# ---------------------------------------------------
@hacfl_bp.route('/complete')
def complete():
    # ★追加: セッションから件数を取得（取得後は削除してゴミを残さない）
    count = session.pop('hacfl_reg_count', 0)
    
    return render_template('hacfl/complete.html', count=count)
# hacfl/__init__.py の末尾に追加
