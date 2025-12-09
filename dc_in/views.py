from flask import render_template, request, redirect, url_for, make_response
import datetime
import io
import csv

from . import dc_in_bp as bp
from common import dc_in_db_logic as db_logic

# ==========================================
# ルート定義
# ==========================================

@bp.route('/')
def home():
    return render_template('dc_in/index.html')

@bp.route('/confirm', methods=['POST'])
def show_confirmation():
    return render_template('dc_in/confirm.html', headers=[], center_groups={}, global_summary=[])

@bp.route('/complete_insertion', methods=['POST'])
def complete_insertion():
    now = datetime.datetime.now()
    new_import_id = f"{now.strftime('%Y%m%d-%H%M')}-demo"
    return render_template('dc_in/complete.html', new_import_id=new_import_id)

@bp.route('/voucher_list', methods=['GET', 'POST'])
def voucher_list():
    if request.method == 'POST':
        # ... (POST処理省略) ...
        pass

    filters = {
        'import_id': request.args.get('import_id'),
        'center': request.args.get('center'),
        'dept': request.args.get('dept'),
        'vendor': request.args.get('vendor'),
        'delivery_date': request.args.get('delivery_date'),
        'type': request.args.get('type'),
        'sort': request.args.get('sort', 'voucher_id'),
        'order': request.args.get('order', 'asc')
    }

    if not filters['delivery_date'] and not filters['import_id']:
        filters['delivery_date'] = datetime.date.today().strftime('%Y/%m/%d')

    # ★画面用: 1行目だけ取得 (is_export=False)
    vouchers = db_logic.get_voucher_list(filters, is_export=False)
    opts = db_logic.get_filter_options()

    return render_template(
        'dc_in/voucher_list.html', 
        vouchers=vouchers, 
        import_ids=opts['import_ids'],
        centers=opts['centers'], depts=opts['depts'], vendors=opts['vendors'], 
        current_filters=filters,
        default_date=filters['delivery_date']
    )

@bp.route('/download_csv', methods=['GET', 'POST'])
def download_csv():
    filters = {}
    
    # ★追加：チェックボックスで選ばれた場合 (POST)
    if request.method == 'POST':
        selected_ids = request.form.getlist('v_ids')
        if not selected_ids:
            return "出力対象が選択されていません。", 400
        filters['voucher_ids'] = selected_ids
    
    # 検索条件の場合 (GET) - 念のため残しておく
    else:
        filters = {
            'import_id': request.args.get('import_id'),
            'center': request.args.get('center'),
            'dept': request.args.get('dept'),
            'vendor': request.args.get('vendor'),
            'delivery_date': request.args.get('delivery_date'),
            'type': request.args.get('type'),
            'sort': request.args.get('sort', 'voucher_id'),
            'order': request.args.get('order', 'asc')
        }
        if not filters['delivery_date'] and not filters['import_id']:
            filters['delivery_date'] = datetime.date.today().strftime('%Y/%m/%d')

    # DBロジックは前回直したので、'voucher_ids' があれば勝手に絞り込んでくれます
    vouchers_list = db_logic.get_voucher_list(filters, is_export=True)

    si = io.StringIO()
    cw = csv.writer(si)
    
    # ヘッダー (変更なし)
    header = [
        '取込ID', '伝票番号', '行', 
        'センターCD', '部門CD', '部門名',
        '取引CD', 'ベンダーCD', '取引先名',
        '商品CD', '商品名', 'メーカー',
        '発注日', '納品日', 
        '発注数量', '原価', '総値引', 
        '手数料(md)', '手数料(dc)', 
        '通過FLG', '伝票FLG', 
        '登録者', '登録日', '更新日', '更新時間'
    ]
    cw.writerow(header)

    for row in vouchers_list:
        cw.writerow([
            row.get('import_id', ''),
            row.get('voucher_id', ''),
            row.get('line_no', ''),
            row.get('center', ''),      # センター(日本語)
            row.get('dept_code', ''),
            row.get('dept_name', ''),
            row.get('trans_code', ''),
            row.get('vendor_code', ''),
            row.get('vendor', ''),
            row.get('item_code', ''),
            row.get('first_p_name', ''),
            row.get('manufacturer', ''),
            row.get('order_date', ''),
            row.get('delivery_date', ''),
            row.get('order_qty', 0),
            row.get('cost_price', 0),
            row.get('total_disc', 0),
            row.get('fee_md', 0),
            row.get('fee_dc', 0),
            row.get('pass_flag', ''),
            row.get('conf_flag', ''),
            row.get('operator', ''),
            row.get('reg_date', ''),
            row.get('update_date', ''),
            row.get('update_time', '')
        ])

    output = make_response(si.getvalue().encode('cp932', 'ignore'))
    output.headers["Content-Disposition"] = "attachment; filename=dc_voucher_list.csv"
    output.headers["Content-type"] = "text/csv"
    return output
# methods=['GET', 'POST'] を必ず追加してください
@bp.route('/print_list', methods=['GET', 'POST'])
def print_list():
    filters = {}
    
    # ★追加：POST（チェックボックス選択）の場合
    if request.method == 'POST':
        # 画面のチェックボックス(name="v_ids")の値を取得
        selected_ids = request.form.getlist('v_ids')
        if not selected_ids:
            return "出力対象が選択されていません。", 400
        # フィルタ条件にIDリストをセット
        filters['voucher_ids'] = selected_ids
        
    else:
        # GET（全件表示など）の場合は既存のまま
        filters = {
            'import_id': request.args.get('import_id'),
            'center': request.args.get('center'),
            'dept': request.args.get('dept'),
            'vendor': request.args.get('vendor'),
            'delivery_date': request.args.get('delivery_date'),
            'type': request.args.get('type'),
            'sort': request.args.get('sort', 'voucher_id'),
            'order': request.args.get('order', 'asc')
        }
        if not filters['delivery_date'] and not filters['import_id']:
            filters['delivery_date'] = datetime.date.today().strftime('%Y/%m/%d')

    # どちらの場合でも同じロジックで取得
    vouchers = db_logic.get_voucher_list(filters, is_export=True)
    
    # テンプレート側で filters を使うことがあるので、POST時も最低限埋めておく
    if request.method == 'POST':
        # 日付などは現在の表示用としてダミーまたは先頭データから取得してもよいが、
        # 印刷ヘッダー用なので一旦空でも動くようにテンプレート側で調整済み
        pass

    return render_template('dc_in/print_list.html', vouchers=vouchers, current_filters=filters)

@bp.route('/voucher_detail/<v_id>')
def voucher_detail(v_id):
    target_data = db_logic.get_voucher_detail(v_id)
    if target_data:
        remote_user = request.environ.get('REMOTE_USER')
        if not target_data.get('operator') and remote_user:
             target_data['operator'] = remote_user.split('\\')[-1]
    return render_template('dc_in/voucher_detail.html', voucher_id=v_id, data=target_data)

@bp.route('/edit_limits', methods=['GET', 'POST'])
def edit_limits():
    message = None
    target_date_str = request.args.get('target_date') or request.form.get('target_date')
    if not target_date_str:
        target_date_str = datetime.date.today().strftime('%Y/%m/%d')

    if request.method == 'POST':
        s_val = request.form.get('s_limit', 0)
        m_val = request.form.get('m_limit', 0)
        scope = request.form.get('update_scope', 'single')
        try:
            db_logic.save_limits(target_date_str, s_val, m_val, scope)
            message = "設定を保存しました。"
        except Exception as e:
            message = f"エラーが発生しました: {e}"

    current = db_logic.get_limits_by_date(target_date_str)
    monthly_list = db_logic.get_monthly_limits(datetime.date.today().strftime('%Y/%m/%d'))

    return render_template('dc_in/edit_limits.html', target_date=target_date_str, data=current, monthly_list=monthly_list, message=message)

@bp.route('/download_list_pdf')
def download_list_pdf():
    output = make_response("PDF未実装")
    output.headers["Content-Disposition"] = "attachment; filename=list_export.pdf"
    output.headers["Content-type"] = "application/pdf"
    return output

@bp.route('/download_voucher_pdf', methods=['POST'])
def download_voucher_pdf():
    dummy_content = "帳票PDFデータ"
    output = make_response(dummy_content)
    output.headers["Content-Disposition"] = "attachment; filename=vouchers.pdf"
    output.headers["Content-type"] = "application/pdf"
    return output

@bp.route('/sample_complete')
def sample_complete():
    dummy_id = "20251127-0945-fujiname" 
    return render_template('dc_in/complete.html', new_import_id=dummy_id)

# ==========================================
# 追加：CSV雛形ダウンロード用（名前を変えて復活）
# ==========================================
@bp.route('/download_template')
def download_template():
    si = io.StringIO()
    cw = csv.writer(si)
    # 雛形用のシンプルなヘッダー
    cw.writerow(['取込ID', '伝票番号', 'センター', '取引先', '納品ケース数'])
    
    output = make_response(si.getvalue().encode('cp932', 'ignore'))
    # ファイル名もわかりやすく template.csv に変更
    output.headers["Content-Disposition"] = "attachment; filename=template.csv"
    output.headers["Content-type"] = "text/csv"
    return output