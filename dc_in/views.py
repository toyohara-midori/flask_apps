from flask import render_template, request, redirect, url_for, make_response, flash
import datetime
import io
import csv
import uuid
from itertools import groupby

# ★追加: ログ書き込み関数
try:
    from common.logger import write_log
except ImportError:
    # 開発環境などでパスが違う場合の保険 (なくてもOK)
    def write_log(*args, **kwargs):
        print(f"[LOG_FALLBACK] {args} {kwargs}")

from . import dc_in_bp as bp
from common import dc_in_db_logic as db_logic
from common.auth_util import get_remote_user

TEMP_DATA_STORE = {}

# ==========================================
# ルート定義
# ==========================================

@bp.route('/')
def home():
    return render_template('dc_in/index.html')

@bp.route('/confirm', methods=['POST'])
def show_confirmation():
    # 1. ユーザー情報の取得
    current_user_id = get_remote_user(request)

    # 2. ファイルチェック
    if 'file' not in request.files:
        return "ファイルがありません", 400
    file = request.files['file']
    if file.filename == '':
        return "ファイル名がありません", 400

    # 3. CSV読み込み
    try:
        stream = io.StringIO(file.stream.read().decode("cp932"), newline=None)
        csv_input = csv.reader(stream)
        rows = list(csv_input)
    except Exception as e:
        # ★追加: CSV自体の読み込み失敗ログ
        write_log('dc_in', current_user_id, 'ERROR', f'CSV読込失敗: ファイル[{file.filename}] / {e}')
        return f"CSV読み込みエラー: {e}", 500

    # 4. DBロジック呼び出し（バリデーション）
    try:
        enriched_list, error_msgs = db_logic.process_upload_csv(rows)
    except Exception as e:
        # ★追加: システムエラーログ
        write_log('dc_in', current_user_id, 'ERROR', f'CSV解析システムエラー: {e}')
        return f"システムエラー: {e}", 500

    # ==========================================
    # ★ エラーチェック分岐 (マスタ不備など)
    # ==========================================
    if error_msgs:
        # ★追加: バリデーションエラーログ (エラー件数を記録)
        err_count = len(error_msgs)
        write_log('dc_in', current_user_id, 'CHECK_NG', f'CSV内容不備: {err_count}件のエラー / ファイル[{file.filename}]')
        
        # エラーがある場合は、index.html に戻してエラーを表示
        return render_template('dc_in/index.html', error_list=error_msgs)

    # ==========================================
    # ★ 正常時の処理 (確認画面へ)
    # ==========================================
    
    # A. バッチID生成
    batch_id = f"{uuid.uuid4()}-{current_user_id}"
    
    # B. ワークテーブルへ保存
    try:
        db_logic.save_to_work_table(batch_id, current_user_id, enriched_list)
        
        # ★追加: 成功ログ (件数と受付番号を記録)
        row_count = len(enriched_list)
        write_log('dc_in', current_user_id, 'UPLOAD', f'CSV確認画面へ遷移: 受付番号[{batch_id}] / {row_count}件 / ファイル[{file.filename}]')
        
    except Exception as e:
        write_log('dc_in', current_user_id, 'ERROR', f'ワークテーブル保存失敗: {e}')
        return f"データ一時保存エラー: {e}", 500

    # (以下、集計処理などは変更なし)
    for center, items_in_center in groupby(enriched_list, key=lambda x: x['center_name']):
        group_list = []
        sub_key = lambda x: (x['delivery_date'], x['vendor_code'], x['dept_code'])
        
        center_items_list = list(items_in_center)
        center_items_list.sort(key=sub_key)
        center_groups = {}

        for (d_date, v_code, dept_code), items in groupby(center_items_list, key=sub_key):
            items = list(items)
            first = items[0]
            
            group_obj = {
                'delivery_date': d_date,
                'vendor_code': v_code,
                'vendor_name': first['vendor_name'],
                'dept_code': dept_code,
                'dept_name': first['dept_name'],
                'details': [item['detail_row'] for item in items]
            }
            group_list.append(group_obj)
        
        center_groups[center] = group_list

    # 全体集計
    global_summary = []
    sum_key = lambda x: (x['center_name'], x['delivery_date'])
    enriched_list.sort(key=sum_key)
    
    for (center, d_date), items in groupby(enriched_list, key=sum_key):
        items = list(items)
        jv_count = sum(item['raw_case'] for item in items if str(item['manufacturer']).startswith('JV'))
        other_count = sum(item['raw_case'] for item in items if not str(item['manufacturer']).startswith('JV'))
        
        global_summary.append({
            'center': center,
            'date': d_date,
            'jv': jv_count,
            'other': other_count
        })

    return render_template(
        'dc_in/confirm.html', 
        center_groups=center_groups, 
        global_summary=global_summary,
        import_id=batch_id 
    )

@bp.route('/complete_insertion', methods=['POST'])
def complete_insertion():
    # 画面から batch_id を受け取る
    import_id = request.form.get('import_id')
    # ユーザーID取得 (ログ用)
    current_user_id = get_remote_user(request)
    
    if not import_id:
        return "不正なリクエストです(ID不足)", 400

    try:
        # 1. ワークテーブルからデータを再取得
        data_list = db_logic.get_data_from_work_table(import_id)
        
        if not data_list:
            write_log('dc_in', current_user_id, 'ERROR', f'本登録失敗: データ期限切れ [{import_id}]')
            return "セッション有効期限切れ、またはデータが見つかりません。最初からやり直してください。", 400

        # 2. 本番テーブル(dcnyu03/04等)へ登録
        user_id = data_list[0]['user_id'] # CSV内のユーザーID
        
        # ★ここで本登録実行
        result_msg = db_logic.insert_voucher_data(data_list, user_id, import_id)
        
        # ==========================================
        # ★追加: 登録成功ログ
        # ==========================================
        write_log('dc_in', user_id, 'INSERT', f'CSV本登録完了: 受付番号[{import_id}] / {result_msg}')

        # 3. 完了画面用の新しいID生成 (表示用)
        now = datetime.datetime.now()
        display_import_id = f"{now.strftime('%Y%m%d-%H%M')}-{user_id}"

        # 4. ワークテーブルのお掃除
        db_logic.delete_work_table(import_id)

        return render_template('dc_in/complete.html', new_import_id=display_import_id)

    except Exception as e:
        # ★追加: 登録失敗ログ
        write_log('dc_in', current_user_id, 'ERROR', f'本登録例外: 受付番号[{import_id}] / {e}')
        return f"登録処理中にエラーが発生しました: {e}", 500

@bp.route('/voucher_list', methods=['GET', 'POST'])
def voucher_list():
    # --- 検索ボタン（POST）が押されたときの処理 ---
    if request.method == 'POST':
        search_id = request.form.get('voucher_id')
        current_user_id = get_remote_user(request)

        # 1. 空文字チェック
        if not search_id or not search_id.strip():
             return f"""
            <div style="padding: 20px; font-family: sans-serif;">
                <h3 style="color: red;">エラー</h3>
                <p>伝票番号が入力されていません。</p>
                <button onclick="window.close()">閉じる</button>
            </div>
            """

        # 2. DB検索
        target_data = db_logic.get_voucher_detail(search_id)

        if target_data:
            # ★追加: 検索ログ (詳細表示)
            write_log('dc_in', current_user_id, 'SEARCH', f'伝票詳細検索: {search_id} (Hit)')
            return redirect(url_for('dc_in.voucher_detail', v_id=search_id))
        else:
            write_log('dc_in', current_user_id, 'SEARCH', f'伝票詳細検索: {search_id} (NotFound)')
            return f"""
            <div style="padding: 20px; font-family: sans-serif;">
                <h3 style="color: red;">該当なし</h3>
                <p>伝票番号 <strong>{search_id}</strong> は見つかりませんでした。</p>
                <button onclick="window.close()">閉じる</button>
            </div>
            """

    # --- 一覧表示ロジック ---
    filters = {
        'batch_id': request.args.get('batch_id'), # import_id -> batch_id
        'center': request.args.get('center'),
        'dept': request.args.get('dept'),
        'vendor': request.args.get('vendor'),
        'delivery_date': request.args.get('delivery_date'),
        'type': request.args.get('type'),
        'sort': request.args.get('sort', 'voucher_id'),
        'order': request.args.get('order', 'asc')
    }

    if not filters['delivery_date'] and not filters['batch_id']:
        filters['delivery_date'] = datetime.date.today().strftime('%Y/%m/%d')

    vouchers = db_logic.get_voucher_list(filters, is_export=False)
    opts = db_logic.get_filter_options()

    return render_template('dc_in/voucher_list.html',
        vouchers=vouchers,
        current_filters=filters,
        batch_options=opts['batch_options'], 
        centers=opts['centers'],
        depts=opts['depts'],
        vendors=opts['vendors'],
    )

@bp.route('/download_csv', methods=['GET', 'POST'])
def download_csv():
    filters = {}
    current_user_id = get_remote_user(request)
    
    if request.method == 'POST':
        selected_ids = request.form.getlist('v_ids')
        if not selected_ids:
            return "出力対象が選択されていません。", 400
        filters['voucher_ids'] = selected_ids
    else:
        filters = {
            'batch_id': request.args.get('batch_id'),
            'center': request.args.get('center'),
            # ... (他フィルタ省略)
        }
        # (日付デフォルトロジックなど)

    vouchers_list = db_logic.get_voucher_list(filters, is_export=True)

    # ★追加: CSV出力ログ
    count = len(vouchers_list)
    write_log('dc_in', current_user_id, 'DOWNLOAD', f'一覧CSV出力: {count}件')

    si = io.StringIO()
    cw = csv.writer(si)
    
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
            row.get('batch_id', ''), # import_id -> batch_id
            row.get('voucher_id', ''),
            row.get('line_no', ''),
            row.get('center', ''),      
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

@bp.route('/print_list', methods=['GET', 'POST'])
def print_list():
    filters = {}
    current_user_id = get_remote_user(request)
    
    if request.method == 'POST':
        selected_ids = request.form.getlist('v_ids')
        if not selected_ids:
            return "出力対象が選択されていません。", 400
        filters['voucher_ids'] = selected_ids
    else:
        # GETの場合のフィルタ処理 (省略)
        pass

    vouchers = db_logic.get_voucher_list(filters, is_export=True)
    
    # ★追加: 帳票印刷ログ
    write_log('dc_in', current_user_id, 'PRINT', f'一覧帳票印刷: {len(vouchers)}件')

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
    
    current_user_id = get_remote_user(request)

    if request.method == 'POST':
        s_val = request.form.get('s_limit', 0)
        m_val = request.form.get('m_limit', 0)
        scope = request.form.get('update_scope', 'single')
        try:
            db_logic.save_limits(target_date_str, s_val, m_val, scope)
            message = "設定を保存しました。"
            # ★追加: 設定変更ログ
            write_log('dc_in', current_user_id, 'UPDATE', f'上限数変更: {target_date_str} / 守谷:{s_val} 狭山:{m_val} ({scope})')
        except Exception as e:
            message = f"エラーが発生しました: {e}"
            write_log('dc_in', current_user_id, 'ERROR', f'上限数変更エラー: {e}')

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
    selected_ids = request.form.getlist('v_ids')
    if not selected_ids:
        return redirect(url_for('dc_in.voucher_list'))

    filters = {'voucher_ids': selected_ids}
    blue_vouchers_raw = db_logic.get_voucher_list(filters, is_export=True)
    red_vouchers_raw = db_logic.get_related_discount_vouchers(selected_ids) or []
    
    # ★追加: 帳票出力ログ
    current_user_id = get_remote_user(request)
    write_log('dc_in', current_user_id, 'PRINT', f'伝票単位帳票出力: 青{len(blue_vouchers_raw)}件 / 赤{len(red_vouchers_raw)}件')

    blue_vouchers_raw.sort(key=lambda x: x['voucher_id'])
    
    blue_dict = {} 
    for vid, rows in groupby(blue_vouchers_raw, key=lambda x: x['voucher_id']):
        blue_dict[vid] = list(rows)

    red_vouchers_raw.sort(key=lambda x: x['parent_id'])
    
    red_dict_by_parent = {} 
    for pid, rows in groupby(red_vouchers_raw, key=lambda x: x['parent_id']):
        red_dict_by_parent[pid] = list(rows)

    print_pages = []
    for vid in sorted(blue_dict.keys()):
        current_page_vouchers = []
        current_page_vouchers.append(blue_dict[vid])
        if vid in red_dict_by_parent:
            current_page_vouchers.append(red_dict_by_parent[vid])
        print_pages.append(current_page_vouchers)

    return render_template('dc_in/print_list.html', pages=print_pages)

@bp.route('/sample_complete')
def sample_complete():
    dummy_id = "20251127-0945-fujiname" 
    return render_template('dc_in/complete.html', new_import_id=dummy_id)

@bp.route('/download_template')
def download_template():
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['取込ID', '伝票番号', 'センター', '取引先', '納品ケース数'])
    output = make_response(si.getvalue().encode('cp932', 'ignore'))
    output.headers["Content-Disposition"] = "attachment; filename=template.csv"
    output.headers["Content-type"] = "text/csv"
    return output