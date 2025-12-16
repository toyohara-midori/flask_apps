from flask import render_template, request, redirect, url_for, make_response
import datetime
import io
import csv
import uuid
from itertools import groupby

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
# 1. ユーザー情報の取得 (本実装)
    # common.auth_util を使って取得
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
        return f"CSV読み込みエラー: {e}", 500

    # 4. DBロジック呼び出し（データ補完 ＆ エラーチェック）
    try:
        enriched_list, error_msgs = db_logic.process_upload_csv(rows)
    except Exception as e:
        return f"システムエラー: {e}", 500

    # ==========================================
    # ★ エラーチェック分岐
    # ==========================================
    if error_msgs:
        # エラーがある場合は、index.html に戻してエラーを表示
        return render_template('dc_in/index.html', error_list=error_msgs)

    # ==========================================
    # ★ 正常時の処理
    # ==========================================
    
    # A. バッチID生成 (UUID + ユーザー名)
    batch_id = f"{uuid.uuid4()}-{current_user_id}"
    
    # B. ワークテーブル (DC_IN_CSV) へ保存
    try:
        db_logic.save_to_work_table(batch_id, current_user_id, enriched_list)
    except Exception as e:
        return f"データ一時保存エラー: {e}", 500

    # センターで大分類
    for center, items_in_center in groupby(enriched_list, key=lambda x: x['center_name']):
        group_list = []
        # (日付, ベンダー, 部門) で小分類
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

    # C. 全体集計 (JVかそれ以外か)
    global_summary = [] # ★ここで定義します
    
    sum_key = lambda x: (x['center_name'], x['delivery_date'])
    enriched_list.sort(key=sum_key)
    
    for (center, d_date), items in groupby(enriched_list, key=sum_key):
        items = list(items)
        # JV判定 (メーカー名の先頭がJVかどうか)
        jv_count = sum(item['raw_case'] for item in items if str(item['manufacturer']).startswith('JV'))
        other_count = sum(item['raw_case'] for item in items if not str(item['manufacturer']).startswith('JV'))
        
        global_summary.append({
            'center': center,
            'date': d_date,
            'jv': jv_count,
            'other': other_count
        })

    # D. 確認画面へ (import_id に batch_id を渡す)
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
    
    if not import_id:
        return "不正なリクエストです(ID不足)", 400

    try:
        # 1. ワークテーブルからデータを再取得
        data_list = db_logic.get_data_from_work_table(import_id)
        
        if not data_list:
            return "セッション有効期限切れ、またはデータが見つかりません。最初からやり直してください。", 400

        # 2. 本番テーブル(dcnyu03/04等)へ登録
        # ユーザーIDはデータに含まれているのでそれを使う
        user_id = data_list[0]['user_id']
        result_msg = db_logic.insert_voucher_data(data_list, user_id)
        
        # 3. 完了画面用の新しいID生成 (これは画面表示用なので適当でOK)
        now = datetime.datetime.now()
        display_import_id = f"{now.strftime('%Y%m%d-%H%M')}-{user_id}"

        # 4. ワークテーブルのお掃除
        db_logic.delete_work_table(import_id)

        return render_template('dc_in/complete.html', new_import_id=display_import_id)

    except Exception as e:
        return f"登録処理中にエラーが発生しました: {e}", 500

@bp.route('/voucher_list', methods=['GET', 'POST'])
def voucher_list():
    # --- ★ここから：検索ボタン（POST）が押されたときの処理 ---
    if request.method == 'POST':
        search_id = request.form.get('voucher_id')

        # 1. 空文字チェック (HTMLのrequiredが効かなかった場合の保険)
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
            # 3. 見つかったら詳細画面へリダイレクト（別タブで開く）
            return redirect(url_for('dc_in.voucher_detail', v_id=search_id))
        else:
            # 4. 見つからなかったらエラー画面を返す（別タブに表示される）
            return f"""
            <div style="padding: 20px; font-family: sans-serif;">
                <h3 style="color: red;">該当なし</h3>
                <p>伝票番号 <strong>{search_id}</strong> は見つかりませんでした。</p>
                <button onclick="window.close()">閉じる</button>
            </div>
            """
    # --- ★ここまでが追加・変更部分 ---


    # --- 以下、既存の一覧表示ロジック（そのまま） ---
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

    vouchers = db_logic.get_voucher_list(filters, is_export=False)
    opts = db_logic.get_filter_options()

    # ※error変数は使わなくなったので削除しました
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
    # 1. 選択されたID
    selected_ids = request.form.getlist('v_ids')
    if not selected_ids:
        return redirect(url_for('dc_in.voucher_list'))

    # 2. データ取得
    filters = {'voucher_ids': selected_ids}
    blue_vouchers_raw = db_logic.get_voucher_list(filters, is_export=True)
    red_vouchers_raw = db_logic.get_related_discount_vouchers(selected_ids) or []

    # 3. 行データを「伝票単位（リストのリスト）」にまとめる処理
    
    # --- A. 青（仕入）を伝票IDごとにまとめる ---
    # groupbyを使うため、まずはID順にソート
    blue_vouchers_raw.sort(key=lambda x: x['voucher_id'])
    
    blue_dict = {} # Key: 仕入伝票ID, Value: [行データのリスト]
    for vid, rows in groupby(blue_vouchers_raw, key=lambda x: x['voucher_id']):
        blue_dict[vid] = list(rows)

    # --- B. 赤（値引）を親IDごとにまとめる ---
    # 赤伝票は "parent_id" (deno11) が青伝票との紐付けキーです
    red_vouchers_raw.sort(key=lambda x: x['parent_id'])
    
    red_dict_by_parent = {} # Key: 親(仕入)伝票ID, Value: [行データのリスト]
    for pid, rows in groupby(red_vouchers_raw, key=lambda x: x['parent_id']):
        # 1つの親IDに対して、赤伝票は通常1枚ですが、念のためリスト化して保持
        # ここでは「赤伝票の明細行すべて」をリストにします
        red_dict_by_parent[pid] = list(rows)

    # 4. ページデータの作成
    # 構造: [ [青伝票の行リスト, 赤伝票の行リスト], [青伝票の行リスト], ... ]
    print_pages = []

    # 青伝票（親）を基準にループ
    for vid in sorted(blue_dict.keys()):
        # 1ページに含まれる伝票リスト（最初は青だけ）
        current_page_vouchers = []
        
        # 1. 青伝票を追加
        current_page_vouchers.append(blue_dict[vid])
        
        # 2. 紐付く赤伝票があれば追加（これで青→赤の順序確定）
        if vid in red_dict_by_parent:
            current_page_vouchers.append(red_dict_by_parent[vid])
            
        # ページリストに追加
        print_pages.append(current_page_vouchers)

    # 5. テンプレートへ
    return render_template('dc_in/print_list.html', pages=print_pages)

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