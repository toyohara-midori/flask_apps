import datetime
import unicodedata
from .db_connection import get_connection
from itertools import groupby
TARGET_DB = 'master'

CENTER_NAME_MAP = {
    'D03': '守谷C',
    'D04': '狭山日高C',
}
# ==========================================
# ヘルパー関数: 文字列のお掃除
# ==========================================
def clean_str(text):
    if not text:
        return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace('"', '').strip()

# ==========================================
# 共通関数: 列名を強制的に小文字にする
# ==========================================
def _row_to_dict(cursor, row):
    # col[0] は列名。これを .lower() してキーにする
    return {col[0].lower(): val for col, val in zip(cursor.description, row)}

def _get_center_name(code):
    return CENTER_NAME_MAP.get(code.strip(), code)


# ==========================================
# 1. 一覧・検索機能
# ==========================================
def get_voucher_list(filters, is_export=False):
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()

    try:
        inner_columns = """
            deno, cocd, no, cucd, bucd, oddt, dldt, trdk, vecd, 
            odsu, dltn, prtn, md, dc, thrflg, conf, sign, rgdt, updt, upti
        """

        sql = f"""
            SELECT 
                T.deno as voucher_id,
                T.no   as line_no,
                CASE T.cucd
                    WHEN 'D03' THEN '守谷C'
                    WHEN 'D04' THEN '狭山日高C'
                    ELSE T.cucd
                END as center,
                T.cucd as center_code,
                T.dldt as delivery_date,
                T.bucd as dept_code,
                T.vecd as vendor_code,
                T.sign as operator,
                T.cocd as item_code,
                T.oddt as order_date,
                T.trdk as trans_code,
                T.odsu as order_qty,
                T.dltn as cost_price,
                T.prtn as total_disc,
                T.md   as fee_md,
                T.dc   as fee_dc,
                T.thrflg as pass_flag,
                T.conf   as conf_flag,
                T.rgdt   as reg_date,
                T.updt   as update_date,
                T.upti   as update_time,
                V.nmkj as vendor,
                N.nmkj as dept_name,
                M.hnam as first_p_name,
                M.mnam as manufacturer
            FROM (
                SELECT {inner_columns} FROM DBA.dcnyu03
                UNION ALL
                SELECT {inner_columns} FROM DBA.dcnyu04
            ) AS T
            LEFT JOIN DBA.venmf AS V ON T.vecd = V.vecd
            LEFT JOIN DBA.nammf04 AS N ON T.bucd = N.bucd AND N.brcd = '00'
            LEFT JOIN DBA.comf1 AS M ON T.cocd = M.cocd
            WHERE 1=1
        """

        params = []
        if filters.get('import_id'): 
            sql += " AND T.deno = ?"
            params.append(filters['import_id'])
        
        c_val = filters.get('center')
        if c_val:
            if '守谷' in c_val or c_val == 'D03':
                sql += " AND T.cucd = 'D03'"
            elif '狭山' in c_val or '日高' in c_val or c_val == 'D04':
                sql += " AND T.cucd = 'D04'"
            else:
                sql += " AND T.cucd = ?"
                params.append(c_val)

        if filters.get('dept'):
            sql += " AND T.bucd = ?"
            params.append(filters['dept'])
        if filters.get('vendor'):
            sql += " AND T.vecd = ?"
            params.append(filters['vendor'])
        if filters.get('delivery_date'):
            sql += " AND T.dldt = ?"
            params.append(filters['delivery_date'])

        v_ids = filters.get('voucher_ids')
        if v_ids:
            placeholders = ','.join('?' * len(v_ids))
            sql += f" AND T.deno IN ({placeholders})"
            params.extend(v_ids)

        t_val = filters.get('type')
        if t_val == 'jv':  
            sql += " AND M.mnam LIKE 'JV%'"
        elif t_val == 'regular':
            sql += " AND (M.mnam NOT LIKE 'JV%' OR M.mnam IS NULL)"

        if not is_export:
            sql += " AND T.no = '1'"

        sort_col = filters.get('sort', 'voucher_id')
        order_dir = filters.get('order', 'asc')
        sort_map = {
            'voucher_id': 'T.deno', 'import_id': 'T.deno', 'dept_code': 'T.bucd',
            'dept_name': 'N.nmkj', 'center': 'T.cucd', 'delivery_date': 'T.dldt',
            'vendor_code': 'T.vecd', 'vendor': 'V.nmkj', 'p_name': 'M.hnam',
            'manufacturer': 'M.mnam'
        }
        sql_sort = sort_map.get(sort_col, 'T.deno')
        sql += f" ORDER BY {sql_sort} {order_dir}"

        cursor.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = {}
            for col, val in zip(columns, row):
                if isinstance(val, str): row_dict[col] = val.strip()
                else: row_dict[col] = val
            results.append(row_dict)
        return results
    finally:
        cursor.close()
        conn.close()

def get_filter_options():
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()
    try:
        options = {'import_ids': [], 'centers': [], 'depts': [], 'vendors': []}
        
        # --- 取込IDとセンターの取得（変更なし） ---
        cursor.execute("SELECT cucd FROM DBA.dcnyu03 UNION SELECT cucd FROM DBA.dcnyu04")
        raw_centers = [r[0] for r in cursor.fetchall() if r[0]]
        options['centers'] = sorted(list(set([_get_center_name(c) for c in raw_centers])))
        
        cursor.execute("SELECT DISTINCT deno FROM (SELECT deno FROM DBA.dcnyu03 UNION SELECT deno FROM DBA.dcnyu04) AS T ORDER BY deno DESC")
        # (取込IDのロジックは元のまま維持してください。必要ならここに追加)
        
        # --- 部門 (コードと名称を取得) ---
        # T.bucd (コード) も取得します
        sql_dept = """
            SELECT DISTINCT T.bucd, N.nmkj 
            FROM (SELECT bucd FROM DBA.dcnyu03 UNION SELECT bucd FROM DBA.dcnyu04) AS T 
            LEFT JOIN DBA.nammf04 AS N ON T.bucd = N.bucd AND N.brcd = '00' 
            ORDER BY T.bucd
        """
        cursor.execute(sql_dept)
        # 辞書リストの形にします: {'code': '10', 'name': '食品部', 'label': '10 食品部'}
        dept_list = []
        for r in cursor.fetchall():
            code = r[0].strip() if r[0] else ''
            name = r[1].strip() if r[1] else '(名称不明)'
            if code:
                dept_list.append({
                    'code': code,
                    'name': name,
                    'label': f"{code} {name}"
                })
        options['depts'] = dept_list
        
        # --- 取引先 (コードと名称を取得) ---
        sql_vendor = """
            SELECT DISTINCT T.vecd, V.nmkj 
            FROM (SELECT vecd FROM DBA.dcnyu03 UNION SELECT vecd FROM DBA.dcnyu04) AS T 
            LEFT JOIN DBA.venmf AS V ON T.vecd = V.vecd 
            ORDER BY T.vecd
        """
        cursor.execute(sql_vendor)
        vendor_list = []
        for r in cursor.fetchall():
            code = r[0].strip() if r[0] else ''
            name = r[1].strip() if r[1] else '(名称不明)'
            if code:
                vendor_list.append({
                    'code': code,
                    'name': name,
                    'label': f"{code} {name}"
                })
        options['vendors'] = vendor_list

        return options
    finally:
        cursor.close()
        conn.close()

# ==========================================
# 2. 詳細画面用 (修正版)
# ==========================================
def get_voucher_detail(voucher_id):
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()
    try:
        sql = """
            SELECT 
                T.deno AS voucher_id,
                T.cucd AS center_code,
                T.bucd AS dept_code,
                T.dldt AS delivery_date,
                T.vecd AS vendor_code,
                T.sign AS operator,
                T.cocd AS item_code,
                T.odsu AS order_qty,
                T.dltn AS cost_price,
                T.prtn AS total_disc,
                T.no   AS line_no,
                
                M.hnam as p_name, 
                M.kika as spec, 
                M.mnam as manufacturer, 
                
                C2.irsu as per_case, 
                C2.janc as jan, 
                
                V.nmkj as vendor_name, 
                N.nmkj as dept_name
            FROM (
                SELECT *, '03' as tbl_src FROM DBA.dcnyu03 WHERE deno = ? 
                UNION ALL 
                SELECT *, '04' as tbl_src FROM DBA.dcnyu04 WHERE deno = ?
            ) AS T
            LEFT JOIN DBA.comf1 AS M ON T.cocd = M.cocd
            LEFT JOIN DBA.comf204 AS C2 ON T.cocd = C2.cocd AND T.bucd = C2.bucd
            LEFT JOIN DBA.venmf AS V ON T.vecd = V.vecd
            LEFT JOIN DBA.nammf04 AS N ON T.bucd = N.bucd AND N.brcd = '00'
            ORDER BY T.no
        """
        cursor.execute(sql, [voucher_id, voucher_id])
        
        # ★修正ポイント1: 列名リストをこの瞬間に確定させてリスト化しておく
        # cursor.description は次のexecuteで消えてしまうため
        cols = [col[0].lower() for col in cursor.description]
        
        rows = cursor.fetchall()
        
        if not rows: return None
        
        # ★修正ポイント2: 最初の行の辞書化も、確保した cols を使う
        first_row = dict(zip(cols, rows[0]))
        
        # --- ここで別のSQLを実行しても大丈夫になります ---
        sql_neb = "SELECT DISTINCT deno FROM DBA.dcneb WHERE deno11 = ? AND trdk = '13'"
        cursor.execute(sql_neb, [voucher_id])
        neb_row = cursor.fetchone()
        discount_id_val = neb_row[0].strip() if neb_row else ''
        # -----------------------------------------------

        d_date = first_row.get('delivery_date')
        if isinstance(d_date, (datetime.date, datetime.datetime)): 
            d_date = d_date.strftime('%Y/%m/%d')
            
        c_name = _get_center_name(first_row.get('center_code', ''))
        data = {
            'voucher_id': first_row.get('voucher_id', '').strip(), 
            'discount_id': discount_id_val,
            'center': c_name, 
            'dept_code': first_row.get('dept_code', ''), 
            'dept_name': first_row.get('dept_name') or '', 
            'delivery_date': d_date, 
            'vendor_code': first_row.get('vendor_code', ''), 
            'vendor': first_row.get('vendor_name') or '', 
            'operator': first_row.get('operator', '').strip(), 
            'details': [], 
            'total_cases': 0, 
            'total_cost': 0
        }
        
        total_cases = 0
        total_cost = 0
        
        for row in rows:
            # ★修正ポイント3: _row_to_dict ではなく、確保しておいた cols と zip する
            d = dict(zip(cols, row))
            
            # --- 以下は変更なし ---
            qty = int(d.get('order_qty') or 0)
            cost_unit = d.get('cost_price') or 0
            discount = d.get('total_disc') or 0
            row_total = (qty * cost_unit) - discount
            total_cases += qty
            total_cost += row_total
            
            data['details'].append({
                'p_code': d.get('item_code', '').strip(), 
                'jan': d.get('jan') or '', 
                'p_name': d.get('p_name') or '', 
                'spec': d.get('spec') or '', 
                'manufacturer': d.get('manufacturer') or '', 
                'per_case': d.get('per_case') or 0, 
                'loose': 0, 
                'case': qty, 
                'cost': "{:,.2f}".format(cost_unit), 
                'row_total': "{:,}".format(int(row_total)), 
                'discount': "{:,}".format(int(discount))
            })
            
        data['total_cases'] = "{:,}".format(total_cases)
        data['total_cost'] = "{:,}".format(int(total_cost))
        return data
        
    finally:
        cursor.close()
        conn.close()

# ==========================================
# 3. 値引伝票取得ロジック
# ==========================================
def get_related_discount_vouchers(parent_voucher_ids):
    if not parent_voucher_ids: return []
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()
    try:
        placeholders = ','.join('?' * len(parent_voucher_ids))
        params = parent_voucher_ids
        sql = f"""
            SELECT
                D.deno   AS voucher_id,
                D.deno11 AS parent_id,
                D.no     AS line_no,
                D.trdk   AS kubun,
                D.oddt   AS order_date,
                D.dldt   AS delivery_date,
                D.cucd   AS shop_code,
                D.vecd   AS vendor_code,
                N.nmkj   AS dept_name,
                V.nmkj   AS vendor,
                D.cocd   AS item_code,
                M.hnam   AS first_p_name,
                M.mnam   AS manufacturer,
                D.odsu   AS order_qty,
                D.nebtan AS cost_price
            FROM DBA.dcneb AS D
            LEFT JOIN DBA.nammf04 AS N ON D.bucd = N.bucd AND N.brcd = '00'
            LEFT JOIN DBA.venmf   AS V ON D.vecd = V.vecd
            LEFT JOIN DBA.comf1   AS M ON D.cocd = M.cocd
            WHERE D.deno11 IN ({placeholders}) AND D.trdk = '13'
            ORDER BY D.deno, D.no
        """
        cursor.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = {}
            for col, val in zip(columns, row):
                if isinstance(val, str): row_dict[col] = val.strip()
                else: row_dict[col] = val
            if row_dict.get('order_qty') is None: row_dict['order_qty'] = 0
            if row_dict.get('cost_price') is None: row_dict['cost_price'] = 0.0
            if '守谷' in _get_center_name(row_dict.get('shop_code', '')):
                row_dict['center'] = '守谷C'
            else:
                row_dict['center'] = '狭山日高C'
            results.append(row_dict)
        return results
    except Exception as e:
        print(f"Error fetching discount vouchers: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# ==========================================
# 4. CSVアップロード処理 (日付柔軟対応版)
# ==========================================
def process_upload_csv(csv_rows):
    """
    CSVを全行チェック。日付形式(YYYY/MM/DD, YYYY-MM-DD)を柔軟に解釈し、
    2025/01/01 のような標準形式に統一して返す。
    """
    conn = get_connection('master')
    cursor = conn.cursor()
    
    processed_list = []
    error_list = []

    try:
        sql_item = """
            SELECT M1.hnam, M1.kika, M1.mnam, M2.bucd, M2.janc, M2.irsu
            FROM DBA.comf1 M1
            LEFT JOIN DBA.comf204 M2 ON M1.cocd = M2.cocd
            WHERE M1.cocd = ?
        """
        sql_vendor = "SELECT nmkj FROM DBA.venmf WHERE vecd = ?"
        sql_dept = "SELECT nmkj FROM DBA.nammf04 WHERE bucd = ? AND brcd = '00'"

        for i, row in enumerate(csv_rows):
            line_no = i + 1
            
            if len(row) < 10:
                if len(row) > 1: 
                    error_list.append(f"{line_no}行目: 項目数が不足しています。(現在{len(row)}列)")
                continue

            # --- 1. データの取得 & お掃除 ---
            center_code = clean_str(row[0])
            raw_date    = clean_str(row[1]) # 元の日付文字列
            vendor_code = clean_str(row[2])
            fee_md      = clean_str(row[3])
            fee_dc      = clean_str(row[4])
            item_code   = clean_str(row[5])
            raw_qty     = clean_str(row[6])
            raw_cost    = clean_str(row[7])
            pass_flag   = clean_str(row[8])
            raw_disc    = clean_str(row[9])

            # --- 2. バリデーション & 型変換 ---

            # A. 日付チェック (ハイフン/スラッシュ、ゼロ埋め有無を許容)
            formatted_date = ""
            date_formats = ['%Y/%m/%d', '%Y-%m-%d'] # 許容するフォーマット
            date_obj = None
            
            for fmt in date_formats:
                try:
                    # ここで 2025-1-1 も 2025-01-01 も解釈されます
                    date_obj = datetime.datetime.strptime(raw_date, fmt)
                    break # 成功したらループを抜ける
                except ValueError:
                    continue # 失敗したら次のフォーマットを試す
            
            if date_obj:
                # 成功: DB登録用に "YYYY/MM/DD" に統一する
                formatted_date = date_obj.strftime('%Y/%m/%d')
            else:
                # 失敗: エラーリストへ
                error_list.append(f"{line_no}行目: 納品日 '{raw_date}' の形式が不正です。(YYYY/MM/DD または YYYY-MM-DD)")

            # B. センターコード
            if center_code not in ['D03', 'D04']:
                error_list.append(f"{line_no}行目: センターコード '{center_code}' が不正です。(D03, D04のみ可)")

            # C. 数値変換
            try:
                qty_case = int(float(raw_qty))
            except ValueError:
                error_list.append(f"{line_no}行目: 数量 '{raw_qty}' は数値で入力してください。")
                qty_case = 0
            
            try:
                cost_unit = float(raw_cost)
            except ValueError:
                error_list.append(f"{line_no}行目: 原単価 '{raw_cost}' は数値で入力してください。")
                cost_unit = 0.0
            
            try:
                disc_unit = float(raw_disc) if raw_disc else 0.0
            except ValueError:
                error_list.append(f"{line_no}行目: 値引単価 '{raw_disc}' は数値で入力してください。")
                disc_unit = 0.0

            # --- 3. DBマスタチェック ---
            
            # 商品マスタ
            cursor.execute(sql_item, [item_code])
            item_res = cursor.fetchone()
            
            p_name = ""
            spec = ""
            manufacturer = ""
            dept_code = "00"
            jan = ""
            per_case = 0

            if item_res:
                p_name = item_res[0] or ""
                spec   = item_res[1] or ""
                manufacturer = item_res[2] or ""
                dept_code = item_res[3] or "00"
                jan = item_res[4] or ""
                per_case = int(item_res[5]) if item_res[5] else 0
            else:
                error_list.append(f"{line_no}行目: 商品コード '{item_code}' がマスタに存在しません。")

            # 取引先マスタ
            cursor.execute(sql_vendor, [vendor_code])
            v_res = cursor.fetchone()
            if v_res:
                vendor_name = v_res[0]
            else:
                error_list.append(f"{line_no}行目: ベンダーコード '{vendor_code}' がマスタに存在しません。")
                vendor_name = "(不明)"

            # 部門名
            cursor.execute(sql_dept, [dept_code])
            d_res = cursor.fetchone()
            dept_name = d_res[0] if d_res else ""

            # --- エラーがなければリストに追加 ---
            if not error_list: 
                total_qty_loose = qty_case * per_case
                row_cost_total = (total_qty_loose * cost_unit)
                row_disc_total = (total_qty_loose * disc_unit)

                detail_row = [
                    item_code, jan, p_name, spec, manufacturer,
                    total_qty_loose, qty_case, fee_md, fee_dc,
                    "{:,.2f}".format(cost_unit),
                    "{:,.0f}".format(row_cost_total),
                    "{:,.0f}".format(row_disc_total)
                ]

                processed_list.append({
                    'center_name': '守谷C' if 'D03' in center_code else '狭山日高C',
                    'delivery_date': formatted_date, # ★整形済みの日付を使用
                    'vendor_code': vendor_code,
                    'vendor_name': vendor_name,
                    'dept_code': dept_code,
                    'dept_name': dept_name,
                    'manufacturer': manufacturer,
                    'detail_row': detail_row,
                    'raw_case': qty_case,
                    'pass_flag': pass_flag
                })

    except Exception as e:
        error_list.append(f"データ処理中に予期せぬエラーが発生しました: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return processed_list, error_list

# ==========================================
# 5. データ登録実行 (トランザクション処理)
# ==========================================
def insert_voucher_data(data_list, user_id):
    """
    一時保存されていたリストを受け取り、グルーピングしてDB登録を行う。
    dcnyu03/04 (仕入) と dcneb (値引) に分割してINSERTする。
    """
    conn = get_connection('master')
    conn.autocommit = False # トランザクション開始
    cursor = conn.cursor()

    total_vouchers = 0
    
    try:
        # 1. 伝票単位にグルーピング (センター > 納品日 > ベンダー > 部門)
        # ソート
        key_func = lambda x: (x['center_name'], x['delivery_date'], x['vendor_code'], x['dept_code'])
        data_list.sort(key=key_func)

        for (center_name, d_date, v_code, dept_code), items in groupby(data_list, key=key_func):
            items = list(items) # 明細行リスト
            
            # --- A. 仕入伝票番号の採番 (Sequence A) ---
            # ※ここで採番ロジックを呼ぶ
            main_voucher_id = _get_next_number(cursor, 'purchase') 
            
            # 挿入先テーブルの決定
            target_table = "DBA.dcnyu03" if "守谷" in center_name else "DBA.dcnyu04"
            center_code_db = "D03" if "守谷" in center_name else "D04"

            # --- B. 仕入伝票(親)のINSERT ---
            line_no = 1
            for row in items:
                # detail_rowの構成:
                # [0]商品CD, [1]JAN, [2]品名, [3]規格, [4]メーカー
                # [5]バラ数, [6]ケース数, [7]本部費, [8]物流費, [9]原単価, [10]原価計, [11]値引計
                d = row['detail_row']
                
                # SQL (カラム名は実際のスキーマに合わせて調整してください)
                sql_insert_main = f"""
                    INSERT INTO {target_table} (
                        deno, no, cucd, bucd, vecd, dldt, 
                        cocd, odsu, dltn, prtn, 
                        md, dc, thrflg, sign, rgdt, upti
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # パラメータ作成
                # dltn(原単価), prtn(値引額)
                # ※ここでの値引(prtn)は「行ごとの値引額」を保持する場合
                val_discount = float(d[11].replace(',','')) # 文字列書式を解除
                
                params_main = [
                    main_voucher_id,    # deno
                    line_no,            # no
                    center_code_db,     # cucd
                    dept_code,          # bucd
                    v_code,             # vecd
                    d_date,             # dldt
                    d[0],               # cocd (Item Code)
                    d[6],               # odsu (ケース数を入れるかバラ数を入れるかは運用による。一旦ケース数)
                    float(d[9].replace(',','')), # dltn (原単価)
                    val_discount,       # prtn (値引金額)
                    d[7],               # md (本部費)
                    d[8],               # dc (物流費)
                    row.get('pass_flag', '0'), # thrflg (通過) ※row自体に持たせておく必要あり(後述)
                    user_id,            # sign
                    datetime.datetime.now().strftime('%Y/%m/%d'), # rgdt
                    datetime.datetime.now().strftime('%H:%M:%S')  # upti
                ]
                
                cursor.execute(sql_insert_main, params_main)
                line_no += 1

            total_vouchers += 1

            # --- C. 値引伝票の処理 (値引がある場合のみ) ---
            # この伝票内の値引合計を計算
            total_discount_in_voucher = sum(float(x['detail_row'][11].replace(',','')) for x in items)

            if total_discount_in_voucher > 0:
                # 値引伝票番号の採番 (Sequence B)
                discount_voucher_id = _get_next_number(cursor, 'discount')
                
                line_no_neb = 1
                for row in items:
                    d = row['detail_row']
                    val_disc = float(d[11].replace(',',''))
                    
                    if val_disc > 0:
                        # 値引テーブル(dcneb)へのINSERT
                        # deno: 自分(値引)の番号, deno11: 親(仕入)の番号
                        sql_insert_neb = """
                            INSERT INTO DBA.dcneb (
                                deno, deno11, no, cucd, bucd, vecd, dldt,
                                cocd, odsu, nebtan, nebngk, sign
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        params_neb = [
                            discount_voucher_id, # deno (New)
                            main_voucher_id,     # deno11 (Link to Parent)
                            line_no_neb,         # no
                            center_code_db,
                            dept_code,
                            v_code,
                            d_date,
                            d[0],                # cocd
                            d[6],                # odsu (数量)
                            float(d[9].replace(',','')), # nebtan (元の単価?)
                            val_disc,            # nebngk (値引額)
                            user_id
                        ]
                        cursor.execute(sql_insert_neb, params_neb)
                        line_no_neb += 1

        conn.commit() # 全て成功したら確定
        return f"登録完了: {total_vouchers}件の伝票を作成しました。"

    except Exception as e:
        conn.rollback() # エラーなら全部取り消し
        raise e
    finally:
        cursor.close()
        conn.close()

# ==========================================
# (内部関数) 採番ロジック
# ==========================================
def _get_next_number(cursor, num_type):
    """
    伝票番号を採番する。
    num_type: 'purchase' (仕入) or 'discount' (値引)
    """
    # ★パターン1: 採番テーブルがある場合 (推奨)
    # table_key = 'NYU' if num_type == 'purchase' else 'NEB'
    # cursor.execute("UPDATE DBA.saiban_tbl SET cur_no = cur_no + 1 WHERE key_col = ?", [table_key])
    # cursor.execute("SELECT cur_no FROM DBA.saiban_tbl WHERE key_col = ?", [table_key])
    # return cursor.fetchone()[0]

    # ★パターン2: 簡易的に現在時刻などでユニークIDを作る場合 (とりあえず動かすならこれ)
    # ただし、レガシーDBのカラム定義(数字8桁など)に合わせる必要があります。
    # ここでは仮に「数字8桁」をランダム生成っぽく作る例ですが、
    # **本来は既存システムの採番ルールに従うSQLをここに書いてください**
    
    import random
    prefix = "1" if num_type == 'purchase' else "9" # 仕入は1始まり、値引は9始まり、など
    # 実際はDBからMAXを取るのが一番衝突しないが遅い
    # cursor.execute("SELECT MAX(deno) FROM DBA.dcnyu03") ...
    
    # ダミー実装: ミリ秒を使って衝突回避
    now_str = datetime.datetime.now().strftime('%d%H%M%S')
    return f"{prefix}{now_str}" # 例: 125143001
