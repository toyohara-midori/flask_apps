import datetime
import unicodedata
from itertools import groupby

# ★相対インポート (commonフォルダのdb_connectionを使う)
from .db_connection import get_connection

TARGET_DB = 'master'

CENTER_NAME_MAP = {
    'D03': '守谷C',
    'D04': '狭山日高C',
}

# ==========================================
# 設定定義: 業務時間設定
# ==========================================
MODE_CONFIG = {
    'normal':  {'start': '08:00', 'end': '20:00'},
    'morning': {'start': '05:00', 'end': '10:50'}
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
# ヘルパー関数: DB時刻取得
# ==========================================
def get_db_server_time(target_db='master'):
    """
    master DBの時刻を取得。失敗したらAPサーバー時刻を返す。
    """
    conn = None
    try:
        conn = get_connection(target_db)
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP")
        row = cursor.fetchone()
        if row:
            return row[0]
    except Exception as e:
        print(f"DB Time Error: {e}")
    finally:
        if conn: conn.close()
    
    return datetime.datetime.now()

# ==========================================
# 共通関数: 列名を強制的に小文字にする
# ==========================================
def _get_center_name(code):
    return CENTER_NAME_MAP.get(code.strip(), code)

# ==========================================
# 共通チェック: 時間判定
# ==========================================
def check_business_time(mode='normal'):
    """
    現在時刻が業務時間内かチェックする。
    NGならエラーメッセージを返す。OKならNone。
    """
    config = MODE_CONFIG.get(mode)
    if not config:
        return None 

    # DBサーバー時刻を取得
    now = get_db_server_time('master')
    current_time_str = now.strftime('%H:%M')

    # 時間判定
    if not (config['start'] <= current_time_str <= config['end']):
        return f"受付時間外です。<br>現在: {current_time_str} (受付: {config['start']} ～ {config['end']})"

    return None

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
        
        cursor.execute("SELECT cucd FROM DBA.dcnyu03 UNION SELECT cucd FROM DBA.dcnyu04")
        raw_centers = [r[0] for r in cursor.fetchall() if r[0]]
        options['centers'] = sorted(list(set([_get_center_name(c) for c in raw_centers])))
        
        # 部門
        sql_dept = """
            SELECT DISTINCT T.bucd, N.nmkj 
            FROM (SELECT bucd FROM DBA.dcnyu03 UNION SELECT bucd FROM DBA.dcnyu04) AS T 
            LEFT JOIN DBA.nammf04 AS N ON T.bucd = N.bucd AND N.brcd = '00' 
            ORDER BY T.bucd
        """
        cursor.execute(sql_dept)
        dept_list = []
        for r in cursor.fetchall():
            code = r[0].strip() if r[0] else ''
            name = r[1].strip() if r[1] else '(名称不明)'
            if code:
                dept_list.append({'code': code, 'name': name, 'label': f"{code} {name}"})
        options['depts'] = dept_list
        
        # 取引先
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
                vendor_list.append({'code': code, 'name': name, 'label': f"{code} {name}"})
        options['vendors'] = vendor_list

        return options
    finally:
        cursor.close()
        conn.close()

# ==========================================
# 2. 詳細画面用
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
        
        cols = [col[0].lower() for col in cursor.description]
        rows = cursor.fetchall()
        
        if not rows: return None
        
        first_row = dict(zip(cols, rows[0]))
        
        sql_neb = "SELECT DISTINCT deno FROM DBA.dcneb WHERE deno11 = ? AND trdk = '13'"
        cursor.execute(sql_neb, [voucher_id])
        neb_row = cursor.fetchone()
        discount_id_val = neb_row[0].strip() if neb_row else ''

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
            d = dict(zip(cols, row))
            
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
    
    # 1. 時間チェック (登録ボタンを押した瞬間にもチェック)
    time_error = check_business_time('normal')
    if time_error:
        raise Exception(time_error)

    conn = get_connection('master')
    conn.autocommit = False # トランザクション開始
    cursor = conn.cursor()

    total_vouchers = 0
    
    try:
        # 2. 伝票単位にグルーピング (センター > 納品日 > ベンダー > 部門)
        key_func = lambda x: (x['center_name'], x['delivery_date'], x['vendor_code'], x['dept_code'])
        data_list.sort(key=key_func)

        for (center_name, d_date, v_code, dept_code), items in groupby(data_list, key=key_func):
            items = list(items) # 明細行リスト
            
            # --- A. 仕入伝票番号の採番 (Sequence A) ---
            main_voucher_id = _get_next_number(cursor, 'purchase') 
            
            # 挿入先テーブルの決定
            target_table = "DBA.dcnyu03" if "守谷" in center_name else "DBA.dcnyu04"
            center_code_db = "D03" if "守谷" in center_name else "D04"

            # --- B. 仕入伝票(親)のINSERT ---
            line_no = 1
            for row in items:
                d = row['detail_row']
                
                sql_insert_main = f"""
                    INSERT INTO {target_table} (
                        deno, no, cucd, bucd, vecd, dldt, 
                        cocd, odsu, dltn, prtn, 
                        md, dc, thrflg, sign, rgdt, upti
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                val_discount = float(d[11].replace(',',''))
                
                params_main = [
                    main_voucher_id,    # deno
                    line_no,            # no
                    center_code_db,     # cucd
                    dept_code,          # bucd
                    v_code,             # vecd
                    d_date,             # dldt
                    d[0],               # cocd (Item Code)
                    d[6],               # odsu (ケース数)
                    float(d[9].replace(',','')), # dltn (原単価)
                    val_discount,       # prtn (値引金額)
                    d[7],               # md (本部費)
                    d[8],               # dc (物流費)
                    row.get('pass_flag', '0'), # thrflg (通過)
                    user_id,            # sign (ADユーザー名)
                    datetime.datetime.now().strftime('%Y/%m/%d'), # rgdt
                    datetime.datetime.now().strftime('%H:%M:%S')  # upti
                ]
                
                cursor.execute(sql_insert_main, params_main)
                line_no += 1

            total_vouchers += 1

            # --- C. 値引伝票の処理 (値引がある場合のみ) ---
            total_discount_in_voucher = sum(float(x['detail_row'][11].replace(',','')) for x in items)

            if total_discount_in_voucher > 0:
                discount_voucher_id = _get_next_number(cursor, 'discount')
                
                line_no_neb = 1
                for row in items:
                    d = row['detail_row']
                    val_disc = float(d[11].replace(',',''))
                    
                    if val_disc > 0:
                        sql_insert_neb = """
                            INSERT INTO DBA.dcneb (
                                deno, deno11, no, cucd, bucd, vecd, dldt,
                                cocd, odsu, nebtan, nebngk, sign
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        params_neb = [
                            discount_voucher_id, # deno
                            main_voucher_id,     # deno11
                            line_no_neb,         # no
                            center_code_db,
                            dept_code,
                            v_code,
                            d_date,
                            d[0],                # cocd
                            d[6],                # odsu
                            float(d[9].replace(',','')), # nebtan
                            val_disc,            # nebngk
                            user_id
                        ]
                        cursor.execute(sql_insert_neb, params_neb)
                        line_no_neb += 1

        conn.commit()
        return f"登録完了: {total_vouchers}件の伝票を作成しました。"

    except Exception as e:
        conn.rollback()
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
    """
    # ★本番環境では必ず採番テーブルを使用するロジックに変更してください
    import random
    prefix = "1" if num_type == 'purchase' else "9" 
    now_str = datetime.datetime.now().strftime('%d%H%M%S')
    return f"{prefix}{now_str}"