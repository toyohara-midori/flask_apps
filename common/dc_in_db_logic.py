import time
import unicodedata
import math
from itertools import groupby
import datetime
import calendar

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

# common/dc_in_db_logic.py

# ==========================================
# ★追加: 検索条件(WHERE句)構築の共通ロジック
# ==========================================
def _build_search_where(filters):
    """
    一覧取得と集計で共通のWHERE句とパラメータを生成する
    戻り値: (where_sql, params_list)
    """
    conditions = []
    params = []

    # 1. バッチID
    if filters.get('batch_id'):
        conditions.append("L.batch_id = ?")
        params.append(filters['batch_id'])

    # 2. センター
    c_val = filters.get('center')
    if c_val:
        if '守谷' in c_val or c_val == 'D03':
            conditions.append("T.cucd = 'D03'")
        elif '狭山' in c_val or '日高' in c_val or c_val == 'D04':
            conditions.append("T.cucd = 'D04'")
        else:
            conditions.append("T.cucd = ?")
            params.append(c_val)

    # 3. 部門
    if filters.get('dept'):
        conditions.append("T.bucd = ?")
        params.append(filters['dept'])

    # 4. 取引先(ベンダー)
    if filters.get('vendor'):
        conditions.append("T.vecd = ?")
        params.append(filters['vendor'])

    # 5. 納品日
    if filters.get('delivery_date'):
        conditions.append("T.dldt = ?")
        params.append(filters['delivery_date'])

    # 6. 伝票ID指定 (CSV出力用など)
    v_ids = filters.get('voucher_ids')
    if v_ids:
        placeholders = ','.join('?' * len(v_ids))
        conditions.append(f"T.deno IN ({placeholders})")
        params.extend(v_ids)

    # 7. 種別 (JV/定番)
    # ※商品マスタ(M)への依存があるため、JOIN済みの前提
    t_val = filters.get('type')
    if t_val == 'jv':
        conditions.append("M.mnam LIKE 'JV%'")
    elif t_val == 'regular':
        conditions.append("(M.mnam NOT LIKE 'JV%' OR M.mnam IS NULL)")

    where_clause = " AND ".join(conditions)
    if where_clause:
        where_clause = " AND " + where_clause
    
    return where_clause, params


# ==========================================
# ★修正: 一覧取得 (共通ロジックを使用)
# ==========================================
def get_voucher_list(filters, is_export=False):
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()

    try:
        inner_columns = """
            deno, cocd, no, cucd, bucd, oddt, dldt, trdk, vecd, 
            odsu, dltn, prtn, md, dc, thrflg, conf, sign, rgdt, updt, upti
        """
        
        # WHERE句とパラメータを生成
        where_sql, params = _build_search_where(filters)

        # 一覧表示用なので、明細行番号 = '1' を条件に追加
        if not is_export:
            where_sql += " AND T.no = '1'"

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
                M.mnam as manufacturer,
                L.batch_id as batch_id

            FROM (
                SELECT {inner_columns} FROM DBA.dcnyu03
                UNION ALL
                SELECT {inner_columns} FROM DBA.dcnyu04
            ) AS T
            LEFT JOIN DBA.dc_batch_log AS L ON T.deno = L.deno_main
            LEFT JOIN DBA.venmf AS V ON T.vecd = V.vecd
            LEFT JOIN DBA.nammf04 AS N ON T.bucd = N.bucd AND N.brcd = '00'
            LEFT JOIN DBA.comf1 AS M ON T.cocd = M.cocd
            WHERE 1=1
            {where_sql}
        """

        # ソート順の処理 (既存のまま)
        sort_col = filters.get('sort', 'voucher_id')
        order_dir = filters.get('order', 'asc')
        sort_map = {
            'voucher_id': 'T.deno', 
            'batch_id': 'L.batch_id',
            'dept_code': 'T.bucd',
            'dept_name': 'N.nmkj', 'center': 'T.cucd', 'delivery_date': 'T.dldt',
            'vendor_code': 'T.vecd', 'vendor': 'V.nmkj', 'p_name': 'M.hnam',
            'manufacturer': 'M.mnam'
        }
        sql_sort = sort_map.get(sort_col, 'T.deno')
        sql += f" ORDER BY {sql_sort} {order_dir}"

        cursor.execute(sql, params)
        
        # ... (結果取得処理は既存と同じ) ...
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = {}
            for col, val in zip(columns, row):
                if isinstance(val, str): row_dict[col] = val.strip()
                else: row_dict[col] = val
            
            if not row_dict.get('batch_id'):
                 row_dict['batch_id'] = ''
                 
            results.append(row_dict)
        return results
    finally:
        cursor.close()
        conn.close()


# ==========================================
# ★追加: 集計用関数 (B案: 入数割り返し計算)
# ==========================================
def get_voucher_summary(filters):
    """
    現在のフィルタ条件に合致する伝票の「総ケース数」を計算して返す。
    ※明細全行を対象とし、入数マスタ(comf204)を使ってケース換算する。
    """
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()
    try:
        # WHERE句生成 (一覧と同じ条件)
        where_sql, params = _build_search_where(filters)
        
        # 集計用SQL
        # 入数(irsu)が0またはNULLの場合は、バラ数(odsu)をそのまま加算するロジックとしています
        # 必要に応じて CAST(... AS INTEGER) で切り捨てを行っています
        sql = f"""
            SELECT 
                SUM(CASE 
                    WHEN T.cucd = 'D03' THEN 
                        (CASE WHEN C.irsu IS NULL OR C.irsu = 0 THEN CAST(T.odsu AS INTEGER) 
                              ELSE CAST(T.odsu AS INTEGER) / CAST(C.irsu AS INTEGER) END)
                    ELSE 0 
                END) as moriya_total,
                
                SUM(CASE 
                    WHEN T.cucd = 'D04' THEN 
                        (CASE WHEN C.irsu IS NULL OR C.irsu = 0 THEN CAST(T.odsu AS INTEGER) 
                              ELSE CAST(T.odsu AS INTEGER) / CAST(C.irsu AS INTEGER) END)
                    ELSE 0 
                END) as sayama_total

            FROM (
                SELECT cucd, bucd, vecd, dldt, cocd, odsu, deno, trdk 
                FROM DBA.dcnyu03 
                UNION ALL 
                SELECT cucd, bucd, vecd, dldt, cocd, odsu, deno, trdk 
                FROM DBA.dcnyu04
            ) AS T
            LEFT JOIN DBA.dc_batch_log AS L ON T.deno = L.deno_main
            LEFT JOIN DBA.comf1 AS M ON T.cocd = M.cocd
            -- 集計用に入数マスタを結合
            LEFT JOIN DBA.comf204 AS C ON T.cocd = C.cocd AND T.bucd = C.bucd
            WHERE 1=1
            {where_sql}
        """
        
        cursor.execute(sql, params)
        row = cursor.fetchone()
        
        summary = {
            'moriya': 0,
            'sayama': 0
        }
        if row:
            summary['moriya'] = int(row[0]) if row[0] else 0
            summary['sayama'] = int(row[1]) if row[1] else 0
            
        return summary
    finally:
        cursor.close()
        conn.close()
# ==========================================
# 1. 一覧・検索機能 (履歴テーブル結合版)
# ==========================================
def get_voucher_list(filters, is_export=False):
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()

    try:
        # 内部でSELECTする列 (dcnyuテーブルの列)
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
                M.mnam as manufacturer,
                
                -- ★追加: ログテーブルからバッチIDを取得
                L.batch_id as batch_id

            FROM (
                SELECT {inner_columns} FROM DBA.dcnyu03
                UNION ALL
                SELECT {inner_columns} FROM DBA.dcnyu04
            ) AS T
            -- ★追加: 履歴テーブル(dc_batch_log)と結合
            LEFT JOIN DBA.dc_batch_log AS L ON T.deno = L.deno_main

            LEFT JOIN DBA.venmf AS V ON T.vecd = V.vecd
            LEFT JOIN DBA.nammf04 AS N ON T.bucd = N.bucd AND N.brcd = '00'
            LEFT JOIN DBA.comf1 AS M ON T.cocd = M.cocd
            WHERE 1=1
        """

        params = []
        
        # ★変更: import_id ではなく batch_id で検索
        if filters.get('batch_id'): 
            sql += " AND L.batch_id = ?"
            params.append(filters['batch_id'])
        
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
            'voucher_id': 'T.deno', 
            'batch_id': 'L.batch_id', # ★追加
            'dept_code': 'T.bucd',
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
            
            if not row_dict.get('batch_id'):
                 row_dict['batch_id'] = ''
                 
            results.append(row_dict)
        return results
    finally:
        cursor.close()
        conn.close()

def get_filter_options():
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()
    try:
        # ★変更: import_ids を廃止し、batch_options を追加
        options = {'batch_options': [], 'centers': [], 'depts': [], 'vendors': []}
        
        # -------------------------------------------------------
        # 1. バッチIDの選択肢を取得 (dc_batch_log から)
        # -------------------------------------------------------
        # 最新の日付順に取得
        sql_batch = """
            SELECT batch_id, MAX(rgdt) as run_time 
            FROM DBA.dc_batch_log 
            GROUP BY batch_id 
            ORDER BY run_time DESC
        """
        cursor.execute(sql_batch)
        
        batch_list = []
        for r in cursor.fetchall():
            b_id = r[0].strip()
            r_time = r[1]
            
            # 表示用ラベルを作成: "12/15 13:00 (abc...)" のような形式
            if isinstance(r_time, datetime.datetime):
                time_str = r_time.strftime('%m/%d %H:%M')
            else:
                time_str = "日時不明"
            
            # IDが長いので短縮表示
            short_id = b_id[:5] + ".."
            
            batch_list.append({
                'val': b_id,
                'text': f"{time_str} ({short_id})"
            })
            
        options['batch_options'] = batch_list
        
        # -------------------------------------------------------
        # 2. センター (既存のまま)
        # -------------------------------------------------------
        cursor.execute("SELECT cucd FROM DBA.dcnyu03 UNION SELECT cucd FROM DBA.dcnyu04")
        raw_centers = [r[0] for r in cursor.fetchall() if r[0]]
        options['centers'] = sorted(list(set([_get_center_name(c) for c in raw_centers])))
        
        # -------------------------------------------------------
        # 3. 部門 (既存のまま)
        # -------------------------------------------------------
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
        
        # -------------------------------------------------------
        # 4. 取引先 (既存のまま)
        # -------------------------------------------------------
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
        
        if not rows:
            return None
        
        first_row = dict(zip(cols, rows[0]))
        
        # 値引伝票の検索
        sql_neb = "SELECT DISTINCT deno FROM DBA.dcneb WHERE deno11 = ? AND trdk = '13'"
        cursor.execute(sql_neb, [voucher_id])
        neb_row = cursor.fetchone()
        discount_id_val = neb_row[0].strip() if neb_row else ''

        # 日付の整形
        d_date = first_row.get('delivery_date')
        if isinstance(d_date, (datetime.date, datetime.datetime)): 
            d_date = d_date.strftime('%Y/%m/%d')
            
        c_name = _get_center_name(first_row.get('center_code', ''))
        
        # データの初期化
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
        
        # --- ここから明細ループ (インデントに注意) ---
        for row in rows:
            d = dict(zip(cols, row))
            
            # 数量・入数の取得
            total_qty_loose = int(d.get('order_qty') or 0)
            per_case = int(d.get('per_case') or 0)
            
            # ケース数の計算 (整数)
            if per_case > 0:
                calc_cases = total_qty_loose // per_case 
            else:
                calc_cases = 0

            cost_unit = d.get('cost_price') or 0
            discount = d.get('total_disc') or 0
            
            # 行ごとの金額計算
            row_total = (total_qty_loose * cost_unit) - discount
            
            # 合計値への加算 (ループ内で毎回実行)
            total_cases += calc_cases
            total_cost += row_total
            
            # 明細リストへの追加
            data['details'].append({
                'p_code': d.get('item_code', '').strip(), 
                'jan': d.get('jan') or '', 
                'p_name': d.get('p_name') or '', 
                'spec': d.get('spec') or '', 
                'manufacturer': d.get('manufacturer') or '', 
                'per_case': per_case, 
                'loose': total_qty_loose,
                'case': calc_cases,
                'cost': "{:,.2f}".format(cost_unit), 
                'row_total': "{:,}".format(int(row_total)), 
                'discount': "{:,}".format(int(discount))
            })

        # --- ループ終了後、合計値をセット (forと同じ高さにする) ---
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
# 4. CSVアップロード処理 (バラ数入力・ケース計算・余りチェック版)
# ==========================================
def process_upload_csv(csv_rows):
    """
    CSVを全行チェック。
    【変更点】
    - 数量(7列目)を「バラ総数」として読み込む
    - マスタの入数で割り、ケース数を計算する
    - 割り切れない(余りが出る)場合はエラーにする
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
            raw_date    = clean_str(row[1])
            vendor_code = clean_str(row[2])
            fee_md      = clean_str(row[3])
            fee_dc      = clean_str(row[4])
            item_code   = clean_str(row[5])
            raw_qty     = clean_str(row[6]) # ★ここは「バラ数」として扱う
            raw_cost    = clean_str(row[7])
            pass_flag   = clean_str(row[8])
            raw_disc    = clean_str(row[9])

            # --- 2. バリデーション & 型変換 ---

            # A. 日付チェック
            formatted_date = ""
            date_formats = ['%Y/%m/%d', '%Y-%m-%d']
            date_obj = None
            
            for fmt in date_formats:
                try:
                    date_obj = datetime.datetime.strptime(raw_date, fmt)
                    break 
                except ValueError:
                    continue
            
            if date_obj:
                formatted_date = date_obj.strftime('%Y/%m/%d')
            else:
                error_list.append(f"{line_no}行目: 納品日 '{raw_date}' の形式が不正です。")

            # B. センターコード
            if center_code not in ['D03', 'D04']:
                error_list.append(f"{line_no}行目: センターコード '{center_code}' が不正です。")

            # C. 数値変換 (バラ数として取得)
            try:
                # ★修正: ここは「バラ総数」
                qty_loose_input = int(float(raw_qty))
            except ValueError:
                error_list.append(f"{line_no}行目: 納品数 '{raw_qty}' は数値で入力してください。")
                qty_loose_input = 0
            
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

            # --- ★追加: ケース計算と余りチェック ---
            calc_cases = 0
            
            if not error_list:
                if per_case > 0:
                    # 割り算の余りをチェック
                    remainder = qty_loose_input % per_case
                    if remainder != 0:
                        error_list.append(f"{line_no}行目: 納品数({qty_loose_input})が入数({per_case})で割り切れません。ケース単位になるよう修正してください。")
                    else:
                        # 割り切れるならケース数を計算
                        calc_cases = qty_loose_input // per_case
                else:
                    # 入数0の場合はケース計算できない（あるいはバラ=ケース？）
                    # ここでは便宜上、ケース数=0 または エラーにする運用などありますが、
                    # 入数未登録商品の場合はエラーにしないよう、ケース数0のまま進めます。
                    calc_cases = 0
                    # もし入数0をエラーにしたいなら以下を解除
                    # error_list.append(f"{line_no}行目: 商品の入数がマスタに設定されていません。")

            # --- エラーがなければリストに追加 ---
            if not error_list: 
                # 金額計算 (バラ総数 × 単価)
                row_cost_total = (qty_loose_input * cost_unit)
                row_disc_total = (qty_loose_input * disc_unit)

                detail_row = [
                    item_code, jan, p_name, spec, manufacturer,
                    qty_loose_input, # [5] バラ総数 (CSV値)
                    calc_cases,      # [6] 計算したケース数
                    fee_md, fee_dc,
                    "{:,.2f}".format(cost_unit),
                    "{:,.0f}".format(row_cost_total),
                    "{:,.0f}".format(row_disc_total)
                ]

                processed_list.append({
                    'center_name': '守谷C' if 'D03' in center_code else '狭山日高C',
                    'delivery_date': formatted_date,
                    'vendor_code': vendor_code,
                    'vendor_name': vendor_name,
                    'dept_code': dept_code,
                    'dept_name': dept_name,
                    'manufacturer': manufacturer,
                    'detail_row': detail_row,
                    'raw_case': calc_cases, # 集計用には計算したケース数を使う
                    'pass_flag': pass_flag
                })

    except Exception as e:
        error_list.append(f"データ処理中に予期せぬエラーが発生しました: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return processed_list, error_list

# ==========================================
# 5. データ登録実行 (6行分割・排他制御・履歴記録)
# ==========================================
def insert_voucher_data(data_list, user_id, batch_id): # ★変更: batch_idを追加
    """
    データリストを受け取り、以下のルールで登録する。
    1. ベンダー > 納品日 > センター > 部門 でグルーピング
    2. 商品コード順にソート
    3. 6行ごとに伝票を分割 (ページング)
    4. 排他制御を行いながら採番
    5. 作成した伝票番号をログテーブルに保存
    """
    # 業務時間チェック
    time_error = check_business_time('normal')
    if time_error:
        raise Exception(time_error)

    conn = get_connection('master')
    conn.autocommit = False 
    cursor = conn.cursor()

    total_vouchers = 0
    
    try:
        # 1. 伝票単位のキーでグルーピング
        # ★修正: 順序を [ベンダー > 納品日 > センター > 部門] に変更
        key_func = lambda x: (x['vendor_code'], x['delivery_date'], x['center_name'], x['dept_code'])
        
        # groupbyの前にキーでソートが必要
        data_list.sort(key=key_func)

        # ★修正: key_funcの順序に合わせて、受け取る変数の順番も入れ替え
        for (v_code, d_date, center_name, dept_code), items in groupby(data_list, key=key_func):
            # イテレータをリスト化
            items = list(items)
            
            # 商品番号順にソート（ここは変更なし）
            items.sort(key=lambda x: x['detail_row'][0])

            # 挿入先テーブルの決定
            target_table = "DBA.dcnyu03" if "守谷" in center_name else "DBA.dcnyu04"
            center_code_db = "D03" if "守谷" in center_name else "D04"
            today_str = datetime.datetime.now().strftime('%Y/%m/%d')
            now_time_str = datetime.datetime.now().strftime('%H:%M:%S')

            # =================================================
            # ★ 6行ごとの分割処理 (Chunking)
            # =================================================
            chunk_size = 6
            # 必要な伝票枚数を計算
            total_chunks = math.ceil(len(items) / chunk_size)

            for i in range(total_chunks):
                # リストを6行分切り出す
                start_idx = i * chunk_size
                end_idx = start_idx + chunk_size
                chunk_items = items[start_idx:end_idx]

                # ---------------------------------------------
                # A. 仕入伝票番号の採番 (ロック -> 取得 -> 解除)
                # ---------------------------------------------
                main_voucher_id = _get_next_number_real(cursor, 'purchase')
                
                # ---------------------------------------------
                # B. 仕入伝票(親)のINSERT
                # ---------------------------------------------
                line_no = 1
                for row in chunk_items:
                    d = row['detail_row']
                    
                    sql_insert_main = f"""
                        INSERT INTO {target_table} (
                            deno, no, cucd, bucd, vecd, dldt, 
                            cocd, odsu, dltn, prtn, 
                            md, dc, thrflg, sign, rgdt, upti,
                            oddt, trdk, updt
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, ?,
                            ?, ?, ?
                        )
                    """
                    
                    val_discount = float(d[11].replace(',',''))
                    
                    params_main = [
                        main_voucher_id,    # deno
                        line_no,            # no (1～6)
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
                        row.get('pass_flag', '0'), # thrflg
                        user_id,            # sign
                        today_str,          # rgdt
                        now_time_str,       # upti
                        today_str,          # oddt
                        '11',               # trdk (仕入=11固定)
                        today_str           # updt
                    ]
                    
                    cursor.execute(sql_insert_main, params_main)
                    line_no += 1
                
                total_vouchers += 1

                # ---------------------------------------------
                # C. 値引伝票の処理 (値引がある場合のみ)
                # ---------------------------------------------
                total_discount_in_chunk = sum(float(x['detail_row'][11].replace(',','')) for x in chunk_items)
                
                # ログ保存用に値引IDを記憶する変数 (値引なしならNone)
                created_discount_id = None 

                if total_discount_in_chunk > 0:
                    # 値引伝票番号の採番
                    discount_voucher_id = _get_next_number_real(cursor, 'discount')
                    created_discount_id = discount_voucher_id # ★ログ用に確保
                    
                    line_no_neb = 1
                    for row in chunk_items:
                        d = row['detail_row'] 
                        val_disc_total = float(d[11].replace(',',''))
                        
                        if val_disc_total > 0:
                            val_qty = float(d[5]) 
                            val_cost_unit = float(d[9].replace(',',''))
                            val_disc_unit = val_disc_total / val_qty if val_qty != 0 else 0

                            sql_insert_neb = """
                                INSERT INTO DBA.dcneb (
                                    deno, deno11, no, cucd, bucd, vecd, dldt, oddt, trdk,
                                    cocd, odsu, dltn, md, dc, nebtan, nebkn, 
                                    thrflg, conf, sign, rgdt_k, updt_k, upti_k
                                ) VALUES (
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                                )
                            """
                            
                            params_neb = [
                                discount_voucher_id, main_voucher_id, str(line_no_neb),
                                center_code_db, dept_code, v_code, d_date, today_str, '13',
                                d[0], val_qty, val_cost_unit, d[7], d[8], val_disc_unit, val_disc_total,
                                '', '1', user_id, today_str, today_str, now_time_str
                            ]
                            cursor.execute(sql_insert_neb, params_neb)
                            line_no_neb += 1

                # =================================================
                # ★追加: ここで「今回の伝票セット」を履歴テーブルに保存
                # =================================================
                sql_log = """
                    INSERT INTO DBA.dc_batch_log (
                        batch_id, user_id, deno_main, deno_neb, center, rgdt
                    ) VALUES (?, ?, ?, ?, ?, CURRENT TIMESTAMP)
                """
                cursor.execute(sql_log, [
                    batch_id,           # 引数で受け取ったID
                    user_id,            # ユーザーID
                    main_voucher_id,    # さっき採番した仕入伝票番号
                    created_discount_id,# 値引伝票番号 (なければNone)
                    center_code_db      # D03 or D04
                ])

        conn.commit()
        return f"登録完了: {total_vouchers}件の伝票を作成しました。"

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

# ==========================================
# (内部関数) 実採番ロジック・排他制御
# ==========================================
def _get_next_number_real(cursor, num_type):
    """
    仕様に基づいた採番処理
    1. excluflg でロックを取得 (最大20秒待機)
    2. ctlmf / henctlmf から番号を取得・更新
    3. excluflg のロックを解除
    """
    
    # ターゲットのテーブル設定
    if num_type == 'purchase':
        tbl_name = "DBA.ctlmf"
        col_name = "deno_j"
        max_val = 999999
        width = 6
    else:
        tbl_name = "DBA.henctlmf"
        col_name = "hendeno_j"
        max_val = 39999 # 値引は39999の次が00000
        width = 5

    # ------------------------------------
    # Step 1. 排他ロック取得 (excluflg)
    # ------------------------------------
    _lock_sequence(cursor)

    try:
        # ------------------------------------
        # Step 2. 現在の番号を取得
        # ------------------------------------
        cursor.execute(f"SELECT {col_name} FROM {tbl_name}")
        row = cursor.fetchone()
        if not row:
            raise Exception(f"採番エラー: {tbl_name} のデータが見つかりません")
        
        current_val = int(row[0])

        # ------------------------------------
        # Step 3. ルールに基づいてカウントアップ
        # ------------------------------------
        # 上限に達していたら 0 にリセット
        if current_val >= max_val:
            next_val = 0
        else:
            next_val = current_val + 1
            
        # 000000 は使わないので、もし0なら 1 にする
        if next_val == 0:
            next_val = 1

        # ------------------------------------
        # Step 4. テーブル更新
        # ------------------------------------
        formatted_num = str(next_val).zfill(width)
        
        cursor.execute(f"UPDATE {tbl_name} SET {col_name} = ?", [formatted_num])
        
        return formatted_num

    finally:
        # ------------------------------------
        # Step 5. ロック解除 (必ず通る道で)
        # ------------------------------------
        _unlock_sequence(cursor)


def _lock_sequence(cursor):
    """
    excluflg テーブルを利用したロック取得
    flg_deno が NULL になるまで待機し、
    connection_property('number') を書き込む。
    
    ★ 20秒経過しても取れなければエラーにする
    """
    max_wait_seconds = 20  # 最大待機秒数
    interval = 0.5         # ポーリング間隔(秒)
    
    start_time = time.time()
    
    while True:
        # 1. 現在時刻チェック
        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            raise Exception("他端末で採番処理中のため、タイムアウトしました。(20秒経過)")
        
        # 2. ロック取得試行
        # NULLの行を探して、自分の接続番号で更新する
        # rowcount > 0 なら更新成功(=ロック取得)
        sql = """
            UPDATE DBA.excluflg 
            SET flg_deno = connection_property('number') 
            WHERE flg_deno IS NULL
        """
        cursor.execute(sql)
        
        if cursor.rowcount > 0:
            # ロック成功！
            return
        
        # 3. 失敗したら少し待つ
        time.sleep(interval)


def _unlock_sequence(cursor):
    """
    ロック解除
    自分の接続番号でロックしたはずだが、
    単純に flg_deno を NULL に戻す処理を行う
    """
    # 念のため、自分のロックだけを解除するようにWHERE句をつけても良いが、
    # 仕様上「終わったらNULLにする」なので全解除で実装
    # (もし厳密にするなら WHERE flg_deno = connection_property('number'))
    
    sql = "UPDATE DBA.excluflg SET flg_deno = NULL"
    # または厳密版:
    # sql = "UPDATE DBA.excluflg SET flg_deno = NULL WHERE flg_deno = connection_property('number')"
    
    cursor.execute(sql)
# ==========================================
# (内部関数) 採番ロジック
# ==========================================
def _get_next_number(cursor, num_type):
    """
    【接続テスト用】
    本番は6桁とのことなので、エラーにならないよう6桁のランダムな値を返す。
    ※本番稼働前には、正式な採番テーブル(SQL)を使うロジックに差し替えること。
    """
    import random
    
    # 6桁 (100000 ～ 999999) の範囲で適当な番号を生成
    # テストデータだと分かりやすいように 900000番台 などにしてもOK
    val = random.randint(100000, 999999)
    
    return str(val)

# ==========================================
# ワークテーブル操作 (DC_IN_CSV)
# ==========================================

def save_to_work_table(batch_id, user_id, data_list):
    """
    解析済みデータ(data_list)をワークテーブル DC_IN_CSV にINSERTする
    """
    conn = get_connection('master')
    cursor = conn.cursor()
    try:
        # ★追加: 同じユーザーの古い一時データを削除 (退職者のデータは残るが、それは許容)
        cursor.execute("DELETE FROM DC_IN_CSV WHERE user_id = ?", [user_id])
        sql = """
            INSERT INTO DC_IN_CSV (
                batch_id, line_no, user_id,
                
                -- CSV項目
                center_code, delivery_date, vendor_code, 
                fee_md, fee_dc, item_code, 
                qty_case, cost_unit, pass_flag, disc_unit,
                
                -- マスタ補完項目
                center_name, vendor_name, dept_code, dept_name,
                item_name, spec, manufacturer, jan_code, per_case,
                
                -- 計算項目
                qty_loose_total, cost_total, disc_total
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?
            )
        """

        for i, row in enumerate(data_list, 1):
            # process_upload_csv で作った detail_row リストの中身を取り出す
            # 構造: [0]商品CD, [1]JAN, [2]商品名, [3]規格, [4]メーカー, 
            #       [5]総数, [6]ケース, [7]MD, [8]DC, 
            #       [9]原単価(str), [10]原価合計(str), [11]値引合計(str)
            d_row = row['detail_row']
            
            # 文字列の金額を数値に戻す
            val_cost_total = float(d_row[10].replace(',', ''))
            val_disc_total = float(d_row[11].replace(',', ''))
            val_cost_unit  = float(d_row[9].replace(',', ''))
            
            # disc_unit (値引単価) は detail_row に含まれていないため、
            # 本来は process_upload_csv で保持すべきですが、
            # ここでは簡易的に「値引合計 ÷ 総数」で逆算するか、一旦 0 で登録します。
            # (もしCSVの生の値が必要なら process_upload_csv の戻り値に追加修正が必要です)
            val_disc_unit = 0
            if d_row[5] > 0:
                val_disc_unit = val_disc_total / d_row[5]

            params = [
                batch_id,           # batch_id
                i,                  # line_no
                user_id,            # user_id
                
                # --- CSV項目 ---
                "D03" if "守谷" in row['center_name'] else "D04", # center_code
                row['delivery_date'], # delivery_date
                row['vendor_code'],   # vendor_code
                d_row[7],             # fee_md
                d_row[8],             # fee_dc
                d_row[0],             # item_code
                row['raw_case'],      # qty_case
                val_cost_unit,        # cost_unit
                row['pass_flag'],     # pass_flag
                val_disc_unit,        # disc_unit
                
                # --- マスタ補完 ---
                row['center_name'],
                row['vendor_name'],
                row['dept_code'],
                row['dept_name'],
                d_row[2], # item_name
                d_row[3], # spec
                row['manufacturer'],
                d_row[1], # jan_code
                0,        # per_case (取得元がない場合は0、あれば入れる)

                # --- 計算 ---
                d_row[5],       # qty_loose_total
                val_cost_total, # cost_total
                val_disc_total  # disc_total
            ]
            
            cursor.execute(sql, params)
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def get_data_from_work_table(batch_id):
    """
    ワークテーブルからデータを取得し、
    process_upload_csv の戻り値と同じ辞書リスト形式に復元して返す。
    (確認画面の表示や、本番登録処理で再利用するため)
    """
    conn = get_connection('master')
    cursor = conn.cursor()
    try:
        # 必要な列を全取得
        sql = """
            SELECT 
                center_name, delivery_date, vendor_code, vendor_name,
                dept_code, dept_name, manufacturer, pass_flag,
                item_code, jan_code, item_name, spec,
                qty_loose_total, qty_case, fee_md, fee_dc,
                cost_unit, cost_total, disc_total,
                user_id
            FROM DC_IN_CSV
            WHERE batch_id = ?
            ORDER BY line_no
        """
        cursor.execute(sql, [batch_id])
        rows = cursor.fetchall()
        
        reconstructed_list = []
        
        for r in rows:
            # DBから取得した値を、HTML表示用(detail_row)のリスト形式に戻す
            # フォーマット: "{:,.2f}" などでカンマ区切り文字列にする
            
            detail_row = [
                r[8],  # item_code
                r[9],  # jan_code
                r[10], # item_name
                r[11], # spec
                r[6],  # manufacturer
                r[12], # qty_loose_total
                r[13], # qty_case
                r[14], # fee_md
                r[15], # fee_dc
                "{:,.2f}".format(r[16]) if r[16] else "0.00", # cost_unit
                "{:,.0f}".format(r[17]) if r[17] else "0",    # cost_total
                "{:,.0f}".format(r[18]) if r[18] else "0"     # disc_total
            ]

            reconstructed_list.append({
                'center_name': r[0],
                'delivery_date': r[1],
                'vendor_code': r[2],
                'vendor_name': r[3],
                'dept_code': r[4],
                'dept_name': r[5],
                'manufacturer': r[6],
                'pass_flag': r[7],
                'raw_case': r[13], # qty_caseと同じ
                'detail_row': detail_row,
                'user_id': r[19]   # 登録用に追加で持たせておく
            })
            
        return reconstructed_list
    finally:
        cursor.close()
        conn.close()

def delete_work_table(batch_id):
    """
    処理が終わったバッチIDのデータを削除
    """
    conn = get_connection('master')
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM DC_IN_CSV WHERE batch_id = ?", [batch_id])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

# ==========================================
# 6. 上限数管理 (edit_limits) 関連ロジック
# ==========================================

def get_limits_by_date(target_date_str):
    """
    指定日の上限設定を取得する
    戻り値: {'m_limit': int, 's_limit': int} (m=守谷/D03, s=狭山/D04)
    """
    conn = get_connection('master')
    cursor = conn.cursor()
    try:
        sql = """
            SELECT cucd, max_qty 
            FROM DBA.dc_limit_master
            WHERE tgt_date = ?
        """
        cursor.execute(sql, [target_date_str])
        rows = cursor.fetchall()
        
        # デフォルト値 (設定がない場合は0)
        result = {'m_limit': 0, 's_limit': 0}
        
        for r in rows:
            cucd = r[0].strip()
            qty = r[1]
            if cucd == 'D03':
                result['m_limit'] = qty
            elif cucd == 'D04':
                result['s_limit'] = qty
                
        return result
    finally:
        cursor.close()
        conn.close()

def save_limits(target_date_str, s_val, m_val, scope):
    """
    上限数を保存する
    scope: 'single' (その日のみ) or 'month' (その日から月末まで一括)
    """
    conn = get_connection('master')
    cursor = conn.cursor()
    
    # 文字列できた数値をintに変換 (空文字やカンマ対策)
    try:
        val_m = int(str(m_val).replace(',', ''))
    except:
        val_m = 0
        
    try:
        val_s = int(str(s_val).replace(',', ''))
    except:
        val_s = 0

    try:
        # 更新対象の日付リストを作成
        date_list = [target_date_str]
        
        if scope == 'month':
            # ターゲット日付のオブジェクト化
            dt = datetime.datetime.strptime(target_date_str, '%Y/%m/%d')
            year = dt.year
            month = dt.month
            # その月の最終日を取得
            last_day = calendar.monthrange(year, month)[1]
            
            # 翌日から月末までの日付を追加
            current_day = dt.day + 1
            while current_day <= last_day:
                next_date = datetime.date(year, month, current_day)
                date_list.append(next_date.strftime('%Y/%m/%d'))
                current_day += 1

        # 対象日すべてに対して Upsert (DELETE -> INSERT) を実行
        for d_str in date_list:
            # 1. まず既存の設定を消す
            cursor.execute("DELETE FROM DBA.dc_limit_master WHERE tgt_date = ?", [d_str])
            
            # 2. 守谷(D03)の登録 (0より大きい場合のみ)
            if val_m > 0:
                cursor.execute("""
                    INSERT INTO DBA.dc_limit_master (tgt_date, cucd, max_qty, reg_date)
                    VALUES (?, 'D03', ?, CURRENT TIMESTAMP)
                """, [d_str, val_m])
                
            # 3. 狭山(D04)の登録 (0より大きい場合のみ)
            if val_s > 0:
                cursor.execute("""
                    INSERT INTO DBA.dc_limit_master (tgt_date, cucd, max_qty, reg_date)
                    VALUES (?, 'D04', ?, CURRENT TIMESTAMP)
                """, [d_str, val_s])

        conn.commit()
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def get_monthly_limits(start_date_str):
    import sys
    import datetime
    import calendar
    
    conn = get_connection('master')
    cursor = conn.cursor()
    
    try:
        # 日付計算
        dt = datetime.datetime.strptime(start_date_str, '%Y/%m/%d')
        first_day_str = dt.strftime('%Y-%m-%d')
        end_date = dt + datetime.timedelta(days=31)
        last_day_str = end_date.strftime('%Y-%m-%d')

        # 1. データ枠作成
        data_map = {}
        current_date = dt
        while current_date <= end_date:
            d_str = current_date.strftime('%Y/%m/%d')
            data_map[d_str] = {
                'm_limit': 0, 'm_jv': 0, 'm_reg': 0, 'm_total': 0,
                's_limit': 0, 's_jv': 0, 's_reg': 0, 's_total': 0
            }
            current_date += datetime.timedelta(days=1)

        # 2. 実績データ取得
        sql_actual = f"""
            SELECT T.dldt, T.cucd, T.is_jv, SUM(T.calc_case)
            FROM (
                -- 守谷 (D03)
                SELECT A.dldt, A.cucd, 
                    (CASE WHEN M1.mnam LIKE 'JV%' THEN 1 ELSE 0 END) AS is_jv,
                    (CASE WHEN M.irsu IS NULL OR M.irsu = 0 THEN A.odsu ELSE CAST(A.odsu AS NUMERIC) / M.irsu END) AS calc_case
                FROM DBA.dcnyu03 A
                LEFT JOIN DBA.comf204 M ON A.cocd = M.cocd AND A.bucd = M.bucd
                LEFT JOIN DBA.comf1 M1  ON A.cocd = M1.cocd
                WHERE A.dldt BETWEEN '{first_day_str}' AND '{last_day_str}'
                UNION ALL
                -- 狭山 (D04)
                SELECT A.dldt, A.cucd, 
                    (CASE WHEN M1.mnam LIKE 'JV%' THEN 1 ELSE 0 END) AS is_jv,
                    (CASE WHEN M.irsu IS NULL OR M.irsu = 0 THEN A.odsu ELSE CAST(A.odsu AS NUMERIC) / M.irsu END) AS calc_case
                FROM DBA.dcnyu04 A
                LEFT JOIN DBA.comf204 M ON A.cocd = M.cocd AND A.bucd = M.bucd
                LEFT JOIN DBA.comf1 M1  ON A.cocd = M1.cocd
                WHERE A.dldt BETWEEN '{first_day_str}' AND '{last_day_str}'
            ) AS T
            GROUP BY T.dldt, T.cucd, T.is_jv
        """
        cursor.execute(sql_actual)
        for r in cursor.fetchall():
            if isinstance(r[0], (datetime.date, datetime.datetime)): d_val = r[0].strftime('%Y/%m/%d')
            else: d_val = str(r[0]).replace('-', '/')
            cucd = r[1].strip()
            is_jv = int(r[2])
            qty = int(float(r[3])) if r[3] is not None else 0
            if d_val in data_map:
                target = 'm' if cucd == 'D03' else 's'
                data_map[d_val][f'{target}_total'] += qty
                if is_jv == 1: data_map[d_val][f'{target}_jv'] += qty
                else: data_map[d_val][f'{target}_reg'] += qty

        # 3. 上限値取得
        sql_limits = f"""
            SELECT tgt_date, cucd, max_qty FROM DBA.dc_limit_master
            WHERE tgt_date BETWEEN '{first_day_str}' AND '{last_day_str}'
        """
        cursor.execute(sql_limits)
        for lr in cursor.fetchall():
            if isinstance(lr[0], (datetime.date, datetime.datetime)): ld_val = lr[0].strftime('%Y/%m/%d')
            else: ld_val = str(lr[0]).replace('-', '/')
            l_cucd = lr[1].strip()
            l_qty = int(lr[2])
            if ld_val in data_map:
                if l_cucd == 'D03': data_map[ld_val]['m_limit'] = l_qty
                elif l_cucd == 'D04': data_map[ld_val]['s_limit'] = l_qty

        # 4. 結果リスト作成 (★ここで is_sunday を追加)
        result_list = []
        for date_key in sorted(data_map.keys()):
            item = data_map[date_key]
            
            # 曜日判定 (6が日曜日)
            dt_obj = datetime.datetime.strptime(date_key, '%Y/%m/%d')
            is_sunday = (dt_obj.weekday() == 6)

            result_list.append({
                'date': date_key,
                'is_sunday': is_sunday, # ★これを使います
                
                's_jv_sched': item['s_jv'], 's_reg_sched': item['s_reg'],
                's_total_sched': item['s_total'], 's_limit': item['s_limit'],
                
                'm_jv_sched': item['m_jv'], 'm_reg_sched': item['m_reg'],
                'm_total_sched': item['m_total'], 'm_limit': item['m_limit']
            })
            
        return result_list

    except Exception as e:
        raise e
    finally:
        cursor.close()
        conn.close()
        # ※ import文 (from sqlalchemy import text) は不要です。削除してください。

# ==========================================
# 7. 出荷予定取得ロジック (シンプル結合版)
# ==========================================
def get_shipment_data(start_date):
    """
    指定日以降の出荷予定を、定番・JV別に集計して返す。
    商品コードでCOMF204を結合し、入数(irsu)でケース換算する。
    """
    conn = get_connection('master')
    cursor = conn.cursor()
    
    try:
        centers = [
            {'prefix': 's', 'table': 'DCSHAC'},  # 狭山日高
            {'prefix': 'm', 'table': 'DCYHAC'}   # 守谷
        ]
        
        ship_data = {}

        for center in centers:
            # SQL構築
            # 余計なサブクエリを削除し、cocdだけでシンプルに結合します
            sql = f"""
                SELECT 
                    T.dldt,
                    
                    -- JVのケース数集計
                    SUM(CASE 
                        WHEN SUBSTRING(M1.mnam, 1, 2) = 'JV' THEN 
                            (CASE 
                                WHEN M2.irsu IS NULL OR M2.irsu = 0 THEN T.odsu 
                                ELSE CAST(T.odsu AS NUMERIC) / M2.irsu 
                            END)
                        ELSE 0 
                    END) AS jv_cases,
                    
                    -- 定番のケース数集計
                    SUM(CASE 
                        WHEN SUBSTRING(M1.mnam, 1, 2) = 'JV' THEN 0 
                        ELSE 
                            (CASE 
                                WHEN M2.irsu IS NULL OR M2.irsu = 0 THEN T.odsu 
                                ELSE CAST(T.odsu AS NUMERIC) / M2.irsu 
                            END)
                    END) AS reg_cases

                FROM {center['table']} T
                LEFT JOIN COMF1 M1 
                       ON T.cocd = M1.cocd
                LEFT JOIN COMF204 M2 
                       ON T.cocd = M2.cocd -- 商品コードのみで結合

                WHERE T.dldt >= '{start_date}'
                GROUP BY T.dldt
            """

            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                d_val = row[0]
                jv_val = row[1]
                reg_val = row[2]

                if isinstance(d_val, (datetime.date, datetime.datetime)):
                    d_str = d_val.strftime('%Y/%m/%d')
                else:
                    d_str = str(d_val).replace('-', '/')
                
                if d_str not in ship_data:
                    ship_data[d_str] = {}

                # 整数(int)として格納
                ship_data[d_str][f"{center['prefix']}_jv_ship"] = int(jv_val or 0)
                ship_data[d_str][f"{center['prefix']}_reg_ship"] = int(reg_val or 0)

        return ship_data

    finally:
        cursor.close()
        conn.close()