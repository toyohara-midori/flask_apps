import datetime
from .db_connection import get_connection

TARGET_DB = 'master'

CENTER_NAME_MAP = {
    'D03': '守谷C',
    'D04': '狭山日高C',
}

def _row_to_dict(cursor, row):
    return {col[0]: val for col, val in zip(cursor.description, row)}

def _get_center_name(code):
    return CENTER_NAME_MAP.get(code.strip(), code)

# ==========================================
# 1. 一覧・検索機能
# ==========================================

def get_voucher_list(filters, is_export=False):
    """
    is_export=False: 画面用。1行目のみ。
    is_export=True : CSV用。全行全項目。
    """
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()

    # 1. 内部クエリ用カラム (DBの物理名)
    inner_columns = """
        deno, cocd, no, cucd, bucd, oddt, dldt, trdk, vecd, 
        odsu, dltn, prtn, md, dc, thrflg, conf, sign, rgdt, updt, upti
    """

    sql = f"""
        SELECT 
            -- ▼ HTML(画面)が待っている名前に合わせる
            T.deno as voucher_id,
            T.no   as line_no,
            
            -- HTMLは {{ info.center }} を表示している
            CASE T.cucd
                WHEN 'D03' THEN '守谷C'
                WHEN 'D04' THEN '狭山日高C'
                ELSE T.cucd
            END as center,

            T.cucd as center_code,    -- CSV用にコードも残す
            T.dldt as delivery_date,
            T.bucd as dept_code,
            T.vecd as vendor_code,
            T.sign as operator,
            
            -- CSV用項目
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

            -- マスタ結合項目
            V.nmkj as vendor,
            N.nmkj as dept_name,
            -- HTMLは {{ info.first_p_name }} を待っている
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

    # --- フィルタ条件の組み立て ---

    # 1. 取込ID
    if filters.get('import_id'): 
        sql += " AND T.deno = ?"
        params.append(filters['import_id'])
    
    # 2. センター (ここが修正のキモ)
    # 画面から「守谷C」という文字列が来るので、DBの「D03」に変換して探す
    c_val = filters.get('center')
    if c_val:
        if '守谷' in c_val or c_val == 'D03':
            sql += " AND T.cucd = 'D03'"
        elif '狭山' in c_val or '日高' in c_val or c_val == 'D04':
            sql += " AND T.cucd = 'D04'"
        else:
            # それ以外ならそのまま検索
            sql += " AND T.cucd = ?"
            params.append(c_val)

    # 3. 部門
    if filters.get('dept'):
        sql += " AND T.bucd = ?"
        params.append(filters['dept'])

    # 4. 取引先
    if filters.get('vendor'):
        sql += " AND T.vecd = ?"
        params.append(filters['vendor'])

    # 5. 納品日
    if filters.get('delivery_date'):
        sql += " AND T.dldt = ?"
        params.append(filters['delivery_date'])

    v_ids = filters.get('voucher_ids')
    if v_ids:
        # IDの数だけ ?,?,? を作る
        placeholders = ','.join('?' * len(v_ids))
        sql += f" AND T.deno IN ({placeholders})"
        # paramsリストにIDを追加
        params.extend(v_ids)

    # 6. 種別 (HTMLの value="jv" / "regular" に合わせる)
    t_val = filters.get('type')
    if t_val == 'jv':  
        # HTMLが小文字の 'jv' を送ってくるのでここで判定
        sql += " AND M.mnam LIKE 'JV%'"
    elif t_val == 'regular':
        # HTMLが 'regular' を送ってくる
        sql += " AND (M.mnam NOT LIKE 'JV%' OR M.mnam IS NULL)"

    # 画面表示用なら1行目だけ
    if not is_export:
        sql += " AND T.no = '1'"

    # --- ソート処理 ---
    sort_col = filters.get('sort', 'voucher_id')
    order_dir = filters.get('order', 'asc')
    
    # HTMLの onclick="applySort('p_name')" などに対応させる
    sort_map = {
        'voucher_id': 'T.deno',
        'import_id':  'T.deno',
        'dept_code':  'T.bucd',
        'dept_name':  'N.nmkj',
        'center':     'T.cucd',
        'delivery_date': 'T.dldt',
        'vendor_code': 'T.vecd',
        'vendor':     'V.nmkj',
        'p_name':     'M.hnam',      # HTMLからのキー 'p_name' に対応
        'manufacturer': 'M.mnam'
    }
    sql_sort = sort_map.get(sort_col, 'T.deno')
    sql += f" ORDER BY {sql_sort} {order_dir}"

    try:
        cursor.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = {}
            for col, val in zip(columns, row):
                if isinstance(val, str):
                    row_dict[col] = val.strip() # 空白カット！
                else:
                    row_dict[col] = val
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
        sql_center = "SELECT cucd FROM DBA.dcnyu03 UNION SELECT cucd FROM DBA.dcnyu04"
        cursor.execute(sql_center)
        raw_centers = [r[0] for r in cursor.fetchall() if r[0]]
        options['centers'] = sorted(list(set([_get_center_name(c) for c in raw_centers])))
        sql_dept = "SELECT DISTINCT N.nmkj FROM (SELECT bucd FROM DBA.dcnyu03 UNION SELECT bucd FROM DBA.dcnyu04) AS T LEFT JOIN DBA.nammf04 AS N ON T.bucd = N.bucd AND N.brcd = '00' ORDER BY N.nmkj"
        cursor.execute(sql_dept)
        options['depts'] = [r[0] for r in cursor.fetchall() if r[0]]
        sql_vendor = "SELECT DISTINCT V.nmkj FROM (SELECT vecd FROM DBA.dcnyu03 UNION SELECT vecd FROM DBA.dcnyu04) AS T LEFT JOIN DBA.venmf AS V ON T.vecd = V.vecd ORDER BY V.nmkj"
        cursor.execute(sql_vendor)
        options['vendors'] = [r[0] for r in cursor.fetchall() if r[0]]
        return options
    finally:
        cursor.close()
        conn.close()

def get_voucher_detail(voucher_id):
    conn = get_connection(TARGET_DB)
    cursor = conn.cursor()
    try:
        sql = """
            SELECT T.*, M.hnam as p_name, M.kika as spec, M.mnam as manufacturer, C2.irsu as per_case, C2.janc as jan, V.nmkj as vendor_name, N.nmkj as dept_name
            FROM (SELECT *, '03' as tbl_src FROM DBA.dcnyu03 WHERE deno = ? UNION ALL SELECT *, '04' as tbl_src FROM DBA.dcnyu04 WHERE deno = ?) AS T
            LEFT JOIN DBA.comf1 AS M ON T.cocd = M.cocd
            LEFT JOIN DBA.comf204 AS C2 ON T.cocd = C2.cocd AND T.bucd = C2.bucd
            LEFT JOIN DBA.venmf AS V ON T.vecd = V.vecd
            LEFT JOIN DBA.nammf04 AS N ON T.bucd = N.bucd AND N.brcd = '00'
            ORDER BY T.no
        """
        cursor.execute(sql, [voucher_id, voucher_id])
        rows = cursor.fetchall()
        if not rows: return None
        first_row = _row_to_dict(cursor, rows[0])
        d_date = first_row['dldt']
        if isinstance(d_date, (datetime.date, datetime.datetime)): d_date = d_date.strftime('%Y/%m/%d')
        c_name = _get_center_name(first_row['cucd'])
        data = {'voucher_id': first_row['deno'].strip(), 'discount_id': '', 'center': c_name, 'dept_code': first_row['bucd'], 'dept_name': first_row['dept_name'] or '', 'delivery_date': d_date, 'vendor_code': first_row['vecd'], 'vendor': first_row['vendor_name'] or '', 'operator': first_row['sign'].strip(), 'details': [], 'total_cases': 0, 'total_cost': 0}
        total_cases = 0
        total_cost = 0
        for row in rows:
            d = _row_to_dict(cursor, row)
            qty = int(d['odsu'] or 0); cost_unit = d['dltn'] or 0; discount = d['prtn'] or 0
            row_total = (qty * cost_unit) - discount
            total_cases += qty; total_cost += row_total
            data['details'].append({'p_code': d['cocd'].strip(), 'jan': d['jan'] or '', 'p_name': d['p_name'] or '', 'spec': d['spec'] or '', 'manufacturer': d['manufacturer'] or '', 'per_case': d['per_case'] or 0, 'loose': 0, 'case': qty, 'cost': "{:,.2f}".format(cost_unit), 'row_total': "{:,}".format(int(row_total)), 'discount': "{:,}".format(int(discount))})
        data['total_cases'] = "{:,}".format(total_cases); data['total_cost'] = "{:,}".format(int(total_cost))
        return data
    finally:
        cursor.close()
        conn.close()