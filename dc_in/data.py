# data.py
# ここにはデータ定義と、計算ロジックだけを置きます

# --- 確認画面用データ ---
DETAIL_HEADERS = [
    "商品コード", "JANCD", "商品名", "規格", "メーカー名",
    "納品数", "納品ケース数",
    "本部費（MD）", "物流費（DC）", "原単価", "合計原価", "値引き金額"
]

CENTER_GROUPS = {
    "守谷C": [
        {"vendor_code": "03012203", "vendor_name": "加藤産業（株）東関東支社", "delivery_date": "2025/11/25", "dept_code": "22", "dept_name": "食品", "details": [
            ["16734470", "4902471103708", "ｷﾚｰﾄﾚﾓﾝPuLemon", "490ML ﾍﾟｯﾄ", "JVﾎﾟｯｶｻｯ", 19752, 823.000000, 0.010, 0.090, 20.30, 400965.60, 0.00],
            ["16530743", "4902471102213", "ｻｸｻｸｶｸｷﾞﾘｾﾞｲﾀｸﾘﾝｺﾞ", "400G ｶﾝ", "ﾎﾟｯｶ", 1104, 46.000000, 0.010, 0.090, 41.30, 45595.20, 0.00]
        ]},
        {"vendor_code": "03010308", "vendor_name": "（株）高山", "delivery_date": "2025/11/25", "dept_code": "22", "dept_name": "食品", "details": [
            ["16606910", "4902443526917", "ﾄﾞﾘﾄｽﾒｷｼｶﾝ･ﾀｺｽｱｼﾞ", "60G", "ﾌﾘﾄﾚｰ", 720, 60.000000, 0.010, 0.060, 79.00, 56880.00, 3600.00],
            ["16643530", "4902443545000", "ﾁｰﾄｽ ﾁｰｽﾞｱｼﾞ", "70G", "ﾌﾘﾄﾚｰ", 1728, 144.000000, 0.010, 0.060, 70.00, 120960.00, 6912.00]
        ]}
    ],
    "狭山日高C": [
        {"vendor_code": "03012203", "vendor_name": "加藤産業（株）東関東支社", "delivery_date": "2025/11/26", "dept_code": "22", "dept_name": "食品", "details": [
            ["16329738", "4901777367814", "ｸﾗﾌﾄﾎﾞｽ ﾌﾞﾗｯｸ ﾎｯﾄ", "450ML ﾍﾟｯﾄ", "JVｻﾝﾄﾘｰ", 23520, 980.000000, 0.010, 0.090, 48.30, 1136016.00, 0.00],
            ["16734357", "4901777292031", "ｻﾝﾄﾘｰ GREEN DA･KA･RAｽｯｷﾘｼ", "350G ｶﾝ", "JVｻﾝﾄﾘｰ", 17736, 739.000000, 0.010, 0.090, 41.30, 732496.80, 0.00]
        ]}
    ]
}

# --- 一覧・詳細画面用データ ---
VOUCHER_DB = {
    "600101": {
        "import_id": "20251125-0900-fujiname",
        "center": "守谷C", "dept_code": "22", "dept_name": "食品", "delivery_date": "2025/11/25", 
        "vendor_code": "03012203", "vendor": "加藤産業（株）", "operator": "fujiname", "discount_id": "201234",
        "total_cases": 869, "total_cost": "446,561",
        "details": [
            {"p_code": "16734470", "jan": "4902471103708", "p_name": "ｷﾚｰﾄﾚﾓﾝPuLemon", "spec": "490ML", "manufacturer": "ポッカサッポロ", "per_case": 24, "case": 823, "loose": 19752, "cost": 20.30, "row_total": "400,966", "discount": "40,096"},
            {"p_code": "16530743", "jan": "4902471102213", "p_name": "ｻｸｻｸｶｸｷﾞﾘｾﾞｲﾀｸﾘﾝｺﾞ", "spec": "400G", "manufacturer": "ポッカサッポロ", "per_case": 24, "case": 46, "loose": 1104, "cost": 41.30, "row_total": "45,595", "discount": "4,559"}
        ]
    },
    "600102": {
        "import_id": "20251125-0900-fujiname",
        "center": "守谷C", "dept_code": "22", "dept_name": "食品", "delivery_date": "2025/11/25", 
        "vendor_code": "03010308", "vendor": "（株）高山", "discount_id": "", "total_cases": 204, "total_cost": "177,840",
        "details": [
            {"p_code": "16606910", "jan": "4902443526917", "p_name": "ﾄﾞﾘﾄｽﾒｷｼｶﾝ･ﾀｺｽｱｼﾞ", "spec": "60G", "manufacturer": "フリトレー", "per_case": 12, "case": 60, "loose": 720, "cost": 79.00, "row_total": "56,880", "discount": "0"},
            {"p_code": "16643530", "jan": "4902443545000", "p_name": "ﾁｰﾄｽ ﾁｰｽﾞｱｼﾞ", "spec": "70G", "manufacturer": "フリトレー", "per_case": 12, "case": 144, "loose": 1728, "cost": 70.00, "row_total": "120,960", "discount": "0"}
        ]
    },
    "600103": { "import_id": "20251124-1830-suzuki", "center": "狭山日高C", "dept_code": "22", "dept_name": "食品", "delivery_date": "2025/11/26", "vendor_code": "01234567", "vendor": "三菱食品（株）", "total_cases": 0, "total_cost": "0", "details": [] },
    "600104": { "import_id": "20251125-0900-fujiname", "center": "守谷C", "dept_code": "22", "dept_name": "食品", "delivery_date": "2025/11/27", "vendor_code": "99999999", "vendor": "伊藤忠食品（株）", "total_cases": 0, "total_cost": "0", "details": [] },
    "600105": { "import_id": "20251124-1830-suzuki", "center": "狭山日高C", "dept_code": "22", "dept_name": "食品", "delivery_date": "2025/11/24", "vendor_code": "88888888", "vendor": "（株）日本アクセス", "total_cases": 0, "total_cost": "0", "details": [] },
    "600106": { "import_id": "20251125-0900-fujiname", "center": "守谷C", "dept_code": "22", "dept_name": "食品", "delivery_date": "2025/11/28", "vendor_code": "77777777", "vendor": "国分首都圏（株）", "total_cases": 0, "total_cost": "0", "details": [] }
}

# --- 上限数管理用データ ---
LIMITS_DB = {
    "2025/11/25": {"m_limit": 20000, "s_limit": 10000},
    "2025/11/26": {"m_limit": 20000, "s_limit": 10000},
}

# --- ヘルパー関数 ---
def calculate_summary(center_groups):
    all_summary = {}
    for center_name, groups in center_groups.items():
        for group in groups:
            key = f"{center_name}-{group['delivery_date']}"
            if key not in all_summary:
                all_summary[key] = {'jv': 0, 'other': 0, 'center': center_name, 'date': group['delivery_date']}
            for detail_row in group['details']:
                try:
                    case_count = int(float(detail_row[6]))
                except ValueError:
                    case_count = 0
                manufacturer_name = str(detail_row[4]).upper().strip()
                if manufacturer_name.startswith('JV'):
                    all_summary[key]['jv'] += case_count
                else:
                    all_summary[key]['other'] += case_count
    return list(all_summary.values())