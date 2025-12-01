from flask import Blueprint, render_template, request, jsonify
from common.db_connection import get_connection
from .services.config_util import get_arsjy04_table
from datetime import datetime
from .services.autosupply_service import chk_jyno, load_odflg
from common.db_master_access import chk_cucd
from common.cucd_logic import get_cucd_list, check_cucd

# Blueprint を定義
autosupply_bp = Blueprint(
    "autosupply_web",
    __name__,
    template_folder="templates",
    static_folder="static"
)

# -------------------------
# 個別登録/変更/削除画面
# -------------------------
@autosupply_bp.route("/", methods=["GET", "POST"])
def page():
    cucd  = request.form.get("cucd", "")
    jyno  = request.form.get("jyno", "")
    days  = {}
    message = ""
    nmkj_cu = ""
    typeflg = "04"  #04固定

    if request.method == "POST":
        action = request.form.get("action")

        # クリアはDBアクセス不要。最優先で処理
        if action == "clear":
            cucd = ""
            jyno = ""
            nmkj_cu = ""
            days = {}
            message = ""
            return render_template(
                "autosupply_single_entry.html",
                cucd=cucd, jyno=jyno, days=days, message=message, nmkj_cu=nmkj_cu
            )

        # 表示ボタン押下
        if action == "view":
            with get_connection("master") as conn:
                ok, msg, cucd_n, nmkj_cu = chk_cucd(conn, cucd)
                if not ok:
                    return render_template(
                        "autosupply_single_entry.html",
                        cucd=cucd, jyno=jyno, days=days, message=msg, nmkj_cu=""
                    )

                ok, msg = chk_jyno(jyno)
                if not ok:
                    return render_template(
                        "autosupply_single_entry.html",
                        cucd=cucd_n, jyno=jyno, days=days, message=msg, nmkj_cu=nmkj_cu
                    )

                found, days = load_odflg(conn, typeflg, cucd_n, jyno)
                if found:
                    message = "表示完了！"
                else:
                    message = "新規登録です"

                cucd = cucd_n

        # 登録ボタン押下
        if action == "insert":
            with get_connection("master") as conn:
                ok, msg, cucd_n, nmkj_cu = chk_cucd(conn, cucd)
                if not ok:
                    return render_template(
                        "autosupply_single_entry.html",
                        cucd=cucd, jyno=jyno, days=days, message=msg, nmkj_cu=""
                    )

                ok, msg = chk_jyno(jyno)
                if not ok:
                    return render_template(
                        "autosupply_single_entry.html",
                        cucd=cucd_n, jyno=jyno, days=days, message=msg, nmkj_cu=nmkj_cu
                    )

                # 画面の曜日チェック状態を取得
                days = {d: (request.form.get(d) == "1") for d in ["sun","mon","tue","wed","thu","fri","sat"]}

                from .services.autosupply_service import insert_record
                success, msg = insert_record(cucd=cucd_n, jyno=jyno, days=days, conn=conn)

                message = msg

        # 削除ボタン押下
        if action == "delete":
            with get_connection("master") as conn:
                # 店舗CDチェック
                ok, msg, cucd_n, nmkj_cu = chk_cucd(conn, cucd)
                if not ok:
                    return render_template(
                        "autosupply_single_entry.html",
                        cucd=cucd, jyno=jyno, days=days, message=msg, nmkj_cu=""
                    )

                # 什器番号チェック
                ok, msg = chk_jyno(jyno)
                if not ok:
                    return render_template(
                        "autosupply_single_entry.html",
                        cucd=cucd_n, jyno=jyno, days=days, message=msg, nmkj_cu=nmkj_cu
                    )

                # 対象テーブルで存在確認 → 削除
                table = get_arsjy04_table()
                cur = conn.cursor()
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE TRIM(cucd)=? AND TRIM(jyno)=?",
                    (cucd_n.strip(), jyno.strip())
                )
                cnt = cur.fetchone()[0] or 0

                if cnt == 0:
                    message = "削除するデータはありません"
                else:
                    cur.execute(
                        f"DELETE FROM {table} WHERE TRIM(cucd)=? AND TRIM(jyno)=?",
                        (cucd_n, jyno)
                    )
                    conn.commit()
                    message = "削除しました"
                    days = {}  # チェック状態クリア

    return render_template(
        "autosupply_single_entry.html",
        cucd=cucd, jyno=jyno, days=days, message=message, nmkj_cu=nmkj_cu
    )

# -------------------------
# 自動補充什器メニュー画面
# -------------------------
from flask import send_file
import csv
import os

@autosupply_bp.route("/autosupply_menu")
def autosupply_menu():
    return render_template("autosupply_menu.html")

@autosupply_bp.route("/autosupply_single_entry", methods=["GET", "POST"])
def autosupply_single_entry():
    if request.method == "POST":
        # Enter の“暗黙送信”でもここに来る
        cucd = (request.form.get("cucd") or "").strip()
        jyno = (request.form.get("jyno") or "").strip()
        action = (request.form.get("action") or "view").lower()  # 未指定なら view 扱い
        return render_template("autosupply_single_entry.html",
                               cucd=cucd, jyno=jyno, action=action)
    # GET時
    return render_template("autosupply_single_entry.html")


# -------------------------
# 自動補充メイン画面
# -------------------------
@autosupply_bp.route("/autosupply_main")
def autosupply_main():
    return render_template("autosupply_main.html")

# ----------------------------------------
# 自動補充什器メイン 「データ抽出」ボタン押下
# ----------------------------------------
@autosupply_bp.route("/autosupply_export", methods=["POST"])
def autosupply_export():
        table = get_arsjy04_table()
        sql = f"""
            SELECT type AS 系統, cucd AS 店CD, jyno AS 什器NO,
               sun AS 日, mon AS 月, tue AS 火, wed AS 水,
               thu AS 木, fri AS 金, sat AS 土,
               upti AS 更新時刻, updt AS 更新日, rgdt AS 登録日,
               LEFT(jyno, 2) AS 部CD
            FROM {table}
            ORDER BY updt DESC, upti DESC;
        """

        with get_connection("master") as conn:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]

        # ダウンロードフォルダに出力（ヘッダー無し）
        import os, csv
        csv_path = os.path.join(os.path.expanduser("~/Downloads"), "arsjy04.csv")
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            for row in rows:
                writer.writerow(row)

        from flask import send_file
        return send_file(csv_path, as_attachment=True)

# -------------------------
# 一括登録画面
# -------------------------
@autosupply_bp.route("/autosupply_bulk_entry")
def autosupply_bulk_entry():
    return render_template("autosupply_bulk_entry.html")

# --- アップロードボタン押下後の、店舗CDチェック
@autosupply_bp.route("/api/check_cucd_bulk", methods=["POST"])
def api_check_cucd_bulk():
    payload = request.get_json(force=True) or {}
    items = payload.get("cucd", [])
    # 正規化（空を除外・trim）
    items = [str(x).strip() for x in items if x is not None and str(x).strip() != ""]
    if not items:
        return jsonify(invalid=[], valid=[])

    # IN 句を動的に組み立て
    placeholders = ",".join(["?"] * len(items))
    sql = f"""
        SELECT DISTINCT cucd
        FROM Cusmf04
        WHERE cukb = '0' 
        AND cucd NOT IN (
            SELECT cucd FROM DBA.closemf04 GROUP BY cucd)
        AND cucd IN ({placeholders})
    """
    with get_connection("master") as conn:
        cur = conn.cursor()
        cur.execute(sql, items)
        valid = { (row[0] or "").strip() for row in cur.fetchall() }

    invalid = [x for x in items if x not in valid]
    return jsonify(invalid=invalid, valid=list(valid))

from flask import request, jsonify

# --- アップロードボタン押下後、削除フラグ行のレコード有無チェック
@autosupply_bp.route("/api/check_arsjy04_exists_bulk", methods=["POST"])
def api_check_arsjy04_exists_bulk():
    payload = request.get_json(force=True) or {}
    items = payload.get("items", [])
    # 正規化
    pairs = []
    for it in items:
        cucd = str(it.get("cucd", "")).strip()
        jyno = str(it.get("jyno", "")).strip()
        if cucd and jyno:
            pairs.append((cucd, jyno))
    if not pairs:
        return jsonify(not_found=[], found=[])

    # (cucd, jyno) の存在チェック
    # 一部DBでは (cucd, jyno) IN ((?,?),(?,?)) が使えない場合があるため OR の連結で対応
    where = " OR ".join(["(cucd=? AND jyno=?)"] * len(pairs))

    table = get_arsjy04_table()
    sql = f"""
        SELECT cucd, jyno 
        FROM {table} 
        WHERE {where}
    """
    params = [v for pair in pairs for v in pair]

    with get_connection("master") as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        found = {(row[0], row[1]) for row in cur.fetchall()}

    not_found = [ {"cucd": c, "jyno": j} for (c, j) in pairs if (c, j) not in found ]
    return jsonify(not_found=not_found, found=[{"cucd": c, "jyno": j} for (c, j) in found])

# --- 登録しますか？⇒OKボタン押下時
@autosupply_bp.route("/api/bulk_apply_arsjy04", methods=["POST"])
def api_bulk_apply_arsjy04():
    from flask import jsonify, request
    payload = request.get_json(force=True) or {}
    items = payload.get("items", [])
    if not isinstance(items, list) or len(items) == 0:
        return jsonify(ok=False, error="items が空です"), 400

    table = get_arsjy04_table()
    inserted = 0
    updated = 0
    deleted = 0

    try:
        with get_connection("master") as conn:
            cur = conn.cursor()
            for it in items:
                cucd = str(it.get("cucd", "")).strip()
                jyno = str(it.get("jyno", "")).strip()
                del_f = str(it.get("del", "")).strip()

                def v1(x): return 'o' if str(x or '').strip() == '1' else ''
                sun = v1(it.get("sun")); mon = v1(it.get("mon")); tue = v1(it.get("tue"))
                wed = v1(it.get("wed")); thu = v1(it.get("thu")); fri = v1(it.get("fri")); sat = v1(it.get("sat"))

                if del_f == "1":
                    cur.execute(f"DELETE FROM {table} WHERE TRIM(cucd)=? AND TRIM(jyno)=?", (cucd, jyno))
                    # rowcount が-1の環境向けの保険
                    if getattr(cur, "rowcount", -1) > 0:
                        deleted += 1
                    else:
                        cur.execute(f"SELECT 1 FROM {table} WHERE TRIM(cucd)=? AND TRIM(jyno)=?", (cucd, jyno))
                        if cur.fetchone():
                            # 存在していたのに消せなかったケースは異常だがスルー
                            pass
                else:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE TRIM(cucd)=? AND TRIM(jyno)=?", (cucd, jyno))
                    cnt = cur.fetchone()[0]
                    now = datetime.now()
                    ti = now.strftime("%H:%M:%S")
                    dt = now.strftime("%Y-%m-%d")
                    #print("jyno:", jyno, "cnt:", cnt, " ", sun, mon, tue, wed, thu, fri, sat)
                    if cnt and int(cnt) > 0:
                        cur.execute(
                            f"UPDATE {table} SET sun=?, mon=?, tue=?, wed=?, thu=?, fri=?, sat=?, upti=?, updt=? WHERE TRIM(cucd)=? AND TRIM(jyno)=?",
                            (sun, mon, tue, wed, thu, fri, sat, ti, dt, cucd, jyno)
                        )
                        updated += 1
                    else:
                        type_val = "004"
                        cur.execute(
                            f"INSERT INTO {table} (type, cucd, jyno, sun, mon, tue, wed, thu, fri, sat, upti, updt, rgdt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (type_val, cucd, jyno, sun, mon, tue, wed, thu, fri, sat, ti, dt, dt)
                        )
                        inserted += 1
            conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify(ok=False, error=str(e))

    return jsonify(ok=True, inserted=inserted, updated=updated, deleted=deleted)
    

# -------------------------
# commonの処理を利用する
# -------------------------
@autosupply_bp.route("/api/cucd_list")
def api_cucd_list():
    return jsonify(get_cucd_list())

@autosupply_bp.route("/api/chk_cucd", methods=["POST"])
def api_chk_cucd():
    payload = request.get_json(force=True) or {}
    cucd = payload.get("cucd", "")
    return jsonify(check_cucd(cucd))


# -------------------------
# テストページ
# -------------------------
# DBG　使用テーブルの確認用
@autosupply_bp.route("/api/debug_table_name")
def api_debug_table_name():
    return {"table": get_arsjy04_table()}

@autosupply_bp.route("/test")
def test_page():
    return "<h3>AutoSupply Web Blueprint OK!</h3>"

@autosupply_bp.route("/test_common")
def test_common():
    from common.cucd_logic import get_cucd_list
    items = get_cucd_list()
    return f"取得件数: {len(items)} 件"

if __name__ == "__main__":
    autosupply_bp.run(debug=True)
