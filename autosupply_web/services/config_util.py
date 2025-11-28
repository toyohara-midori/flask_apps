############################
# config_util.py
#  設定ファイルの読込・モード/テーブル切替を管理
############################
import os, json
from pathlib import Path
import re

# --- 設定ファイルのロード（db.py と同じフォルダ or AUTOSUPPLY_CONFIG で指定）---
_CFG_CACHE = None

def _strip_json_comments(s: str) -> str:
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)     # /* ... */ を削除
    s = re.sub(r"^\s*//.*$", "", s, flags=re.M)     # // ... を削除
    return s

def load_autosupply_config() -> dict:
    """autosupply.config を探索して読み込む（ワイルドカード *.config 対応）"""
    global _CFG_CACHE
    if _CFG_CACHE is not None:
        return _CFG_CACHE

    candidates = []
    # 1) 環境変数で明示指定
    if os.getenv("AUTOSUPPLY_CONFIG"):
        candidates.append(Path(os.getenv("AUTOSUPPLY_CONFIG")))

    # 2) プロジェクト直下にある *.config ファイルをすべて探索
    here = Path(__file__).resolve().parent
    project_root = here.parent
    for p in project_root.glob("*.config"):   # ← ★ ワイルドカード検索
        candidates.append(p)

    # 3) 念のため "config" ファイル（拡張子なし）も対象に
    candidates.append(project_root / "config")

    # --- ファイルを順にチェック ---
    for p in candidates:
        try:
            if p.exists():
                raw = _strip_json_comments(p.read_text(encoding="utf-8"))
                _CFG_CACHE = json.loads(raw)
                return _CFG_CACHE
        except Exception:
            pass

    _CFG_CACHE = {}
    return _CFG_CACHE

def get_mode() -> str:
    """動作モードを返す（prod / test）"""
    cfg = load_autosupply_config()
    return (cfg.get("AUTOSUPPLY_MODE") or os.getenv("AUTOSUPPLY_MODE") or "prod").lower().strip()

def get_arsjy04_table() -> str:
    """本番・テスト切替を考慮したテーブル名を返す"""
    cfg = load_autosupply_config()

    # 明示テーブル名があれば最優先（config > env）
    tbl = (cfg.get("ARSJY04_TABLE") or os.getenv("ARSJY04_TABLE") or "").strip()
    if tbl:
        return tbl

    # モードで切替（config > env）
    mode = get_mode()
    return "dba.arsjy04_toyohara_test" if mode in ("test", "toyohara", "toyohara_test") else "dba.arsjy04"

