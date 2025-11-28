from pathlib import Path
import json
import os
#######################
#  DB接続
#######################
import pyodbc

def get_connection():
    # ここは環境に合わせて調整してね
    conn_str = (
        "DRIVER={SQL Anywhere 12};"
        "UID=dba;"
        "PWD=jsndba;"
        "DBN=master;"                       # ← 実DB名（本当に 'master' か要確認）
        "ENG=asantkikan01;"                 # ← SQL Anywhere サーバ（エンジン）名
        "LINKS=TCPIP(HOST=asantkikan01x64;PORT=2638);"  # ← ホスト/IP とポート
    )
    return pyodbc.connect(conn_str)

_CFG_CACHE = None

def _strip_json_comments(s: str) -> str:
    import re as _re
    s = _re.sub(r"/\*.*?\*/", "", s, flags=_re.S)
    s = _re.sub(r"^\s*//.*$", "", s, flags=_re.M)
    return s

def _load_autosupply_config() -> dict:
    """設定ファイルを 1) AUTOSUPPLY_CONFIG, 2) db.py と同じDir, 3) CWD から探索して読み込む。
       対応ファイル名: autosupply.config (推奨), autosupply_config.json, config.json
       フォーマット: JSON（// と /* */ のコメント可）
    """
    global _CFG_CACHE
    if _CFG_CACHE is not None:
        return _CFG_CACHE

    candidates = []
    env_path = os.getenv("AUTOSUPPLY_CONFIG")
    if env_path:
        candidates.append(Path(env_path))

    here = Path(__file__).resolve().parent
    candidates += [here / "autosupply.config", here / "autosupply_config.json", here / "config.json"]

    cwd = Path.cwd()
    candidates += [cwd / "autosupply.config", cwd / "autosupply_config.json", cwd / "config.json"]

    for p in candidates:
        try:
            if p.exists():
                raw = _strip_json_comments(p.read_text(encoding="utf-8"))
                cfg = json.loads(raw)
                cfg["_loaded_from"] = str(p)
                _CFG_CACHE = cfg
                return _CFG_CACHE
        except Exception:
            pass

    _CFG_CACHE = {}
    return _CFG_CACHE



def get_arsjy04_table() -> str:
    cfg = _load_autosupply_config()
    tbl = (cfg.get("ARSJY04_TABLE") or os.getenv("ARSJY04_TABLE") or "").strip() if isinstance(cfg, dict) else (os.getenv("ARSJY04_TABLE") or "").strip()
    if tbl:
        return tbl
    mode = (cfg.get("AUTOSUPPLY_MODE") if isinstance(cfg, dict) else None) or os.getenv("AUTOSUPPLY_MODE") or "prod"
    mode = str(mode).lower().strip()
    return "dba.arsjy04_toyohara_test" if mode in ("test", "toyohara", "toyohara_test") else "dba.arsjy04"

