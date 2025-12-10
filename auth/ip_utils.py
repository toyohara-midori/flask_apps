# =============================
# IPアドレス　→　店舗番号変換関数
# =============================

def extract_store_from_ip(ip: str) -> str | None:
    """
    IPアドレスから店舗番号を抽出して変換する。
    ・10.10.4.aa → aa
    ・10.11.aa.bb → aa
    ・aaが75〜98 → 'B' + aa
    ・それ以外 → ゼロ埋め3桁
    """
    try:
        parts = ip.split(".")
        if len(parts) != 4:
            return None

        # パターンA: 10.10.4.aa
        #if parts[0] == "10" and parts[1] == "10" and parts[2] == "4":
        #DBG vm-toyoharm2(10.10.20.23)からのログインを許容する
        if parts[0] == "10" and parts[1] == "10":
            num = int(parts[3])

        # パターンB: 10.11.aa.bb
        elif parts[0] == "10" and parts[1] == "11":
            num = int(parts[2])

        else:
            return None

        # 75〜98 → Bxx
        if 75 <= num <= 98:
            return f"B{num}"

        # その他は3桁ゼロパディング
        return f"{num:03d}"

    except Exception:
        return None
