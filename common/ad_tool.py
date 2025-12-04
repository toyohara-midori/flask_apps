from ldap3 import Server, Connection, ALL, NTLM

# ==========================================
AD_SERVER_ADDRESS = 'ldap://10.10.254.5'  # ADサーバーのIPまたはホスト名
AD_SEARCH_BASE = 'dc=jason,dc=intra'   # 検索範囲（ドメイン名など）

# ADを検索するための「読み取り専用ユーザー」
# （ADにアクセスできるなら、あなたのID/PASSでも実験はできます）
AD_BIND_USER = 'JASON\\fujiname2r'       
AD_BIND_PASSWORD = 'fujinametest'
# ==========================================

def is_user_in_group(target_full_name, target_group_name):
    """
    ユーザーが指定されたADグループに入っているかチェックする関数
    Args:
        target_full_name (str): "DOMAIN\\User" 形式のユーザー名
        target_group_name (str): チェックしたいグループ名（例: "SalesTeam"）
    Returns:
        bool: 入っていれば True, 入っていなければ False
    """

    # --- 【重要】開発環境用の抜け道 ---
    # デバッグ用のダミーユーザーが来たら、無条件でOKを返す
    if 'Debug_User' in target_full_name:
        print(f"★開発モード: {target_full_name} なので {target_group_name} へのアクセスを許可しました")
        return True
    # -------------------------------

    # 1. ユーザー名からドメイン部分 "DOMAIN\" をカットする
    #    (AD検索は "User" だけで行うため)
    if '\\' in target_full_name:
        username = target_full_name.split('\\')[1]
    else:
        username = target_full_name

    try:
        # 2. ADサーバーに接続
        server = Server(AD_SERVER_ADDRESS, get_info=ALL)
        conn = Connection(server, user=AD_BIND_USER, password=AD_BIND_PASSWORD, authentication=NTLM, auto_bind=True)

        # 3. そのユーザーを検索して、所属グループ(memberOf)を取得
        #    sAMAccountName は WindowsのログインIDのこと
        conn.search(search_base=AD_SEARCH_BASE,
                    search_filter=f'(sAMAccountName={username})',
                    attributes=['memberOf'])

        # ユーザー自体が見つからなかった場合
        if not conn.entries:
            print(f"User {username} not found in AD.")
            return False

        # 4. 所属グループリストを取得
        #    memberOf は ['CN=Sales,OU=Users...', 'CN=Admins...'] というリストで返ってくる
        user_groups_dn = conn.entries[0].memberOf.value
        
        # グループリストが空の場合（何も所属していない）の対策
        if not user_groups_dn:
            return False

        # 5. 指定されたグループ名が含まれているかチェック
        #    "CN=SalesTeam," のように、CN= と , で挟んで完全一致を探すのが安全
        target_cn = f"CN={target_group_name},"
        
        for group_dn in user_groups_dn:
            # 大文字小文字を区別しないように両方小文字にしてチェック
            if target_cn.lower() in str(group_dn).lower():
                return True
        
        # 最後まで見つからなかった
        return False

    except Exception as e:
        # サーバーにつながらない、パスワード間違いなどのエラー
        print(f"AD Connection Error: {e}")
        return False