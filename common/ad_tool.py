import subprocess

def check_permission_via_command(target_full_name, allowed_group_list):
    """
    IIS環境対応版: net user コマンドを使用して
    指定されたグループリストのいずれかに所属しているかチェックする関数
    
    Args:
        target_full_name (str): "DOMAIN\\User" または "User" 形式のユーザー名
        allowed_group_list (list): 許可するグループ名のリスト (例: ['Domain Admins', 'G-商品部'])
        
    Returns:
        bool: リスト内のグループのいずれかに所属していれば True
    """

    # --- 【重要】開発環境用の抜け道 ---
    # デバッグ用のダミーユーザーが来たら、無条件でOKを返す
    # (main.py の mock_login_info_for_debug 等で設定した値)
    if 'Debug_User' in str(target_full_name):
        return True
    # -------------------------------

    # 1. ユーザー名からドメイン部分 "DOMAIN\" をカットする
    #    (net user コマンドはユーザー名単体で実行するため)
    if target_full_name and '\\' in target_full_name:
        username = target_full_name.split('\\')[1]
    else:
        username = target_full_name

    if not username:
        return False

    try:
        # 2. net user コマンドを実行してADに問い合わせる
        #    コマンド: net user <ユーザー名> /domain
        #    ※IISの実行ユーザー(AppPool)でも、AD情報の読み取り権限は通常持っています
        cmd = f"net user {username} /domain"
        
        # 日本語Windows環境での文字化けを防ぐため encoding='cp932' (Shift_JIS) を指定
        output = subprocess.check_output(cmd, shell=True, encoding='cp932')

        # 3. 出力結果の中に、許可リストのグループ名が含まれているかチェック
        for group in allowed_group_list:
            # 大文字小文字の揺れを吸収してチェックするのが安全ですが、
            # net user の出力と厳密に合わせるならそのまま in で判定
            if group in output:
                return True
                
        # ループを抜けてもTrueにならなかった＝どのグループにも入っていない
        return False

    except subprocess.CalledProcessError:
        # ユーザーが存在しない、またはADに接続できない場合
        # (net user コマンドがエラーコードを返した場合)
        return False
        
    except Exception as e:
        # その他の予期せぬエラー
        print(f"[AD Tool Error] {e}")
        return False

def create_access_denied_html(target_username):
    """
    権限エラー画面のHTMLとステータスコードを返すヘルパー関数
    """
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <title>Access Denied</title>
        <style>
            body {{ font-family: sans-serif; padding: 40px; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; border: 1px solid #ccc; padding: 20px; border-radius: 8px; }}
            h2 {{ color: #d9534f; border-bottom: 2px solid #d9534f; padding-bottom: 10px; }}
            .user-info {{ background-color: #f9f9f9; padding: 10px; border-left: 4px solid #5bc0de; margin: 20px 0; }}
            .note {{ font-size: 0.9em; color: #666; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>アクセス権限がありません</h2>
            <p>申し訳ありませんが、このページを表示する許可がありません。</p>
            
            <div class="user-info">
                現在のログインユーザー: <strong>{target_username}</strong>
            </div>
            
            <p class="note">
               ※正しいアカウントにも関わらずこの画面が表示される場合は、
               システム管理者へ「<strong>{target_username}</strong> の権限確認」をご依頼ください。
            </p>
        </div>
    </body>
    </html>
    """
    # HTML本文と、ステータスコード403をセットで返す
    return html_content, 403

def is_user_in_group(target_full_name, target_group_name):
    """
    旧仕様の関数。
    単一のグループ名をチェックしたい場合用。
    内部で新しい check_permission_via_command を呼び出す。
    """
    # 新しい関数はリストを受け取るので、リストに包んで渡す
    return check_permission_via_command(target_full_name, [target_group_name])