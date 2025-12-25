import os

def get_remote_user(request):
    """
    Webサーバー(IIS)から渡されたドメインユーザー名を取得する。
    戻り値: "DOMAIN\\user" から "DOMAIN\\" を削った "user" を返す
    """
    # 1. IIS (Windows認証) 経由の場合
    user = request.environ.get('REMOTE_USER')
    
    # 2. ローカル開発環境などで取れない場合のフォールバック (PCのログインユーザ名)
    if not user:
        user = os.environ.get('USERNAME') or "Unknown"

    # "DOMAIN\user" の形式なら、\より後ろだけを取り出す
    if '\\' in user:
        user = user.split('\\')[1]
        
    return user