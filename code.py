import requests


class generate_access_token():
    payload = {
        'refresh_token': 'c5ddc6d3b7355d263630c02105431b20486112af',
        "client_id": "56cd6d805728de4a0b9e59ac5267990011370549ab1b18ee6845ff4be5f25ed0",
        "client_secret": "9b4172f89b6647b5556186e11e6944d9d46aed5aa3333083c87bd71e28ee0342",
        'grant_type': 'refresh_token',
    }

    r = requests.post('https://cloud.lightspeedapp.com/oauth/access_token.php', data=payload).json()
    print(r)