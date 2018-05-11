import requests

url = "https://cloud.lightspeedapp.com/oauth/access_token.php"

payload = {
    "client_id": "01f474395cbd3e81791c9ac655446ccc90976041f8b22b3e3684f922e382cd7a",
    "client_secret": "118346d07d8f887995ae7f05231e0b34ee262757238d230c77d2f356ff688bda",
    "code": "965025c3c2e6aba5b7b9ba2b6701e92f76e79d14",
    "grant_type": "authorization_code"}

response = requests.request("POST", url, data=payload)

print(response.text)