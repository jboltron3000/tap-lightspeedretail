import requests


class generate_access_token():
    payload = {
        'refresh_token': '61c11da3041a354016f035be607de4b07324207d',
        "client_id": "1401747925fa658f4138f61cba102eca7f869d3eab5fcc49811b2f1c4f8cc2f3",
         "client_secret": "0a8170d3b1d6bbae3c1cb0aeb56fc60e87c1002199414b21151f72f806e38eaf",
        'grant_type': 'refresh_token',
    }

    r = requests.post('https://cloud.lightspeedapp.com/oauth/access_token.php', data=payload).json()
    print(r['access_token'])