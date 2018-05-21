import requests


class generate_access_token():
    payload = {
        'refresh_token': 'ee8992fc9ff87fad0e902bc7a4202c6871c5e27b',
        'client_secret': '1c344475a985e5cd8feaf1d5532cf9122756c04bcdc9df1e6a1e0f5ee84b6804',
        'client_id': 'a6b48d3ff56360d22a70e2a930d5c46b04765c4756d5e60c6a44441d7dbe5ae3',
        'grant_type': 'refresh_token',
    }

    r = requests.post('https://cloud.lightspeedapp.com/oauth/access_token.php', data=payload).json()
    print(r['access_token'])