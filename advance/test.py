import requests


API_URL = (
    "https://www.district.in/gw/consumer/movies/v3/cinema"
    "?meta=1&reqData=1&version=3"
    "&site_id=1&channel=mweb&child_site_id=1"
    "&platform=district"
    "&cinemaId={cid}"
    "&date={date}"
)

HEADERS = {
    "accept": "application/json",
    "x-access-token": "1766761850603884769_6215138833624750724_65db173408bbff8e6314d934566b5ba9e698a994cd8136ee590a1b5245d1501c",
    "x-app-type": "ed_mweb",
    "x-device-id": "945e8082-a763-4302-9508-2fd3f5dbb404",
    "api_source": "district",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Mobile Safari/537.36"
    ),
    "referer": "https://www.district.in/"
}

DATE= "2025-12-27"
cid= 20856

data = requests.get(API_URL.format(cid=cid, date=DATE), headers=HEADERS).json()

print(data)