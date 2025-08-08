import requests, json

URL = "https://open.xkw.com/doc/api/xopqbm/questions/similar-recommend"

# ① 把整串 cookie 原样放进来
COOKIE_STR = (
    "xkw-device-id=8A94734F7C3E090D246AE4D0EF4412B6; "
    "SESSION=NDFjNTRjMGMtNDM1NC00N2NlLTg3NDgtYjdiYjY4N2Q2NDlj; "
    "acw_tc=0a47308617544638320526390ea1b008c267fae90b6bad4427858a4125075c; "
    "Hm_lvt_9cd71e5642aa75f2ab449541c4c00473=1754451485; "
    "Hm_lpvt_9cd71e5642aa75f2ab449541c4c00473=1754460780"
)

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://open.xkw.com",
    "Referer": "https://open.xkw.com/document/sg12/9f8c98ccebf5f26c/get-similar-questions-post_1",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"),
    # —— Knife4j 专用头：保持拼写错误 & 用浏览器里看到的同一个值 ——
    "Request-Origion": "Knife4j",
    "knfie4j-gateway-request": "0fe0b215bf088ae079501835e52f5477",
    # —— 平台鉴权 —— 
    "Xop-App-Id": "101891658823024500",
    "secret":     "UmUAkOUxxN0nK2nZWzZOOXDU3GuRZoZ4",
    # —— UA 细节头，少了也可能 404 —— 
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    # 直接写进 headers 省事
    "Cookie": COOKIE_STR,
}

DATA = {"question_id": "1561283083976704", "count": 5}

r = requests.post(URL, headers=HEADERS, json=DATA, timeout=10)

print("HTTP", r.status_code, r.headers.get("Content-Type"))
print(r.text[:500])    # 先看前缀应以 { 开头，再 json.loads