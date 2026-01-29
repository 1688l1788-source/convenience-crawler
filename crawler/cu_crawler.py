import requests
from bs4 import BeautifulSoup

# 테스트 1: depth2=4, depth3=4 (아이스크림)
print("=== 테스트 1: depth2=4, depth3=4 ===")
url = "https://cu.bgfretail.com/product/productAjax.do"
payload = {
    "pageIndex": 1,
    "depth2": "4",
    "depth3": "4",
    "listType": 0,
}
headers = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}

response = requests.post(url, data=payload, headers=headers, timeout=10)
response.encoding = 'utf-8'
soup = BeautifulSoup(response.text, 'html.parser')
items = soup.select("li.prod_list")

print(f"발견: {len(items)}개")
print("처음 3개:")
for i, item in enumerate(items[:3], 1):
    name = item.select_one(".name p")
    print(f"  {i}. {name.text.strip() if name else 'Unknown'}")

# 테스트 2: searchMainCategory=40 (예전 방식)
print("\n=== 테스트 2: searchMainCategory=40 ===")
payload2 = {
    "pageIndex": 1,
    "searchMainCategory": "40",
    "listType": 0,
}

response2 = requests.post(url, data=payload2, headers=headers, timeout=10)
response2.encoding = 'utf-8'
soup2 = BeautifulSoup(response2.text, 'html.parser')
items2 = soup2.select("li.prod_list")

print(f"발견: {len(items2)}개")
print("처음 3개:")
for i, item in enumerate(items2[:3], 1):
    name = item.select_one(".name p")
    print(f"  {i}. {name.text.strip() if name else 'Unknown'}")
