import os
import time
import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

MAX_PRODUCTS = 50
START_PAGE = 40  # 넉넉하게 40페이지부터 거꾸로 탐색

def parse_product(item):
    name_tag = item.select_one(".name p")
    title = (name_tag.get_text(strip=True) if name_tag else "").strip()
    
    price_tag = item.select_one(".price strong")
    price_text = (price_tag.get_text(strip=True) if price_tag else "0").replace(",", "").replace("원", "")
    price = int(price_text) if price_text.isdigit() else 0

    img_tag = item.select_one("img")
    image_url = ""
    if img_tag:
        image_url = img_tag.get("src") or img_tag.get("data-src") or ""
        if image_url.startswith("//"):
            image_url = "https:" + image_url
        elif image_url.startswith("/"):
            image_url = "https://cu.bgfretail.com" + image_url

    badge_tag = item.select_one(".badge")
    promotion_type = badge_tag.get_text(strip=True) if badge_tag else None

    product_url = "https://cu.bgfretail.com/product/view.do?category=product"
    onclick_div = item.select_one("div[onclick*='view']")
    if onclick_div:
        onclick = onclick_div.get("onclick", "")
        m = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick)
        if m:
            gdIdx = m.group(1)
            product_url = f"https://cu.bgfretail.com/product/view.do?gdIdx={gdIdx}&category=product"

    if not title:
        return None

    return {
        "title": title,
        "price": price,
        "image_url": image_url,
        "category": "아이스크림",
        "promotion_type": promotion_type,
        "source_url": product_url,
        "is_active": True,
        "brand_id": 1
    }

def crawl_icecream():
    products = []
    
    # ✅ 중요: 뒤에서부터 앞으로 (40 -> 39 -> ... -> 1)
    # 그래야 최신 상품(마지막 페이지)부터 긁음
    print(f"🔄 최신 상품을 찾기 위해 페이지 {START_PAGE}부터 역순 탐색 시작...")
    
    for page in range(START_PAGE, 0, -1):
        if len(products) >= MAX_PRODUCTS:
            break

        url = "https://cu.bgfretail.com/product/productAjax.do"
        payload = {
            "pageIndex": page,
            "searchMainCategory": "40",
            "listType": 0,
            "searchCondition": "", 
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

        try:
            r = requests.post(url, data=payload, headers=headers, timeout=10)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("li.prod_list")

            if not items:
                # 데이터가 없는 페이지는 건너뜀 (아직 끝 페이지 도달 전일 수 있음)
                # print(f"  페이지 {page}: 없음")
                continue

            print(f"  ✅ 페이지 {page}: {len(items)}개 발견 (최신순 수집 중)")

            # 페이지 내에서는 위->아래 순서인데, 
            # 페이지 자체가 최신이면 그 안의 상품들도 최신일 확률 높음.
            # 하지만 정확한 순서를 위해 일단 수집하고 나중에 정렬/저장함.
            
            # 페이지 안에서도 거꾸로(아래쪽이 더 최신일 수도 있음) 뒤집어서 수집?
            # 보통 한 페이지 내에서는 최신->구형일 수도, 구형->최신일 수도 있음.
            # 일단 그대로 수집.
            
            # 여기서 items를 reversed 해야 할까?
            # 만약 전체 순서가 1(구) -> 10(신) 이라면,
            # 10페이지의 마지막 아이템이 "가장 최신"일 가능성 큼.
            # 따라서 items도 reversed 해서 수집!
            
            for item in reversed(items):
                if len(products) >= MAX_PRODUCTS: break
                p = parse_product(item)
                if p:
                    products.append(p)
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Error on page {page}: {e}")
            
    return products

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("환경변수 없음")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 1. 삭제
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    # 2. 크롤링 (뒤 페이지부터 역순 수집)
    products = crawl_icecream()

    # 3. 저장
    # products 리스트: [가장 최신(찰옥수수), ..., 덜 최신]
    # 우리가 원하는 앱 결과(DESC): id가 클수록 최신이어야 함.
    # 즉, 덜 최신 → 먼저 저장(id=1), 가장 최신 → 나중에 저장(id=50)
    # 따라서 products를 "뒤집어서" 저장해야 함!
    
    products_to_insert = list(reversed(products))

    if products_to_insert:
        try:
            supabase.table("new_products").insert(products_to_insert).execute()
        except Exception:
            for part in chunk(products_to_insert, 10):
                supabase.table("new_products").insert(part).execute()

    print(f"완료: 크롤링 {len(products)}개 / 저장 {len(products_to_insert)}개")
    if products_to_insert:
        # 가장 마지막에 저장된 것(=가장 최신, 앱 1등) 출력 확인
        print(f"1등 예상 상품 (앱 기준): {products_to_insert[-1]['title']}")

if __name__ == "__main__":
    main()
