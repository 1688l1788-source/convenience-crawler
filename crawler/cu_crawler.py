import os
import time
import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

MAX_PRODUCTS = 50
MAX_PAGES = 10  # 50개 채울 때까지 돌고, 다 채우면 중단

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
        "category": "아이스크림",   # ✅ 무조건 고정 (필터 누락 방지)
        "promotion_type": promotion_type,
        "source_url": product_url,
        "is_active": True,
        "brand_id": 1
    }

def crawl_icecream():
    products = []
    for page in range(1, MAX_PAGES + 1):
        if len(products) >= MAX_PRODUCTS:
            break

        url = "https://cu.bgfretail.com/product/productAjax.do"
        payload = {
            "pageIndex": page,
            "searchMainCategory": "40",  # 아이스크림
            "listType": 0,
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

        r = requests.post(url, data=payload, headers=headers, timeout=15)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("li.prod_list")

        if not items:
            break

        for item in items:
            if len(products) >= MAX_PRODUCTS:
                break
            p = parse_product(item)
            if p:
                products.append(p)

        time.sleep(0.3)

    return products

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY 환경변수 없음")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ✅ 가장 빠른 정상화: CU(brand_id=1) 전체 삭제 후 아이스크림만 재삽입
    # (필터 누락 원인인 '카테고리 값 다른 기존 데이터'를 한 번에 제거)
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    products = crawl_icecream()

    # ✅ 최신순 보장(앱이 id DESC로 정렬할 때):
    # CU 페이지가 '최신이 앞'으로 내려오는 경우가 많아서,
    # DB에는 "오래된 것 먼저 → 최신을 마지막"으로 넣어야 id가 최신이 가장 큼.
    products_to_insert = list(reversed(products))

    # ✅ 한번에 넣다가 실패하면(유니크 충돌 등) 청크로 넣음
    if products_to_insert:
        try:
            supabase.table("new_products").insert(products_to_insert).execute()
        except Exception:
            for part in chunk(products_to_insert, 10):
                supabase.table("new_products").insert(part).execute()

    print(f"완료: 크롤링 {len(products)}개 / 저장 {len(products_to_insert)}개")

if __name__ == "__main__":
    main()
