import os
import time
import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

MAX_PRODUCTS = 50
MAX_PAGES = 10

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
    for page in range(1, MAX_PAGES + 1):
        if len(products) >= MAX_PRODUCTS:
            break

        url = "https://cu.bgfretail.com/product/productAjax.do"
        payload = {
            "pageIndex": page,
            "searchMainCategory": "40",
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

    # CU 전체 삭제
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    products = crawl_icecream()

    # ✅ reversed() 제거 - 그냥 순서대로 저장
    # 페이지 1(최신) → id 작음 → ORDER BY id DESC에서 먼저 나옴
    products_to_insert = products

    if products_to_insert:
        try:
            supabase.table("new_products").insert(products_to_insert).execute()
        except Exception:
            for part in chunk(products_to_insert, 10):
                supabase.table("new_products").insert(part).execute()

    print(f"완료: 크롤링 {len(products)}개 / 저장 {len(products_to_insert)}개")

if __name__ == "__main__":
    main()
