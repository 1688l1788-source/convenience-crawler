import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
import time
import re

# --- 설정 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

MAX_PAGES = 10
MAX_PRODUCTS = 200

def crawl_category(category_id, category_name):
    """카테고리별 크롤링"""
    print(f"🛒 {category_name} 크롤링 중...")
    products = []
    
    for page in range(1, MAX_PAGES + 1):
        if len(products) >= MAX_PRODUCTS:
            break
            
        url = "https://cu.bgfretail.com/product/productAjax.do"
        payload = {
            "pageIndex": page,
            "searchMainCategory": category_id,
            "listType": 0,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

        try:
            response = requests.post(url, data=payload, headers=headers, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.select("li.prod_list")

            if not items:
                print(f"  페이지 {page}: 데이터 없음, 종료")
                break

            print(f"  페이지 {page}: {len(items)}개 발견")

            for item in items:
                if len(products) >= MAX_PRODUCTS: break
                product = parse_product(item, category_name)
                if product:
                    products.append(product)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"  ❌ 페이지 {page} 요청 에러: {e}")
    
    print(f"✅ {category_name} {len(products)}개 크롤링 완료\n")
    return products


def parse_product(item, category_name):
    """공통 파싱 함수"""
    try:
        name_tag = item.select_one(".name p")
        title = name_tag.text.strip() if name_tag else "이름없음"

        price_tag = item.select_one(".price strong")
        price_text = price_tag.text.strip().replace(",", "").replace("원", "") if price_tag else "0"
        price = int(price_text) if price_text.isdigit() else 0

        img_tag = item.select_one("img")
        image_url = ""
        if img_tag:
            image_url = img_tag.get('src') or img_tag.get('data-src') or ""
            if image_url.startswith('//'):
                image_url = f"https:{image_url}"
            elif image_url.startswith('/'):
                image_url = f"https://cu.bgfretail.com{image_url}"

        badge_tag = item.select_one(".badge")
        promotion_type = badge_tag.text.strip() if badge_tag else None

        product_url = "https://cu.bgfretail.com/product/view.do?category=product"
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            onclick = onclick_div.get('onclick', '')
            match = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick)
            if match:
                gdIdx = match.group(1)
                product_url = f"https://cu.bgfretail.com/product/view.do?gdIdx={gdIdx}&category=product"

        return {
            "title": title,
            "price": price,
            "image_url": image_url,
            "category": category_name,
            "promotion_type": promotion_type,
            "source_url": product_url,
            "is_active": True,
            "brand_id": 1
        }
    except Exception as e:
        print(f"    ⚠️ 파싱 에러: {e}")
        return None


def main():
    print("🚀 CU 크롤러 시작\n")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 에러: Supabase 환경 변수가 없습니다.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # ✅ CU 전체 데이터 삭제 (brand_id=1)
    print("🗑️  기존 CU 데이터 전체 삭제 중...")
    try:
        # brand_id=1인 모든 데이터 삭제
        result = supabase.table("new_products").delete().eq("brand_id", 1).execute()
        print(f"✅ 삭제 완료\n")
    except Exception as e:
        print(f"⚠️ 삭제 에러: {e}\n")
    
    # 크롤링할 카테고리 정의
    categories = [
        ("40", "아이스크림"),
        ("30", "과자"),
        ("60", "음료"),
        ("10", "식품"),
        ("50", "생활용품"),
    ]
    
    all_products = []
    
    # 각 카테고리 크롤링
    for category_id, category_name in categories:
        products = crawl_category(category_id, category_name)
        all_products.extend(products)
    
    print(f"💾 저장 시작... (총 {len(all_products)}개)\n")
    
    # ✅ 그냥 순서대로 저장 (reversed 제거)
    saved_count = 0
    for product in all_products:
        if not product: continue
        try:
            supabase.table("new_products").insert(product).execute()
            saved_count += 1
            if saved_count % 50 == 0:
                print(f"  {saved_count}개 저장 중...")
        except Exception as e:
            print(f"⚠️ 저장 실패: {product.get('title', 'Unknown')} - {e}")
    
    print(f"\n✅ 저장 완료: {saved_count}개")
    print(f"🎉 크롤링 완료!")

if __name__ == "__main__":
    main()
