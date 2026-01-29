import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
import time
import re

# --- 설정 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

TARGET_CATEGORIES = {'40': '아이스크림'}
MAX_PAGES = 2
MAX_PRODUCTS = 50

CATEGORY_KEYWORDS = {
    '아이스크림': ['아이스', '아이스크림', '콘', '바', '빙과', '소르베', '젤라또', '하드', '투게더'],
}

def crawl_general_products(cat_code, cat_name):
    """일반 상품 크롤링"""
    print(f"  🛒 일반 {cat_name} 크롤링 중...")
    products = []
    
    for page in range(1, MAX_PAGES + 1):
        if len(products) >= MAX_PRODUCTS:
            break
            
        url = "https://cu.bgfretail.com/product/productAjax.do"
        payload = {
            "pageIndex": page,
            "searchMainCategory": cat_code,
            "searchSubCategory": "",
            "listType": 0,
            "searchCondition": "setC",
            "searchUseYn": "N",
            "codeParent": cat_code
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
                break

            for item in items:
                if len(products) >= MAX_PRODUCTS: break
                product = parse_product(item, cat_name)
                if product:
                    products.append(product)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"    ❌ 요청 에러: {e}")
    
    print(f"    ✅ 일반 {len(products)}개 발견")
    return products


def crawl_all_pb_products():
    """PB 상품 전체 크롤링"""
    print(f"\n🏪 PB 전체 상품 크롤링 시작...")
    products = []
    
    for page in range(1, 10):
        url = "https://cu.bgfretail.com/product/pbAjax.do"
        payload = {
            "pageIndex": page,
            "listType": 0,
            "searchCondition": "setA",
            "searchUseYn": "",
            "gdIdx": "0",
            "searchgubun": "CUG",
            "search1": "",
            "search2": "",
            "searchKeyword": ""
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
                break

            for item in items:
                product = parse_product(item, None)
                if product:
                    products.append(product)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"    ❌ PB 요청 에러: {e}")
    
    print(f"  ✅ PB 전체 {len(products)}개 크롤링 완료\n")
    return products


def filter_pb_by_keywords(all_pb_products, category_name):
    """카테고리별 키워드로 PB 상품 필터링"""
    keywords = CATEGORY_KEYWORDS.get(category_name, [])
    if not keywords:
        return []
    
    filtered = []
    for product in all_pb_products:
        title = product.get('title', '').lower()
        if any(keyword in title for keyword in keywords):
            # ✅ 새 딕셔너리로 복사하고 category 설정
            filtered_product = product.copy()
            filtered_product['category'] = category_name
            filtered.append(filtered_product)
    
    return filtered


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
    print("🚀 CU 크롤러 시작 (일반 + PB 통합)")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 에러: Supabase 환경 변수가 없습니다.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. PB 상품 전체 크롤링
    all_pb_products = crawl_all_pb_products()
    
    total_count = 0

    # 2. 카테고리별 처리
    for cat_code, cat_name in TARGET_CATEGORIES.items():
        print(f"📂 [{cat_name}] 처리 시작...")
        
        # 기존 데이터 삭제
        try:
            supabase.table("new_products").delete().eq("category", cat_name).execute()
            print(f"  🗑️  기존 {cat_name} 데이터 삭제")
        except Exception as e:
            print(f"  ⚠️ 삭제 에러: {e}")
        
        # 일반 상품
        general_items = crawl_general_products(cat_code, cat_name)
        
        # PB 상품 필터링
        print(f"  🔍 PB {cat_name} 필터링 중...")
        pb_items = filter_pb_by_keywords(all_pb_products, cat_name)
        print(f"    ✅ PB {len(pb_items)}개 발견")
        
        # ✅ 디버깅: PB 샘플 확인
        if pb_items:
            print(f"  📝 PB 샘플 (처음 3개):")
            for i, p in enumerate(pb_items[:3], 1):
                print(f"    {i}. {p.get('title')} | category={p.get('category')}")
        
        # 합치기
        all_items = general_items + pb_items
        
        # 저장
        print(f"  💾 저장 중... (일반 {len(general_items)} + PB {len(pb_items)} = {len(all_items)}개)")
        
        saved_count = 0
        for product in reversed(all_items):
            if not product: continue
            try:
                supabase.table("new_products").insert(product).execute()
                saved_count += 1
            except Exception as e:
                print(f"  ⚠️ 저장 실패: {e}")
        
        total_count += saved_count
        print(f"  ✅ {cat_name} 완료: {saved_count}개 저장\n")

    print(f"🎉 전체 완료! 총 {total_count}개 상품 업데이트")

if __name__ == "__main__":
    main()
