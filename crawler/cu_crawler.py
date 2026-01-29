import os
import time
import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

MAX_SEARCH_PAGES = 100
CHUNK_SIZE = 50

# ✅ CU 모든 카테고리
CATEGORIES = [
    {"id": "40", "name": "아이스크림"},
    {"id": "30", "name": "과자류"},
    {"id": "10", "name": "간편식사"},
    {"id": "11", "name": "즉석조리"},
    {"id": "20", "name": "식품"},
    {"id": "60", "name": "음료"},
    {"id": "50", "name": "생활용품"},
]
def parse_product(item, category_name):
    """상품 파싱"""
    try:
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

        # ✅ gdIdx 추출
        gdIdx = None
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            onclick = onclick_div.get("onclick", "")
            m = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick)
            if m:
                gdIdx = int(m.group(1))
        
        product_url = f"https://cu.bgfretail.com/product/view.do?gdIdx={gdIdx}&category=product" if gdIdx else "https://cu.bgfretail.com/product/view.do?category=product"
        
        if not title:
            return None

        return {
            "title": title,
            "price": price,
            "image_url": image_url,
            "category": category_name,
            "promotion_type": promotion_type,
            "source_url": product_url,
            "is_active": True,
            "brand_id": 1,
            "external_id": gdIdx
        }
    except Exception as e:
        print(f"파싱 에러: {e}")
        return None

def fetch_new_products(supabase, category_id, category_name, max_gdIdx):
    """신상품만 크롤링"""
    new_products = []
    print(f"\n🔄 [{category_name}] max_gdIdx={max_gdIdx}보다 큰 상품 찾기...")
    
    for page in range(1, MAX_SEARCH_PAGES + 1):
        url = "https://cu.bgfretail.com/product/productAjax.do"
        payload = {
            "pageIndex": page, 
            "searchMainCategory": category_id,
            "listType": 0
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

        try:
            r = requests.post(url, data=payload, headers=headers, timeout=8)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("li.prod_list")

            if not items:
                print(f"  🛑 페이지 {page}: 끝! (총 {len(new_products)}개)")
                break
            
            count_in_page = 0
            for item in items:
                p = parse_product(item, category_name)
                if p and p['external_id'] is not None:
                    if p['external_id'] > max_gdIdx:
                        new_products.append(p)
                        count_in_page += 1
            
            if count_in_page > 0:
                print(f"  ✅ 페이지 {page}: 신상품 {count_in_page}개 (누적 {len(new_products)})")
            else:
                print(f"  PASS 페이지 {page}")
            
            time.sleep(0.1)
            
        except Exception as e:
            print(f"  ❌ 페이지 {page}: {e}")
            break
    
    return new_products

def remove_duplicates(products):
    """external_id 기준 중복 제거"""
    unique = {}
    for p in products:
        if p['external_id'] not in unique:
            unique[p['external_id']] = p
    
    result = list(unique.values())
    if len(products) != len(result):
        print(f"  중복 제거: {len(products)} → {len(result)}개")
    return result

def chunk(lst, size):
    """청크 나누기"""
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("환경변수 없음")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    total_saved = 0
    
    # 각 카테고리별 크롤링
    for cat in CATEGORIES:
        cat_id = cat["id"]
        cat_name = cat["name"]
        
        print(f"\n{'='*60}")
        print(f"📦 카테고리: {cat_name} (ID: {cat_id})")
        print(f"{'='*60}")
        
        # 1. 최대 external_id 조회 (NULL 제외)
        try:
            last_item = supabase.table("new_products") \
                .select("external_id") \
                .eq("brand_id", 1) \
                .eq("category", cat_name) \
                .not_.is_("external_id", None) \
                .order("external_id", desc=True) \
                .limit(1) \
                .execute()
            
            max_gdIdx = last_item.data[0]['external_id'] if last_item.data else 0
            print(f"📊 현재 DB 마지막 상품 번호: {max_gdIdx}")
            
        except Exception as e:
            print(f"DB 조회 에러: {e}")
            max_gdIdx = 0

        # 2. 신상품 크롤링
        raw_products = fetch_new_products(supabase, cat_id, cat_name, max_gdIdx)

        if not raw_products:
            print(f"✨ {cat_name}에 새로운 상품이 없습니다.")
            continue

        # 3. 중복 제거
        unique_products = remove_duplicates(raw_products)

        # 4. 저장
        if unique_products:
            print(f"\n💾 {len(unique_products)}개 저장 중...")
            
            saved_count = 0
            for chunk_list in chunk(unique_products, CHUNK_SIZE):
                try:
                    supabase.table("new_products").insert(chunk_list).execute()
                    saved_count += len(chunk_list)
                    print(f"  {saved_count}/{len(unique_products)} 저장 완료")
                except Exception as e:
                    print(f"  저장 실패: {e}")
                    break
            
            print(f"🎉 {cat_name} 저장 완료: {saved_count}개")
            if unique_products:
                print(f"   - 최신 1위: {unique_products[-1]['title']}")
                print(f"   - 최신 2위: {unique_products[-2]['title'] if len(unique_products)>1 else '없음'}")
            
            total_saved += saved_count
    
    print(f"\n{'='*60}")
    print(f"🎉 전체 크롤링 완료! 총 {total_saved}개 저장됨")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()

