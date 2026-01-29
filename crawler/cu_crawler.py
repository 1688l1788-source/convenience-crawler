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

# ✅ 통합 카테고리 (브랜드별)
BRANDS = {
    "CU": {
        "id": 1,
        "base_url": "https://cu.bgfretail.com",
        "ajax_url": "https://cu.bgfretail.com/product/productAjax.do",
        "categories": [
            {"id": "40", "name": "아이스크림"},
            {"id": "30", "name": "과자류"},
            {"id": "10", "name": "간편식사"},
            {"id": "20", "name": "식품"},
            {"id": "60", "name": "음료"},
            {"id": "50", "name": "생활용품"},
        ]
    },
    "GS25": {
        "id": 2,
        "base_url": "https://gs25.gsretail.com",
        "ajax_url": "https://gs25.gsretail.com/goods/goodsListAjax.do", 
        "categories": [
            {"id": "1001", "name": "아이스크림"},  # 실제 ID는 사이트에서 확인
            {"id": "1002", "name": "과자류"},
            {"id": "1003", "name": "간편식사"},
            {"id": "1004", "name": "식품"},
            {"id": "1005", "name": "음료"},
            {"id": "1006", "name": "생활용품"},
        ]
    }
}

def parse_product(item, category_name, brand_info):
    """브랜드별 상품 파싱"""
    try:
        if brand_info["id"] == 1:  # CU
            name_tag = item.select_one(".name p")
            price_tag = item.select_one(".price strong")
            onclick_div = item.select_one("div[onclick*='view']")
            
            title = (name_tag.get_text(strip=True) if name_tag else "").strip()
            price_text = (price_tag.get_text(strip=True) if price_tag else "0").replace(",", "").replace("원", "")
            price = int(price_text) if price_text.isdigit() else 0
            
            # CU gdIdx
            gdIdx = None
            if onclick_div:
                onclick = onclick_div.get("onclick", "")
                m = re.search(r"view\\s*\\(\\s*(\\d+)\\s*\\)", onclick)
                if m:
                    gdIdx = int(m.group(1))
            external_id = gdIdx
            
        else:  # GS25
            name_tag = item.select_one(".goods_info .name, .name")
            price_tag = item.select_one(".goods_info .price, .price")
            
            title = (name_tag.get_text(strip=True) if name_tag else "").strip()
            price_text = (price_tag.get_text(strip=True) if price_tag else "0").replace(",", "").replace("원", "")
            price = int(price_text) if price_text.isdigit() else 0
            
            # GS25 goodsNo
            external_id = None
            data_goods = item.get("data-goods-no")
            if data_goods:
                external_id = int(data_goods)
            else:
                onclick_div = item.select_one("[onclick]")
                if onclick_div:
                    onclick = onclick_div.get("onclick", "")
                    m = re.search(r"(?:fnDetailView|detail)\s*\(\s*'(\d+)'", onclick)
                    if m:
                        external_id = int(m.group(1))

        # 공통 이미지 처리
        img_tag = item.select_one("img")
        image_url = ""
        if img_tag:
            image_url = img_tag.get("src") or img_tag.get("data-src") or ""
            if image_url.startswith("//"):
                image_url = "https:" + image_url
            elif image_url.startswith("/"):
                image_url = brand_info["base_url"] + image_url

        badge_tag = item.select_one(".badge, .ico_event, .event")
        promotion_type = badge_tag.get_text(strip=True) if badge_tag else None
        
        product_url = f"{brand_info['base_url']}/product/view.do?gdIdx={external_id}" if brand_info["id"] == 1 else f"{brand_info['base_url']}/goods/goodsView.do?goodsNo={external_id}"
        
        if not title or external_id is None:
            return None

        return {
            "title": title,
            "price": price,
            "image_url": image_url,
            "category": category_name,
            "promotion_type": promotion_type,
            "source_url": product_url if external_id else brand_info["base_url"],
            "is_active": True,
            "brand_id": brand_info["id"],
            "external_id": external_id
        }
    except Exception as e:
        print(f"파싱 에러 ({brand_info['id']}): {e}")
        return None

def fetch_new_products(supabase, brand_info, category_id, category_name, max_external_id):
    """브랜드/카테고리별 신상품 크롤링"""
    new_products = []
    brand_name = "CU" if brand_info["id"] == 1 else "GS25"
    
    print(f"\n🔄 [{brand_name} {category_name}] max_id={max_external_id}보다 큰 상품...")
    
    for page in range(1, MAX_SEARCH_PAGES + 1):
        if brand_info["id"] == 1:  # CU
            payload = {
                "pageIndex": page,
                "searchMainCategory": category_id,
                "listType": 0
            }
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            }
        else:  # GS25
            payload = {
                "nowPage": page,
                "cateNo": category_id,
                "dispCtgryNo": "",
                "searchType": "",
                "searchWord": "",
                "sortType": "01",
                "listLimt": "48"
            }
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": f"{brand_info['base_url']}/goods/goodsList.do",
            }

        try:
            r = requests.post(brand_info["ajax_url"], data=payload, headers=headers, timeout=10)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            
            # 브랜드별 선택자
            selector = "li.prod_list" if brand_info["id"] == 1 else "ul.goods_list li.goods_item, li.goods_item"
            items = soup.select(selector)
            
            if not items:
                print(f"  🛑 페이지 {page}: 끝! (총 {len(new_products)}개)")
                break
            
            count_in_page = 0
            for item in items:
                p = parse_product(item, category_name, brand_info)
                if p and p['external_id'] > max_external_id:
                    new_products.append(p)
                    count_in_page += 1
            
            print(f"  ✅ 페이지 {page}: {count_in_page}개 (누적 {len(new_products)})")
            time.sleep(0.2)
            
        except Exception as e:
            print(f"  ❌ 페이지 {page}: {e}")
            break
    
    return new_products

def remove_duplicates(products):
    unique = {}
    for p in products:
        key = f"{p['brand_id']}_{p['external_id']}"
        unique[key] = p
    return list(unique.values())

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("환경변수 없음")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    total_saved = 0
    
    # ✅ 모든 브랜드 순차 처리
    for brand_name, brand_info in BRANDS.items():
        print(f"\n{'='*70}")
        print(f"🚀 {brand_name} 크롤링 시작!")
        print(f"{'='*70}")
        
        brand_saved = 0
        
        for cat in brand_info["categories"]:
            cat_id = cat["id"]
            cat_name = cat["name"]
            
            print(f"\n📦 [{brand_name}] {cat_name} (ID: {cat_id})")
            
            # 최대 external_id 조회
            try:
                last_item = supabase.table("new_products") \
                    .select("external_id") \
                    .eq("brand_id", brand_info["id"]) \
                    .eq("category", cat_name) \
                    .not_.is_("external_id", None) \
                    .order("external_id", desc=True) \
                    .limit(1) \
                    .execute()
                
                max_id = last_item.data[0]['external_id'] if last_item.data else 0
            except:
                max_id = 0

            # 신상품 크롤링
            raw_products = fetch_new_products(supabase, brand_info, cat_id, cat_name, max_id)
            
            if raw_products:
                unique_products = remove_duplicates(raw_products)
                saved_count = 0
                
                for chunk_list in chunk(unique_products, CHUNK_SIZE):
                    try:
                        supabase.table("new_products").insert(chunk_list).execute()
                        saved_count += len(chunk_list)
                    except Exception as e:
                        print(f"저장 실패: {e}")
                        break
                
                print(f"💾 [{brand_name} {cat_name}] {saved_count}개 저장!")
                brand_saved += saved_count
                total_saved += saved_count
        
        print(f"✅ {brand_name} 완료: {brand_saved}개")
    
    print(f"\n{'='*70}")
    print(f"🎉 전체 완료! CU+GS25 총 {total_saved}개 신상품 저장")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
