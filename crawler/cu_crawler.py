import os
import time
import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

MAX_SEARCH_PAGES = 100  # 충분히 크게 (전체 스캔용)

def parse_product(item):
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

        product_url = "https://cu.bgfretail.com/product/view.do?category=product"
        gdIdx = None
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            onclick = onclick_div.get("onclick", "")
            m = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick)
            if m:
                gdIdx = int(m.group(1)) # 숫자형으로 저장
                product_url = f"https://cu.bgfretail.com/product/view.do?gdIdx={gdIdx}&category=product"
        
        if not title: return None

        return {
            "title": title,
            "price": price,
            "image_url": image_url,
            "category": "아이스크림",
            "promotion_type": promotion_type,
            "source_url": product_url,
            "is_active": True,
            "brand_id": 1,
            "external_id": gdIdx  # ✅ 중복 체크용 고유 ID
        }
    except Exception:
        return None

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("환경변수 없음")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. DB에 있는 가장 최신 external_id(gdIdx) 조회
    # (이 번호보다 큰 것만 새로 추가하면 됨)
    try:
        last_item = supabase.table("new_products") \
            .select("external_id") \
            .eq("brand_id", 1) \
            .order("external_id", desc=True) \
            .limit(1) \
            .execute()
        
        max_gdIdx = last_item.data[0]['external_id'] if last_item.data else 0
        print(f"📊 현재 DB 마지막 상품 번호: {max_gdIdx}")
    except Exception:
        max_gdIdx = 0

    new_products = []
    
    # 2. 전체 페이지 스캔 (페이지 1부터 끝까지 가면서 데이터 수집)
    # CU는 페이지 1이 가장 오래된 것 -> 뒤로 갈수록 최신
    # 따라서 페이지 1부터 쭉 훑으면서 max_gdIdx보다 큰 것만 담으면 됨.
    # 만약 DB가 비어있으면(max_gdIdx=0) 전체가 다 담김.
    
    print("🔄 업데이트 스캔 시작...")
    
    for page in range(1, MAX_SEARCH_PAGES + 1):
        url = "https://cu.bgfretail.com/product/productAjax.do"
        payload = {"pageIndex": page, "searchMainCategory": "40", "listType": 0}
        headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

        try:
            r = requests.post(url, data=payload, headers=headers, timeout=5)
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("li.prod_list")

            if not items:
                print(f"  🛑 페이지 {page}: 끝!")
                break
            
            count_in_page = 0
            for item in items:
                p = parse_product(item)
                if p and p['external_id']:
                    # 이미 있는 상품(번호가 작거나 같음)은 스킵? 
                    # 아니면 가격 변동 업데이트를 위해 덮어쓰기?
                    # "서버 부하 적게"가 목표라면 스킵이 맞음.
                    # 하지만 CU 구조상 뒤페이지에 새 상품이 나오므로, 일단 다 훑어야 함.
                    
                    if p['external_id'] > max_gdIdx:
                        new_products.append(p)
                        count_in_page += 1
            
            # 진행 상황 출력
            if count_in_page > 0:
                print(f"  ✅ 페이지 {page}: 신상품 {count_in_page}개 발견")
            else:
                # 이 페이지에 신상품이 하나도 없다면?
                # CU 구조상: 앞 페이지(1, 2..)는 옛날 상품이므로 신상품이 없을 수 있음.
                # 계속 뒤로 가야 함.
                print(f"  PASS 페이지 {page} (기존 상품들)")
                
            time.sleep(0.1)

        except Exception as e:
            print(f"❌ 에러: {e}")
            break
    
    # 3. 신상품만 저장 (bulk insert)
    if new_products:
        print(f"\n💾 신상품 {len(new_products)}개 저장 중...")
        
        # 순서대로 저장 (오래된 신상 -> 아주 최신 신상)
        # new_products 리스트는 이미 오름차순(페이지 순서대로)으로 쌓였음.
        # 그대로 넣으면 됨.
        
        # 청크 나누어 저장
        for i in range(0, len(new_products), 50):
            chunk = new_products[i:i+50]
            supabase.table("new_products").insert(chunk).execute()
            
        print("🎉 업데이트 완료!")
    else:
        print("\n✨ 새로운 상품이 없습니다. 최신 상태입니다.")

if __name__ == "__main__":
    main()
