import os
import time
import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# 안전장치: 최대 50페이지까지 탐색 (보통 20~30페이지면 끝남)
MAX_SEARCH_PAGES = 50 
TARGET_COUNT = 50  # 최종적으로 저장할 최신 상품 개수

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
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            onclick = onclick_div.get("onclick", "")
            m = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick)
            if m:
                gdIdx = m.group(1)
                product_url = f"https://cu.bgfretail.com/product/view.do?gdIdx={gdIdx}&category=product"
        
        # 제목이 없으면 스킵
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
    except Exception:
        return None

def fetch_all_icecream():
    all_products = []
    print("🔄 전체 페이지 스캔 시작 (끝까지 찾기)...")
    
    for page in range(1, MAX_SEARCH_PAGES + 1):
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
            r = requests.post(url, data=payload, headers=headers, timeout=5)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("li.prod_list")

            if not items:
                print(f"  🛑 페이지 {page}: 데이터 없음. 여기가 끝!")
                break
            
            # 페이지 내 상품 수집
            page_products = []
            for item in items:
                p = parse_product(item)
                if p:
                    page_products.append(p)
            
            all_products.extend(page_products)
            print(f"  ✅ 페이지 {page}: {len(page_products)}개 수집 (누적 {len(all_products)}개)")
            
            time.sleep(0.1) # 빠르게
            
        except Exception as e:
            print(f"  ❌ 페이지 {page} 에러: {e}")
            break
            
    return all_products

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("환경변수 없음")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 1. 기존 데이터 삭제
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    # 2. 전체 크롤링 (페이지 1부터 끝까지)
    # 리스트 순서: [오래된 상품(1페이지) ..... 최신 상품(마지막페이지)]
    full_list = fetch_all_icecream()

    if not full_list:
        print("❌ 수집된 데이터가 없습니다.")
        return

    # 3. 마지막 50개만 자르기 (이게 진짜 최신 상품들)
    # 뒤에서 50개 슬라이싱
    latest_products = full_list[-TARGET_COUNT:]
    
    print(f"\n✂️ 전체 {len(full_list)}개 중 최신 {len(latest_products)}개만 선택함.")
    print(f"   - 최신 1위 예상: {latest_products[-1]['title']}")
    print(f"   - 최신 2위 예상: {latest_products[-2]['title']}")

    # 4. 저장
    # latest_products 리스트는 [덜 최신 -> 더 최신 -> 가장 최신] 순서임
    # 이대로 insert하면 ID가 순서대로 부여됨 (1 -> 2 -> ... -> 50)
    # 가장 최신이 ID 50이 됨.
    # 앱에서 ORDER BY id DESC 하면 ID 50(가장 최신)이 맨 위에 나옴. ✅ 정답!
    
    if latest_products:
        try:
            supabase.table("new_products").insert(latest_products).execute()
        except Exception:
            for part in chunk(latest_products, 50): # 50개 한번에
                supabase.table("new_products").insert(part).execute()

    print(f"\n🎉 저장 완료! 앱에서 확인해보세요.")

if __name__ == "__main__":
    main()
