import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
import time
import re

# --- 설정 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# ✅ 전체 카테고리 추가 (10단위 추측)
TARGET_CATEGORIES = {
    '10': '간편식사',
    '20': '즉석조리',
    '30': '과자',
    '40': '아이스크림',
    '50': '식품',
    '60': '음료',
    '70': '생활용품'
}

MAX_PAGES = 5


def main():
    print("🚀 CU 크롤러 시작 (전체 카테고리)")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 에러: Supabase 환경 변수가 없습니다.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. 기존 데이터 초기화
    print("🗑️ 기존 데이터 삭제 중...")
    try:
        supabase.table("new_products").delete().neq("id", 0).execute()
    except Exception as e:
        print(f"⚠️ 삭제 중 오류 (무시 가능): {e}")

    all_products = []
    category_stats = {}

    # 2. 카테고리별 크롤링
    for cat_code, cat_name in TARGET_CATEGORIES.items():
        print(f"\n📂 카테고리 {cat_code} ({cat_name}) 크롤링 시작...")
        category_count = 0
        
        for page in range(1, MAX_PAGES + 1):
            print(f"  - 페이지 {page} 요청 중...")
            
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
                
                if response.status_code != 200:
                    print(f"❌ 요청 실패: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                items = soup.select("li.prod_list")

                if not items:
                    print("    ℹ️ 더 이상 제품이 없습니다.")
                    break

                print(f"    ✅ {len(items)}개 제품 발견")
                category_count += len(items)
                
                # 첫 페이지 첫 제품 확인
                if page == 1 and items:
                    first_title = items[0].select_one(".name p")
                    if first_title:
                        print(f"    🔝 첫 제품: {first_title.text.strip()}")

                for item in items:
                    try:
                        # 1. 제품명
                        name_tag = item.select_one(".name p")
                        if not name_tag:
                            continue
                        title = name_tag.text.strip()

                        # 2. 가격
                        price_tag = item.select_one(".price strong")
                        price_text = price_tag.text.strip().replace(",", "").replace("원", "") if price_tag else "0"
                        price = int(price_text) if price_text.isdigit() else 0

                        # 3. 이미지 URL
                        img_tag = item.select_one("img")
                        image_url = ""
                        
                        if img_tag:
                            image_url = (
                                img_tag.get('src') or 
                                img_tag.get('data-src') or 
                                img_tag.get('data-original') or 
                                ""
                            )
                            
                            if image_url:
                                if image_url.startswith('//'):
                                    image_url = f"https:{image_url}"
                                elif image_url.startswith('/'):
                                    image_url = f"https://cu.bgfretail.com{image_url}"
                                elif not image_url.startswith('http'):
                                    image_url = f"https://cu.bgfretail.com/{image_url}"

                        # 4. 카테고리 및 행사 정보
                        badge_tag = item.select_one(".badge")
                        promotion_type = badge_tag.text.strip() if badge_tag else None

                        # 5. 상품 상세 링크
                        product_url = "https://cu.bgfretail.com/product/view.do?category=product"
                        onclick_div = item.select_one("div[onclick*='view']")
                        if onclick_div:
                            onclick = onclick_div.get('onclick', '')
                            match = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick)
                            if match:
                                gdIdx = match.group(1)
                                product_url = f"https://cu.bgfretail.com/product/view.do?gdIdx={gdIdx}&category=product"

                        product = {
                            "title": title,
                            "price": price,
                            "image_url": image_url,
                            "category": cat_name,  # ✅ 카테고리명 사용
                            "promotion_type": promotion_type,
                            "source_url": product_url,
                            "is_active": True,
                            "brand_id": 1
                        }
                        
                        all_products.append(product)

                    except Exception as e:
                        print(f"    ⚠️ 제품 파싱 에러: {e}")
                        continue
                
                time.sleep(1)

            except Exception as e:
                print(f"❌ 페이지 요청 에러: {e}")
        
        category_stats[cat_name] = category_count
        print(f"  📊 {cat_name}: 총 {category_count}개 제품")

    # 3. DB 저장
    print(f"\n💾 Supabase에 저장 중... (총 {len(all_products)}개)")
    count = 0
    
    if all_products:
        print(f"  🔝 첫 크롤링: {all_products[0]['title']} ({all_products[0]['category']})")
        print(f"  🔚 마지막 크롤링: {all_products[-1]['title']} ({all_products[-1]['category']})")
    
    # 역순으로 저장
    for product in reversed(all_products):
        try:
            supabase.table("new_products").insert(product).execute()
            count += 1
            if count % 50 == 0:
                print(f"  - {count}개 저장 완료...")
        except Exception as e:
            print(f"  ⚠️ 저장 실패 ({product['title']}): {e}")

    # 카테고리별 통계 출력
    print(f"\n🎉 완료! 총 {count}개 제품이 업데이트되었습니다.")
    print("\n📊 카테고리별 통계:")
    for cat_name, cat_count in category_stats.items():
        print(f"  - {cat_name}: {cat_count}개")

if __name__ == "__main__":
    main()
