import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
import time

# --- 설정 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# 빈 문자열은 "전체 카테고리"를 의미합니다.
TARGET_CATEGORIES = ['']  
MAX_PAGES = 5

def main():
    print("🚀 CU 크롤러 시작 (전체 카테고리 모드)")

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

    # 2. 전체 카테고리 크롤링 (역순 5→1)
    # 카테고리가 ''(전체) 하나이므로 전체 신상품 순서대로 가져옵니다.
    for cat_code in TARGET_CATEGORIES:
        print(f"\n📂 전체 신상품(All) 크롤링 시작...")
        
        # 페이지를 역순으로 크롤링 (5→4→3→2→1)
        for page in range(MAX_PAGES, 0, -1):
            print(f"  - 페이지 {page} 요청 중...")
            
            url = "https://cu.bgfretail.com/product/productAjax.do"
            payload = {
                "pageIndex": page,
                "searchMainCategory": cat_code, # 전체
                "searchSubCategory": "",
                "listType": 1,
                "searchCondition": "",
                "searchUseYn": "N",
                "codeParent": ""
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
                    print("    ℹ️ 제품이 없습니다.")
                    continue

                print(f"    ✅ {len(items)}개 제품 발견")
                
                # 각 페이지 내에서도 역순으로 처리 (하단 → 상단)
                items_reversed = list(reversed(items))

                for item in items_reversed:
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

                        # 4. 행사 정보
                        badge_tag = item.select_one(".badge")
                        category_name = badge_tag.text.strip() if badge_tag else "일반"

                        # 5. 상품 상세 링크
                        detail_link = item.select_one("a")
                        product_url = "https://cu.bgfretail.com/product/product.do"
                        if detail_link and detail_link.get('href'):
                            href = detail_link['href']
                            if href.startswith('http'):
                                product_url = href
                            elif href.startswith('/'):
                                product_url = f"https://cu.bgfretail.com{href}"
                            elif '?' in href or 'product' in href:
                                product_url = f"https://cu.bgfretail.com/product/{href}"

                        product = {
                            "title": title,
                            "price": price,
                            "image_url": image_url,
                            "category": category_name,
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

    # 3. DB 저장
    print(f"\n💾 Supabase에 저장 중... (총 {len(all_products)}개)")
    
    if all_products:
        print(f"  🔝 시작 (가장 작은 ID - 5페이지 하단): {all_products[0]['title']}")
        print(f"  🔚 끝 (가장 큰 ID - 1페이지 상단): {all_products[-1]['title']}")
        
        # 마지막 아이템이 찰옥수수인지 확인용 로그
        if "찰옥수수" in all_products[-1]['title']:
            print("  ✨ 확인: 찰옥수수가 가장 마지막(최신)으로 감지되었습니다!")
        else:
            print(f"  ⚠️ 주의: 1페이지 최상단이 '{all_products[-1]['title']}' 입니다.")

    count = 0
    for product in all_products:
        try:
            supabase.table("new_products").insert(product).execute()
            count += 1
            if count % 50 == 0:
                print(f"  - {count}개 저장 완료...")
        except Exception as e:
            print(f"  ⚠️ 저장 실패 ({product['title']}): {e}")

    print(f"\n🎉 완료! 총 {count}개 제품 업데이트.")

if __name__ == "__main__":
    main()
