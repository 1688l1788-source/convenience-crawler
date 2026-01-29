import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
import time

# --- 설정 ---
# Supabase 환경 변수 (GitHub Secrets에 설정된 값 사용)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# 크롤링할 카테고리 설정
# 40: 아이스크림/스낵 (사용자 요청)
# 필요시 리스트에 추가: ['10'(간편식), '20'(즉석요리), '30'(음료), '40'(과자/아이스크림)]
TARGET_CATEGORIES = ['40'] 
MAX_PAGES = 5  # 가져올 페이지 수 (페이지당 20~40개)

def main():
    print("🚀 CU 크롤러 시작 (API 모드)")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 에러: Supabase 환경 변수가 없습니다.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. 기존 데이터 초기화 (선택사항: 깔끔하게 새로 시작하려면 주석 해제)
    print("🗑️ 기존 데이터 삭제 중...")
    try:
        supabase.table("new_products").delete().neq("id", 0).execute()
    except Exception as e:
        print(f"⚠️ 삭제 중 오류 (무시 가능): {e}")

    all_products = []

    # 2. 카테고리별 크롤링
    for cat_code in TARGET_CATEGORIES:
        print(f"\n📂 카테고리 {cat_code} 크롤링 시작...")
        
        for page in range(1, MAX_PAGES + 1):
            print(f"  - 페이지 {page} 요청 중...")
            
            # CU API URL 및 파라미터 (사용자가 찾은 값 적용)
            url = "https://cu.bgfretail.com/product/productAjax.do"
            payload = {
                "pageIndex": page,
                "searchMainCategory": cat_code,
                "searchSubCategory": "",
                "listType": 1,
                "searchCondition": "",
                "searchUseYn": "N",
                "codeParent": cat_code,
            }
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            }

            try:
                response = requests.post(url, data=payload, headers=headers, timeout=10)
                response.encoding = 'utf-8' # 한글 깨짐 방지
                
                if response.status_code != 200:
                    print(f"❌ 요청 실패: {response.status_code}")
                    continue

                # HTML 파싱
                soup = BeautifulSoup(response.text, 'html.parser')
                items = soup.select("li.prod_list") # 제품 리스트 선택자

                if not items:
                    print("    ℹ️ 더 이상 제품이 없습니다.")
                    break

                print(f"    ✅ {len(items)}개 제품 발견")

                for item in items:
                    try:
                        # 1. 제품명
                        name_tag = item.select_one(".name > p")
                        if not name_tag: continue
                        title = name_tag.text.strip()

                        # 2. 가격
                        price_tag = item.select_one(".price > strong")
                        price_text = price_tag.text.strip().replace(",", "").replace("원", "") if price_tag else "0"
                        price = int(price_text)

                        # 3. 이미지 URL
                        img_tag = item.select_one(".photo img")
                        image_url = img_tag['src'] if img_tag else ""
                        if image_url and not image_url.startswith("http"):
                            image_url = f"https:{image_url}" if image_url.startswith("//") else image_url

                        # 4. 행사 정보 (New, 1+1 등)
                        badge_tag = item.select_one(".badge")
                        category_name = "기타"
                        if badge_tag:
                            category_name = badge_tag.text.strip()
                        if not category_name:
                            category_name = "일반"

                        # 데이터 저장용 딕셔너리
                        product = {
                            "title": title,
                            "price": price,
                            "image_url": image_url,
                            "category": category_name, # DB 컬럼에 맞게 조정 (예: 1+1, NEW 등)
                            "source_url": "https://cu.bgfretail.com/product/product.do?category=product",
                            "is_active": True
                        }
                        
                        all_products.append(product)

                    except Exception as e:
                        print(f"    ⚠️ 제품 파싱 에러: {e}")
                        continue
                
                time.sleep(1) # 서버 부하 방지

            except Exception as e:
                print(f"❌ 페이지 요청 에러: {e}")

    # 3. 데이터 뒤집기 (오래된 것 -> 최신 순으로 정렬)
    # 이렇게 해야 DB에 먼저 들어간게 ID가 낮고, 나중에 들어간게 ID가 높아져서
    # 앱에서 '최신순(ID 역순/생성일 역순)'으로 볼 때 최신 제품이 맨 위에 뜹니다.
    print(f"\n🔄 데이터 정렬 중... (총 {len(all_products)}개)")
    all_products.reverse()

    # 4. DB 저장
    print("💾 Supabase에 저장 중...")
    count = 0
    for product in all_products:
        try:
            # 중복 체크 없이 단순 삽입 (앞에서 전체 삭제 했으므로)
            # 만약 upsert를 원하면 .upsert() 사용
            supabase.table("new_products").insert(product).execute()
            count += 1
            if count % 10 == 0:
                print(f"  - {count}개 저장 완료...")
        except Exception as e:
            print(f"  ⚠️ 저장 실패 ({product['title']}): {e}")

    print(f"\n🎉 완료! 총 {count}개 제품이 업데이트되었습니다.")

if __name__ == "__main__":
    main()
