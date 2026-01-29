import os
import time
import re
import json
import requests
from supabase import create_client

# 환경변수 로드
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# --- 1. CU 크롤링 함수 (기존 사용자님의 코드를 이 함수 안에 두시면 됩니다) ---
def crawl_cu(supabase):
    print("🚀 CU 크롤링 시작 (기존 로직 유지)...")
    # 여기에 기존 CU 크롤링 및 저장 로직을 그대로 두세요.
    # brand_id가 1인 CU 데이터는 아래 GS25 삭제 로직에 영향을 받지 않습니다.
    pass

# --- 2. GS25 크롤링 관련 함수들 ---

def create_gs25_session():
    """GS25 세션 생성 및 CSRF 토큰 획득"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
    })
    
    try:
        r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=10)
        # 다양한 형태의 CSRF 토큰 패턴 대응
        csrf_match = re.search(r'name="CSRFToken" value="([^"]+)"', r.text)
        if not csrf_match:
            csrf_match = re.search(r'CSRFToken\s*[:=]\s*["\']([^"\']+)["\']', r.text)
            
        csrf_token = csrf_match.group(1) if csrf_match else None
        return session, csrf_token
    except Exception as e:
        print(f"❌ GS25 세션 생성 실패: {e}")
        return session, None

def parse_gs25_promotion(item, p_type):
    """GS25 상품 데이터를 Supabase 형식으로 변환"""
    try:
        title = item.get("goodsNm", "")[:255]
        price = int(item.get("price", 0))
        att_file_id = item.get("attFileId", "")
        
        # 외부 ID 추출 (이미지 ID 등 활용)
        id_match = re.search(r'(\d+)', att_file_id)
        external_id = int(id_match.group(1)) if id_match else int(time.time() * 1000)
        
        # 행사 타입 한글 매핑
        promo_display = "1+1" if p_type == "ONE_TO_ONE" else "2+1" if p_type == "TWO_TO_ONE" else "덤증정"

        if title:
            return {
                "title": title,
                "price": price,
                "image_url": item.get("attFileNm", ""),
                "category": "GS25행사상품",
                "promotion_type": promo_display,
                "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                "is_active": True,
                "brand_id": 2, # GS25 고유 ID
                "external_id": external_id 
            }
    except:
        pass
    return None

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Supabase 환경변수가 설정되지 않았습니다.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 1. CU 크롤링 실행 (기존 로직)
    crawl_cu(supabase)

    # 2. GS25 크롤링 및 기존 데이터 초기화 시작
    print("\n🧹 GS25 기존 데이터 삭제 중 (brand_id=2)...")
    try:
        # brand_id가 2인 GS25 데이터만 골라서 삭제 (CU 데이터는 보존됨)
        supabase.table("new_products").delete().eq("brand_id", 2).execute()
        print("✅ 기존 GS25 데이터 삭제 완료")
    except Exception as e:
        print(f"❌ 데이터 삭제 실패: {e}")

    print("\n🚀 GS25 신규 크롤링 시작...")
    session, csrf_token = create_gs25_session()
    
    if not csrf_token:
        print("❌ GS25 CSRF 토큰을 찾을 수 없어 종료합니다.")
        return
    
    # 크롤링할 행사 유형
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    all_gs25_products = []

    for p_type in promo_types:
        print(f"🔎 행사 유형 조회: {p_type}")
        for page in range(1, 15): # 최대 15페이지까지 확인
            payload = {
                "CSRFToken": csrf_token,
                "pageNum": str(page),
                "pageSize": "50",
                "searchType": "",
                "searchWord": "",
                "parameterList": p_type
            }
            
            try:
                r = session.post(url, data=payload, timeout=15)
                # ✅ [중요] 한글 깨짐 방지를 위해 인코딩을 utf-8로 강제 지정
                r.encoding = 'utf-8' 
                
                # JSON 추출
                data_match = re.search(r'(\{.*\})', r.text, re.DOTALL)
                if data_match:
                    data = json.loads(data_match.group(1))
                    items = data.get("results", [])
                    
                    if not items: 
                        break # 데이터가 없으면 다음 행사 유형으로
                    
                    for item in items:
                        product = parse_gs25_promotion(item, p_type)
                        if product:
                            all_gs25_products.append(product)
                    
                    print(f"   - {page}페이지 완료 ({len(items)}개 수집됨)")
                    time.sleep(0.5)
                else:
                    break
            except Exception as e:
                print(f"   ❌ {page}페이지 에러: {e}")
                break
    
    # 3. 수집된 GS25 데이터를 Supabase에 저장
    if all_gs25_products:
        print(f"\n💾 총 {len(all_gs25_products)}개의 GS25 상품 저장 중...")
        try:
            # 100개씩 끊어서 저장 (안정성)
            for i in range(0, len(all_gs25_products), 100):
                chunk = all_gs25_products[i:i + 100]
                supabase.table("new_products").insert(chunk).execute()
            print("🎉 GS25 모든 데이터 저장 성공!")
        except Exception as e:
            print(f"❌ 저장 중 에러 발생: {e}")
    else:
        print("😴 수집된 GS25 데이터가 없습니다.")

if __name__ == "__main__":
    main()
