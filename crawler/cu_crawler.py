import os
import time
import re
import json # JSON 라이브러리 추가
import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# --- CU 크롤링 함수 (기존 코드 유지) ---
def crawl_cu():
    # 사용자님의 CU 크롤링 로직이 들어가는 곳입니다.
    # 예시: print("CU 크롤링 시작...")
    pass

# --- GS25 관련 함수 업데이트 ---

def create_gs25_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
    })
    
    r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods")
    csrf_match = re.search(r'"CSRFToken"\s*:\s*"([^"]+)"', r.text)
    csrf_token = csrf_match.group(1) if csrf_match else None
    
    print(f"✅ GS25 CSRFToken 획득 완료")
    return session, csrf_token

def parse_gs25_promotion(item, p_type):
    """Supabase 테이블에 맞춤 파싱 (업데이트)"""
    try:
        title = item.get("goodsNm", "")[:255]
        price = int(item.get("price", 0))
        att_file_id = item.get("attFileId", "")
        
        # external_id 추출 (이미지 ID 등에서 숫자 추출)
        id_match = re.search(r'(\d+)', att_file_id)
        external_id = int(id_match.group(1)) if id_match else int(time.time() * 1000)
        
        # 행사 타입 한글화 (업데이트)
        promo_display = "1+1" if p_type == "ONE_TO_ONE" else "2+1" if p_type == "TWO_TO_ONE" else "덤증정"

        if title:
            return {
                "title": title,
                "price": price,
                "image_url": item.get("attFileNm", ""),
                "category": "GS25행사상품",
                "promotion_type": promo_display, # 👈 1+1, 2+1 등으로 저장
                "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                "is_active": True,
                "brand_id": 2,
                "external_id": external_id 
            }
    except Exception as e:
        print(f"❌ 파싱 에러: {e}")
    return None

def main():
    # 1. CU 크롤링 실행
    crawl_cu()

    # 2. GS25 크롤링 시작 (업데이트된 로직)
    print("\n🚀 GS25 크롤링 시작...")
    session, csrf_token = create_gs25_session()
    if not csrf_token:
        print("❌ GS25 세션 실패")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 행사 상품 종류 정의 (1+1, 2+1, 덤증정) - 업데이트 부분
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    all_promotions = []

    for p_type in promo_types:
        print(f"🔎 행사 유형: {p_type} 크롤링 중...")
        
        # 각 행사 유형별로 최대 10페이지까지 탐색 (필요시 조절)
        for page in range(1, 11):
            payload = {
                "CSRFToken": csrf_token,
                "pageNum": str(page),
                "pageSize": "50",
                "searchType": "",
                "searchWord": "",
                "parameterList": p_type # 👈 1+1, 2+1 등 파라미터 변경
            }
            
            r = session.post(url, data=payload)
            
            # JSON 응답 정규식 추출
            outer_match = re.search(r'^\s*(\{.*\})\s*$', r.text, re.DOTALL)
            if outer_match:
                try:
                    data = json.loads(outer_match.group(1))
                    items = data.get("results", [])
                    
                    if not items: # 결과가 없으면 다음 행사 유형으로
                        break
                        
                    for item in items:
                        p = parse_gs25_promotion(item, p_type)
                        if p:
                            all_promotions.append(p)
                    
                    print(f"   📄 {p_type} {page}페이지 완료 ({len(items)}개)")
                    
                    # GS25 서버 부하 방지를 위한 미세 대기
                    time.sleep(0.5)
                except:
                    break
            else:
                break
    
    # 3. 데이터 저장 (Upsert 방식 권장)
    if all_promotions:
        print(f"\n💾 총 {len(all_promotions)}개 데이터 저장 시도...")
        try:
            # upsert를 사용하면 중복 데이터는 업데이트하고 새 데이터는 추가합니다.
            # on_conflict는 중복을 체크할 컬럼(예: title, brand_id 등)이 DB에 unique 설정되어 있어야 합니다.
            # 여기서는 사용자님의 기존 방식대로 insert를 사용하되 전체 데이터를 한번에 넣습니다.
            
            # 팁: 데이터가 너무 많으면 100개씩 끊어서 넣는 것이 안전합니다.
            chunk_size = 100
            for i in range(0, len(all_promotions), chunk_size):
                chunk = all_promotions[i:i + chunk_size]
                supabase.table("new_products").upsert(chunk, on_conflict="title,brand_id").execute()
            
            print(f"🎉 GS25 크롤링 및 저장 완료!")
            
        except Exception as e:
            print(f"❌ 저장 중 오류 발생: {e}")
    else:
        print("😴 크롤링된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
