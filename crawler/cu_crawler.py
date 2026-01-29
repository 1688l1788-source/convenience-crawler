import os
import time
import re
import json
import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def create_gs25_session():
    session = requests.Session()
    # 헤더를 실제 브라우저와 더 유사하게 설정
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
    })
    
    try:
        r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=10)
        
        # 방식 1: <input type="hidden" name="CSRFToken" value="..."> 형태 찾기
        csrf_match = re.search(r'name="CSRFToken" value="([^"]+)"', r.text)
        
        # 방식 2: 방식 1 실패 시 기존 스크립트 형태 찾기
        if not csrf_match:
            csrf_match = re.search(r'CSRFToken\s*[:=]\s*["\']([^"\']+)["\']', r.text)
            
        csrf_token = csrf_match.group(1) if csrf_match else None
        
        if csrf_token:
            print(f"✅ GS25 CSRFToken 획득 성공: {csrf_token[:10]}...")
            return session, csrf_token
        else:
            print("❌ CSRFToken을 찾을 수 없습니다. (응답 길이는: ", len(r.text), ")")
            return session, None
    except Exception as e:
        print(f"❌ 세션 생성 중 네트워크 에러: {e}")
        return session, None

def parse_gs25_promotion(item, p_type):
    """Supabase 테이블에 맞춤 파싱"""
    try:
        title = item.get("goodsNm", "")[:255]
        price = int(item.get("price", 0))
        att_file_id = item.get("attFileId", "")
        
        # external_id 추출
        id_match = re.search(r'(\d+)', att_file_id)
        external_id = int(id_match.group(1)) if id_match else int(time.time() * 1000)
        
        # 행사 타입 한글화
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
                "brand_id": 2,
                "external_id": external_id 
            }
    except:
        pass
    return None

def main():
    # --- CU 크롤링 부분은 여기에 그대로 두시면 됩니다 ---
    # print("CU 크롤링 시작...") 
    
    # --- GS25 업데이트 부분 ---
    print("\n🚀 GS25 크롤링 시작...")
    session, csrf_token = create_gs25_session()
    
    if not csrf_token:
        print("❌ GS25 세션 준비 실패로 종료합니다.")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 행사 상품 종류 (GS는 카테고리 대신 행사 코드로 조회)
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    all_promotions = []

    for p_type in promo_types:
        print(f"🔎 행사 유형: {p_type} 조회 중...")
        for page in range(1, 11): # 최대 10페이지까지
            payload = {
                "CSRFToken": csrf_token,
                "pageNum": str(page),
                "pageSize": "50",
                "searchType": "",
                "searchWord": "",
                "parameterList": p_type
            }
            
            try:
                r = session.post(url, data=payload, timeout=10)
                # 응답에서 JSON 데이터만 추출 (앞뒤 공백 제거 및 매칭)
                data_match = re.search(r'(\{.*\})', r.text, re.DOTALL)
                
                if data_match:
                    data = json.loads(data_match.group(1))
                    items = data.get("results", [])
                    
                    if not items: break # 더 이상 데이터 없으면 중단
                    
                    for item in items:
                        p = parse_gs25_promotion(item, p_type)
                        if p: all_promotions.append(p)
                    
                    print(f"   - {page}페이지 완료 ({len(items)}개)")
                    time.sleep(0.5) # 서버 부하 방지
                else:
                    break
            except Exception as e:
                print(f"   ❌ {page}페이지 에러: {e}")
                break
    
    # Supabase 저장
    if all_promotions:
        print(f"\n💾 총 {len(all_promotions)}개 저장 중...")
        try:
            # 중복 데이터 방지를 위해 100개씩 upsert
            for i in range(0, len(all_promotions), 100):
                chunk = all_promotions[i:i + 100]
                supabase.table("new_products").upsert(chunk).execute()
            print("🎉 GS25 저장 완료!")
        except Exception as e:
            print(f"❌ 저장 에러: {e}")

if __name__ == "__main__":
    main()
