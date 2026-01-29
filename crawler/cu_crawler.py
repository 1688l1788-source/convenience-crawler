import os
import time
import re
import json
import requests
from supabase import create_client

# 환경변수 로드
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# --- 1. CU 크롤링 함수 (기존 로직 유지) ---
def crawl_cu(supabase):
    print("🚀 CU 크롤링 시작...")
    # 여기에 기존 CU 크롤링 코드가 있다고 가정합니다.
    pass

# --- 2. GS25 크롤링 관련 함수들 ---

def create_gs25_session():
    """GS25 세션 생성 및 CSRF 토큰 획득"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "X-Requested-With": "XMLHttpRequest", # 👈 필수: AJAX 요청임을 명시
    })
    
    try:
        r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
        # CSRFToken 추출 (input 태그 또는 스크립트 내부)
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
        title = item.get("goodsNm", "").strip()
        price = int(item.get("price", 0))
        # 이미지 파일명에서 ID 추출 시도 (실패 시 타임스탬프)
        att_file_id = item.get("attFileId", "")
        id_match = re.search(r'(\d+)', att_file_id)
        external_id = int(id_match.group(1)) if id_match else int(time.time() * 1000)
        
        promo_display = "1+1" if p_type == "ONE_TO_ONE" else "2+1" if p_type == "TWO_TO_ONE" else "덤증정"

        if title:
            return {
                "title": title[:255],
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
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Supabase 설정이 없습니다.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 1. CU 크롤링 실행
    crawl_cu(supabase)

    # 2. GS25 기존 데이터 삭제 (brand_id=2만)
    print("\n🧹 GS25 기존 데이터 삭제 중...")
    try:
        supabase.table("new_products").delete().eq("brand_id", 2).execute()
        print("✅ 삭제 완료")
    except Exception as e:
        print(f"❌ 삭제 실패: {e}")

    # 3. GS25 크롤링 시작
    print("\n🚀 GS25 신규 크롤링 시작...")
    session, csrf_token = create_gs25_session()
    
    if not csrf_token:
        print("❌ GS25 CSRF 토큰 획득 실패")
        return
    
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    all_gs25_products = []

    for p_type in promo_types:
        print(f"🔎 행사 유형: {p_type}")
        for page in range(1, 11):
            payload = {
                "CSRFToken": csrf_token,
                "pageNum": str(page),
                "pageSize": "50",
                "parameterList": p_type
            }
            
            try:
                # 👈 POST 요청 시 전송 방식 확인
                r = session.post(url, data=payload, timeout=15)
                r.encoding = 'utf-8'
                
                # 만약 응답이 JSON 형식이면 r.json()이 성공함
                try:
                    data = r.json()
                except:
                    # JSON이 아닐 경우 정규식으로 다시 한번 시도
                    content = re.search(r'(\{.*\})', r.text, re.DOTALL)
                    if content:
                        data = json.loads(content.group(1))
                    else:
                        raise ValueError("응답이 JSON 형식이 아닙니다.")

                # 데이터 파싱
                # GS25 응답 구조는 보통 'results'가 아니라 'JSON.parse'된 문자열일 수 있어 2차 확인
                if isinstance(data, str):
                    data = json.loads(data)
                
                results = data.get("results", [])
                if not results:
                    break
                
                for item in results:
                    product = parse_gs25_promotion(item, p_type)
                    if product:
                        all_gs25_products.append(product)
                
                print(f"   - {page}페이지 완료 ({len(results)}개)")
                time.sleep(0.8)
                
            except Exception as e:
                print(f"   ❌ {page}페이지 실패: {e}")
                # 에러 디버깅을 위해 응답 앞부분 출력
                # print(f"      응답샘플: {r.text[:100]}")
                break
    
    # 4. 결과 저장
    if all_gs25_products:
        print(f"\n💾 총 {len(all_gs25_products)}개 저장 중...")
        try:
            for i in range(0, len(all_gs25_products), 100):
                chunk = all_gs25_products[i:i + 100]
                supabase.table("new_products").insert(chunk).execute()
            print("🎉 GS25 저장 완료!")
        except Exception as e:
            print(f"❌ 저장 에러: {e}")

if __name__ == "__main__":
    main()
