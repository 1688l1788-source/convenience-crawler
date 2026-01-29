import os
import time
import re
import requests
import json
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

CHUNK_SIZE = 50

def create_gs25_session():
    """완전한 GS25 세션 생성"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Origin": "http://gs25.gsretail.com",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    })
    
    # 1. 메인 페이지 접속 (쿠키 설정)
    print("🔄 GS25 메인 페이지 접속...")
    r1 = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=10)
    print(f"📄 메인 응답: {len(r1.text)} bytes")
    
    # 2. CSRFToken 추출 (여러 패턴 시도)
    csrf_patterns = [
        r'CSRFToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
        r'name=["\']CSRFToken["\'][^>]*value=["\']([^"\']+)["\']',
        r'csrf["\']?\s*:\s*["\']([^"\']+)["\']',
    ]
    
    csrf_token = None
    for pattern in csrf_patterns:
        match = re.search(pattern, r1.text)
        if match:
            csrf_token = match.group(1)
            print(f"✅ CSRFToken: {csrf_token[:20]}...")
            break
    
    if not csrf_token:
        print("❌ CSRFToken 추출 실패. HTML 일부:")
        print(r1.text[:500])
        raise RuntimeError("CSRFToken 없음")
    
    return session, csrf_token

def fetch_gs25_event_products(session, csrf_token, page_num=1, page_size=50):
    """GS25 행사상품 API (강화된 에러 처리)"""
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    payload = {
        "CSRFToken": csrf_token,
        "pageNum": str(page_num),
        "pageSize": str(page_size),
        "searchType": "",
        "searchWord": "",
        "parameterList": "ONE_TO_ONE"
    }
    
    print(f"📡 API 호출: page={page_num}, token={csrf_token[:8]}...")
    
    try:
        r = session.post(url, data=payload, timeout=15)
        print(f"📊 응답 상태: {r.status_code}, 길이: {len(r.text)}")
        
        # 응답 디버깅
        if "로그인" in r.text or "CSRF" in r.text:
            print("❌ 로그인/CSRF 에러 감지")
            print(r.text[:300])
            return []
        
        # JSON 파싱 시도
        try:
            data = r.json()
        except json.JSONDecodeError:
            print("❌ JSON 파싱 실패. 응답 내용:")
            print(r.text[:500])
            return []
        
        results = data.get("results", [])
        pagination = data.get("pagination", {})
        total_count = pagination.get("totalNumberOfResults", 0)
        
        print(f"✅ 페이지 {page_num}: {len(results)}개 (총 {total_count}개)")
        return results
        
    except Exception as e:
        print(f"❌ API 상세 에러: {e}")
        print(f"응답 헤더: {dict(r.headers) if 'r' in locals() else '없음'}")
        return []

def parse_gs25_json_product(item):
    """JSON 파싱 (안전)"""
    try:
        title = item.get("goodsNm") or ""
        price = int(float(item.get("price") or 0))
        image_url = item.get("attFileNm") or ""
        att_file_id = item.get("attFileId") or ""
        
        # external_id: attFileId 또는 goodsNm 해시
        external_id = None
        if att_file_id and att_file_id.startswith("MD"):
            external_id = int(re.sub(r'\D', '', att_file_id))
        elif title:
            external_id = abs(hash(title)) % 10000000  # fallback
        
        event_type = item.get("eventTypeNm", "")
        promotion_type = f"{event_type} ({item.get('prmtCd', '')})"
        
        if not title or external_id is None:
            return None
        
        return {
            "title": title[:200],  # Supabase 제한
            "price": price,
            "image_url": image_url,
            "category": "행사상품",
            "promotion_type": promotion_type[:100],
            "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
            "is_active": item.get("goodsStat", {}).get("code") == "M",
            "brand_id": 2,
            "external_id": external_id
        }
    except Exception as e:
        print(f"파싱 에러: {e}")
        return None

def fetch_new_gs25_products(supabase):
    """GS25 완전 크롤링"""
    session, csrf_token = create_gs25_session()
    new_products = []
    
    # 최대 ID 조회
    try:
        last_item = supabase.table("new_products") \
            .select("external_id") \
            .eq("brand_id", 2) \
            .eq("category", "행사상품") \
            .not_.is_("external_id", None) \
            .order("external_id", desc=True) \
            .limit(1).execute()
        max_id = last_item.data[0]['external_id'] if last_item.data else 0
    except:
        max_id = 0
    
    print(f"📊 GS25 기존 최대ID: {max_id}")
    
    page = 1
    while page <= 10:  # 최대 10페이지 테스트
        products = fetch_gs25_event_products(session, csrf_token, page)
        
        if not products:
            break
        
        page_products = [p for p in [parse_gs25_json_product(item) for item in products] if p and p['external_id'] > max_id]
        new_products.extend(page_products)
        
        print(f"➕ 페이지 {page}: {len(page_products)}개 신상품")
        page += 1
        time.sleep(1)
    
    return new_products

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 환경변수 없음")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    gs_products = fetch_new_gs25_products(supabase)
    
    if gs_products:
        saved = 0
        for chunk_list in [gs_products[i:i+CHUNK_SIZE] for i in range(0, len(gs_products), CHUNK_SIZE)]:
            try:
                supabase.table("new_products").insert(chunk_list).execute()
                saved += len(chunk_list)
                print(f"💾 {saved}/{len(gs_products)} 저장")
            except Exception as e:
                print(f"❌ 저장 실패: {e}")
                break
        
        print(f"🎉 GS25 총 {saved}개 완료!")
    else:
        print("😴 신상품 없음")

if __name__ == "__main__":
    main()
