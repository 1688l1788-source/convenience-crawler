import os
import time
import re
import json
import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

CHUNK_SIZE = 50

def create_gs25_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Origin": "http://gs25.gsretail.com",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    })
    
    # 쿠키 먼저 설정
    r1 = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods")
    
    # CSRFToken 추출 (첨부파일 패턴 기준)
    csrf_match = re.search(r'"CSRFToken"["\']?\s*:\s*["\']([^"\']+)["\']', r1.text)
    if not csrf_match:
        csrf_match = re.search(r'CSRFToken["\']?\s*[:=]\s*["\']([^"\']+)["\']', r1.text)
    
    csrf_token = csrf_match.group(1) if csrf_match else None
    print(f"✅ CSRFToken: {csrf_token[:20] if csrf_token else '없음'}...")
    
    return session, csrf_token

def safe_json_parse(response_text):
    """강화된 JSON 파싱"""
    try:
        # 앞뒤 공백/따옴표 제거
        cleaned = response_text.strip().strip('"').strip("'")
        
        # json.loads 시도
        data = json.loads(cleaned)
        return data
    except:
        print("🔍 raw 응답 (처음 500자):")
        print(repr(response_text[:500]))
        return None

def fetch_gs25_event_products(session, csrf_token, page_num=1):
    """API 호출 + raw 파싱"""
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    payload = {
        "CSRFToken": csrf_token,
        "pageNum": str(page_num),
        "pageSize": "50",
        "searchType": "",
        "searchWord": "",
        "parameterList": "ONE_TO_ONE"
    }
    
    print(f"📡 페이지 {page_num} 요청...")
    r = session.post(url, data=payload, timeout=15)
    
    print(f"📊 상태: {r.status_code}, 길이: {len(r.text)}")
    print(f"📋 응답 시작: {r.text[:100]}")
    
    # raw 텍스트 파싱 (첨부파일처럼)
    json_match = re.search(r'\[.*\]', r.text, re.DOTALL)
    if json_match:
        raw_json = json_match.group(0)
        data = safe_json_parse(raw_json)
        
        if data and isinstance(data, list):
            print(f"✅ {len(data)}개 상품 파싱 성공!")
            return data
        elif data and "results" in data:
            print(f"✅ {len(data.get('results', []))}개 결과!")
            return data.get("results", [])
    
    print("❌ 파싱 실패")
    return []

def parse_gs25_product(item):
    """첨부파일 데이터 기준 파싱"""
    try:
        # 실제 데이터 구조 (첨부파일 기준)
        title = item.get("goodsNm", "")
        price = int(item.get("price", 0))
        image_url = item.get("attFileNm", "")
        att_file_id = item.get("attFileId", "")
        
        external_id = None
        if att_file_id:
            # MD0000001133387 → 1133387
            num_match = re.search(r'(\d+)', att_file_id)
            external_id = int(num_match.group(1)) if num_match else None
        
        if external_id and title:
            return {
                "title": title[:200],
                "price": price,
                "image_url": image_url,
                "category": "행사상품",
                "promotion_type": item.get("eventTypeNm", ""),
                "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                "is_active": True,
                "brand_id": 2,
                "external_id": external_id
            }
    except:
        pass
    return None

def main():
    session, csrf_token = create_gs25_session()
    if not csrf_token:
        print("❌ 세션 실패")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 최대 ID
    try:
        res = supabase.table("new_products").select("external_id").eq("brand_id", 2).eq("category", "행사상품").order("external_id", desc=True).limit(1).execute()
        max_id = res.data[0]['external_id'] if res.data else 0
    except:
        max_id = 0
    
    all_products = []
    for page in range(1, 6):  # 5페이지 테스트
        products = fetch_gs25_event_products(session, csrf_token, page)
        
        if not products:
            break
        
        valid_products = [parse_gs25_product(p) for p in products if parse_gs25_product(p) and parse_gs25_product(p)['external_id'] > max_id]
        all_products.extend(valid_products)
        print(f"✅ 페이지 {page}: {len(valid_products)}개 신상품")
        
        time.sleep(1)
    
    # 저장
    if all_products:
        for chunk in [all_products[i:i+CHUNK_SIZE] for i in range(0, len(all_products), CHUNK_SIZE)]:
            supabase.table("new_products").insert(chunk).execute()
        print(f"🎉 {len(all_products)}개 GS25 저장 완료!")
    else:
        print("😴 신상품 없음")

if __name__ == "__main__":
    main()
