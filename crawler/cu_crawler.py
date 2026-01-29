import os
import time
import re
import requests
import json
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

CHUNK_SIZE = 50

def get_gs25_csrf_token():
    """CSRFToken 동적 획득"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    
    # 메인 페이지에서 CSRFToken 추출
    r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods")
    csrf_match = re.search(r'CSRFToken["\']?\s*[:=]\s*["\']([^"\']+)["\']', r.text)
    
    if csrf_match:
        token = csrf_match.group(1)
        print(f"✅ CSRFToken 획득: {token[:20]}...")
        return session, token
    else:
        raise RuntimeError("CSRFToken 추출 실패")

def fetch_gs25_event_products(session, csrf_token, page_num=1, page_size=50):
    """GS25 행사상품 API 호출"""
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    payload = {
        "CSRFToken": csrf_token,
        "pageNum": page_num,
        "pageSize": page_size,
        "searchType": "",
        "searchWord": "",
        "parameterList": "ONE_TO_ONE"  # 1+1 이벤트
    }
    
    try:
        r = session.post(url, data=payload, timeout=10)
        r.raise_for_status()
        
        data = r.json()
        results = data.get("results", [])
        total_count = data["pagination"]["totalNumberOfResults"]
        
        print(f"📦 페이지 {page_num}: {len(results)}개 (총 {total_count}개)")
        return results
        
    except Exception as e:
        print(f"❌ API 호출 실패: {e}")
        return []

def parse_gs25_json_product(item, brand_id=2):
    """JSON 데이터 파싱"""
    try:
        title = item.get("goodsNm", "").strip()
        price = int(item.get("price", 0))
        image_url = item.get("attFileNm", "")
        
        # external_id: attFileId 사용 (고유)
        external_id = int(item.get("attFileId", 0)) if item.get("attFileId", "").isdigit() else None
        
        # 프로모션 정보
        event_type = item.get("eventTypeNm", "")
        promotion_type = f"{event_type} ({item.get('prmtCd', '')})"
        
        if not title or not external_id:
            return None
            
        return {
            "title": title,
            "price": price,
            "image_url": image_url,
            "category": "행사상품",  # GS25는 카테고리 없음
            "promotion_type": promotion_type,
            "source_url": f"http://gs25.gsretail.com/gscvs/ko/products/event-goods#{external_id}",
            "is_active": item.get("goodsStat", {}).get("code") == "M",  # M=정상
            "brand_id": brand_id,
            "external_id": external_id
        }
    except Exception as e:
        print(f"JSON 파싱 에러: {e}")
        return None

def fetch_new_gs25_products(supabase):
    """GS25 신상품만 크롤링"""
    print("\n🚀 GS25 행사상품 크롤링 시작!")
    
    session, csrf_token = get_gs25_csrf_token()
    new_products = []
    
    # DB에서 최대 external_id 조회
    try:
        last_item = supabase.table("new_products") \
            .select("external_id") \
            .eq("brand_id", 2) \
            .eq("category", "행사상품") \
            .not_.is_("external_id", None) \
            .order("external_id", desc=True) \
            .limit(1) \
            .execute()
        
        max_id = last_item.data[0]['external_id'] if last_item.data else 0
        print(f"📊 현재 GS25 최대 ID: {max_id}")
    except:
        max_id = 0
    
    page = 1
    while True:
        products = fetch_gs25_event_products(session, csrf_token, page, 50)
        
        if not products:
            break
            
        page_products = []
        for item in products:
            p = parse_gs25_json_product(item)
            if p and p['external_id'] > max_id:
                page_products.append(p)
        
        new_products.extend(page_products)
        
        if len(products) < 50:  # 마지막 페이지
            break
            
        page += 1
        time.sleep(0.5)
    
    print(f"✅ GS25 신상품 {len(new_products)}개 수집!")
    return new_products

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. GS25 행사상품
    gs_products = fetch_new_gs25_products(supabase)
    
    if gs_products:
        saved = 0
        for chunk_list in chunk(gs_products, CHUNK_SIZE):
            try:
                supabase.table("new_products").insert(chunk_list).execute()
                saved += len(chunk_list)
                print(f"💾 GS25 {saved}/{len(gs_products)} 저장")
            except Exception as e:
                print(f"저장 실패: {e}")
                break
        
        print(f"🎉 GS25 {saved}개 완료!")
    
    print("✅ GS25 크롤링 완료!")

if __name__ == "__main__":
    main()
