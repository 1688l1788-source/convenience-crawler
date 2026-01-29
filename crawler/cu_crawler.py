import os
import time
import re
import json
import requests
from supabase import create_client
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def extract_csrf_token(html):
    """다중 패턴 CSRFToken 추출"""
    patterns = [
        r'"CSRFToken"\s*:\s*"([^"]+)"',
        r'CSRFToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
        r'name=["\']CSRFToken["\'][^>]*value=["\']([^"\']+)["\']',
        r'csrf["\']?\s*:\s*["\']([^"\']+)["\']',
        r'CSRF["\']?\s*:\s*["\']([^"\']+)["\']',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            token = match.group(1)
            print(f"✅ CSRFToken 패턴{match.re.pattern[:30]}: {token[:20]}...")
            return token
    
    print("❌ 모든 패턴 실패. HTML 일부:")
    print(html[:1000])
    return None

def create_gs25_session():
    """강화된 세션"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    
    # 1. 메인 페이지
    print("🔄 GS25 메인 페이지 로드...")
    r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
    print(f"📄 응답: {len(r.text)} bytes")
    
    # 2. CSRFToken 추출
    csrf_token = extract_csrf_token(r.text)
    if not csrf_token:
        return None, None
    
    # 3. 헤더 업데이트 (API용)
    session.headers.update({
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Origin": "http://gs25.gsretail.com",
    })
    
    return session, csrf_token

def fetch_gs25_promotions(session, csrf_token):
    """행사상품 크롤링"""
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    promotions = []
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 기존 최대 ID
    try:
        res = supabase.table("new_products").select("external_id").eq("brand_id", 2).eq("category", "GS25행사상품").order("external_id", desc=True).limit(1).execute()
        max_id = res.data[0]['external_id'] if res.data else 0
    except:
        max_id = 0
    
    print(f"📊 기준 ID: {max_id}")
    
    for page in range(1, 6):
        payload = {
            "CSRFToken": csrf_token,
            "pageNum": str(page),
            "pageSize": "50",
            "searchType": "",
            "searchWord": "",
            "parameterList": "ONE_TO_ONE"
        }
        
        print(f"📡 페이지 {page}...")
        r = session.post(url, data=payload, timeout=10)
        
        # 파싱
        outer_match = re.search(r'^\s*(\{.*\})\s*$', r.text, re.DOTALL)
        if outer_match:
            data = json.loads(outer_match.group(1))
            items = data.get("results", [])
            
            new_items = []
            for item in items:
                p = parse_promotion(item)
                if p and p['external_id'] > max_id:
                    new_items.append(p)
            
            promotions.extend(new_items)
            print(f"   {len(new_items)}개 신규")
            
            if len(items) < 50:
                break
        
        time.sleep(0.5)
    
    return promotions

def parse_promotion(item):
    """파싱"""
    try:
        title = item.get("goodsNm", "")
        price = int(item.get("price", 0))
        att_file_id = item.get("attFileId", "")
        
        id_match = re.search(r'MD0*(\d+)', att_file_id)
        external_id = int(id_match.group(1)) if id_match else None
        
        if external_id and title:
            return {
                "title": title[:200],
                "price": price,
                "image_url": item.get("attFileNm", ""),
                "category": "GS25행사상품",
                "promotion_type": f"{item.get('eventTypeNm', '')} ({item.get('prmtCd', '')})",
                "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                "is_active": True,
                "brand_id": 2,
                "external_id": external_id,
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
            }
    except:
        pass
    return None

def main():
    session, csrf_token = create_gs25_session()
    if not csrf_token:
        print("❌ CSRFToken 획득 실패")
        return
    
    promotions = fetch_gs25_promotions(session, csrf_token)
    
    if promotions:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        supabase.table("new_products").upsert(promotions, on_conflict=["external_id", "brand_id"]).execute()
        
        print(f"\n🎉 {len(promotions)}개 GS25 행사상품 저장!")
        for p in promotions[:3]:
            print(f"   {p['title'][:30]}... | {p['promotion_type']}")
    else:
        print("😴 신규 행사상품 없음")

if __name__ == "__main__":
    main()
