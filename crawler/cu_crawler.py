import os
import time
import re
import json
import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def extract_csrf_token(html):
    """CSRFToken 추출"""
    patterns = [
        r'"CSRFToken"\s*:\s*"([^"]+)"',
        r'CSRFToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            return match.group(1)
    return None

def create_gs25_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
    })
    
    r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods")
    csrf_token = extract_csrf_token(r.text)
    
    print(f"✅ CSRFToken: {csrf_token[:20] if csrf_token else '실패'}")
    return session, csrf_token

def fetch_gs25_promotions(session, csrf_token):
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    promotions = []
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 기존 최대 ID
    try:
        res = supabase.table("new_products") \
            .select("external_id") \
            .eq("brand_id", 2) \
            .eq("category", "GS25행사상품") \
            .order("external_id", desc=True) \
            .limit(1).execute()
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
        r = session.post(url, data=payload)
        
        outer_match = re.search(r'^\s*(\{.*\})\s*$', r.text, re.DOTALL)
        if outer_match:
            data = json.loads(outer_match.group(1))
            items = data.get("results", [])
            
            new_items = []
            for item in items:
                p = parse_gs25_promotion(item)
                if p and p['external_id'] > max_id:
                    new_items.append(p)
            
            promotions.extend(new_items)
            print(f"   ➕ {len(new_items)}개")
            
            if len(items) < 50:
                break
        
        time.sleep(0.5)
    
    return promotions

def parse_gs25_promotion(item):
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
    
    promotions = fetch_gs25_promotions(session, csrf_token)
    
    if promotions:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        supabase.table("new_products") \
            .upsert(promotions, on_conflict=["external_id", "brand_id"]) \
            .execute()
        
        print(f"\n🎉 {len(promotions)}개 GS25행사상품 저장 완료!")
        for p in promotions[:3]:
            print(f"   📦 {p['title'][:40]}... | {p['promotion_type']} | {p['price']}원")
    else:
        print("😴 신규 행사상품 없음")

if __name__ == "__main__":
    main()
