import os
import time
import re
import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def create_gs25_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
    })
    
    r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods")
    csrf_match = re.search(r'"CSRFToken"\s*:\s*"([^"]+)"', r.text)
    csrf_token = csrf_match.group(1) if csrf_match else None
    
    print(f"✅ CSRFToken: {csrf_token[:20] if csrf_token else '없음'}")
    return session, csrf_token

def parse_gs25_promotion(item):
    """Supabase 테이블에 맞춤 파싱"""
    try:
        title = item.get("goodsNm", "")[:255]  # varchar 제한
        price = int(item.get("price", 0))
        att_file_id = item.get("attFileId", "")
        
        # external_id: bigint (긴 숫자)
        id_match = re.search(r'MD0*(\d+)', att_file_id)
        external_id = int(id_match.group(1)) if id_match else None
        
        if external_id and title:
            return {
                "title": title,
                "price": price,
                "image_url": item.get("attFileNm", ""),
                "category": "GS25행사상품",
                "promotion_type": f"{item.get('eventTypeNm', '')} ({item.get('prmtCd', '')})",
                "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                "is_active": True,
                "brand_id": 2,
                "external_id": external_id  # 👈 bigint 호환
                # id, normalized_title, timestamps는 자동
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
    
    # 기존 최대 ID (bigint)
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
    
    print(f"📊 기준 external_id: {max_id}")
    
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    promotions = []
    
    for page in range(1, 6):
        payload = {
            "CSRFToken": csrf_token,
            "pageNum": str(page),
            "pageSize": "50",
            "searchType": "",
            "searchWord": "",
            "parameterList": "ONE_TO_ONE"
        }
        
        print(f"📡 페이지 {page}/5...")
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
            print(f"   ➕ {len(new_items)}개 신규")
    
    if promotions:
        print(f"\n💾 {len(promotions)}개 저장...")
        try:
            supabase.table("new_products") \
                .insert(promotions) \
                .execute()  # 👈 insert 사용 (upsert 대신)
            
            print(f"🎉 {len(promotions)}개 GS25행사상품 저장 완료!")
            
            # 확인
            count = supabase.table("new_products") \
                .select("count", count="external_id") \
                .eq("brand_id", 2) \
                .eq("category", "GS25행사상품") \
                .execute()
            
            print(f"📊 최종 GS25 수: {count.count}")
            
        except Exception as e:
            print(f"❌ 저장 에러: {e}")
            print("샘플 데이터:", promotions[0] if promotions else "없음")
    else:
        print("😴 신규 없음 (이미 최신)")

if __name__ == "__main__":
    main()
