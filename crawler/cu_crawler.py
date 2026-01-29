import os
import time
import re
import json
import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def create_gs25_session():
    """GS25 세션 + CSRFToken"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Origin": "http://gs25.gsretail.com",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    })
    
    # 메인 페이지
    r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods")
    
    # CSRFToken 추출
    csrf_match = re.search(r'"CSRFToken"\s*:\s*"([^"]+)"', r.text)
    csrf_token = csrf_match.group(1) if csrf_match else None
    
    print(f"✅ CSRFToken: {csrf_token[:20] if csrf_token else '없음'}...")
    return session, csrf_token

def fetch_gs25_promotions(session, csrf_token, max_pages=5):
    """GS25 행사상품 전체 크롤링"""
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    
    all_promotions = []
    updated_count = 0
    
    # DB 기존 데이터 확인
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    try:
        res = supabase.table("new_products") \
            .select("external_id") \
            .eq("brand_id", 2) \
            .eq("category", "GS25행사상품") \
            .not_.is_("external_id", None) \
            .order("external_id", desc=True) \
            .limit(1).execute()
        max_id = res.data[0]['external_id'] if res.data else 0
        print(f"📊 기존 GS25 최대 ID: {max_id}")
    except:
        max_id = 0
    
    for page in range(1, max_pages + 1):
        payload = {
            "CSRFToken": csrf_token,
            "pageNum": str(page),
            "pageSize": "50",
            "searchType": "",
            "searchWord": "",
            "parameterList": "ONE_TO_ONE"  # 1+1 행사
        }
        
        print(f"📡 행사상품 페이지 {page}/5...")
        r = session.post(url, data=payload)
        
        # JSON 파싱
        outer_match = re.search(r'^\s*(\{.*\})\s*$', r.text, re.DOTALL)
        if outer_match:
            data = json.loads(outer_match.group(1))
            products = data.get("results", [])
            
            page_new = []
            for item in products:
                promotion = parse_gs25_promotion(item)
                if promotion and promotion['external_id'] > max_id:
                    page_new.append(promotion)
            
            all_promotions.extend(page_new)
            print(f"   ➕ {len(page_new)}개 신행사상품")
            updated_count += len(page_new)
            
            if len(products) < 50:  # 마지막 페이지
                break
        
        time.sleep(0.5)
    
    print(f"\n✅ 총 {updated_count}개 GS25 행사상품 업데이트!")
    return all_promotions

def parse_gs25_promotion(item):
    """GS25 행사상품 파싱"""
    try:
        title = item.get("goodsNm", "").strip()
        price = int(float(item.get("price", 0)))
        image_url = item.get("attFileNm", "")
        att_file_id = item.get("attFileId", "")
        
        # external_id: MD0000001133387 → 1133387
        id_match = re.search(r'MD0*(\d+)', att_file_id)
        external_id = int(id_match.group(1)) if id_match else None
        
        promotion_type = item.get("eventTypeNm", "")
        prmt_cd = item.get("prmtCd", "")
        
        if external_id and title:
            return {
                "title": title[:200],
                "price": price,
                "image_url": image_url,
                "category": "GS25행사상품",  # ✅ 명확히 구분
                "promotion_type": f"{promotion_type} ({prmt_cd})",
                "source_url": f"http://gs25.gsretail.com/gscvs/ko/products/event-goods#{external_id}",
                "is_active": item.get("goodsStat", {}).get("code") == "M",
                "brand_id": 2,  # GS25
                "external_id": external_id,
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
            }
    except Exception as e:
        print(f"파싱 에러: {e}")
    
    return None

def save_to_supabase(products):
    """배치 저장"""
    if not products:
        return 0
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    saved = 0
    
    # 중복 체크 후 삽입 (upsert)
    for chunk in [products[i:i+CHUNK_SIZE] for i in range(0, len(products), CHUNK_SIZE)]:
        try:
            # 기존 데이터 upsert (external_id, brand_id 기준)
            supabase.table("new_products") \
                .upsert(chunk, on_conflict=["external_id", "brand_id"]) \
                .execute()
            saved += len(chunk)
        except Exception as e:
            print(f"저장 에러: {e}")
            break
    
    return saved

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 환경변수 확인")
        return
    
    print("🚀 GS25 행사상품 크롤러 시작!")
    
    session, csrf_token = create_gs25_session()
    if not csrf_token:
        print("❌ 세션 실패")
        return
    
    # 행사상품 수집
    promotions = fetch_gs25_promotions(session, csrf_token)
    
    # 저장
    saved = save_to_supabase(promotions)
    
    print("\n" + "="*50)
    print(f"🎉 GS25 행사상품 {saved}개 완료!")
    print(f"📂 카테고리: GS25행사상품")
    print(f"🏪 브랜드: GS25 (ID: 2)")
    print("="*50)
    
    # 최근 3개 미리보기
    if promotions:
        print("\n📦 최근 행사상품:")
        for p in promotions[-3:]:
            print(f"   {p['promotion_type']} | {p['title'][:40]}... | {p['price']}원")

if __name__ == "__main__":
    main()
