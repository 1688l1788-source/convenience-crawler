import os
import time
import re
import json
import requests
from supabase import create_client

# 환경변수 로드
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# --- 카테고리 분류 함수 (강화 버전) ---
def classify_category(title):
    """상품명을 분석하여 카테고리를 반환 (브랜드 및 키워드 확장)"""
    
    # 1. 음료 (가장 상품이 많으므로 상세하게 설정)
    if any(k in title for k in [
        '우유', '커피', '에이드', '콜라', '사이다', '물', '드링크', '주스', '차', '라떼', '음료', 
        '아메리카노', '워터', '펩시', '스프라이트', '닥터페퍼', '웰치스', '제로', '몬스터', 
        '핫식스', '토닉', '소다', '보리차', '비타', '밀키스', '환타', '트레비', '하늘보리', 
        '옥수수수염차', '조지아', '티오피', '칸타타', '스타벅스', '바리스타'
    ]):
        return "음료"
    
    # 2. 과자/간식
    if any(k in title for k in [
        '칩', '껌', '캔디', '젤리', '초코', '쿠키', '스낵', '봉지', '강정', '파이', 
        '카라멜', '양갱', '프레첼', '아몬드', '빼빼로', '소시지', '육포', '바바리안', '맛밤'
    ]):
        return "과자"
    
    # 3. 식사/면류 (냉장식품 포함)
    if any(k in title for k in [
        '라면', '면', '도시락', '김밥', '밥', '햄버거', '샌드위치', '주먹밥', '햇반', 
        '국밥', '파스타', '떡볶이', '우동', '짬뽕', '빵', '호떡', '만두', '샌드', '죽', 
        '스프', '치킨', '핫바'
    ]):
        return "식사"
    
    # 4. 아이스크림
    if any(k in title for k in [
        '아이스', '콘', '바', '홈', '하겐다즈', '나뚜루', '샤베트', '설레임', '폴라포', '붕어싸만코'
    ]):
        return "아이스크림"
    
    # 5. 생활용품
    if any(k in title for k in [
        '샴푸', '린스', '치약', '칫솔', '면도기', '물티슈', '세제', '마스크', '밴드', 
        '건전지', '스타킹', '비누', '로션', '가글'
    ]):
        return "생활용품"
    
    return "기타"

# --- GS25 관련 함수들 ---

def create_gs25_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "X-Requested-With": "XMLHttpRequest",
    })
    
    try:
        r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
        csrf_match = re.search(r'name="CSRFToken" value="([^"]+)"', r.text)
        if not csrf_match:
            csrf_match = re.search(r'CSRFToken\s*[:=]\s*["\']([^"\']+)["\']', r.text)
            
        csrf_token = csrf_match.group(1) if csrf_match else None
        return session, csrf_token
    except Exception as e:
        print(f"❌ GS25 세션 생성 실패: {e}")
        return session, None

def parse_gs25_promotion(item, p_type):
    try:
        title = item.get("goodsNm", "").strip()
        price = int(item.get("price", 0))
        att_file_id = item.get("attFileId", "")
        
        # bigint 범위 초과 방지 (뒤에서 18자리 숫자만 사용)
        id_match = re.search(r'(\d+)', att_file_id)
        if id_match:
            raw_id_str = id_match.group(1)
            external_id = int(raw_id_str[-18:])
        else:
            external_id = int(time.time() * 1000)
        
        promo_display = "1+1" if p_type == "ONE_TO_ONE" else "2+1" if p_type == "TWO_TO_ONE" else "덤증정"
        
        # 강화된 카테고리 분류 적용
        category_name = classify_category(title)

        if title:
            return {
                "title": title[:255],
                "price": price,
                "image_url": item.get("attFileNm", ""),
                "category": category_name,
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
        print("❌ 환경변수 누락")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 1. CU 크롤링 (기존 로직 유지)
    # crawl_cu(supabase)

    # 2. GS25 기존 데이터 삭제 (brand_id=2만)
    print("\n🧹 GS25 기존 데이터 삭제 중 (brand_id=2)...")
    try:
        supabase.table("new_products").delete().eq("brand_id", 2).execute()
        print("✅ 삭제 완료")
    except Exception as e:
        print(f"❌ 삭제 실패: {e}")

    # 3. GS25 크롤링 시작
    print("\n🚀 GS25 신규 크롤링 시작 (브랜드 분류 강화)...")
    session, csrf_token = create_gs25_session()
    
    if not csrf_token:
        print("❌ 토큰 획득 실패")
        return
    
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    all_gs25_products = []

    for p_type in promo_types:
        print(f"🔎 행사 유형: {p_type}")
        for page in range(1, 16): # 페이지 범위 넉넉히 설정
            payload = {"CSRFToken": csrf_token, "pageNum": str(page), "pageSize": "50", "parameterList": p_type}
            try:
                r = session.post(url, data=payload, timeout=15)
                r.encoding = 'utf-8'
                
                try:
                    data = r.json()
                except:
                    content = re.search(r'(\{.*\})', r.text, re.DOTALL)
                    data = json.loads(content.group(1)) if content else {}

                if isinstance(data, str): data = json.loads(data)
                
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    product = parse_gs25_promotion(item, p_type)
                    if product: all_gs25_products.append(product)
                
                print(f"   - {page}페이지 완료 ({len(results)}개)")
                time.sleep(0.5)
            except Exception as e:
                print(f"   ❌ {page}페이지 실패: {e}")
                break
    
    # 4. 결과 저장
    if all_gs25_products:
        print(f"\n💾 총 {len(all_gs25_products)}개 저장 중...")
        try:
            for i in range(0, len(all_gs25_products), 100):
                chunk = all_gs25_products[i:i + 100]
                supabase.table("new_products").insert(chunk).execute()
            print("🎉 GS25 수집 및 분류 저장 성공!")
        except Exception as e:
            print(f"❌ 최종 저장 에러: {e}")
    else:
        print("😴 수집된 데이터 없음")

if __name__ == "__main__":
    main()
