import os
import time
import re
import json
import requests
from supabase import create_client

# 환경변수 로드
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# ==========================================
# 🧠 통합 카테고리 분류기 (앱 필터링용)
# ==========================================
def get_standard_category(title, raw_category=None):
    """
    제목과 원본 카테고리를 분석하여 앱에서 사용할 '표준 카테고리'를 반환합니다.
    우선순위: 생활용품 > 식사/라면 > 과자/간식 > 아이스 > 음료 > 원본기반 매핑
    """
    
    # 1. 생활용품 (가장 강력한 필터링)
    if any(k in title for k in [
        '치약', '칫솔', '가그린', '가글', '페리오', '메디안', '2080', '리치', '덴탈', '마우스', '쉐이빙', '면도기',
        '물티슈', '티슈', '마스크', '생리대', '중형', '대형', '소형', '오버나이트', '입는오버', '패드', '라이너', '탐폰', '팬티',
        '라엘', '쏘피', '화이트', '좋은느낌', '시크릿데이', '애니데이', '디어스킨', '순수한면',
        '샴푸', '린스', '트리트먼트', '헤어', '세럼', '비누', '엘라스틴', '케라시스', '오가니스트', '온더바디', '바디워시',
        '로션', '핸드크림', '수딩젤', '클렌징', '워터마이드', '에센셜', '존슨즈', '아비노', '니베아', '메디힐', '립케어', '오일',
        '세제', '락스', '슈가버블', '무균무때', '퐁퐁', '피지', '건전지', '스타킹', '밴드', '일회용', '제거', '클린핏', '우산', '양말'
    ]):
        return "생활용품"

    # 2. 식사/라면 (식품 중에서도 구체적인 식사류)
    if any(k in title for k in [
        '도시락', '김밥', '주먹밥', '샌드위치', '햄버거', '라면', '면', '우동', '국밥', '죽', '탕', '찌개', 
        '햇반', '컵반', '핫바', '소시지', '만두', '닭가슴살', '치킨', '육개장', '그래놀라', '통곡물밥', '크랩', '튀김', '브리또', '파스타'
    ]):
        return "식사/라면"

    # 3. 과자/간식
    if any(k in title for k in [
        '스낵', '젤리', '사탕', '껌', '초코', '쿠키', '칩', '빵', '케익', '약과', '양갱', '프레첼', '팝콘', 
        '아몬드', '육포', '어묵', '맛밤', '말차빵', '허쉬', '그릭요거트', '오팜', '푸딩', '디저트', '킷캣'
    ]):
        return "과자/간식"

    # 4. 아이스
    if any(k in title for k in ['아이스', '바', '콘', '파인트', '하겐다즈', '나뚜루', '설레임', '폴라포', '스크류', '돼지바', '빙수', '샤베트']):
        return "아이스"

    # 5. 음료
    if any(k in title for k in [
        '우유', '커피', '라떼', '아메리카노', '콜라', '사이다', '에이드', '주스', '보리차', '옥수수수염차', 
        '비타', '박카스', '쌍화', '두유', '요구르트', '요거트', '물', '워터', '프로틴', '콤부차', '드링크', '이온'
    ]):
        return "음료"

    # 6. 키워드에 안 걸렸지만 CU 원본 카테고리가 있는 경우 매핑
    if raw_category:
        if raw_category in ["간편식사", "즉석조리", "식품"]:
            return "식사/라면"
        if raw_category == "과자류":
            return "과자/간식"
        if raw_category == "아이스크림":
            return "아이스"
        if raw_category == "생활용품":
            return "생활용품"
        if raw_category == "음료":
            return "음료"

    return "기타"

# ==========================================
# 🏪 1. CU 크롤링
# ==========================================
def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작...")
    
    # 기존 데이터 삭제 (brand_id=1)
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://cu.bgfretail.com/event/product.do",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    # CU 내부 카테고리 코드 (원본 이름 저장용)
    cu_categories = {
        "GD_01": "간편식사",
        "GD_02": "즉석조리",
        "GD_03": "과자류",
        "GD_04": "아이스크림",
        "GD_05": "식품",
        "GD_06": "음료",
        "GD_07": "생활용품"
    }
    
    url = "https://cu.bgfretail.com/event/product.do"
    all_products = []

    for code, raw_cat_name in cu_categories.items():
        print(f"🔎 CU 조회: {raw_cat_name} ({code})")
        
        for page in range(1, 15): 
            payload = {"pageIndex": str(page), "listType": "1", "searchCondition": code, "user_id": ""}
            try:
                r = requests.post(url, data=payload, headers=headers, timeout=10)
                r.encoding = 'utf-8'
                
                # HTML 파싱 (정규식)
                titles = re.findall(r'<div class="name">.*?<p>(.*?)</p>.*?</div>', r.text, re.DOTALL)
                prices = re.findall(r'<strong>\s*([0-9,]+)\s*</strong>', r.text)
                images = re.findall(r'<img\s+src="(.*?)"', r.text)
                promos = re.findall(r'class="badge">.*?<span>(.*?)</span>', r.text, re.DOTALL)

                if not titles: break

                for i in range(len(titles)):
                    title = titles[i].strip()
                    # 1. 표준 카테고리 결정 (키워드 우선 + 원본 카테고리 보조)
                    std_category = get_standard_category(title, raw_cat_name)
                    
                    img = images[i]
                    if not img.startswith('http'): img = "https:" + img

                    all_products.append({
                        "title": title,
                        "price": int(prices[i].replace(',', '')),
                        "image_url": img,
                        "category": std_category,       # 앱 필터링용 표준 카테고리
                        "original_category": raw_cat_name, # CU 원본 카테고리 (DB 보존용)
                        "promotion_type": promos[i].strip() if i < len(promos) else "행사",
                        "brand_id": 1,
                        "source_url": "https://cu.bgfretail.com/event/product.do",
                        "is_active": True,
                        "external_id": int(time.time() * 1000) + i + (int(code[-2:]) * 10000)
                    })
                time.sleep(0.2)
            except Exception as e:
                print(f"   ❌ 에러: {e}")
                break

    if all_products:
        print(f"💾 CU {len(all_products)}개 저장 중...")
        for i in range(0, len(all_products), 100):
            supabase.table("new_products").insert(all_products[i:i+100]).execute()

# ==========================================
# 🏪 2. GS25 크롤링
# ==========================================
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    # 기존 데이터 삭제 (brand_id=2)
    supabase.table("new_products").delete().eq("brand_id", 2).execute()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "X-Requested-With": "XMLHttpRequest"
    })
    
    try:
        r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=10)
        csrf_token = re.search(r'name="CSRFToken" value="([^"]+)"', r.text).group(1)
    except:
        print("❌ GS25 토큰 획득 실패")
        return

    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    all_products = []

    for p_type in promo_types:
        print(f"🔎 GS25 조회: {p_type}")
        for page in range(1, 20):
            payload = {"CSRFToken": csrf_token, "pageNum": str(page), "pageSize": "50", "parameterList": p_type}
            try:
                r = session.post(url, data=payload, timeout=10)
                r.encoding = 'utf-8'
                data = json.loads(r.text) if isinstance(r.json(), str) else r.json()
                results = data.get("results", [])
                
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    
                    # GS25는 원본 카테고리가 없으므로 None 처리하고 키워드로 분류
                    std_category = get_standard_category(title, None)
                    
                    # ID 추출
                    id_match = re.search(r'(\d+)', item.get("attFileId", ""))
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time() * 1000)

                    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1", "GIFT": "덤증정"}

                    all_products.append({
                        "title": title,
                        "price": price,
                        "image_url": item.get("attFileNm", ""),
                        "category": std_category,       # 앱 필터링용 표준 카테고리
                        "original_category": None,      # GS는 원본 카테고리 정보 없음
                        "promotion_type": promo_map.get(p_type, "행사"),
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id
                    })
                time.sleep(0.3)
            except: break

    if all_products:
        print(f"💾 GS25 {len(all_products)}개 저장 중...")
        for i in range(0, len(all_products), 100):
            supabase.table("new_products").insert(all_products[i:i+100]).execute()

# ==========================================
# 🚀 메인 실행
# ==========================================
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 설정 오류: 환경변수 누락")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    crawl_cu(supabase)
    crawl_gs25(supabase)
    
    print("\n🎉 크롤링 완료")

if __name__ == "__main__":
    main()
