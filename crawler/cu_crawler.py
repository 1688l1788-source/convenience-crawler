import os
import time
import re
import json
import requests
from bs4 import BeautifulSoup
from supabase import create_client

# 환경변수 로드
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# ==========================================
# 🧠 통합 카테고리 분류기 (규칙 강화 버전)
# ==========================================
def get_standard_category(title, raw_category=None):
    """
    상품명과 원본 카테고리를 분석하여 표준 카테고리를 반환합니다.
    순서: 무조건 아이스 > 생활용품 > 식사(바g 제외 등) > 과자 > 아이스(바 끝남) > 음료
    """
    
    # 0. [무조건 아이스] (브랜드명 등 강력한 키워드)
    if any(k in title for k in ['하겐', '소르베', '라라스윗', '나뚜루', '벤앤']):
        return "아이스"

    # 1. [생활용품] (가장 명확하므로 최우선)
    if any(k in title for k in [
        '치약', '칫솔', '가글', '가그린', '페리오', '메디안', '2080', '리치', '덴탈', '마우스', '쉐이빙', '면도기',
        '물티슈', '티슈', '마스크', '생리대', '중형', '대형', '소형', '오버나이트', '입는오버', '패드', '라이너', '탐폰', '팬티',
        '라엘', '쏘피', '화이트', '좋은느낌', '시크릿데이', '애니데이', '디어스킨', '순수한면',
        '샴푸', '린스', '트리트먼트', '헤어', '세럼', '비누', '엘라스틴', '케라시스', '오가니스트', '온더바디', '바디워시',
        '로션', '핸드크림', '수딩젤', '클렌징', '워터마이드', '에센셜', '존슨즈', '아비노', '니베아', '메디힐', '립케어', '오일',
        '세제', '락스', '슈가버블', '무균무때', '퐁퐁', '피지', '건전지', '스타킹', '밴드', '일회용', '제거', '클린핏', '우산', '양말'
    ]):
        return "생활용품"

    # 2. [식사/라면] (식품으로 오인될 수 있는 '바' 처리 포함)
    # 정규식: "바" 뒤에 "숫자+g"가 붙는 패턴 (예: 핫바80g, 바 90g) -> 식품임
    is_food_bar_regex = re.search(r'바\s*\d+g', title)
    
    if is_food_bar_regex or any(k in title for k in [
        '직화', '후랑크바', '꼬치바', # 👈 요청하신 식품 키워드
        '도시락', '김밥', '주먹밥', '샌드위치', '햄버거', '라면', '면', '우동', '국밥', '죽', '탕', '찌개', 
        '햇반', '컵반', '핫바', '소시지', '만두', '닭가슴살', '치킨', '육개장', '그래놀라', '통곡물밥', '크랩', '튀김', '브리또', '파스타', 
        '3XL', '킬바사', '오징어', '밥바', '버거', '샐러드', '야끼소바', '베이컨', '스테이크'
    ]):
        return "식사/라면"

    # 3. [과자/간식] (바, 콘 등이 들어간 과자류 처리)
    if any(k in title for k in [
        '꼬깔콘', '콘초', '콘푸라이트', '오곡코코볼', '에너지바', '단백질바', '프로틴바', '크런키', '초코바', '자유시간',
        '스낵', '젤리', '사탕', '껌', '초코', '쿠키', '칩', '빵', '케익', '약과', '양갱', '프레첼', '팝콘', 
        '아몬드', '육포', '어묵', '맛밤', '말차빵', '허쉬', '그릭요거트', '오팜', '푸딩', '디저트', '킷캣', '도넛'
    ]):
        return "과자/간식"

    # 4. [아이스] (위에서 식품/과자의 '바', '콘'을 걸러냈으므로 나머지는 아이스크림일 확률 높음)
    # 조건: '바'로 끝나거나, 아이스크림 관련 키워드 포함
    if title.endswith('바') or any(k in title for k in [
        '아이스', '콘', '파인트', '설레임', '폴라포', '스크류', '돼지바', '빙수', '샤베트', '찰옥수수',
        '미니컵', '비비빅', '메로나', '누가바', '쌍쌍바', '바밤바', '옥동자', '와일드바디', '붕어싸만코', 
        '더위사냥', '빵빠레', '구슬', '탱크보이', '빠삐코', '요맘때', '쿠앤크', '수박바', '죠스바', 
        '제로윗', '로우윗', '서주', '동그린', '삼우', '파르페', '쿨리쉬'
    ]):
        return "아이스"

    # 5. [음료]
    if any(k in title for k in [
        '우유', '커피', '라떼', '아메리카노', '콜라', '사이다', '에이드', '주스', '보리차', '옥수수수염차', 
        '비타', '박카스', '쌍화', '두유', '요구르트', '요거트', '물', '워터', '프로틴', '콤부차', '드링크', '이온', 
        '티', 'TEA', '바리스타', '콘트라', '카페', '마이노멀', '서울FB', '맥주', '하이볼'
    ]):
        return "음료"

    # 6. [CU 원본 카테고리 매핑] (키워드로 분류 안 된 경우 최후의 수단)
    if raw_category:
        if raw_category == "아이스크림": return "아이스"
        if raw_category == "음료": return "음료"
        if raw_category == "과자류": return "과자/간식"
        if raw_category == "생활용품": return "생활용품"
        if raw_category in ["간편식사", "즉석조리", "식품"]: return "식사/라면"

    return "기타"

# ==========================================
# 🏪 1. CU 크롤링 (listType:0 전체조회 + 원본카테고리 우선)
# ==========================================
def parse_cu_product(item, raw_cat_name):
    try:
        name_tag = item.select_one(".name p")
        if not name_tag: return None
        title = name_tag.get_text(strip=True)
        
        price_tag = item.select_one(".price strong")
        price = int(price_tag.get_text(strip=True).replace(",", "")) if price_tag else 0
        
        img_tag = item.select_one("img")
        img_src = ""
        if img_tag:
            img_src = img_tag.get("src") or ""
            if img_src.startswith("//"): img_src = "https:" + img_src
            elif img_src.startswith("/"): img_src = "https://cu.bgfretail.com" + img_src
        
        badge = item.select_one(".badge")
        promo = "행사"
        if badge:
            span = badge.select_one("span")
            promo = span.get_text(strip=True) if span else badge.get_text(strip=True)

        gdIdx = None
        onclick = item.select_one("div[onclick*='view']")
        if onclick:
            m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick.get('onclick', ''))
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx: return None

        # ✅ 수정된 분류기 사용
        std_category = get_standard_category(title, raw_cat_name)

        return {
            "title": title,
            "price": price,
            "image_url": img_src,
            "category": std_category,
            "original_category": raw_cat_name,
            "promotion_type": promo,
            "brand_id": 1,
            "source_url": f"https://cu.bgfretail.com/product/view.do?category=product&gdIdx={gdIdx}",
            "is_active": True,
            "external_id": gdIdx
        }
    except: return None

def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작...")
    
    # DB 초기화 (카테고리 재정렬을 위해)
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    cu_categories = [
        {"id": "40", "name": "아이스크림"},
        {"id": "10", "name": "간편식사"},
        {"id": "20", "name": "즉석조리"},
        {"id": "30", "name": "과자류"},
        {"id": "50", "name": "식품"},
        {"id": "60", "name": "음료"},
        {"id": "70", "name": "생활용품"}
    ]
    
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    
    total_count = 0
    
    for cat in cu_categories:
        cat_id = cat["id"]
        cat_name = cat["name"]
        print(f"🔎 CU [{cat_name}] 전체 스캔 중...")
        
        items_in_cat = []
        for page in range(1, 25): 
            try:
                r = requests.post("https://cu.bgfretail.com/product/productAjax.do", 
                                data={"pageIndex": page, "searchMainCategory": cat_id, "listType": 0},
                                headers=headers, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                
                if not items: break
                
                for item in items:
                    p = parse_cu_product(item, cat_name)
                    if p:
                        items_in_cat.append(p)
                
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ 에러: {e}")
                break
        
        if items_in_cat:
            unique = {p['external_id']: p for p in items_in_cat}.values()
            chunk_list = list(unique)
            print(f"   💾 {len(chunk_list)}개 저장...")
            
            for i in range(0, len(chunk_list), 100):
                supabase.table("new_products").insert(chunk_list[i:i+100]).execute()
            total_count += len(chunk_list)

    print(f"📊 CU 총 {total_count}개 완료")

# ==========================================
# 🏪 2. GS25 크롤링 (안전한 전체 갱신)
# ==========================================
def get_gs25_token():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    })
    
    for i in range(3):
        try:
            r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            token_input = soup.find("input", {"name": "CSRFToken"})
            if token_input and token_input.get('value'):
                return session, token_input['value']
            m = re.search(r"CSRFToken\s*[:=]\s*['\"]([^'\"]+)['\"]", r.text)
            if m: return session, m.group(1)
            time.sleep(1)
        except: time.sleep(1)
    return session, None

def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    session, token = get_gs25_token()
    if not token:
        print("❌ GS25 토큰 획득 실패.")
        return

    print(f"   🔑 토큰 획득 성공 ({token[:10]}...)")
    
    session.headers.update({
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://gs25.gsretail.com"
    })

    all_gs_items = []
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1", "GIFT": "덤증정"}

    for p_type in promo_types:
        print(f"🔎 GS25 조회: {p_type}")
        for page in range(1, 20):
            try:
                url = "https://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
                payload = {
                    "CSRFToken": token, "pageNum": str(page), "pageSize": "50", 
                    "parameterList": p_type
                }
                r = session.post(url, data=payload, timeout=10)
                try: data = r.json()
                except: data = json.loads(r.text)
                if isinstance(data, str): data = json.loads(data)
                
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    
                    all_gs_items.append({
                        "title": title,
                        "price": int(item.get("price", 0)),
                        "image_url": item.get("attFileNm", ""),
                        "category": get_standard_category(title, None), # 키워드 분류
                        "original_category": None,
                        "promotion_type": promo_map.get(p_type, "행사"),
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id
                    })
                time.sleep(0.1)
            except Exception as e: break

    if len(all_gs_items) > 0:
        print(f"✅ GS25 총 {len(all_gs_items)}개 수집 성공. DB 갱신 중...")
        try:
            supabase.table("new_products").delete().eq("brand_id", 2).execute()
            unique_gs = {p['external_id']: p for p in all_gs_items}.values()
            items_list = list(unique_gs)
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").insert(items_list[i:i+100]).execute()
            print("🎉 GS25 데이터 업데이트 완료!")
        except Exception as e:
            print(f"❌ GS25 저장 실패: {e}")
    else:
        print("😱 경고: GS25 데이터를 찾지 못했습니다.")

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
    
    print("\n🎉 모든 크롤링 작업이 안전하게 완료되었습니다!")

if __name__ == "__main__":
    main()
