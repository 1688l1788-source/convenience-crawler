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
# 🧠 통합 카테고리 분류기 (순서 및 예외처리 강화)
# ==========================================
def get_standard_category(title, raw_category=None):
    """
    상품명을 분석하여 앱 표준 카테고리로 분류합니다.
    검사 순서: 생활용품 > 식사 > 음료 > 과자 > 아이스 (오분류 방지)
    """
    
    # 1. 생활용품 (가장 명확하므로 최우선)
    if any(k in title for k in [
        '치약', '칫솔', '가글', '가그린', '페리오', '메디안', '2080', '리치', '덴탈', '마우스', '쉐이빙', '면도기',
        '물티슈', '티슈', '마스크', '생리대', '중형', '대형', '소형', '오버나이트', '입는오버', '패드', '라이너', '탐폰', '팬티',
        '라엘', '쏘피', '화이트', '좋은느낌', '시크릿데이', '애니데이', '디어스킨', '순수한면',
        '샴푸', '린스', '트리트먼트', '헤어', '세럼', '비누', '엘라스틴', '케라시스', '오가니스트', '온더바디', '바디워시',
        '로션', '핸드크림', '수딩젤', '클렌징', '워터마이드', '에센셜', '존슨즈', '아비노', '니베아', '메디힐', '립케어', '오일',
        '세제', '락스', '슈가버블', '무균무때', '퐁퐁', '피지', '건전지', '스타킹', '밴드', '일회용', '제거', '클린핏', '우산', '양말', '바디'
    ]):
        return "생활용품"

    # 2. 식사/라면 (햄버거, 샌드위치, 밥바 등 '바'나 '콘'이 들어갈 수 있는 식품 먼저 처리)
    if any(k in title for k in [
        '도시락', '김밥', '주먹밥', '밥바', '샌드위치', '샌드', '햄버거', '버거', '샐러드', '야끼소바',
        '라면', '면', '우동', '국밥', '죽', '탕', '찌개', '국', '컵반', '햇반', 
        '핫바', '소시지', '후랑크', '만두', '닭가슴살', '치킨', '육개장', '폭탄콘', '베이컨', '스테이크',
        '그래놀라', '통곡물밥', '크랩', '튀김', '브리또', '파스타', '3XL', '킬바사', '오징어'
    ]):
        return "식사/라면"

    # 3. 음료 ('커피', '라떼'가 아이스크림으로 오분류되는 것 방지)
    if any(k in title for k in [
        '우유', '커피', '라떼', '아메리카노', '콜라', '사이다', '에이드', '주스', '보리차', '옥수수수염차', 
        '비타', '박카스', '쌍화', '두유', '요구르트', '요거트', '물', '워터', '프로틴', '콤부차', '드링크', '이온', 
        '티', 'TEA', '바리스타', '콘트라', '카페', '마이노멀', '서울FB'
    ]):
        return "음료"

    # 4. 과자/간식 ('꼬깔콘', '에너지바' 등 오분류 방지)
    if any(k in title for k in [
        '꼬깔콘', '치토스', '콘초', '코코볼', '콘푸라이트', '단백질바', '에너지바', '초코바', '크런키',
        '스낵', '젤리', '사탕', '껌', '초코', '쿠키', '칩', '빵', '케익', '약과', '양갱', '프레첼', '팝콘', 
        '아몬드', '육포', '어묵', '맛밤', '말차빵', '허쉬', '그릭요거트', '오팜', '푸딩', '디저트', '킷캣', '도넛'
    ]):
        return "과자/간식"

    # 5. 아이스 (위에서 안 걸러진 것들 중 아이스크림 키워드)
    if any(k in title for k in [
        '아이스', '파인트', '하겐다즈', '나뚜루', '설레임', '폴라포', '스크류', '돼지바', '빙수', '샤베트', 
        '미니컵', '비비빅', '메로나', '누가바', '쌍쌍바', '바밤바', '옥동자', '와일드바디', '붕어싸만코', 
        '더위사냥', '빵빠레', '구슬', '소프트콘', '탱크보이', '빠삐코', '요맘때', '쿠앤크', '수박바', '죠스바', 
        '벤앤', '라라스윗', '제로윗', '로우윗', '서주', '동그린', '삼우'
    ]):
        return "아이스"
    
    # 5-1. 아이스 보조 (단어 '콘', '바'는 위험하므로 조건부 검사)
    # 위 식사/과자 단계에서 '꼬깔콘', '핫바' 등이 이미 걸러졌으므로 여기서는 비교적 안전
    if title.endswith('바') or title.endswith('콘') or '파르페' in title:
        return "아이스"

    # 6. CU 원본 카테고리 매핑 (최후의 보루)
    if raw_category:
        if raw_category in ["간편식사", "즉석조리", "식품"]: return "식사/라면"
        if raw_category == "과자류": return "과자/간식"
        if raw_category == "아이스크림": return "아이스"
        if raw_category == "생활용품": return "생활용품"
        if raw_category == "음료": return "음료"

    return "기타"

# ==========================================
# 🏪 1. CU 크롤링 (GD_xx -> 숫자 코드 사용)
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
            if img_src and not img_src.startswith("http"):
                img_src = "https:" + img_src
        
        badge = item.select_one(".badge")
        promo = badge.get_text(strip=True) if badge else "행사"
        if promo not in ["1+1", "2+1"]: return None # 행사 상품만

        # ID 추출
        gdIdx = None
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            m = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick_div.get("onclick", ""))
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx: return None

        # 정밀 분류 적용
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
    
    # 카테고리 코드 (숫자)
    cu_categories = {
        "10": "간편식사", "20": "즉석조리", "30": "과자류",
        "40": "아이스크림", "50": "식품", "60": "음료", "70": "생활용품"
    }
    
    all_items = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://cu.bgfretail.com"
    }
    
    for code, name in cu_categories.items():
        print(f"🔎 CU 조회: {name}")
        # DB에서 최신 ID 조회
        try:
            res = supabase.table("new_products").select("external_id").eq("brand_id", 1).eq("original_category", name).order("external_id", desc=True).limit(1).execute()
            max_id = res.data[0]['external_id'] if res.data else 0
        except: max_id = 0

        for page in range(1, 20):
            try:
                r = requests.post("https://cu.bgfretail.com/product/productAjax.do", 
                                data={"pageIndex": page, "searchMainCategory": code, "listType": 1}, 
                                headers=headers, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                
                if not items: break
                
                page_count = 0
                for item in items:
                    p = parse_cu_product(item, name)
                    if p:
                        # 증분: DB보다 새로운 상품만 (단, DB가 비었으면 모두)
                        if max_id == 0 or p['external_id'] > max_id:
                            all_items.append(p)
                            page_count += 1
                
                if page_count == 0 and max_id > 0: break # 더 이상 신규 없음
                time.sleep(0.1)
            except: break

    if all_items:
        print(f"✅ CU {len(all_items)}개 신규 저장 중...")
        # 중복 제거
        unique = {p['external_id']: p for p in all_items}.values()
        for i in range(0, len(list(unique)), 100):
            supabase.table("new_products").upsert(list(unique)[i:i+100], on_conflict="external_id,brand_id").execute()
        print("🎉 CU 완료!")
    else:
        print("✨ CU 최신 상태.")

# ==========================================
# 🏪 2. GS25 크롤링
# ==========================================
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "X-Requested-With": "XMLHttpRequest"
    })
    
    token = None
    try:
        r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods")
        token = BeautifulSoup(r.text, "html.parser").find("input", {"name": "CSRFToken"})['value']
    except:
        # 백업 방식
        try:
            m = re.search(r"CSRFToken\s*[:=]\s*['\"]([^'\"]+)['\"]", r.text)
            if m: token = m.group(1)
        except: pass

    if not token:
        print("❌ GS25 토큰 실패")
        return

    all_items = []
    
    for p_type in ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]:
        print(f"🔎 GS25 조회: {p_type}")
        for page in range(1, 20):
            try:
                r = session.post("https://gs25.gsretail.com/gscvs/ko/products/event-goods-search",
                               data={"CSRFToken": token, "pageNum": str(page), "pageSize": "50", "parameterList": p_type},
                               timeout=10)
                try: data = r.json()
                except: data = json.loads(r.text)
                
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    std_cat = get_standard_category(title, None)
                    
                    id_match = re.search(r'(\d+)', item.get("attFileId", ""))
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    
                    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1", "GIFT": "덤증정"}
                    
                    all_items.append({
                        "title": title,
                        "price": price,
                        "image_url": item.get("attFileNm", ""),
                        "category": std_cat,
                        "original_category": None,
                        "promotion_type": promo_map.get(p_type, "행사"),
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id
                    })
                time.sleep(0.2)
            except: break

    if all_items:
        print(f"✅ GS25 {len(all_items)}개 저장 중 (전체 갱신)...")
        supabase.table("new_products").delete().eq("brand_id", 2).execute()
        for i in range(0, len(all_items), 100):
            supabase.table("new_products").insert(all_items[i:i+100]).execute()
        print("🎉 GS25 완료!")
    else:
        print("😱 GS25 데이터 0개.")

def main():
    if not SUPABASE_URL or not SUPABASE_KEY: return
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    crawl_cu(supabase)
    crawl_gs25(supabase)
    print("\n🎉 모든 작업 완료")

if __name__ == "__main__":
    main()
