import os
import time
import re
import json
import requests
from bs4 import BeautifulSoup
from supabase import create_client

# ==========================================
# ⚙️ 환경 변수 설정
# ==========================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# ==========================================
# 🧠 통합 카테고리 분류기 V2 (정밀도 대폭 향상)
# ==========================================
def get_standard_category(title, raw_category=None):
    # 전처리: 공백 제거 및 소문자 변환 (매칭 확률 높임)
    clean_title = title.replace(" ", "").lower()
    
    # [0] 원본 카테고리 신뢰 (가장 확실함)
    if raw_category:
        raw_map = {
            "간편식사": "간편식사", "도시락": "간편식사", "김밥": "간편식사", "샌드위치": "간편식사", "햄버거": "간편식사",
            "과자류": "과자류", "스낵": "과자류", "껌": "과자류", "캔디": "과자류", "초콜릿": "과자류",
            "아이스크림": "아이스크림", 
            "음료": "음료", "유제품": "음료",
            "생활용품": "생활용품", "위생용품": "생활용품", "잡화": "생활용품",
            "식품": "식품", "안주류": "식품", "가공식품": "식품", "반찬": "식품", "즉석조리": "식품"
        }
        # CU/GS에서 내려오는 카테고리명이 raw_map에 있으면 바로 반환
        for key, val in raw_map.items():
            if key in raw_category:
                return val

    # ==========================================
    # [1] 생활용품 (브랜드/키워드가 매우 독특하므로 1순위)
    # ==========================================
    life_keywords = [
        '치약', '칫솔', '가글', '가그린', '페리오', '2080', '리스테린', # 구강
        '생리대', '오버나이트', '라이너', '탐폰', '쏘피', '화이트', '좋은느낌', '입는오버', # 여성
        '샴푸', '린스', '트리트먼트', '바디워시', '폼클렌징', '면도기', '왁스', '로션', '핸드크림', '립케어', '니베아', # 뷰티
        '물티슈', '롤티슈', '키친타월', '마스크', '건전지', '우산', '양말', '스타킹', '이어폰', '충전기',
        '퐁퐁', '세제', '섬유유연제', '락스', '방충제', '제습제', '콘돔', '러브젤'
    ]
    if any(k in clean_title for k in life_keywords):
        return "생활용품"

    # ==========================================
    # [2] 단위(Unit) 기반 강력 필터링 (음료 vs 고체 구분)
    # ==========================================
    # ml, l, 리터 등으로 끝나는지 확인 (예: 500ml, 1.5l)
    is_liquid = re.search(r'\d+(ml|l|리터)$', clean_title) or re.search(r'\d+(ml|l|리터)[^\w가-힣]', clean_title)
    
    # 음료 키워드
    drink_keywords = [
        '아메리카노', '라떼', '에이드', '주스', '보리차', '옥수수수염차', '헛개', 
        '비타500', '박카스', '두유', '우유', '요구르트', '이온', '탄산', '콜라', '사이다', 
        '맥주', '하이볼', '소주', '와인', '워터', '생수', '콤부차', '토레타', '게토레이', 
        '파워에이드', '밀키스', '웰치스', '스타벅스', '칸타타', 'top', '바리스타', '티오피'
    ]

    # 액체 단위가 있거나 음료 키워드가 있으면 음료 (단, 샴푸 등 생활용품은 위에서 이미 걸러짐)
    if is_liquid or any(k in clean_title for k in drink_keywords):
        # 예외: '우유'가 들어갔지만 '식빵', '크림빵' 등인 경우 방지
        if not any(ex in clean_title for ex in ['식빵', '크림빵', '샌드', '쿠키', '빙수', '파르페', '캔디', '젤리']):
            return "음료"

    # ==========================================
    # [3] 아이스크림 (여름철 오분류 많음)
    # ==========================================
    ice_keywords = [
        '아이스크림', '파인트', '콘', '설레임', '폴라포', '스크류바', '돼지바', 
        '메로나', '비비빅', '쌍쌍바', '바밤바', '월드콘', '부라보', '구슬아이스', 
        '하겐다즈', '나뚜루', '벤앤제리스', '빵빠레', '더위사냥', '붕어싸만코', '찰옥수수',
        '소르베', '라라스윗', '허쉬초코바', '옥동자'
    ]
    # '바'로 끝나는 경우 체크 (단, 핫바/에너지바/초코바 제외 주의)
    # 초코바는 과자일수도 아이스일수도 있으나 보통 아이스가 많음. 하지만 에너지바는 과자.
    ends_with_bar = clean_title.endswith('바') and not any(k in clean_title for k in ['핫바', '에너지바', '프로틴바', '시리얼바', '후랑크바'])
    
    if ends_with_bar or any(k in clean_title for k in ice_keywords):
        return "아이스크림"

    # ==========================================
    # [4] 간편식사 (도시락, 김밥 등)
    # ==========================================
    meal_keywords = ['도시락', '김밥', '주먹밥', '삼각김밥', '샌드위치', '햄버거', '버거', '죽', '샐러드', '파스타', '스파게티', '컵반']
    if any(k in clean_title for k in meal_keywords):
        # 젤리(햄버거젤리) 같은 함정 피하기
        if '젤리' not in clean_title and '사탕' not in clean_title:
            return "간편식사"

    # ==========================================
    # [5] 과자류 vs 식품 (가장 모호한 경계)
    # ==========================================
    snack_keywords = [
        '스낵', '젤리', '사탕', '캔디', '껌', '초코', '초콜릿', '쿠키', '칩', '포카칩', '새우깡',
        '비스킷', '크래커', '약과', '양갱', '팝콘', '프레첼', '웨하스', '마카롱', 
        '빵', '도넛', '케익', '카스테라', '오예스', '몽쉘', '초코파이', '에너지바', '프로틴바'
    ]
    
    # 식품 키워드 (조리가 필요하거나 반찬류)
    food_keywords = [
        '라면', '우동', '국수', '쌀국수', '찌개', '국', '탕', '햇반', '컵반', '오뚜기밥', 
        '핫바', '후랑크', '소시지', '비엔나', '만두', '교자', '치킨', '닭가슴살', 
        '족발', '편육', '곱창', '안주', '육포', '어묵', '맛살', '크래미', '두부', '계란', '김치',
        '직화', '꼬치'
    ]

    # 과자류 체크 (식품보다 먼저 체크하되, 식품 키워드가 섞여 있으면 식품 우선일 수 있음)
    if any(k in clean_title for k in snack_keywords):
        return "과자류"

    if any(k in clean_title for k in food_keywords):
        return "식품"

    # ==========================================
    # [6] 최후의 보루 (기타 처리)
    # ==========================================
    # 이름에 '바'가 포함되고 g단위면 보통 식품(핫바)이나 과자(초코바)
    # 위에서 안 걸러진 '바+g'는 식품으로 간주 (예: 숯불바 80g)
    if '바' in clean_title and 'g' in clean_title:
        return "식품"

    return "기타"

# ==========================================
# 🏪 1. CU 크롤링
# ==========================================
def parse_cu_product(item, raw_cat_name):
    try:
        name_tag = item.find("div", class_="name")
        if not name_tag: return None
        title = name_tag.get_text(strip=True)
        
        if raw_cat_name == "즉석조리": return None # 제외

        price_tag = item.find("div", class_="price")
        price = int(price_tag.find("strong").get_text(strip=True).replace(",", "")) if price_tag else 0

        img_tag = item.find("img")
        img_src = ""
        if img_tag:
            img_src = img_tag.get("src") or ""
            if img_src and not img_src.startswith("http"):
                if img_src.startswith("//"): img_src = "https:" + img_src
                else: img_src = "https://cu.bgfretail.com" + img_src

        # NEW / 행사 감지
        is_new = False
        promo = "일반"

        # 1. 이미지로 NEW 감지
        for img in item.find_all("img"):
            if "tag_new.png" in img.get("src", ""):
                is_new = True
                break
        
        # 2. 배지 텍스트
        badge_tag = item.find("div", class_="badge")
        if badge_tag:
            if "NEW" in badge_tag.get_text(strip=True).upper(): is_new = True
            
            span = badge_tag.find("span")
            promo = span.get_text(strip=True) if span else badge_tag.get_text(strip=True)

        if "덤" in promo or "증정" in promo: return None

        gdIdx = None
        onclick = item.find("div", onclick=re.compile(r"view\("))
        if onclick:
            m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick.get('onclick'))
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx:
            # 백업: photo div
            photo = item.find("div", class_="photo")
            if photo and photo.find("a"):
                m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", photo.find("a").get('onclick', ''))
                if m: gdIdx = int(m.group(1))

        if not gdIdx: return None

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
            "external_id": gdIdx,
            "is_new": is_new
        }
    except: return None

def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작...")
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    cu_categories = [
        {"id": "10", "name": "간편식사"}, {"id": "30", "name": "과자류"},
        {"id": "40", "name": "아이스크림"}, {"id": "50", "name": "식품"},
        {"id": "60", "name": "음료"}, {"id": "70", "name": "생활용품"}
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://cu.bgfretail.com"
    }
    
    all_items = []
    for cat in cu_categories:
        print(f"🔎 CU 조회: {cat['name']}")
        for page in range(1, 21):
            try:
                r = requests.post("https://cu.bgfretail.com/product/productAjax.do", 
                                data={"pageIndex": page, "searchMainCategory": cat['id'], "listType": 0},
                                headers=headers, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                if not items: break
                
                for item in items:
                    p = parse_cu_product(item, cat['name'])
                    if p: all_items.append(p)
                time.sleep(0.1)
            except: break

    if all_items:
        print(f"✅ CU {len(all_items)}개 저장 중...")
        unique = {p['external_id']: p for p in all_items}.values()
        items_list = list(unique)
        for i in range(0, len(items_list), 100):
            supabase.table("new_products").insert(items_list[i:i+100]).execute()
        print("🎉 CU 완료")

# ==========================================
# 🏪 2. GS25 크롤링
# ==========================================
def get_gs25_token():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0", "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods"
    })
    for i in range(3):
        try:
            r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            token = soup.find("input", {"name": "CSRFToken"})
            if token and token.get('value'): return session, token['value']
            time.sleep(1)
        except: pass
    return session, None

def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    session, token = get_gs25_token()
    if not token: 
        print("❌ GS25 토큰 실패")
        return

    session.headers.update({"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
    all_items = []

    for p_type in ["ONE_TO_ONE", "TWO_TO_ONE"]:
        print(f"🔎 GS25 조회: {p_type}")
        for page in range(1, 20):
            try:
                r = session.post("https://gs25.gsretail.com/gscvs/ko/products/event-goods-search",
                               data={"CSRFToken": token, "pageNum": str(page), "pageSize": "50", "parameterList": p_type})
                data = json.loads(r.text) if not isinstance(r.json(), dict) else r.json()
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1"}
                    
                    all_items.append({
                        "title": title,
                        "price": price,
                        "image_url": item.get("attFileNm", ""),
                        "category": get_standard_category(title, None), # GS는 키워드 분류
                        "original_category": None,
                        "promotion_type": promo_map.get(p_type, "행사"),
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id,
                        "is_new": False
                    })
                time.sleep(0.1)
            except: break

    if all_items:
        print(f"✅ GS25 {len(all_items)}개 저장 중...")
        supabase.table("new_products").delete().eq("brand_id", 2).execute()
        unique = {p['external_id']: p for p in all_items}.values()
        items_list = list(unique)
        for i in range(0, len(items_list), 100):
            supabase.table("new_products").insert(items_list[i:i+100]).execute()
        print("🎉 GS25 완료")

def main():
    if not SUPABASE_URL or not SUPABASE_KEY: return
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 🧹 덤증정 삭제
    try:
        supabase.table("new_products").delete().or_("promotion_type.eq.덤,promotion_type.eq.덤증정,promotion_type.ilike.%GIFT%").execute()
    except: pass

    crawl_cu(supabase)
    crawl_gs25(supabase)
    print("\n🎉 모든 작업 완료!")

if __name__ == "__main__":
    main()
