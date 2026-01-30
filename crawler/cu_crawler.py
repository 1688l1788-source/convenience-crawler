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
# 🧠 통합 카테고리 분류기 (로직 유지)
# ==========================================
def get_standard_category(title, raw_category=None):
    """
    앱 카테고리: [간편식사, 과자류, 아이스크림, 식품, 음료, 생활용품]
    """
    
    # [1] CU 원본 카테고리 절대 적용
    if raw_category:
        if raw_category == "간편식사": return "간편식사"
        if raw_category == "과자류": return "과자류"
        if raw_category == "아이스크림": return "아이스크림"
        if raw_category == "음료": return "음료"
        if raw_category == "생활용품": return "생활용품"
        if raw_category == "식품": return "식품"
        if raw_category == "즉석조리": return None # 수집 제외

    # [2] 키워드 분류 (GS25 등)
    # 1. 생활용품
    if any(k in title for k in [
        '치약', '칫솔', '가글', '가그린', '페리오', '메디안', '2080', '리치', '덴탈', '마우스', '쉐이빙', '면도기',
        '물티슈', '티슈', '마스크', '생리대', '중형', '대형', '소형', '오버나이트', '입는오버', '패드', '라이너', '탐폰', '팬티',
        '라엘', '쏘피', '화이트', '좋은느낌', '시크릿데이', '애니데이', '디어스킨', '순수한면',
        '샴푸', '린스', '트리트먼트', '헤어', '세럼', '비누', '엘라스틴', '케라시스', '오가니스트', '온더바디', '바디워시',
        '로션', '핸드크림', '수딩젤', '클렌징', '워터마이드', '에센셜', '존슨즈', '아비노', '니베아', '메디힐', '립케어', '오일',
        '세제', '락스', '슈가버블', '무균무때', '퐁퐁', '피지', '건전지', '스타킹', '밴드', '일회용', '제거', '클린핏', '우산', '양말', '바디'
    ]):
        return "생활용품"

    # 2. 간편식사
    if any(k in title for k in ['도시락', '김밥', '주먹밥', '샌드위치', '햄버거', '버거', '샐러드', '죽', '컵반']):
        return "간편식사"

    # 3. 식품
    is_food_bar = re.search(r'바\s*\d+g', title)
    if is_food_bar or any(k in title for k in [
        '라면', '면', '우동', '국밥', '탕', '찌개', '국', '햇반', '핫바', '소시지', '후랑크', '만두', 
        '닭가슴살', '치킨', '육개장', '베이컨', '스테이크', '육포', '어묵', '크랩', '튀김', '브리또', '파스타', 
        '직화', '꼬치', '떡볶이', '3XL', '킬바사', '오징어', '밥바'
    ]):
        return "식품"

    # 4. 과자류
    if any(k in title for k in [
        '스낵', '젤리', '사탕', '껌', '초코', '쿠키', '칩', '빵', '케익', '약과', '양갱', '프레첼', '팝콘', 
        '아몬드', '맛밤', '말차빵', '허쉬', '그릭요거트', '오팜', '푸딩', '디저트', '킷캣', '도넛', '크런키', '자유시간'
    ]):
        return "과자류"

    # 5. 아이스크림
    if title.endswith('바') or any(k in title for k in [
        '하겐', '소르베', '라라스윗', '나뚜루', '벤앤',
        '아이스', '콘', '파인트', '설레임', '폴라포', '스크류', '돼지바', '빙수', '샤베트', '찰옥수수',
        '미니컵', '비비빅', '메로나', '누가바', '쌍쌍바', '바밤바', '옥동자', '와일드바디', '붕어싸만코', 
        '더위사냥', '빵빠레', '구슬', '탱크보이', '빠삐코', '요맘때', '쿠앤크', '수박바', '죠스바', 
        '제로윗', '로우윗', '서주', '동그린', '삼우', '파르페', '쿨리쉬'
    ]):
        return "아이스크림"

    # 6. 음료
    if any(k in title for k in [
        '우유', '커피', '라떼', '아메리카노', '콜라', '사이다', '에이드', '주스', '보리차', '옥수수수염차', 
        '비타', '박카스', '쌍화', '두유', '요구르트', '요거트', '물', '워터', '프로틴', '콤부차', '드링크', '이온', 
        '티', 'TEA', '바리스타', '콘트라', '카페', '마이노멀', '서울FB', '맥주', '하이볼'
    ]):
        return "음료"

    return "기타"

# ==========================================
# 🏪 1. CU 크롤링 (NEW 감지 + 증분 백업)
# ==========================================
def parse_cu_product(item, raw_cat_name):
    try:
        name_tag = item.find("div", class_="name")
        if not name_tag: return None
        title = name_tag.get_text(strip=True)
        
        # [제외] 즉석조리 및 GET커피
        if raw_cat_name == "즉석조리": return None
        if "GET" in title and ("아메리카노" in title or "라떼" in title or "커피" in title): return None

        price_tag = item.find("div", class_="price")
        price = 0
        if price_tag:
            strong = price_tag.find("strong")
            if strong:
                price = int(strong.get_text(strip=True).replace(",", ""))

        img_tag = item.find("img")
        img_src = ""
        if img_tag:
            img_src = img_tag.get("src") or ""
            if img_src and not img_src.startswith("http"):
                if img_src.startswith("//"): img_src = "https:" + img_src
                else: img_src = "https://cu.bgfretail.com" + img_src

        # 🚨 [핵심] NEW 라벨 및 행사 정보 파싱
        badge_tag = item.find("div", class_="badge")
        promo = "일반"
        is_new = False

        if badge_tag:
            badge_text = badge_tag.get_text(strip=True).upper() # 대문자 변환
            
            # NEW 스티커 확인
            if "NEW" in badge_text:
                is_new = True
            
            # 행사명 추출 (1+1, 2+1 등)
            span = badge_tag.find("span")
            if span:
                promo_text = span.get_text(strip=True)
                # 배지 안에 'NEW' 글자만 있고 행사명이 없는 경우 '일반' 취급
                if promo_text not in ["NEW"]:
                    promo = promo_text
            else:
                # span이 없는 경우 전체 텍스트 사용하되 NEW 제거
                clean_promo = badge_text.replace("NEW", "").strip()
                if clean_promo:
                    promo = clean_promo

        # [제외] 덤, 증정
        if "덤" in promo or "증정" in promo:
            return None

        # ID 추출
        gdIdx = None
        onclick = item.find("div", onclick=re.compile(r"view\("))
        if onclick:
            m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick.get('onclick'))
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx:
            photo_div = item.find("div", class_="photo")
            if photo_div and photo_div.find("a"):
                onclick = photo_div.find("a").get("onclick") or ""
                m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick)
                if m: gdIdx = int(m.group(1))

        if not gdIdx: return None

        std_category = get_standard_category(title, raw_cat_name)
        if not std_category: return None # 분류 불가 시 제외 (혹은 기타)

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
            "is_new": is_new # NEW 라벨 저장
        }
    except: return None

def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작 (증분 백업 모드)...")
    
    # 🚨 기존 데이터 삭제 코드 제거됨 (증분 업데이트를 위해)

    cu_categories = [
        {"id": "10", "name": "간편식사"},
        # 즉석조리(20) 제외
        {"id": "30", "name": "과자류"},
        {"id": "40", "name": "아이스크림"},
        {"id": "50", "name": "식품"},
        {"id": "60", "name": "음료"},
        {"id": "70", "name": "생활용품"}
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://cu.bgfretail.com"
    }
    
    for cat in cu_categories:
        print(f"🔎 CU 조회: {cat['name']} (ID: {cat['id']})")
        
        all_cu_items = []
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
                    if p:
                        all_cu_items.append(p)
                
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ 오류: {e}")
                break

        # [증분 저장] Upsert 사용
        if len(all_cu_items) > 0:
            print(f"   💾 {len(all_cu_items)}개 Upsert 중...")
            try:
                unique_items = {p['external_id']: p for p in all_cu_items}.values()
                items_list = list(unique_items)
                
                for i in range(0, len(items_list), 100):
                    # on_conflict 지정: brand_id와 external_id가 같으면 update, 없으면 insert
                    supabase.table("new_products").upsert(
                        items_list[i:i+100], 
                        on_conflict="brand_id,external_id"
                    ).execute()
                print(f"   ✅ 저장 완료")
            except Exception as e:
                print(f"   ❌ 저장 실패: {e}")

# ==========================================
# 🏪 2. GS25 크롤링 (덤증정 제외, 증분 백업)
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
    print("\n🚀 GS25 크롤링 시작 (증분 모드)...")
    session, token = get_gs25_token()
    if not token:
        print("❌ GS25 토큰 획득 실패.")
        return

    print(f"   🔑 토큰 획득 성공")
    
    session.headers.update({
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://gs25.gsretail.com"
    })

    # GIFT 제외
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE"] 
    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1"}

    for p_type in promo_types:
        print(f"🔎 GS25 조회: {p_type}")
        all_gs_items = []
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
                        "category": get_standard_category(title, None),
                        "original_category": None,
                        "promotion_type": promo_map.get(p_type, "행사"),
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id,
                        "is_new": False # GS는 신상품 정보 없음
                    })
                time.sleep(0.1)
            except Exception as e: break
        
        # [증분 저장]
        if len(all_gs_items) > 0:
            print(f"   💾 {len(all_gs_items)}개 Upsert 중...")
            try:
                unique_gs = {p['external_id']: p for p in all_gs_items}.values()
                items_list = list(unique_gs)
                for i in range(0, len(items_list), 100):
                    supabase.table("new_products").upsert(
                        items_list[i:i+100], 
                        on_conflict="brand_id,external_id"
                    ).execute()
            except Exception as e:
                print(f"   ❌ 저장 실패: {e}")

# ==========================================
# 🚀 메인 실행
# ==========================================
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 설정 오류")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 🧹 [안전장치] 쓰레기 데이터 정리 (실행할 때마다 정리)
    try:
        supabase.table("new_products").delete().or_("promotion_type.eq.덤,promotion_type.eq.덤증정,promotion_type.ilike.%GIFT%,original_category.eq.즉석조리").execute()
    except: pass

    crawl_cu(supabase)
    crawl_gs25(supabase)
    
    print("\n🎉 모든 크롤링 작업 완료!")

if __name__ == "__main__":
    main()
