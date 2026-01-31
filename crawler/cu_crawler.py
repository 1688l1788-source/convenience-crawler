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
# 🧠 통합 카테고리 분류기
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
        if raw_category == "즉석조리": return None

    # [2] 키워드 분류 (GS25, 세븐일레븐)
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
    if any(k in title for k in ['도시락', '김밥', '주먹밥', '샌드위치', '햄버거', '버거', '샐러드', '죽', '컵반', '비빔밥']):
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
# 🏪 1. CU 크롤링
# ==========================================
def parse_cu_product(item, raw_cat_name):
    try:
        name_tag = item.find("div", class_="name")
        if not name_tag: return None
        title = name_tag.get_text(strip=True)
        
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

        badge_tag = item.find("div", class_="badge")
        promo = "일반"
        is_new = False

        if badge_tag:
            badge_text = badge_tag.get_text(strip=True).upper()
            if "NEW" in badge_text: is_new = True
            
            span = badge_tag.find("span")
            if span:
                promo = span.get_text(strip=True)
            else:
                clean = badge_text.replace("NEW", "").strip()
                if clean: promo = clean
        
        all_imgs = item.find_all("img")
        for img in all_imgs:
            src = img.get("src", "").upper()
            alt = img.get("alt", "").upper()
            if "TAG_NEW" in src or "NEW" in alt:
                is_new = True
                break

        if "덤" in promo or "증정" in promo: return None

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
    
    # 1. 기존 데이터 삭제 (전체 갱신)
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    cu_categories = [
        {"id": "10", "name": "간편식사"},
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
    
    all_cu_items = []
    
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
                    if p: all_cu_items.append(p)
                time.sleep(0.1)
            except: break

    if len(all_cu_items) > 0:
        print(f"   💾 CU {len(all_cu_items)}개 Upsert 중...")
        try:
            unique_items = {p['external_id']: p for p in all_cu_items}.values()
            items_list = list(unique_items)
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").upsert(
                    items_list[i:i+100], 
                    on_conflict="brand_id,external_id"
                ).execute()
            print("🎉 CU 업데이트 완료!")
        except Exception as e: print(f"❌ CU 저장 실패: {e}")
    else:
        print("😱 경고: CU 데이터 없음")

# ==========================================
# 🏪 2. GS25 크롤링
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
            if token_input and token_input.get('value'): return session, token_input['value']
            m = re.search(r"CSRFToken\s*[:=]\s*['\"]([^'\"]+)['\"]", r.text)
            if m: return session, m.group(1)
            time.sleep(1)
        except: time.sleep(1)
    return session, None

def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    session, token = get_gs25_token()
    if not token:
        print("❌ GS25 토큰 실패")
        return

    session.headers.update({"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
    all_gs_items = []

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
                        "is_new": False
                    })
                time.sleep(0.1)
            except: break

    if len(all_gs_items) > 0:
        print(f"   💾 GS25 {len(all_gs_items)}개 Upsert 중...")
        try:
            unique_gs = {p['external_id']: p for p in all_gs_items}.values()
            items_list = list(unique_gs)
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").upsert(items_list[i:i+100], on_conflict="brand_id,external_id").execute()
            print("🎉 GS25 업데이트 완료")
        except Exception as e: print(f"❌ GS25 저장 실패: {e}")

# ==========================================
# 🏪 3. 7-Eleven 크롤링 (헤더 수정됨)
# ==========================================
def parse_seven_eleven(item, fixed_category=None):
    try:
        name_tag = item.find("div", class_="tit_product")
        if not name_tag: return None
        title = name_tag.get_text(strip=True)

        price_tag = item.find("div", class_="price")
        price = 0
        if price_tag:
            span = price_tag.find("span")
            if span:
                price = int(span.get_text(strip=True).replace(",", ""))

        img_tag = item.find("div", class_="pic_product").find("img")
        img_src = ""
        if img_tag:
            img_src = img_tag.get("src")
            if img_src and not img_src.startswith("http"):
                img_src = "https://www.7-eleven.co.kr" + img_src

        promo = "일반"
        tag_list = item.find("ul", class_="tag_list_01")
        if tag_list:
            tags = tag_list.find_all("li")
            for tag in tags:
                text = tag.get_text(strip=True)
                if "1+1" in text: promo = "1+1"
                elif "2+1" in text: promo = "2+1"
                elif "신상품" in text: promo = "NEW"

        gdIdx = None
        link = item.find("a", href=True)
        if link:
            m = re.search(r"fncGoView\('(\d+)'\)", link['href'])
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx: return None

        if fixed_category:
            std_category = fixed_category
        else:
            std_category = get_standard_category(title, None)

        return {
            "title": title,
            "price": price,
            "image_url": img_src,
            "category": std_category,
            "original_category": fixed_category if fixed_category else None,
            "promotion_type": promo,
            "brand_id": 3,
            "source_url": f"https://www.7-eleven.co.kr/product/productView.asp?pCd={gdIdx}",
            "is_active": True,
            "external_id": gdIdx,
            "is_new": (promo == "NEW")
        }
    except: return None

def crawl_seven_eleven(supabase):
    print("\n🚀 7-Eleven 크롤링 시작...")
    
    # 7-Eleven 초기화 (전체 갱신 권장)
    supabase.table("new_products").delete().eq("brand_id", 3).execute()
    
    all_711_items = []
    
    # [중요] 세븐일레븐 맞춤 헤더
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.7-eleven.co.kr"
    }

    # 1. Fresh Food (도시락 등) - Referer 필수
    print("🔎 7-Eleven: Fresh Food")
    headers["Referer"] = "https://www.7-eleven.co.kr/product/bestdosirakList.asp"
    
    for page in range(1, 10):
        try:
            r = requests.post("https://www.7-eleven.co.kr/product/dosirakNewMoreAjax.asp",
                            data={"intPageSize": 10, "intCurrPage": page}, # 사이즈 10으로 조정
                            headers=headers, timeout=10)
            r.encoding = 'utf-8'
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.find_all("li")
            
            if not items or len(items) == 0: break
            
            count = 0
            for item in items:
                if "데이터가 없습니다" in item.get_text(): break
                p = parse_seven_eleven(item, fixed_category="간편식사")
                if p: 
                    all_711_items.append(p)
                    count += 1
            if count == 0: break
            time.sleep(0.1)
        except Exception as e: 
            # print(f"7-Eleven FF Error: {e}")
            break

    # 2. 행사 상품
    print("🔎 7-Eleven: 행사 상품")
    headers["Referer"] = "https://www.7-eleven.co.kr/product/presentList.asp"
    
    for tab_id, promo_name in {1: "1+1", 2: "2+1"}.items():
        for page in range(1, 20):
            try:
                r = requests.post("https://www.7-eleven.co.kr/product/listMoreAjax.asp",
                                data={"intPageSize": 10, "intCurrPage": page, "pTab": tab_id},
                                headers=headers, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.find_all("li")
                
                if not items or (len(items) == 1 and "데이터가 없습니다" in items[0].get_text()): break
                
                count = 0
                for item in items:
                    p = parse_seven_eleven(item, fixed_category=None)
                    if p:
                        p['promotion_type'] = promo_name
                        all_711_items.append(p)
                        count += 1
                if count == 0: break
                time.sleep(0.1)
            except Exception as e: 
                # print(f"7-Eleven Event Error: {e}")
                break

    if len(all_711_items) > 0:
        print(f"   💾 7-Eleven {len(all_711_items)}개 Upsert 중...")
        try:
            unique_items = {p['external_id']: p for p in all_711_items}.values()
            items_list = list(unique_items)
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").upsert(
                    items_list[i:i+100], 
                    on_conflict="brand_id,external_id"
                ).execute()
            print("🎉 7-Eleven 업데이트 완료")
        except Exception as e: print(f"❌ 7-Eleven 저장 실패: {e}")
    else:
        print("😱 경고: 7-Eleven 데이터 없음. 헤더나 URL 확인 필요.")

# ==========================================
# 🚀 메인 실행
# ==========================================
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 설정 오류")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    try:
        supabase.table("new_products").delete().or_("promotion_type.eq.덤,promotion_type.eq.덤증정,promotion_type.ilike.%GIFT%,original_category.eq.즉석조리").execute()
    except: pass

    crawl_cu(supabase)
    crawl_gs25(supabase)
    crawl_seven_eleven(supabase)
    
    print("\n🎉 모든 크롤링 작업 완료!")

if __name__ == "__main__":
    main()
