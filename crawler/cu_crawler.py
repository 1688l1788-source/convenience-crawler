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
# 🧠 통합 카테고리 분류기 (CU 우선 + 키워드 보정)
# ==========================================
def get_standard_category(title, raw_category=None):
    """
    앱의 탭 이름과 일치하는 표준 카테고리를 반환합니다.
    1순위: CU의 원본 카테고리 정보가 있다면 그것을 절대적으로 신뢰
    2순위: 정보가 없는 GS25나 모호한 경우 키워드로 분석
    """
    
    # [1단계] CU 원본 카테고리 절대 우선 (오분류 방지)
    if raw_category:
        if raw_category == "아이스크림": return "아이스"
        if raw_category == "음료": return "음료"
        if raw_category == "과자류": return "과자/간식"
        if raw_category == "생활용품": return "생활용품"
        if raw_category in ["간편식사", "즉석조리"]: return "식사/라면"
        # '식품' 카테고리는 라면, 통조림 등이 섞여 있어 아래 키워드 로직을 태움

    # [2단계] 키워드 기반 정밀 분류 (GS25 및 CU '식품'용)
    # 순서: 생활용품 > 식사 > 음료 > 과자 > 아이스
    
    # 1. 생활용품
    if any(k in title for k in [
        '치약', '칫솔', '가글', '가그린', '페리오', '메디안', '2080', '리치', '덴탈', '마우스', '쉐이빙', '면도기',
        '물티슈', '티슈', '마스크', '생리대', '중형', '대형', '소형', '오버나이트', '입는오버', '패드', '라이너', '탐폰', '팬티',
        '라엘', '쏘피', '화이트', '좋은느낌', '시크릿데이', '애니데이', '디어스킨', '순수한면',
        '샴푸', '린스', '트리트먼트', '헤어', '세럼', '비누', '엘라스틴', '케라시스', '오가니스트', '온더바디', '바디워시',
        '로션', '핸드크림', '수딩젤', '클렌징', '워터마이드', '에센셜', '존슨즈', '아비노', '니베아', '메디힐', '립케어', '오일',
        '세제', '락스', '슈가버블', '무균무때', '퐁퐁', '피지', '건전지', '스타킹', '밴드', '일회용', '제거', '클린핏', '우산', '양말'
    ]):
        return "생활용품"

    # 2. 식사/라면
    if any(k in title for k in [
        '도시락', '김밥', '주먹밥', '샌드위치', '햄버거', '라면', '면', '우동', '국밥', '죽', '탕', '찌개', 
        '햇반', '컵반', '핫바', '소시지', '만두', '닭가슴살', '치킨', '육개장', '그래놀라', '통곡물밥', '크랩', '튀김', '브리또', '파스타', '볶음면'
    ]):
        return "식사/라면"

    # 3. 과자/간식 (찰옥수수 같은 아이스크림이 여기서 걸리지 않게 주의 - CU는 1단계에서 해결됨)
    if any(k in title for k in [
        '스낵', '젤리', '사탕', '껌', '초코', '쿠키', '칩', '빵', '케익', '약과', '양갱', '프레첼', '팝콘', 
        '아몬드', '육포', '어묵', '맛밤', '말차빵', '허쉬', '그릭요거트', '오팜', '푸딩', '디저트', '킷캣'
    ]):
        return "과자/간식"

    # 4. 아이스 (GS25용)
    if any(k in title for k in ['아이스', '바', '콘', '파인트', '하겐다즈', '나뚜루', '설레임', '폴라포', '스크류', '돼지바', '빙수', '샤베트', '구슬', '빵빠레', '찰옥수수']):
        return "아이스"

    # 5. 음료
    if any(k in title for k in [
        '우유', '커피', '라떼', '아메리카노', '콜라', '사이다', '에이드', '주스', '보리차', '옥수수수염차', 
        '비타', '박카스', '쌍화', '두유', '요구르트', '요거트', '물', '워터', '프로틴', '콤부차', '드링크', '이온', '하이볼', '맥주'
    ]):
        return "음료"

    return "기타"

# ==========================================
# 🏪 1. CU 크롤링 (안전한 전체 갱신)
# ==========================================
def parse_cu_product(item, raw_cat_name):
    try:
        # 1. 제목
        name_tag = item.select_one(".name p")
        if not name_tag: return None
        title = name_tag.get_text(strip=True)
        
        # 2. 가격
        price_tag = item.select_one(".price strong")
        price = int(price_tag.get_text(strip=True).replace(",", "")) if price_tag else 0
        
        # 3. 이미지
        img_tag = item.select_one("img")
        img_src = ""
        if img_tag:
            img_src = img_tag.get("src") or ""
            if img_src.startswith("//"): img_src = "https:" + img_src
            elif img_src.startswith("/"): img_src = "https://cu.bgfretail.com" + img_src

        # 4. 행사 정보
        badge_tag = item.select_one(".badge")
        promo = "행사"
        if badge_tag:
            span = badge_tag.select_one("span")
            promo = span.get_text(strip=True) if span else badge_tag.get_text(strip=True)

        # 5. gdIdx 추출 (링크용)
        gdIdx = None
        onclick = item.select_one("div[onclick*='view']")
        if onclick:
            m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick.get('onclick', ''))
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx: return None

        # 6. 카테고리 분류
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
    
    # CU 내부 카테고리 코드 (숫자형)
    cu_categories = [
        {"id": "10", "name": "간편식사"},
        {"id": "20", "name": "즉석조리"},
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
        print(f"🔎 CU 조회: {cat['name']} (ID: {cat['id']})")
        
        # listType: 0 (전체 조회) -> 찰옥수수 등 스테디셀러 포함
        for page in range(1, 21):
            try:
                r = requests.post("https://cu.bgfretail.com/product/productAjax.do", 
                                data={"pageIndex": page, "searchMainCategory": cat['id'], "listType": 0},
                                headers=headers, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                
                if not items: break
                
                count = 0
                for item in items:
                    p = parse_cu_product(item, cat['name'])
                    if p:
                        all_cu_items.append(p)
                        count += 1
                
                if count == 0: break
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ CU 페이지 {page} 에러: {e}")
                break

    # [안전장치] 데이터가 하나라도 있어야 DB 작업 수행
    if len(all_cu_items) > 0:
        print(f"✅ CU 총 {len(all_cu_items)}개 수집 성공. DB 갱신 중...")
        try:
            # 1. 기존 CU 데이터 삭제
            supabase.table("new_products").delete().eq("brand_id", 1).execute()
            
            # 2. 중복 제거 (external_id 기준)
            unique_items = {p['external_id']: p for p in all_cu_items}.values()
            items_list = list(unique_items)
            
            # 3. 배치 저장
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").insert(items_list[i:i+100]).execute()
            print("🎉 CU 데이터 업데이트 완료!")
        except Exception as e:
            print(f"❌ CU 저장 실패: {e}")
    else:
        print("😱 경고: CU 데이터를 수집하지 못했습니다. (사이트 변경 가능성)")

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
    
    # 재시도 로직
    for i in range(3):
        try:
            r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            
            # 1. Input 태그
            token_input = soup.find("input", {"name": "CSRFToken"})
            if token_input and token_input.get('value'):
                return session, token_input['value']
            
            # 2. Script 태그
            m = re.search(r"CSRFToken\s*[:=]\s*['\"]([^'\"]+)['\"]", r.text)
            if m: return session, m.group(1)
            
            time.sleep(1)
        except:
            time.sleep(1)
            
    return session, None

def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    session, token = get_gs25_token()
    if not token:
        print("❌ GS25 토큰 획득 실패. 크롤링을 중단합니다.")
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
                
                # JSON 파싱 안전하게
                try: data = r.json()
                except: data = json.loads(r.text)
                if isinstance(data, str): data = json.loads(data)
                
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    # ID 추출
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    
                    all_gs_items.append({
                        "title": title,
                        "price": price,
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
            except Exception as e:
                # print(f"   ⚠️ GS25 통신 에러: {e}")
                break

    # [안전장치] 데이터 수집 확인 후 DB 갱신
    if len(all_gs_items) > 0:
        print(f"✅ GS25 총 {len(all_gs_items)}개 수집 성공. DB 갱신 중...")
        try:
            # 1. GS25 기존 데이터 삭제
            supabase.table("new_products").delete().eq("brand_id", 2).execute()
            
            # 2. 중복 제거 및 저장
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
