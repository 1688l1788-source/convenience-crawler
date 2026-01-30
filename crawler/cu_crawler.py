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
# 🧠 통합 카테고리 분류기 (CU 우선 적용 수정됨)
# ==========================================
def get_standard_category(title, raw_category=None):
    """
    1순위: CU 원본 카테고리가 명확하면 그것을 그대로 매핑 (아이스크림 등)
    2순위: 원본 카테고리가 없거나(GS25), 모호하면(식품) 키워드로 분석
    """
    
    # [1단계] CU 원본 카테고리 우선 매핑
    if raw_category:
        if raw_category == "아이스크림":
            return "아이스"
        if raw_category == "음료":
            return "음료"
        if raw_category == "과자류":
            return "과자/간식"
        if raw_category == "생활용품":
            return "생활용품"
        if raw_category in ["간편식사", "즉석조리"]:
            return "식사/라면"
        # '식품'은 라면, 통조림, 안주 등이 섞여 있으므로 아래 키워드 로직으로 넘김

    # [2단계] 키워드 기반 정밀 분류 (GS25 또는 CU '식품' 카테고리용)
    
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
        '햇반', '컵반', '핫바', '소시지', '만두', '닭가슴살', '치킨', '육개장', '그래놀라', '통곡물밥', '크랩', '튀김', '브리또', '파스타'
    ]):
        return "식사/라면"

    # 3. 과자/간식
    if any(k in title for k in [
        '스낵', '젤리', '사탕', '껌', '초코', '쿠키', '칩', '빵', '케익', '약과', '양갱', '프레첼', '팝콘', 
        '아몬드', '육포', '어묵', '맛밤', '말차빵', '허쉬', '그릭요거트', '오팜', '푸딩', '디저트', '킷캣'
    ]):
        return "과자/간식"

    # 4. 아이스 (GS25용 키워드)
    if any(k in title for k in ['아이스', '바', '콘', '파인트', '하겐다즈', '나뚜루', '설레임', '폴라포', '스크류', '돼지바', '빙수', '샤베트']):
        return "아이스"

    # 5. 음료
    if any(k in title for k in [
        '우유', '커피', '라떼', '아메리카노', '콜라', '사이다', '에이드', '주스', '보리차', '옥수수수염차', 
        '비타', '박카스', '쌍화', '두유', '요구르트', '요거트', '물', '워터', '프로틴', '콤부차', '드링크', '이온'
    ]):
        return "음료"

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
            if img_src and not img_src.startswith("http"):
                img_src = "https:" + img_src
        
        badge = item.select_one(".badge")
        promo = badge.get_text(strip=True) if badge else "행사"
        
        if promo not in ["1+1", "2+1"]: return None

        # gdIdx 추출
        gdIdx = None
        onclick = item.select_one("div[onclick*='view']")
        if onclick:
            m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick['onclick'])
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx: return None

        # ✅ 수정된 분류기 사용 (raw_cat_name 전달)
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
        for page in range(1, 25): # 충분한 페이지 수
            try:
                # listType: 0 (전체 상품)
                r = requests.post("https://cu.bgfretail.com/product/productAjax.do", 
                                data={"pageIndex": page, "searchMainCategory": cat_id, "listType": 0},
                                headers=headers, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                
                if not items: break
                
                valid_cnt = 0
                for item in items:
                    p = parse_cu_product(item, cat_name)
                    if p:
                        items_in_cat.append(p)
                        valid_cnt += 1
                
                # print(f"   - {page}페이지: {valid_cnt}개")
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ 에러: {e}")
                break
        
        if items_in_cat:
            # 중복 제거 및 저장
            unique = {p['external_id']: p for p in items_in_cat}.values()
            chunk_list = list(unique)
            print(f"   💾 {len(chunk_list)}개 저장...")
            
            for i in range(0, len(chunk_list), 100):
                supabase.table("new_products").insert(chunk_list[i:i+100]).execute()
            total_count += len(chunk_list)

    print(f"📊 CU 총 {total_count}개 완료")

# ==========================================
# 🏪 2. GS25 크롤링 (기존 로직 유지)
# ==========================================
def get_gs25_token():
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://gs25.gsretail.com/"})
    try:
        r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods")
        soup = BeautifulSoup(r.text, "html.parser")
        return session, soup.find("input", {"name": "CSRFToken"})['value']
    except: return session, None

def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    session, token = get_gs25_token()
    if not token: 
        print("❌ GS25 토큰 실패")
        return

    all_gs = []
    # GS25용 헤더
    session.headers.update({
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    })

    for p_type in ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]:
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
                    id_match = re.search(r'(\d+)', item.get("attFileId", ""))
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1", "GIFT": "덤증정"}
                    
                    # GS25는 raw_category가 없으므로 None 전달 (키워드 분류 사용)
                    all_gs.append({
                        "title": title,
                        "price": int(item.get("price", 0)),
                        "image_url": item.get("attFileNm", ""),
                        "category": get_standard_category(title, None),
                        "original_category": None,
                        "promotion_type": promo_map[p_type],
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id
                    })
                time.sleep(0.1)
            except: break

    if all_gs:
        print(f"   💾 GS25 {len(all_gs)}개 갱신 중...")
        supabase.table("new_products").delete().eq("brand_id", 2).execute()
        for i in range(0, len(all_gs), 100):
            supabase.table("new_products").insert(all_gs[i:i+100]).execute()
        print("🎉 GS25 완료")

def main():
    if not SUPABASE_URL or not SUPABASE_KEY: return
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    crawl_cu(supabase)
    crawl_gs25(supabase)
    print("\n🎉 모든 작업 완료")

if __name__ == "__main__":
    main()
