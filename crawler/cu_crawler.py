import os
import time
import re
import json
import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# ==========================================
# 🧠 통합 카테고리 분류기
# ==========================================
def get_standard_category(title, raw_category=None):
    # 1단계: CU 원본 카테고리가 명확하면 우선 적용
    if raw_category:
        if raw_category == "아이스크림": return "아이스"
        if raw_category == "음료": return "음료"
        if raw_category == "과자류": return "과자/간식"
        if raw_category == "생활용품": return "생활용품"
        if raw_category in ["간편식사", "즉석조리"]: return "식사/라면"

    # 2단계: 키워드 분석 (GS25 또는 CU '식품' 카테고리용)
    if any(k in title for k in ['치약', '칫솔', '가그린', '가글', '페리오', '생리대', '샴푸', '린스', '바디', '로션', '면도기', '세제', '마스크', '물티슈', '스타킹', '립케어', '니베아']):
        return "생활용품"
    if any(k in title for k in ['도시락', '김밥', '주먹밥', '샌드위치', '햄버거', '라면', '면', '우동', '국밥', '탕', '찌개', '햇반', '컵반', '핫바', '소시지', '닭가슴살', '육개장', '파스타', '떡볶이']):
        return "식사/라면"
    if any(k in title for k in ['스낵', '젤리', '사탕', '껌', '초코', '쿠키', '칩', '빵', '케익', '약과', '양갱', '아몬드', '육포', '어묵', '맛밤', '말차빵', '허쉬', '그릭요거트', '푸딩', '디저트', '킷캣']):
        return "과자/간식"
    if any(k in title for k in ['아이스', '바', '콘', '파인트', '하겐다즈', '나뚜루', '설레임', '폴라포', '스크류', '돼지바', '빙수', '샤베트', '찰옥수수', '빵빠레']):
        return "아이스"
    if any(k in title for k in ['우유', '커피', '라떼', '아메리카노', '콜라', '사이다', '에이드', '주스', '보리차', '비타', '박카스', '두유', '요구르트', '물', '워터', '프로틴', '하이볼']):
        return "음료"

    return "기타"

# ==========================================
# 🏪 1. CU 크롤링 (최신순 + 필터 완화)
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

        # ✅ [핵심 수정] 배지 텍스트 추출 방식 개선
        badge_tag = item.select_one(".badge")
        promo = "행사" # 기본값
        if badge_tag:
            # <span> 태그 안의 텍스트만 깔끔하게 가져오기
            span = badge_tag.select_one("span")
            if span:
                promo = span.get_text(strip=True)
            else:
                promo = badge_tag.get_text(strip=True)
        
        # [디버깅] 찰옥수수 발견 시 로그 출력
        if "찰옥수수" in title:
            print(f"   👀 [디버깅] '{title}' 발견! 행사태그: '{promo}'")

        # 1+1, 2+1 이 아니더라도 수집하도록 변경 (신상품, 할인 등도 포함하고 싶으신 경우)
        # 만약 1+1, 2+1만 원하시면 아래 주석을 푸세요.
        # if promo not in ["1+1", "2+1"]: return None 

        gdIdx = None
        onclick = item.select_one("div[onclick*='view']")
        if onclick:
            m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick['onclick'])
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx: return None

        return {
            "title": title,
            "price": price,
            "image_url": img_src,
            "category": get_standard_category(title, raw_cat_name),
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
    
    # DB 초기화 (깔끔한 재수집을 위해)
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
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://cu.bgfretail.com"
    }
    
    total_count = 0
    
    for cat in cu_categories:
        cat_id = cat["id"]
        cat_name = cat["name"]
        print(f"🔎 CU [{cat_name}] 최신순 검색 중...")
        
        cat_items = []
        for page in range(1, 15):
            try:
                # ✅ listType: 1 (최신순)으로 변경
                r = requests.post("https://cu.bgfretail.com/product/productAjax.do", 
                                data={"pageIndex": page, "searchMainCategory": cat_id, "listType": 1},
                                headers=headers, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                
                if not items: break
                
                for item in items:
                    p = parse_cu_product(item, cat_name)
                    if p:
                        cat_items.append(p)
                
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ 에러: {e}")
                break
        
        if cat_items:
            # 중복 제거 및 저장
            unique = {p['external_id']: p for p in cat_items}.values()
            chunk_list = list(unique)
            print(f"   💾 {len(chunk_list)}개 저장...")
            
            for i in range(0, len(chunk_list), 100):
                supabase.table("new_products").insert(chunk_list[i:i+100]).execute()
            total_count += len(chunk_list)

    print(f"📊 CU 총 {total_count}개 완료")

# ==========================================
# 🏪 2. GS25 크롤링 (유지)
# ==========================================
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://gs25.gsretail.com/"})
    
    token = None
    try:
        r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods")
        soup = BeautifulSoup(r.text, "html.parser")
        token = soup.find("input", {"name": "CSRFToken"})['value']
    except:
        print("❌ GS25 토큰 실패")
        return

    session.headers.update({"X-Requested-With": "XMLHttpRequest", "Accept": "application/json, text/javascript, */*; q=0.01"})
    all_gs = []

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
        print(f"   💾 GS25 {len(all_gs)}개 갱신...")
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
