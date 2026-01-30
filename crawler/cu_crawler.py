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
# 🧠 통합 카테고리 분류기 (앱 탭 이름과 일치)
# ==========================================
def get_standard_category(title, raw_category=None):
    # 1. 생활용품 (최우선)
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

    # 4. 아이스
    if any(k in title for k in ['아이스', '바', '콘', '파인트', '하겐다즈', '나뚜루', '설레임', '폴라포', '스크류', '돼지바', '빙수', '샤베트']):
        return "아이스"

    # 5. 음료
    if any(k in title for k in [
        '우유', '커피', '라떼', '아메리카노', '콜라', '사이다', '에이드', '주스', '보리차', '옥수수수염차', 
        '비타', '박카스', '쌍화', '두유', '요구르트', '요거트', '물', '워터', '프로틴', '콤부차', '드링크', '이온'
    ]):
        return "음료"

    # 6. 원본 카테고리 매핑
    if raw_category:
        if raw_category in ["간편식사", "즉석조리", "식품"]: return "식사/라면"
        if raw_category == "과자류": return "과자/간식"
        if raw_category == "아이스크림": return "아이스"
        if raw_category == "생활용품": return "생활용품"
        if raw_category == "음료": return "음료"

    return "기타"

# ==========================================
# 🏪 1. CU 크롤링 (User Logic 복원: 증분 수집)
# ==========================================
def parse_cu_product(item, category_name):
    """CU 상품 HTML 파싱 및 링크 생성"""
    try:
        name_tag = item.select_one(".name p")
        title = (name_tag.get_text(strip=True) if name_tag else "").strip()
        
        price_tag = item.select_one(".price strong")
        price_text = (price_tag.get_text(strip=True) if price_tag else "0").replace(",", "").replace("원", "")
        price = int(price_text) if price_text.isdigit() else 0

        img_tag = item.select_one("img")
        image_url = ""
        if img_tag:
            image_url = img_tag.get("src") or img_tag.get("data-src") or ""
            if image_url.startswith("//"):
                image_url = "https:" + image_url
            elif image_url.startswith("/"):
                image_url = "https://cu.bgfretail.com" + image_url

        badge_tag = item.select_one(".badge")
        # 1+1, 2+1이 아니면 건너뛰려면 여기서 필터링 가능
        promotion_type = badge_tag.get_text(strip=True) if badge_tag else "행사"

        # ✅ gdIdx (실제 상품 ID) 추출 - 링크 생성용
        gdIdx = None
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            onclick = onclick_div.get("onclick", "")
            m = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick)
            if m:
                gdIdx = int(m.group(1))
        
        if not gdIdx: return None # ID 없으면 링크 못 만드므로 스킵

        # ✅ 실제 작동하는 링크 생성
        product_url = f"https://cu.bgfretail.com/product/view.do?category=product&gdIdx={gdIdx}"
        
        if not title: return None

        # 카테고리 정밀 분류
        std_category = get_standard_category(title, category_name)

        return {
            "title": title,
            "price": price,
            "image_url": image_url,
            "category": std_category,
            "original_category": category_name,
            "promotion_type": promotion_type,
            "source_url": product_url,
            "is_active": True,
            "brand_id": 1,
            "external_id": gdIdx  # 실제 CU ID 사용
        }
    except Exception as e:
        return None

def fetch_cu_new_products(supabase, category_id, category_name, max_gdIdx):
    """DB에 있는 것보다 최신 상품만 가져오기"""
    new_products = []
    print(f"   🔄 [{category_name}] (ID:{category_id}) 신규 검색 (기준 ID: {max_gdIdx})...")
    
    # CU는 페이지당 40개 정도 나옴. 100페이지면 충분히 커버.
    for page in range(1, 51): 
        url = "https://cu.bgfretail.com/product/productAjax.do"
        payload = {
            "pageIndex": page, 
            "searchMainCategory": category_id,
            "listType": 1
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        }

        try:
            r = requests.post(url, data=payload, headers=headers, timeout=8)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("li.prod_list")

            if not items: break
            
            count_in_page = 0
            for item in items:
                p = parse_cu_product(item, category_name)
                if p:
                    # ✅ 증분 수집 로직: 기존 DB보다 큰 ID만 수집
                    # (max_gdIdx가 0이면 초기 상태이므로 모두 수집)
                    if max_gdIdx == 0 or p['external_id'] > max_gdIdx:
                        new_products.append(p)
                        count_in_page += 1
            
            # 최신순 정렬되어 있다고 가정하면, 
            # 이번 페이지에 신규 상품이 하나도 없으면 더 뒤져볼 필요 없음 (효율성)
            if count_in_page == 0 and max_gdIdx > 0:
                break
            
            time.sleep(0.1)
            
        except Exception as e:
            print(f"      ❌ 페이지 {page} 오류: {e}")
            break
    
    return new_products

def run_cu_crawler(supabase):
    print("\n🚀 CU 크롤링 (증분 업데이트 모드) 시작...")
    
    # CU 숫자 코드 사용
    cu_categories = [
        {"id": "10", "name": "간편식사"},
        {"id": "20", "name": "즉석조리"},
        {"id": "30", "name": "과자류"},
        {"id": "40", "name": "아이스크림"},
        {"id": "50", "name": "식품"},
        {"id": "60", "name": "음료"},
        {"id": "70", "name": "생활용품"}
    ]
    
    total_new = 0
    
    for cat in cu_categories:
        cat_id = cat["id"]
        cat_name = cat["name"]
        
        # 1. DB에서 해당 카테고리의 가장 큰 ID 조회
        try:
            res = supabase.table("new_products") \
                .select("external_id") \
                .eq("brand_id", 1) \
                .eq("original_category", cat_name) \
                .order("external_id", desc=True) \
                .limit(1).execute()
            max_gdIdx = res.data[0]['external_id'] if res.data else 0
        except:
            max_gdIdx = 0
            
        # 2. 신규 상품 수집
        items = fetch_cu_new_products(supabase, cat_id, cat_name, max_gdIdx)
        
        # 3. 중복 제거 (혹시 모를 중복 방지)
        unique_items = {p['external_id']: p for p in items}.values()
        
        # 4. 저장 (INSERT only - 증분이므로)
        if unique_items:
            print(f"      💾 {len(unique_items)}개 신규 저장 중...")
            items_list = list(unique_items)
            # 100개씩 끊어서 저장
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").insert(items_list[i:i+100]).execute()
            total_new += len(unique_items)
        else:
            print("      ✨ 최신 상태입니다.")

    print(f"📊 CU 총 {total_new}개 신규 업데이트 완료")


# ==========================================
# 🏪 2. GS25 크롤링 (전체 갱신 모드)
# ==========================================
def run_gs25_crawler(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    # GS25는 ID 체계가 복잡하여 전체 갱신 방식 유지 (삭제 후 재등록)
    try:
        supabase.table("new_products").delete().eq("brand_id", 2).execute()
        # print("   🧹 기존 GS25 데이터 정리 완료")
    except:
        pass

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Origin": "https://gs25.gsretail.com",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    })
    
    # HTTPS로 토큰 획득
    try:
        r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        token = soup.find("input", {"name": "CSRFToken"})['value']
    except:
        print("❌ GS25 토큰 획득 실패")
        return

    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    all_gs_products = []

    for p_type in promo_types:
        print(f"   🔎 GS25 조회: {p_type}")
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
                
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    std_cat = get_standard_category(title, None)
                    
                    # ID 추출 (MD000.. -> 숫자)
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    
                    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1", "GIFT": "덤증정"}
                    
                    all_gs_products.append({
                        "title": title,
                        "price": int(item.get("price", 0)),
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

    if all_gs_products:
        print(f"   💾 GS25 {len(all_gs_products)}개 저장 중...")
        for i in range(0, len(all_gs_products), 100):
            supabase.table("new_products").insert(all_gs_products[i:i+100]).execute()
        print("   ✅ GS25 저장 완료")
    else:
        print("   ✨ GS25 데이터 없음")

# ==========================================
# 🚀 메인 실행
# ==========================================
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 환경변수 없음")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    run_cu_crawler(supabase)
    run_gs25_crawler(supabase)
    
    print("\n🎉 모든 업데이트 완료!")

if __name__ == "__main__":
    main()
