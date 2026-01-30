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
# 🧠 통합 카테고리 분류기 (정밀 분류)
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
# 🏪 1. CU 크롤링 (링크 복구 + 데이터 누락 해결)
# ==========================================
def parse_cu_product(item, raw_cat_name):
    """CU 상품 파싱 및 gdIdx 추출"""
    try:
        # 1. 제목
        name_tag = item.select_one(".name p")
        title = (name_tag.get_text(strip=True) if name_tag else "").strip()
        if not title: return None
        
        # 2. 가격
        price_tag = item.select_one(".price strong")
        price_text = (price_tag.get_text(strip=True) if price_tag else "0").replace(",", "").replace("원", "")
        price = int(price_text) if price_text.isdigit() else 0

        # 3. 이미지
        img_tag = item.select_one("img")
        img_src = ""
        if img_tag:
            img_src = img_tag.get("src") or img_tag.get("data-src") or ""
            if img_src.startswith("//"): img_src = "https:" + img_src
            elif img_src.startswith("/"): img_src = "https://cu.bgfretail.com" + img_src

        # 4. 행사 정보
        badge_tag = item.select_one(".badge")
        promotion_type = badge_tag.get_text(strip=True) if badge_tag else "행사"
        if promotion_type not in ["1+1", "2+1"]: return None # 덤증정 등 제외하려면 유지

        # 5. [중요] gdIdx 추출 (링크 생성용)
        # 예: <div class="prod_img" onclick="view(12345);">
        gdIdx = None
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            onclick = onclick_div.get("onclick", "")
            # 숫자만 추출
            m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick)
            if m:
                gdIdx = int(m.group(1))
        
        # gdIdx가 없으면 상품 링크를 만들 수 없으므로 스킵 (혹은 임시 ID)
        if not gdIdx: return None

        # 6. 정상적인 상세 페이지 링크 생성
        product_url = f"https://cu.bgfretail.com/product/view.do?category=product&gdIdx={gdIdx}"
        
        # 7. 카테고리 분류
        std_category = get_standard_category(title, raw_cat_name)

        return {
            "title": title,
            "price": price,
            "image_url": img_src,
            "category": std_category,
            "original_category": raw_cat_name,
            "promotion_type": promotion_type,
            "source_url": product_url,
            "is_active": True,
            "brand_id": 1,
            "external_id": gdIdx
        }
    except Exception as e:
        # print(f"파싱 에러: {e}")
        return None

def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작...")
    
    # 1. CU 데이터 초기화 (전체 갱신을 위해 삭제)
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    # CU 숫자 카테고리 코드 (사용자님 원본 소스 참조)
    # 10: 간편식사, 20: 즉석조리, 30: 과자류, 40: 아이스크림, 50: 식품, 60: 음료, 70: 생활용품
    cu_categories = [
        {"id": "10", "name": "간편식사"},
        {"id": "20", "name": "즉석조리"},
        {"id": "30", "name": "과자류"},
        {"id": "40", "name": "아이스크림"},
        {"id": "50", "name": "식품"},
        {"id": "60", "name": "음료"},
        {"id": "70", "name": "생활용품"}
    ]
    
    total_count = 0
    
    for cat in cu_categories:
        cat_id = cat["id"]
        cat_name = cat["name"]
        print(f"🔎 CU 조회: {cat_name} (ID:{cat_id})")
        
        cat_items = []
        
        # 충분한 페이지 수 탐색 (데이터 누락 방지)
        for page in range(1, 21):
            url = "https://cu.bgfretail.com/product/productAjax.do"
            # listType: 0 (전체/기본정렬) 사용 - 원본 소스 참조
            payload = {
                "pageIndex": page, 
                "searchMainCategory": cat_id, 
                "listType": 0 
            }
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": "https://cu.bgfretail.com/product/product.do?category=product"
            }

            try:
                r = requests.post(url, data=payload, headers=headers, timeout=10)
                r.encoding = "utf-8"
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")

                if not items: break
                
                count_in_page = 0
                for item in items:
                    p = parse_cu_product(item, cat_name)
                    if p:
                        cat_items.append(p)
                        count_in_page += 1
                
                # print(f"   - {page}페이지: {count_in_page}개 수집")
                time.sleep(0.1)
                
            except Exception as e:
                print(f"      ❌ 페이지 {page} 오류: {e}")
                break
        
        if cat_items:
            print(f"   💾 {len(cat_items)}개 저장 중...")
            # 중복 제거 (external_id 기준)
            unique_items = {p['external_id']: p for p in cat_items}.values()
            
            # 100개씩 나눠서 저장
            items_list = list(unique_items)
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").insert(items_list[i:i+100]).execute()
            total_count += len(items_list)
        else:
            print("   ✨ 데이터 없음")

    print(f"📊 CU 총 {total_count}개 업데이트 완료")


# ==========================================
# 🏪 2. GS25 크롤링 (HTTPS + 토큰)
# ==========================================
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    # GS25 초기화
    supabase.table("new_products").delete().eq("brand_id", 2).execute()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    })

    token = None
    try:
        r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods")
        # 1. Input 태그에서 찾기
        soup = BeautifulSoup(r.text, "html.parser")
        input_token = soup.find("input", {"name": "CSRFToken"})
        if input_token:
            token = input_token['value']
        
        # 2. 없으면 스크립트에서 찾기
        if not token:
            m = re.search(r"CSRFToken\s*[:=]\s*['\"]([^'\"]+)['\"]", r.text)
            if m: token = m.group(1)
    except:
        print("❌ GS25 접속 실패")
        return

    if not token:
        print("❌ GS25 토큰 획득 실패")
        return

    all_gs_products = []
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
                
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    std_cat = get_standard_category(title, None)
                    
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    
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
        print("🎉 GS25 완료")
    else:
        print("😱 GS25 데이터 0개")

# ==========================================
# 🚀 메인
# ==========================================
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Supabase 환경변수 없음")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    crawl_cu(supabase)
    crawl_gs25(supabase)
    
    print("\n🎉 모든 작업 완료")

if __name__ == "__main__":
    main()
