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
# 🏪 1. CU 크롤링 (사용자 원본 로직 복구: 최신순 증분)
# ==========================================
def parse_cu_product(item, category_name):
    """CU 상품 파싱"""
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
        image_url = ""
        if img_tag:
            image_url = img_tag.get("src") or img_tag.get("data-src") or ""
            if image_url.startswith("//"): image_url = "https:" + image_url
            elif image_url.startswith("/"): image_url = "https://cu.bgfretail.com" + image_url

        # 4. 행사 정보
        badge_tag = item.select_one(".badge")
        promotion_type = badge_tag.get_text(strip=True) if badge_tag else "행사"

        # 5. gdIdx 추출 (링크용, 정렬용) - 사용자님 원본 정규식 사용
        gdIdx = None
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            onclick = onclick_div.get("onclick", "")
            m = re.search(r"view\s*\(\s*(\d+)\s*\)", onclick)
            if m:
                gdIdx = int(m.group(1))
        
        # gdIdx가 없으면 무시 (링크 생성 불가)
        if not gdIdx: return None

        # 6. 실제 링크 생성
        product_url = f"https://cu.bgfretail.com/product/view.do?category=product&gdIdx={gdIdx}"
        
        # 7. 카테고리 분류 (통합 분류기 사용)
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
            "external_id": gdIdx # 정렬 및 중복 체크용
        }
    except Exception as e:
        return None

def run_cu_crawler(supabase):
    print("\n" + "="*50)
    print("🚀 CU 크롤링 (최신순 증분 업데이트) 시작")
    print("="*50)
    
    # CU 카테고리 (숫자 코드 사용)
    CU_CATEGORIES = [
        {"id": "10", "name": "간편식사"},
        {"id": "20", "name": "즉석조리"},
        {"id": "30", "name": "과자류"},
        {"id": "40", "name": "아이스크림"},
        {"id": "50", "name": "식품"},
        {"id": "60", "name": "음료"},
        {"id": "70", "name": "생활용품"}
    ]
    
    total_added = 0
    
    for cat in CU_CATEGORIES:
        cat_id = cat["id"]
        cat_name = cat["name"]
        
        # 1. DB에서 가장 최신(큰) external_id 조회
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
            
        print(f"🔎 [{cat_name}] (기준 ID: {max_gdIdx}) 검색 중...")
        
        new_products = []
        
        # 최대 20페이지 탐색 (신상품은 앞페이지에 있음)
        for page in range(1, 21):
            url = "https://cu.bgfretail.com/product/productAjax.do"
            payload = {
                "pageIndex": page, 
                "searchMainCategory": cat_id,
                "listType": 1 # 1: 최신순 (아마도)
            }
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
            }

            try:
                r = requests.post(url, data=payload, headers=headers, timeout=10)
                r.encoding = "utf-8"
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")

                if not items: break
                
                page_count = 0
                for item in items:
                    p = parse_cu_product(item, cat_name)
                    if p:
                        # 이미 DB에 있는 최신 ID보다 큰 것만 담기
                        if max_gdIdx == 0 or p['external_id'] > max_gdIdx:
                            new_products.append(p)
                            page_count += 1
                
                # 이번 페이지에 신규 상품이 하나도 없으면 중단 (이미 최신까지 다 봤다는 뜻)
                if page_count == 0 and max_gdIdx > 0:
                    break
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"   ❌ 페이지 {page} 오류: {e}")
                break
        
        # 저장 (중복 제거 후)
        if new_products:
            unique_new = {p['external_id']: p for p in new_products}.values()
            print(f"   💾 {len(unique_new)}개 신규 상품 저장...")
            
            # 100개씩 나눠서 저장
            items_list = list(unique_new)
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").insert(items_list[i:i+100]).execute()
            total_added += len(unique_new)
        else:
            print("   ✨ 최신 상태입니다.")

    print(f"📊 CU 총 {total_added}개 추가됨")


# ==========================================
# 🏪 2. GS25 크롤링 (HTTPS + 토큰 + BS4 파싱)
# ==========================================
def run_gs25_crawler(supabase):
    print("\n" + "="*50)
    print("🚀 GS25 크롤링 시작 (전체 갱신)")
    print("="*50)

    # 1. 세션 설정 및 토큰 획득
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "X-Requested-With": "XMLHttpRequest"
    })

    token = None
    try:
        # HTTPS로 접속
        url = "https://gs25.gsretail.com/gscvs/ko/products/event-goods"
        r = session.get(url, timeout=15)
        
        # 토큰 찾기 (우선순위 1: Input 태그)
        soup = BeautifulSoup(r.text, "html.parser")
        input_token = soup.find("input", {"name": "CSRFToken"})
        if input_token:
            token = input_token['value']
        
        # 토큰 찾기 (우선순위 2: 정규식)
        if not token:
            match = re.search(r'CSRFToken\s*[:=]\s*["\']([^"\']+)["\']', r.text)
            if match: token = match.group(1)
            
    except Exception as e:
        print(f"❌ GS25 접속 실패: {e}")
        return

    if not token:
        print("❌ GS25 토큰 획득 실패. 스킵합니다.")
        return

    # 2. 크롤링 진행
    all_items = []
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1", "GIFT": "덤증정"}
    search_url = "https://gs25.gsretail.com/gscvs/ko/products/event-goods-search"

    for p_type in promo_types:
        print(f"   🔎 GS25 조회: {p_type}")
        for page in range(1, 25): # 넉넉하게 25페이지
            payload = {
                "CSRFToken": token, 
                "pageNum": str(page), 
                "pageSize": "50", 
                "parameterList": p_type
            }
            
            try:
                r = session.post(search_url, data=payload, timeout=15)
                r.encoding = 'utf-8'
                
                try:
                    data = r.json()
                except:
                    # JSON 파싱 실패 시 문자열 정제 후 재시도
                    data = json.loads(r.text)
                
                # 가끔 문자열로 한 번 더 감싸진 경우가 있음
                if isinstance(data, str):
                    data = json.loads(data)
                
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    
                    # 키워드로 분류
                    std_cat = get_standard_category(title, None)
                    
                    # ID 추출
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time() * 1000)
                    
                    all_items.append({
                        "title": title,
                        "price": price,
                        "image_url": item.get("attFileNm", ""),
                        "category": std_cat,
                        "original_category": None, # GS는 없음
                        "promotion_type": promo_map.get(p_type, "행사"),
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id
                    })
                
                time.sleep(0.3)
            except Exception as e:
                # print(f"      오류 발생: {e}")
                break

    # 3. 저장 (GS25는 전체 삭제 후 재등록이 깔끔함)
    if len(all_items) > 0:
        print(f"   💾 GS25 총 {len(all_items)}개 데이터 갱신 중...")
        try:
            supabase.table("new_products").delete().eq("brand_id", 2).execute()
            
            for i in range(0, len(all_items), 100):
                chunk = all_items[i:i+100]
                supabase.table("new_products").insert(chunk).execute()
            print("   ✅ GS25 갱신 완료")
        except Exception as e:
            print(f"   ❌ GS25 저장 실패: {e}")
    else:
        print("   😱 GS25 데이터를 찾지 못했습니다.")

# ==========================================
# 🚀 메인
# ==========================================
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 설정 오류: 환경변수 누락")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    try:
        run_cu_crawler(supabase)
    except Exception as e:
        print(f"❌ CU 실행 중 오류: {e}")
        
    try:
        run_gs25_crawler(supabase)
    except Exception as e:
        print(f"❌ GS25 실행 중 오류: {e}")
    
    print("\n🎉 모든 크롤링 작업 완료")

if __name__ == "__main__":
    main()
