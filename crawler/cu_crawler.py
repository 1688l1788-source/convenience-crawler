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
# 🧠 통합 카테고리 분류기
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
# 🏪 1. CU 크롤링 (ID 수정됨)
# ==========================================
def parse_cu_item(item, raw_cat_name):
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
        
        # 1+1, 2+1이 아니면 수집 제외 (선택사항, 필요 없으면 삭제)
        if promo not in ["1+1", "2+1"]:
            return None

        std_category = get_standard_category(title, raw_cat_name)
        
        return {
            "title": title,
            "price": price,
            "image_url": img_src,
            "category": std_category,
            "original_category": raw_cat_name,
            "promotion_type": promo,
            "brand_id": 1,
            "source_url": "https://cu.bgfretail.com/event/product.do",
            "is_active": True
        }
    except:
        return None

def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작...")
    
    # ✅ [중요] CU 카테고리 코드를 숫자로 변경 (GD_xx -> 숫자)
    # 10: 간편식사, 20: 즉석조리, 30: 과자류, 40: 아이스크림, 50: 식품, 60: 음료, 70: 생활용품
    cu_categories = {
        "10": "간편식사",
        "20": "즉석조리",
        "30": "과자류",
        "40": "아이스크림",
        "50": "식품",
        "60": "음료",
        "70": "생활용품"
    }
    
    all_items = []
    
    for code, name in cu_categories.items():
        print(f"🔎 CU 조회: {name} (Code: {code})")
        
        # 페이지 탐색
        for page in range(1, 20):
            url = "https://cu.bgfretail.com/product/productAjax.do"
            # ✅ [중요] searchMainCategory 파라미터 사용
            payload = {
                "pageIndex": page, 
                "searchMainCategory": code, 
                "listType": 1
            }
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=1",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
            }
            
            try:
                r = requests.post(url, data=payload, headers=headers, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                
                # 데이터가 없으면 루프 종료
                if not items: 
                    # print(f"   - {page}페이지: 데이터 없음. 종료.")
                    break
                
                count_in_page = 0
                for item in items:
                    p = parse_cu_item(item, name)
                    if p:
                        # 고유 ID 생성 (Timestamp + 순서)
                        p['external_id'] = int(time.time() * 1000) + len(all_items)
                        all_items.append(p)
                        count_in_page += 1
                
                if count_in_page == 0:
                    # 아이템은 있었으나 파싱 결과가 0개면 종료 (행사상품이 아닐 수 있음)
                    # print(f"   - {page}페이지: 유효 상품 없음.")
                    break
                    
                time.sleep(0.2)
            except Exception as e:
                print(f"   ❌ CU 통신 에러: {e}")
                break

    if len(all_items) > 0:
        print(f"✅ CU 총 {len(all_items)}개 수집됨. DB 저장 시작...")
        try:
            # 1. 기존 데이터 삭제
            supabase.table("new_products").delete().eq("brand_id", 1).execute()
            
            # 2. 데이터 저장 (100개씩)
            for i in range(0, len(all_items), 100):
                chunk = all_items[i:i+100]
                supabase.table("new_products").insert(chunk).execute()
            print("🎉 CU 저장 완료!")
        except Exception as e:
            print(f"❌ CU 저장 실패: {e}")
    else:
        print("😱 CU 데이터가 0개입니다. 크롤링 파라미터를 확인하세요.")

# ==========================================
# 🏪 2. GS25 크롤링
# ==========================================
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "X-Requested-With": "XMLHttpRequest"
    })
    
    try:
        r = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=10)
        csrf = re.search(r'name="CSRFToken" value="([^"]+)"', r.text)
        if not csrf:
            csrf = re.search(r'CSRFToken\s*[:=]\s*["\']([^"\']+)["\']', r.text)
        token = csrf.group(1) if csrf else None
    except:
        token = None

    if not token:
        print("❌ GS25 토큰 획득 실패. 건너뜁니다.")
        return

    all_items = []
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    
    for p_type in promo_types:
        print(f"🔎 GS25 조회: {p_type}")
        for page in range(1, 20):
            url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
            payload = {"CSRFToken": token, "pageNum": str(page), "pageSize": "50", "parameterList": p_type}
            
            try:
                r = session.post(url, data=payload, timeout=10)
                r.encoding = 'utf-8'
                data = json.loads(r.text)
                results = data.get("results", [])
                
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    std_cat = get_standard_category(title, None)
                    
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time() * 1000)
                    
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
                time.sleep(0.3)
            except: break

    if len(all_items) > 0:
        print(f"✅ GS25 총 {len(all_items)}개 수집됨. DB 저장 시작...")
        try:
            supabase.table("new_products").delete().eq("brand_id", 2).execute()
            for i in range(0, len(all_items), 100):
                chunk = all_items[i:i+100]
                supabase.table("new_products").insert(chunk).execute()
            print("🎉 GS25 저장 완료!")
        except Exception as e:
            print(f"❌ GS25 저장 실패: {e}")
    else:
        print("😱 GS25 데이터 0개.")

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
    
    print("\n🎉 모든 크롤링 완료")

if __name__ == "__main__":
    main()
