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
# 🏪 1. CU 크롤링 (정상 작동 버전)
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
        if promo not in ["1+1", "2+1"]: return None

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
    except: return None

def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작...")
    
    cu_categories = {
        "10": "간편식사", "20": "즉석조리", "30": "과자류",
        "40": "아이스크림", "50": "식품", "60": "음료", "70": "생활용품"
    }
    
    all_items = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://cu.bgfretail.com/product/product.do",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
    }
    
    for code, name in cu_categories.items():
        print(f"🔎 CU 조회: {name} ({code})")
        for page in range(1, 20):
            url = "https://cu.bgfretail.com/product/productAjax.do"
            payload = {"pageIndex": page, "searchMainCategory": code, "listType": 1}
            try:
                r = requests.post(url, data=payload, headers=headers, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                if not items: break
                
                count = 0
                for item in items:
                    p = parse_cu_item(item, name)
                    if p:
                        p['external_id'] = int(time.time() * 1000) + len(all_items)
                        all_items.append(p)
                        count += 1
                if count == 0: break
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ CU 에러: {e}")
                break

    if len(all_items) > 0:
        print(f"✅ CU 총 {len(all_items)}개 수집됨. DB 업데이트...")
        try:
            supabase.table("new_products").delete().eq("brand_id", 1).execute()
            for i in range(0, len(all_items), 100):
                supabase.table("new_products").insert(all_items[i:i+100]).execute()
            print("🎉 CU 저장 완료!")
        except Exception as e:
            print(f"❌ CU 저장 실패: {e}")
    else:
        print("😱 CU 데이터 0개.")

# ==========================================
# 🏪 2. GS25 크롤링 (수정된 강화 버전)
# ==========================================
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    session = requests.Session()
    # 헤더 강화: 최신 브라우저 모방 + HTTPS Referer
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Origin": "https://gs25.gsretail.com",
        "X-Requested-With": "XMLHttpRequest"
    })
    
    # 1. CSRF 토큰 획득 (HTTPS 사용 및 BS4 파싱 추가)
    print("   🔑 GS25 토큰 획득 시도...")
    token = None
    try:
        # http -> https 로 변경
        url = "https://gs25.gsretail.com/gscvs/ko/products/event-goods"
        r = session.get(url, timeout=15)
        
        # 방식 A: 정규식 (Input 태그)
        csrf_match = re.search(r'name="CSRFToken" value="([^"]+)"', r.text)
        if csrf_match:
            token = csrf_match.group(1)
        
        # 방식 B: 정규식 (Script 변수)
        if not token:
            csrf_match = re.search(r'CSRFToken\s*[:=]\s*["\']([^"\']+)["\']', r.text)
            if csrf_match: token = csrf_match.group(1)

        # 방식 C: BeautifulSoup (가장 확실)
        if not token:
            soup = BeautifulSoup(r.text, "html.parser")
            input_tag = soup.find("input", {"name": "CSRFToken"})
            if input_tag:
                token = input_tag.get("value")

    except Exception as e:
        print(f"❌ GS25 접속 실패: {e}")
        return

    if not token:
        print("❌ GS25 CSRF 토큰을 찾을 수 없습니다. (사이트 구조 변경 가능성)")
        return
    else:
        print(f"   ✅ 토큰 획득 성공 ({token[:10]}...)")

    all_items = []
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    
    for p_type in promo_types:
        print(f"🔎 GS25 조회: {p_type}")
        # https 사용
        search_url = "https://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
        
        for page in range(1, 20):
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
                    # JSON 파싱 시도 (가끔 텍스트가 섞여올 수 있음)
                    data = json.loads(r.text)
                except:
                    # JSON이 아닐 경우 BS4로 에러 메시지 확인 혹은 재시도
                    print(f"      ⚠️ JSON 파싱 실패 (페이지 {page})")
                    break
                
                results = data.get("results", [])
                if not results: 
                    # print(f"      - {page}페이지: 데이터 없음 (끝)")
                    break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    
                    # 키워드 분류
                    std_cat = get_standard_category(title, None)
                    
                    # ID 추출
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
                
                # print(f"      - {page}페이지: {len(results)}개 수집")
                time.sleep(0.3)
                
            except Exception as e:
                print(f"   ❌ GS25 통신 에러: {e}")
                break

    if len(all_items) > 0:
        print(f"✅ GS25 총 {len(all_items)}개 수집됨. DB 업데이트...")
        try:
            # 1. GS25 기존 데이터 삭제
            supabase.table("new_products").delete().eq("brand_id", 2).execute()
            
            # 2. 데이터 저장 (100개씩)
            for i in range(0, len(all_items), 100):
                chunk = all_items[i:i+100]
                supabase.table("new_products").insert(chunk).execute()
            print("🎉 GS25 저장 완료!")
        except Exception as e:
            print(f"❌ GS25 저장 실패: {e}")
    else:
        print("😱 경고: GS25 데이터를 찾지 못했습니다. 토큰이나 URL을 확인하세요.")

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
