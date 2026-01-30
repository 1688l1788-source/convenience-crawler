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
# 🧠 통합 카테고리 분류기 (수정 없음)
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
# 🏪 1. CU 크롤링 (안전한 증분 수집)
# ==========================================
def parse_cu_product(item, raw_cat_name):
    try:
        name_tag = item.select_one(".name p")
        title = (name_tag.get_text(strip=True) if name_tag else "").strip()
        if not title: return None
        
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
        onclick_div = item.select_one("div[onclick*='view']")
        if onclick_div:
            onclick = onclick_div.get("onclick", "")
            m = re.search(r"view\s*\(\s*['\"]?(\d+)['\"]?\s*\)", onclick)
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
    
    cu_categories = [
        {"id": "10", "name": "간편식사"}, {"id": "20", "name": "즉석조리"},
        {"id": "30", "name": "과자류"}, {"id": "40", "name": "아이스크림"},
        {"id": "50", "name": "식품"}, {"id": "60", "name": "음료"}, {"id": "70", "name": "생활용품"}
    ]
    
    total_added = 0
    
    for cat in cu_categories:
        cat_id = cat["id"]
        cat_name = cat["name"]
        
        # 최신 ID 조회
        try:
            res = supabase.table("new_products").select("external_id").eq("brand_id", 1).eq("original_category", cat_name).order("external_id", desc=True).limit(1).execute()
            max_id = res.data[0]['external_id'] if res.data else 0
        except: max_id = 0
        
        print(f"🔎 CU [{cat_name}] 검색 (기준 ID: {max_id})")
        new_items = []
        
        for page in range(1, 21):
            try:
                r = requests.post("https://cu.bgfretail.com/product/productAjax.do", 
                                data={"pageIndex": page, "searchMainCategory": cat_id, "listType": 0},
                                headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                if not items: break
                
                count = 0
                for item in items:
                    p = parse_cu_product(item, cat_name)
                    if p and (max_id == 0 or p['external_id'] > max_id):
                        new_items.append(p)
                        count += 1
                
                if count == 0 and max_id > 0: break
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ 오류: {e}")
                break
        
        if new_items:
            # 중복 제거 및 저장
            unique = {p['external_id']: p for p in new_items}.values()
            items_list = list(unique)
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").upsert(items_list[i:i+100], on_conflict="external_id,brand_id").execute()
            total_added += len(unique)
            print(f"   💾 {len(unique)}개 신규 저장 완료")

    print(f"📊 CU 총 {total_added}개 추가됨")

# ==========================================
# 🏪 2. GS25 크롤링 (안전 로직: 수집 후 삭제)
# ==========================================
def get_gs25_token():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    })
    
    # 3번 재시도
    for _ in range(3):
        try:
            r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            
            # 1. Input 태그
            token_input = soup.find("input", {"name": "CSRFToken"})
            if token_input and token_input.get('value'):
                return session, token_input['value']
                
            # 2. 스크립트 정규식
            match = re.search(r'CSRFToken\s*[:=]\s*["\']([^"\']+)["\']', r.text)
            if match:
                return session, match.group(1)
                
            time.sleep(1)
        except:
            time.sleep(1)
            continue
            
    return session, None

def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    
    session, token = get_gs25_token()
    
    if not token:
        print("❌ GS25 토큰 획득 실패. (기존 데이터 보존)")
        return

    print(f"   🔑 토큰 획득 성공 ({token[:10]}...)")
    
    # JSON API 헤더 설정
    session.headers.update({
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://gs25.gsretail.com"
    })

    all_gs_products = []
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    
    for p_type in promo_types:
        print(f"🔎 GS25 조회: {p_type}")
        for page in range(1, 25): # 충분히 조회
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
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    # GS ID 생성
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    
                    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1", "GIFT": "덤증정"}
                    
                    all_gs_products.append({
                        "title": title,
                        "price": int(item.get("price", 0)),
                        "image_url": item.get("attFileNm", ""),
                        "category": get_standard_category(title, None), # 키워드 분류 적용
                        "original_category": None,
                        "promotion_type": promo_map.get(p_type, "행사"),
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id
                    })
                time.sleep(0.2)
            except Exception as e:
                # print(f"   ⚠️ 페이지 에러: {e}")
                break

    # ✅ [중요] 데이터가 실제로 수집되었을 때만 DB 삭제 및 업데이트
    if len(all_gs_products) > 0:
        print(f"   💾 GS25 총 {len(all_gs_products)}개 데이터 갱신 중...")
        try:
            # 1. GS25 기존 데이터 삭제 (이제서야 삭제함)
            supabase.table("new_products").delete().eq("brand_id", 2).execute()
            
            # 2. 새 데이터 입력
            # 중복 ID 제거
            unique_gs = {p['external_id']: p for p in all_gs_products}.values()
            items_list = list(unique_gs)
            
            for i in range(0, len(items_list), 100):
                supabase.table("new_products").insert(items_list[i:i+100]).execute()
                
            print("🎉 GS25 데이터 복구/갱신 완료!")
        except Exception as e:
            print(f"❌ GS25 저장 실패: {e}")
    else:
        print("😱 경고: GS25 수집 결과 0개. 기존 데이터를 삭제하지 않고 유지합니다.")

# ==========================================
# 🚀 메인
# ==========================================
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 설정 오류")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    crawl_cu(supabase)
    crawl_gs25(supabase)
    
    print("\n🎉 모든 크롤링 종료")

if __name__ == "__main__":
    main()
