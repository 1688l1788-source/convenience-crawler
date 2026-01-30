import os
import time
import re
import json
import requests
from supabase import create_client

# 환경변수 로드
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# --- [공통] 카테고리 정밀 분류 함수 (CU/GS25 통합 적용) ---
def classify_category(title, original_category=""):
    """
    상품명을 분석하여 카테고리를 반환.
    오분류를 막기 위해 생활용품 -> 식사 -> 과자 -> 음료 순으로 우선순위 검사.
    """
    # 1. 생활용품 (최우선 순위: 브랜드 및 위생/뷰티 키워드)
    if any(k in title for k in [
        '치약', '칫솔', '치솔', '가그린', '가글', '페리오', '메디안', '2080', '리치', '덴탈', '마우스스프레이', '쉐이빙', '면도기',
        '물티슈', '티슈', '마스크', '생리대', '중형', '대형', '소형', '오버나이트', '입는오버', '패드', '라이너', '탐폰', '팬티',
        '라엘', '쏘피', '화이트', '좋은느낌', '시크릿데이', '애니데이', '디어스킨', '순수한면',
        '샴푸', '린스', '트리트먼트', '헤어세럼', '세럼', '비누', '엘라스틴', '케라시스', '오가니스트', '온더바디', '바디워시',
        '로션', '핸드크림', '수딩젤', '클렌징', '워터마이드', '에센셜마스크', '존슨즈', '아비노', '니베아', '메디힐', '립케어', '오일', '히아루론산',
        '세제', '락스', '슈가버블', '무균무때', '퐁퐁', '피지', '건전지', '스타킹', '밴드', '일회용', '제거', '클린핏'
    ]):
        return "생활용품"

    # 2. 식사/면류/간편식 (Ramen 포함)
    if any(k in title for k in [
        '그래놀라', '통곡물밥', '크랩', '닭껍질튀김', '라면', '면', '우동', '짬뽕', '파스타', 
        '육개장', '국밥', '떡볶이', '수제비', '찌개', '탕', '국', '닭가슴살', '도시락', 
        '김밥', '주먹밥', '샌드위치', '햄버거', '햇반', '치킨', '핫바', '소시지', '만두', '브리또', '컵반'
    ]):
        return "식사"

    # 3. 과자/간식/디저트
    if any(k in title for k in [
        '말차빵', '허쉬다크', '그릭요거트', '킷캣', '캔디', '젤리', '초코', '초콜릿', '쿠키', '껌', 
        '스낵', '칩', '봉지', '프레첼', '아몬드', '땅콩', '찹쌀떡', '케익', '양갱', '파이', 
        '강정', '카라멜', '빼빼로', '육포', '돌자반', '맛밤', '카스텔라', '푸딩', '오팜'
    ]):
        return "과자"

    # 4. 아이스크림
    if any(k in title for k in ['아이스', '콘', '바', '홈', '하겐다즈', '나뚜루', '설레임', '폴라포', '붕어싸만코', '샤베트']):
        return "아이스크림"

    # 5. 음료 (마지막 순위)
    if any(k in title for k in ['우유', '커피', '에이드', '콜라', '사이다', '물', '드링크', '주스', '차', '라떼', '음료', '아메리카노', '워터', '펩시', '스프라이트', '닥터페퍼', '웰치스', '제로', '몬스터', '핫식스', '토닉', '보리차', '비타', '밀키스', '환타', '트레비', '하늘보리', '옥수수수염차', '조지아', '티오피', '칸타타', '스타벅스', '바리스타', '원컵', '에스프레소', '모카치노', '콜드브루', '허쉬', '따옴', '아카페라', '더단백', '링티', '원웨이브', '티즐', '맥콜', '슈가로로', '애사비', '프로틴', '아인슈페너', '콤부차', '요아정']):
        return "음료"

    # 위 키워드에 해당하지 않는 경우 원래 카테고리 유지 혹은 기타
    return original_category if original_category else "기타"

# --- [CU] 크롤링 함수 ---
def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 및 카테고리 정정 시작...")
    
    # [정정 로직] 기존 CU 데이터를 삭제하여 오분류된 데이터를 초기화 (brand_id=1)
    supabase.table("new_products").delete().eq("brand_id", 1).execute()

    categories = {
        "GD_01": "과자",
        "GD_02": "음료",
        "GD_03": "식사",
        "GD_04": "아이스크림",
        "GD_05": "생활용품"
    }
    
    url = "https://cu.bgfretail.com/event/product.do"
    all_cu_products = []

    for code, name in categories.items():
        print(f"🔎 CU 조회 중: {name}")
        for page in range(1, 15): 
            payload = {"pageIndex": str(page), "listType": "1", "searchCondition": code, "user_id": ""}
            try:
                r = requests.post(url, data=payload, timeout=15)
                titles = re.findall(r'<div class="name"><p>(.*?)</p></div>', r.text)
                prices = re.findall(r'<strong>([\d,]+)</strong>', r.text)
                images = re.findall(r'<img src="(.*?)"', r.text)
                promos = re.findall(r'<em>(.*?)</em>', r.text)

                if not titles: break

                for i in range(len(titles)):
                    raw_title = titles[i].strip()
                    # CU 공식 카테고리 코드가 있더라도 정밀 분류기를 한 번 더 거쳐서 확정
                    final_category = classify_category(raw_title, name)

                    all_cu_products.append({
                        "title": raw_title,
                        "price": int(prices[i].replace(',', '')),
                        "image_url": images[i] if images[i].startswith('http') else "https:" + images[i],
                        "category": final_category,
                        "promotion_type": promos[i] if i < len(promos) else "행사",
                        "brand_id": 1,
                        "source_url": "https://cu.bgfretail.com/event/product.do",
                        "is_active": True,
                        "external_id": int(time.time() * 1000) + i
                    })
                time.sleep(0.3)
            except: break

    if all_cu_products:
        print(f"💾 CU 상품 {len(all_cu_products)}개 저장 중...")
        for i in range(0, len(all_cu_products), 100):
            supabase.table("new_products").insert(all_cu_products[i:i+100]).execute()

# --- [GS25] 크롤링 함수 ---
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 및 카테고리 정정 시작...")
    
    # GS25 데이터 초기화 (brand_id=2)
    supabase.table("new_products").delete().eq("brand_id", 2).execute()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "X-Requested-With": "XMLHttpRequest",
    })
    
    r_init = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
    csrf_token = re.search(r'name="CSRFToken" value="([^"]+)"', r_init.text).group(1)

    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    all_gs25_products = []

    for p_type in promo_types:
        print(f"🔎 GS25 조회 중: {p_type}")
        for page in range(1, 25):
            payload = {"CSRFToken": csrf_token, "pageNum": str(page), "pageSize": "50", "parameterList": p_type}
            try:
                r = session.post(url, data=payload, timeout=15)
                r.encoding = 'utf-8'
                data = r.json()
                if isinstance(data, str): data = json.loads(data)
                results = data.get("results", [])
                if not results: break
                
                for item in results:
                    title = item.get("goodsNm", "").strip()
                    id_match = re.search(r'(\d+)', item.get("attFileId", ""))
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time() * 1000)
                    
                    all_gs25_products.append({
                        "title": title[:255],
                        "price": int(item.get("price", 0)),
                        "image_url": item.get("attFileNm", ""),
                        "category": classify_category(title), # 통합 분류기 적용
                        "promotion_type": "1+1" if p_type=="ONE_TO_ONE" else "2+1" if p_type=="TWO_TO_ONE" else "덤증정",
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id
                    })
                time.sleep(0.5)
            except: break

    if all_gs25_products:
        print(f"💾 GS25 상품 {len(all_gs25_products)}개 저장 중...")
        for i in range(0, len(all_gs25_products), 100):
            supabase.table("new_products").insert(all_gs25_products[i:i+100]).execute()

# --- 메인 실행부 ---
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 설정 오류: 환경변수 누락")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # CU와 GS25 모두 데이터 삭제 후 재수집하여 카테고리 완전 정정
    try: crawl_cu(supabase)
    except Exception as e: print(f"❌ CU 오류: {e}")
    
    try: crawl_gs25(supabase)
    except Exception as e: print(f"❌ GS25 오류: {e}")

    print("\n🎉 모든 카테고리 정정 및 크롤링이 완료되었습니다!")

if __name__ == "__main__":
    main()
