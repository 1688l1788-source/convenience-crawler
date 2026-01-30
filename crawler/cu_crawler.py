import os
import time
import re
import json
import requests
from supabase import create_client

# 환경변수 로드
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# --- [GS25용] 카테고리 정밀 분류 함수 (GS는 카테고리 정보가 없으므로 키워드 분석) ---
def classify_category_gs25(title):
    # 1. 생활용품 (최우선 순위)
    if any(k in title for k in [
        '치약', '칫솔', '가그린', '가글', '페리오', '메디안', '2080', '리치', '덴탈', '마우스스프레이', '쉐이빙', '면도기',
        '물티슈', '티슈', '마스크', '생리대', '중형', '대형', '소형', '오버나이트', '입는오버', '패드', '라이너', '탐폰', '팬티',
        '라엘', '쏘피', '화이트', '좋은느낌', '시크릿데이', '애니데이', '디어스킨', '순수한면',
        '샴푸', '린스', '트리트먼트', '헤어세럼', '세럼', '비누', '엘라스틴', '케라시스', '오가니스트', '온더바디', '바디워시',
        '로션', '핸드크림', '수딩젤', '클렌징', '워터마이드', '에센셜마스크', '존슨즈', '아비노', '니베아', '메디힐', '립케어', '오일', '히아루론산',
        '세제', '락스', '슈가버블', '무균무때', '퐁퐁', '피지', '건전지', '스타킹', '밴드', '일회용', '제거', '클린핏', '우산'
    ]):
        return "생활용품"

    # 2. 식사/라면
    if any(k in title for k in [
        '그래놀라', '통곡물밥', '크랩', '닭껍질튀김', '라면', '면', '우동', '짬뽕', '파스타', 
        '육개장', '국밥', '떡볶이', '수제비', '찌개', '탕', '국', '닭가슴살', '도시락', 
        '김밥', '주먹밥', '샌드위치', '햄버거', '햇반', '치킨', '핫바', '소시지', '만두', '브리또', '컵반', '죽'
    ]):
        return "식사/라면"

    # 3. 과자/간식
    if any(k in title for k in [
        '말차빵', '허쉬다크', '그릭요거트', '킷캣', '캔디', '젤리', '초코', '쿠키', '껌', 
        '스낵', '칩', '봉지', '프레첼', '아몬드', '땅콩', '찹쌀떡', '케익', '양갱', '파이', 
        '강정', '카라멜', '빼빼로', '육포', '돌자반', '맛밤', '카스텔라', '푸딩', '오팜', '빵', '디저트'
    ]):
        return "과자/간식"

    # 4. 아이스
    if any(k in title for k in ['아이스', '콘', '바', '홈', '하겐다즈', '나뚜루', '설레임', '폴라포', '붕어싸만코', '샤베트']):
        return "아이스"

    # 5. 음료
    if any(k in title for k in ['우유', '커피', '에이드', '콜라', '사이다', '물', '드링크', '주스', '차', '라떼', '음료', '아메리카노', '워터', '펩시', '스프라이트', '닥터페퍼', '웰치스', '제로', '몬스터', '핫식스', '토닉', '보리차', '비타', '밀키스', '환타', '트레비', '하늘보리', '옥수수수염차', '조지아', '티오피', '칸타타', '스타벅스', '바리스타', '원컵', '에스프레소', '모카치노', '콜드브루', '허쉬', '따옴', '아카페라', '더단백', '링티', '원웨이브', '티즐', '맥콜', '슈가로로', '애사비', '프로틴', '아인슈페너', '콤부차', '요아정']):
        return "음료"

    return "기타"

# --- [CU] 크롤링 함수 ---
def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작...")
    
    # 1. CU 데이터만 안전하게 초기화
    try:
        supabase.table("new_products").delete().eq("brand_id", 1).execute()
        print("   🧹 기존 CU 데이터 삭제 완료")
    except Exception as e:
        print(f"   ⚠️ 데이터 삭제 중 경고: {e}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://cu.bgfretail.com/event/product.do",
        "Origin": "https://cu.bgfretail.com",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest" # 👈 CU 크롤링 핵심 헤더
    }
    
    # CU 공식 카테고리 코드 매핑 (앱 탭 이름과 일치시킴)
    # GD_01: 간편식사, GD_02: 즉석조리, GD_03: 과자류, GD_04: 아이스크림, GD_05: 식품, GD_06: 음료, GD_07: 생활용품
    cu_categories = {
        "GD_01": "식사/라면",
        "GD_02": "식사/라면",
        "GD_03": "과자/간식",
        "GD_04": "아이스",
        "GD_05": "식사/라면", # 식품은 일단 식사로 두고, 아래에서 키워드로 보정
        "GD_06": "음료",
        "GD_07": "생활용품"
    }
    
    url = "https://cu.bgfretail.com/event/product.do"
    all_cu_products = []

    for code, category_name in cu_categories.items():
        print(f"🔎 CU 카테고리 조회: {category_name} ({code})")
        
        for page in range(1, 15): 
            payload = {
                "pageIndex": str(page),
                "listType": "1",
                "searchCondition": code,
                "user_id": ""
            }
            try:
                r = requests.post(url, data=payload, headers=headers, timeout=15)
                r.encoding = 'utf-8' # 한글 깨짐 방지
                
                # HTML 파싱 (정규식 개선: 공백이나 줄바꿈에 유연하게 대응)
                # CU HTML 구조: <div class="name"><p>상품명</p></div>
                titles = re.findall(r'<div class="name">.*?<p>(.*?)</p>.*?</div>', r.text, re.DOTALL)
                prices = re.findall(r'<strong>\s*([0-9,]+)\s*</strong>', r.text)
                images = re.findall(r'<img\s+src="(.*?)"', r.text)
                promos = re.findall(r'class="badge">.*?<span>(.*?)</span>', r.text, re.DOTALL)

                if not titles: 
                    # 데이터가 없으면 페이지 루프 종료
                    break

                current_batch_count = 0
                for i in range(len(titles)):
                    raw_title = titles[i].strip()
                    img_src = images[i]
                    if not img_src.startswith('http'):
                        img_src = "https:" + img_src
                        
                    # [중요] 카테고리 우선순위 로직
                    # 1. CU가 정해준 카테고리를 기본으로 사용
                    final_category = category_name
                    
                    # 2. 만약 '식품' 같은 모호한 카테고리거나, 확실한 생활용품 키워드가 있다면 보정
                    if code == "GD_05" or "생활용품" not in final_category:
                        # GS25용 분류기를 재사용하여 생활용품인지 더블 체크
                        check_cat = classify_category_gs25(raw_title)
                        if check_cat == "생활용품":
                            final_category = "생활용품"

                    all_cu_products.append({
                        "title": raw_title,
                        "price": int(prices[i].replace(',', '')),
                        "image_url": img_src,
                        "category": final_category,
                        "promotion_type": promos[i].strip() if i < len(promos) else "행사",
                        "brand_id": 1, # CU
                        "source_url": "https://cu.bgfretail.com/event/product.do",
                        "is_active": True,
                        "external_id": int(time.time() * 1000) + i + (int(code[-2:]) * 10000) # 고유 ID 생성 규칙
                    })
                    current_batch_count += 1
                
                print(f"   - {page}페이지: {current_batch_count}개 수집")
                time.sleep(0.3)
                
            except Exception as e:
                print(f"   ❌ 오류 발생: {e}")
                break

    if all_cu_products:
        print(f"💾 총 {len(all_cu_products)}개의 CU 상품 저장 중...")
        try:
            # 100개씩 끊어서 저장
            for i in range(0, len(all_cu_products), 100):
                chunk = all_cu_products[i:i+100]
                supabase.table("new_products").insert(chunk).execute()
            print("🎉 CU 데이터 저장 완료!")
        except Exception as e:
            print(f"❌ CU 저장 실패: {e}")
    else:
        print("😱 경고: CU 상품이 하나도 수집되지 않았습니다. 사이트 구조 변경이나 차단을 확인하세요.")

# --- [GS25] 크롤링 함수 ---
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작...")
    supabase.table("new_products").delete().eq("brand_id", 2).execute()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "X-Requested-With": "XMLHttpRequest",
    })
    
    try:
        r_init = session.get("http://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
        csrf_token = re.search(r'name="CSRFToken" value="([^"]+)"', r_init.text).group(1)
    except:
        print("❌ GS25 CSRF 토큰 획득 실패")
        return

    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    url = "http://gs25.gsretail.com/gscvs/ko/products/event-goods-search"
    all_gs25_products = []

    for p_type in promo_types:
        print(f"🔎 GS25 유형: {p_type}")
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
                        "category": classify_category_gs25(title), # 키워드 기반 분류
                        "promotion_type": "1+1" if p_type=="ONE_TO_ONE" else "2+1" if p_type=="TWO_TO_ONE" else "덤증정",
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id
                    })
                time.sleep(0.5)
            except: break

    if all_gs25_products:
        print(f"💾 GS25 {len(all_gs25_products)}개 저장 중...")
        for i in range(0, len(all_gs25_products), 100):
            supabase.table("new_products").insert(all_gs25_products[i:i+100]).execute()

# --- 메인 실행부 ---
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Supabase 환경변수 없음")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    crawl_cu(supabase)
    crawl_gs25(supabase)
    
    print("\n🎉 모든 편의점 데이터 업데이트 완료!")

if __name__ == "__main__":
    main()
