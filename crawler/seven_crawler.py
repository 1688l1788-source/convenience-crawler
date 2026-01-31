import os
import time
import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import urllib3

# SSL 경고 무시 (세븐일레븐 구형 서버 호환성)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 환경 변수 확인
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 환경변수(SUPABASE_URL, SUPABASE_KEY)가 설정되지 않았습니다.")
    exit()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 카테고리 분류기 (기존 로직 유지) ---
def get_standard_category(title, raw_category=None):
    if raw_category == "간편식사": return "간편식사"
    
    # 1. 생활용품
    if any(k in title for k in ['치약', '칫솔', '가글', '생리대', '샴푸', '린스', '면도기', '물티슈', '마스크', '스타킹', '건전지', '비누', '로션', '립케어', '세제', '락스', '우산', '양말']):
        return "생활용품"
    # 2. 간편식사
    if any(k in title for k in ['도시락', '김밥', '주먹밥', '샌드위치', '햄버거', '버거', '샐러드']):
        return "간편식사"
    # 3. 식품
    if re.search(r'바\s*\d+g', title) or any(k in title for k in ['라면', '우동', '국수', '햇반', '핫바', '후랑크', '소시지', '만두', '치킨', '육개장', '죽', '탕', '찌개']):
        return "식품"
    # 4. 과자류
    if any(k in title for k in ['스낵', '젤리', '사탕', '껌', '초코', '쿠키', '칩', '빵', '약과', '양갱', '팝콘', '아몬드']):
        return "과자류"
    # 5. 아이스
    if title.endswith('바') or any(k in title for k in ['하겐', '소르베', '나뚜루', '아이스', '콘', '파인트', '설레임', '폴라포', '스크류', '돼지바', '빙수', '구슬', '빵빠레']):
        return "아이스"
    # 6. 음료
    if any(k in title for k in ['우유', '커피', '라떼', '콜라', '사이다', '에이드', '주스', '보리차', '비타', '박카스', '두유', '요거트', '물', '워터', '맥주', '하이볼']):
        return "음료"
    
    return "기타"

# --- 세븐일레븐 파싱 함수 ---
def parse_seven_eleven(item, fixed_category=None):
    try:
        name_tag = item.find("div", class_="tit_product")
        if not name_tag: return None
        title = name_tag.get_text(strip=True)

        price_tag = item.find("div", class_="price")
        price = 0
        if price_tag:
            span = price_tag.find("span")
            if span:
                price = int(span.get_text(strip=True).replace(",", ""))

        img_tag = item.find("div", class_="pic_product").find("img")
        img_src = ""
        if img_tag:
            img_src = img_tag.get("src")
            if img_src and not img_src.startswith("http"):
                img_src = "https://www.7-eleven.co.kr" + img_src

        promo = "일반"
        tag_list = item.find("ul", class_="tag_list_01")
        if tag_list:
            for tag in tag_list.find_all("li"):
                text = tag.get_text(strip=True)
                if "1+1" in text: promo = "1+1"
                elif "2+1" in text: promo = "2+1"
                elif "신상품" in text: promo = "NEW"

        gdIdx = None
        link = item.find("a", href=True)
        if link:
            m = re.search(r"fncGoView\('(\d+)'\)", link['href'])
            if m: gdIdx = int(m.group(1))
        
        if not gdIdx: return None

        return {
            "title": title,
            "price": price,
            "image_url": img_src,
            "category": fixed_category if fixed_category else get_standard_category(title, None),
            "original_category": fixed_category,
            "promotion_type": promo,
            "brand_id": 3,
            "source_url": f"https://www.7-eleven.co.kr/product/productView.asp?pCd={gdIdx}",
            "is_active": True,
            "external_id": gdIdx,
            "is_new": (promo == "NEW")
        }
    except Exception as e:
        print(f"   ⚠️ 파싱 에러: {e}")
        return None

# --- 크롤링 메인 로직 ---
def run_seven_debug():
    print("\n🚀 7-Eleven 크롤링 (디버그 모드) 시작...")
    
    # 세븐일레븐은 헤더가 매우 중요함
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.7-eleven.co.kr",
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest"
    }

    all_items = []

    # 1. 도시락 (Fresh Food)
    print("\n[1] 도시락(Fresh Food) 테스트")
    headers["Referer"] = "https://www.7-eleven.co.kr/product/bestdosirakList.asp"
    
    for page in range(1, 3): # 테스트로 2페이지만
        print(f"   📄 페이지 {page} 요청 중...", end=" ")
        try:
            r = requests.post("https://www.7-eleven.co.kr/product/dosirakNewMoreAjax.asp",
                            data={"intPageSize": 10, "intCurrPage": page},
                            headers=headers, timeout=15, verify=False)
            
            print(f"응답코드: {r.status_code}, 길이: {len(r.text)}")
            
            if r.status_code != 200 or not r.text.strip():
                print("   ❌ 응답이 비어있거나 실패했습니다.")
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.find_all("li")
            
            if not items:
                print("   ❌ li 태그를 찾을 수 없습니다. (HTML 구조 변경 가능성)")
                # 디버깅용: HTML 앞부분 출력
                print(f"   🔍 HTML 샘플: {r.text[:200]}")
                break
                
            count = 0
            for item in items:
                if "데이터가 없습니다" in item.get_text(): 
                    print("   ℹ️ 데이터 없음 메시지 발견")
                    break
                p = parse_seven_eleven(item, fixed_category="간편식사")
                if p:
                    all_items.append(p)
                    count += 1
            print(f"   ✅ {count}개 아이템 파싱 성공")
            
        except Exception as e:
            print(f"   ❌ 요청 중 에러: {e}")

    # 2. 행사 상품 (1+1, 2+1)
    print("\n[2] 행사 상품 테스트")
    headers["Referer"] = "https://www.7-eleven.co.kr/product/presentList.asp"
    
    for tab_id, promo_name in {1: "1+1", 2: "2+1"}.items():
        print(f"   🔎 {promo_name} (Tab {tab_id}) 조회")
        for page in range(1, 3): # 테스트로 2페이지만
            try:
                r = requests.post("https://www.7-eleven.co.kr/product/listMoreAjax.asp",
                                data={"intPageSize": 10, "intCurrPage": page, "pTab": tab_id},
                                headers=headers, timeout=15, verify=False)
                
                if not r.text.strip(): 
                    print("   ❌ 응답 없음")
                    break
                    
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.find_all("li")
                
                count = 0
                for item in items:
                    if "데이터가 없습니다" in item.get_text(): break
                    p = parse_seven_eleven(item, fixed_category=None)
                    if p:
                        p['promotion_type'] = promo_name
                        all_items.append(p)
                        count += 1
                print(f"      📄 페이지 {page}: {count}개")
                
            except Exception as e:
                print(f"      ❌ 에러: {e}")

    # 저장 테스트
    if len(all_items) > 0:
        print(f"\n💾 총 {len(all_items)}개 데이터 Upsert 시도...")
        try:
            # 중복 제거
            unique_items = {p['external_id']: p for p in all_items}.values()
            items_list = list(unique_items)
            
            # Upsert
            for i in range(0, len(items_list), 100):
                chunk = items_list[i:i+100]
                supabase.table("new_products").upsert(chunk, on_conflict="brand_id,external_id").execute()
            print("🎉 DB 저장 성공!")
        except Exception as e:
            print(f"❌ DB 저장 실패: {e}")
    else:
        print("\n😱 수집된 데이터가 하나도 없습니다. 위의 로그를 확인하세요.")

if __name__ == "__main__":
    run_seven_debug()
