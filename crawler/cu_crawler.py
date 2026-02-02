import os
import time
import re
import json
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# ⚙️ 환경 변수 설정
# ==========================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# ==========================================
# 🧠 통합 카테고리 분류기 (최신 확정판)
# ==========================================
def get_standard_category(title, raw_category=None):
    # ... (기존 함수 유지)
    pass

# ==========================================
# 🏪 1. CU 크롤링 (NEW 이미지 감지 복구 / 증분만 수행)
# ==========================================
def parse_cu_product(item, raw_cat_name):
    # ... (기존 함수 유지)
    pass

def crawl_cu(supabase):
    print("\n🚀 CU 크롤링 시작 (증분 백업)...")

    cu_categories = [
        {"id": "10", "name": "간편식사"},
        {"id": "30", "name": "과자류"},
        {"id": "40", "name": "아이스크림"},
        {"id": "50", "name": "식품"},
        {"id": "60", "name": "음료"},
        {"id": "70", "name": "생활용품"}
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://cu.bgfretail.com"
    }

    for cat in cu_categories:
        print(f"🔎 CU 조회: {cat['name']}")

        all_cu_items = []
        for page in range(1, 21):
            try:
                r = requests.post("https://cu.bgfretail.com/product/productAjax.do", 
                                data={"pageIndex": page, "searchMainCategory": cat['id'], "listType": 0},
                                headers=headers, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("li.prod_list")
                if not items: break

                for item in items:
                    p = parse_cu_product(item, cat['name'])
                    if p: all_cu_items.append(p)
                time.sleep(0.1)
            except: break

        if len(all_cu_items) > 0:
            print(f"   💾 {len(all_cu_items)}개 증분 업데이트 중...")

            # 기존 데이터 조회
            existing = supabase.table("new_products").select("external_id, title, category").eq("brand_id", 1).execute()
            existing_map = {item["external_id"]: item for item in existing.data}

            for item in all_cu_items:
                ext_id = item["external_id"]
                if ext_id in existing_map:
                    # 기존 카테고리 유지
                    item["category"] = existing_map[ext_id]["category"]
                    item["title"] = existing_map[ext_id]["title"]

            try:
                unique_items = {p['external_id']: p for p in all_cu_items}.values()
                items_list = list(unique_items)
                for i in range(0, len(items_list), 100):
                    supabase.table("new_products").upsert(
                        items_list[i:i+100], 
                        on_conflict="brand_id,external_id"
                    ).execute()
            except Exception as e: print(f"❌ CU 저장 실패: {e}")

# ==========================================
# 🏪 2. GS25 크롤링 (증분 백업)
# ==========================================
def get_gs25_token():
    # ... (기존 함수 유지)
    pass

def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작 (증분)...")

    session, token = get_gs25_token()
    if not token:
        print("❌ GS25 토큰 실패")
        return

    session.headers.update({"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})

    for p_type in ["ONE_TO_ONE", "TWO_TO_ONE"]:
        print(f"🔎 GS25 조회: {p_type}")
        all_gs_items = []
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
                if isinstance(data, str): data = json.loads(data)

                results = data.get("results", [])
                if not results: break

                for item in results:
                    title = item.get("goodsNm", "").strip()
                    price = int(item.get("price", 0))
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1"}

                    all_gs_items.append({
                        "title": title,
                        "price": int(item.get("price", 0)),
                        "image_url": item.get("attFileNm", ""),
                        "category": get_standard_category(title, None),
                        "original_category": None,
                        "promotion_type": promo_map.get(p_type, "행사"),
                        "brand_id": 2,
                        "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                        "is_active": True,
                        "external_id": ext_id,
                        "is_new": False
                    })
                time.sleep(0.1)
            except: break

        if len(all_gs_items) > 0:
            print(f"   💾 GS25 {len(all_gs_items)}개 증분 업데이트 중...")

            # 기존 데이터 조회
            existing = supabase.table("new_products").select("external_id, title, category").eq("brand_id", 2).execute()
            existing_map = {item["external_id"]: item for item in existing.data}

            for item in all_gs_items:
                ext_id = item["external_id"]
                if ext_id in existing_map:
                    # 기존 카테고리 유지
                    item["category"] = existing_map[ext_id]["category"]
                    item["title"] = existing_map[ext_id]["title"]

            try:
                unique_gs = {p['external_id']: p for p in all_gs_items}.values()
                items_list = list(unique_gs)
                for i in range(0, len(items_list), 100):
                    supabase.table("new_products").upsert(items_list[i:i+100], on_conflict="brand_id,external_id").execute()
            except Exception as e: print(f"❌ GS25 저장 실패: {e}")

# ==========================================
# 🚀 메인 실행
# ==========================================
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 설정 오류")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 🧹 [안전장치] 쓰레기 데이터만 삭제
    try:
        supabase.table("new_products").delete().or_("promotion_type.eq.덤,promotion_type.eq.덤증정,promotion_type.ilike.%GIFT%,original_category.eq.즉석조리").execute()
    except: pass

    crawl_cu(supabase)
    crawl_gs25(supabase)

    print("\n🎉 모든 크롤링 작업 완료!")

if __name__ == "__main__":
    main()
