# ==========================================
# 🏪 2. GS25 크롤링 (Upsert 방식으로 변경)
# ==========================================
def crawl_gs25(supabase):
    print("\n🚀 GS25 크롤링 시작 (증분/갱신 모드)...")
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://gs25.gsretail.com/gscvs/ko/products/event-goods",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    })

    # 1. 토큰 획득
    token = None
    try:
        r = session.get("https://gs25.gsretail.com/gscvs/ko/products/event-goods", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        token_input = soup.find("input", {"name": "CSRFToken"})
        if token_input:
            token = token_input['value']
        else:
            m = re.search(r"CSRFToken\s*[:=]\s*['\"]([^'\"]+)['\"]", r.text)
            if m: token = m.group(1)
    except:
        print("❌ GS25 접속/토큰 실패")
        return

    if not token:
        print("❌ GS25 토큰 없음")
        return

    # 2. 크롤링 수행
    all_items = []
    promo_types = ["ONE_TO_ONE", "TWO_TO_ONE", "GIFT"]
    promo_map = {"ONE_TO_ONE": "1+1", "TWO_TO_ONE": "2+1", "GIFT": "덤증정"}

    for p_type in promo_types:
        print(f"🔎 GS25 조회: {p_type}")
        for page in range(1, 20): # 충분히 조회
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
                    
                    # ID 추출 (이미지 번호가 클수록 신상품일 가능성 높음)
                    att_id = item.get("attFileId", "")
                    id_match = re.search(r'(\d+)', att_id)
                    ext_id = int(id_match.group(1)[-18:]) if id_match else int(time.time()*1000)
                    
                    # 카테고리 분류
                    std_cat = get_standard_category(title, None)

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
                time.sleep(0.1)
            except Exception as e:
                # print(f"   ⚠️ 에러: {e}")
                break

    # 3. 저장 (Upsert 사용)
    # 기존 데이터를 지우지 않고 덮어쓰기 때문에, ID가 높은 신상품이 자연스럽게 정렬됨
    if len(all_items) > 0:
        print(f"   💾 GS25 총 {len(all_items)}개 Upsert 중...")
        
        # 중복 제거 (external_id 기준)
        unique_gs = {p['external_id']: p for p in all_items}.values()
        items_list = list(unique_gs)
        
        # 100개씩 나누어 Upsert
        for i in range(0, len(items_list), 100):
            try:
                # on_conflict를 external_id와 brand_id 복합키로 잡거나, id가 있다면 id로 잡아야 함.
                # 여기서는 title + brand_id 조합이 유니크하다고 가정하거나
                # Supabase 테이블에서 external_id + brand_id를 Unique Constraints로 설정해두는 것이 가장 좋습니다.
                # 설정이 없다면 아래 코드는 중복 에러가 날 수 있으므로, 
                # 가장 안전한 방법은 'external_id'를 기준으로 upsert하는 것입니다.
                
                supabase.table("new_products").upsert(
                    items_list[i:i+100], 
                    on_conflict="external_id,brand_id" # 👈 DB에 이 두 컬럼으로 유니크 제약조건이 있어야 완벽함
                ).execute()
            except Exception as e:
                # 만약 유니크 제약조건이 없다면, 그냥 insert 시도하고 에러나면 무시(기존 데이터 유지)하는 전략도 가능
                print(f"   ⚠️ 저장 중 경고 (중복 등): {e}")
                
        print("🎉 GS25 업데이트 완료 (기존 데이터 유지됨)")
    else:
        print("😱 GS25 데이터 없음")
