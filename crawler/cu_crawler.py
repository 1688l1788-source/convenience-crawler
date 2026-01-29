# cu_crawler.py 마지막 부분만 수정 (154줄 부근)

def parse_promotion(item):
    """파싱 (last_updated 제거)"""
    try:
        title = item.get("goodsNm", "")
        price = int(item.get("price", 0))
        att_file_id = item.get("attFileId", "")
        
        id_match = re.search(r'MD0*(\d+)', att_file_id)
        external_id = int(id_match.group(1)) if id_match else None
        
        if external_id and title:
            return {
                "title": title[:200],
                "price": price,
                "image_url": item.get("attFileNm", ""),
                "category": "GS25행사상품",
                "promotion_type": f"{item.get('eventTypeNm', '')} ({item.get('prmtCd', '')})",
                "source_url": "http://gs25.gsretail.com/gscvs/ko/products/event-goods",
                "is_active": True,
                "brand_id": 2,
                "external_id": external_id
                # 👈 last_updated 제거
            }
    except:
        pass
    return None

# main() 저장 부분도 수정
if promotions:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    supabase.table("new_products").upsert(promotions, on_conflict=["external_id", "brand_id"]).execute()
    
    print(f"\n🎉 {len(promotions)}개 GS25 행사상품 저장 완료!")
    for p in promotions[:3]:
        print(f"   {p['title'][:30]}... | {p['promotion_type']}")
