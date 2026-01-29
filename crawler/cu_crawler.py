for item in items:
    try:
        # 1. 제품명
        name_tag = item.select_one(".name p")
        if not name_tag: 
            continue
        title = name_tag.text.strip()

        # 2. 가격
        price_tag = item.select_one(".price strong")
        price_text = price_tag.text.strip().replace(",", "").replace("원", "") if price_tag else "0"
        price = int(price_text) if price_text.isdigit() else 0

        # 3. 이미지 URL (개선)
        img_tag = item.select_one("img")
        image_url = ""
        
        if img_tag:
            # src, data-src, data-original 등 다양한 속성 확인
            image_url = (
                img_tag.get('src') or 
                img_tag.get('data-src') or 
                img_tag.get('data-original') or 
                ""
            )
            
            # URL 정규화
            if image_url:
                if image_url.startswith('//'):
                    image_url = f"https:{image_url}"
                elif image_url.startswith('/'):
                    image_url = f"https://cu.bgfretail.com{image_url}"
                elif not image_url.startswith('http'):
                    image_url = f"https://cu.bgfretail.com/{image_url}"
        
        # 4. 행사 정보
        badge_tag = item.select_one(".badge")
        category_name = badge_tag.text.strip() if badge_tag else "일반"

        product = {
            "title": title,
            "price": price,
            "image_url": image_url,
            "category": category_name,
            "source_url": "https://cu.bgfretail.com/product/product.do",
            "is_active": True,
            "brand_id": 1
        }
        
        all_products.append(product)
        
        # 이미지 없는 제품 로깅
        if not image_url:
            print(f"⚠️ No image: {title}")

    except Exception as e:
        print(f"⚠️ 제품 파싱 에러: {e}")
        continue
