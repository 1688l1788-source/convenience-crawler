import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import os
from supabase import create_client
import time

class CUCrawler:
    def __init__(self):
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
        
        if not supabase_url or not supabase_key:
            raise Exception("Supabase 환경변수가 설정되지 않았습니다")
        
        self.supabase = create_client(supabase_url, supabase_key)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.brand_id = 1  # CU
    
    def crawl(self):
        """CU 신제품 크롤링"""
        print("🏪 CU 크롤링 시작...")
        
        # CU 이벤트 상품 페이지 (실제 URL은 확인 필요)
        url = "https://cu.bgfretail.com/product/product.do?category=product&depth2=6&sf=N"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            products = []
            
            # CU 웹사이트 구조에 맞게 선택자 조정 필요
            # 아래는 일반적인 구조 예시
            items = soup.select('.prodListWrap .prod_item')
            
            if not items:
                # 대체 선택자 시도
                items = soup.select('.product-list .product-item')
            
            print(f"📦 발견된 상품: {len(items)}개")
            
            for idx, item in enumerate(items[:20]):  # 최대 20개만
                try:
                    # 제품명
                    title_elem = item.select_one('.prodName, .prod_name, .product-name')
                    if not title_elem:
                        continue
                    title = title_elem.text.strip()
                    
                    # 가격
                    price_elem = item.select_one('.price, .prod_price, .product-price')
                    price = None
                    if price_elem:
                        price_text = price_elem.text.strip()
                        price_match = re.findall(r'\d+', price_text.replace(',', ''))
                        if price_match:
                            price = int(''.join(price_match))
                    
                    # 이미지
                    img_elem = item.select_one('img')
                    image_url = None
                    if img_elem:
                        image_url = img_elem.get('src') or img_elem.get('data-src')
                        if image_url and not image_url.startswith('http'):
                            image_url = 'https://cu.bgfretail.com' + image_url
                    
                    # 링크
                    link_elem = item.select_one('a')
                    source_url = url
                    if link_elem:
                        href = link_elem.get('href')
                        if href:
                            if href.startswith('http'):
                                source_url = href
                            elif href.startswith('/'):
                                source_url = 'https://cu.bgfretail.com' + href
                    
                    product = {
                        'brand_id': self.brand_id,
                        'title': title,
                        'normalized_title': self.normalize_title(title),
                        'price': price,
                        'category': self.categorize(title),
                        'launch_date': datetime.now().date().isoformat(),
                        'image_url': image_url,
                        'source_url': source_url,
                        'is_active': True
                    }
                    
                    products.append(product)
                    print(f"  ✓ {idx+1}. {title[:30]}...")
                    
                except Exception as e:
                    print(f"  ✗ 상품 파싱 실패: {e}")
                    continue
            
            return products
            
        except Exception as e:
            print(f"❌ 크롤링 실패: {e}")
            return []
    
    def normalize_title(self, title):
        """제품명 정규화"""
        normalized = re.sub(r'\s+', ' ', title)
        normalized = re.sub(r'[^\w\s가-힣]', '', normalized)
        return normalized.strip().upper()
    
    def categorize(self, title):
        """카테고리 자동 분류"""
        title_lower = title.lower()
        
        keywords = {
            '음료': ['음료', '주스', '커피', '우유', '차', '워터', '사이다', '콜라'],
            '과자': ['과자', '초콜릿', '사탕', '젤리', '쿠키', '비스킷', '스낵'],
            '즉석식품': ['도시락', '김밥', '샌드위치', '삼각김밥', '핫도그', '햄버거'],
            '라면': ['라면', '컵라면', '짜파게티', '짜장면'],
            '아이스크림': ['아이스크림', '빙과', '아이스바', '콘', '파인트']
        }
        
        for category, words in keywords.items():
            if any(word in title_lower for word in words):
                return category
        
        return '기타'
    
    def save_to_db(self, products):
        """신규 제품만 DB에 저장"""
        if not products:
            print("💤 수집된 제품 없음")
            return 0
        
        print(f"\n💾 DB 저장 시작...")
        
        try:
            # 최근 30일 기존 데이터 조회
            thirty_days_ago = (datetime.now() - timedelta(days=30)).date().isoformat()
            
            existing = self.supabase.table('new_products')\
                .select('normalized_title, launch_date')\
                .eq('brand_id', self.brand_id)\
                .gte('launch_date', thirty_days_ago)\
                .execute()
            
            existing_keys = {
                f"{p['normalized_title']}_{p['launch_date']}" 
                for p in existing.data
            }
            
            # 신규 제품 필터링
            new_products = [
                p for p in products 
                if f"{p['normalized_title']}_{p['launch_date']}" not in existing_keys
            ]
            
            if new_products:
                # 배치로 삽입
                self.supabase.table('new_products').insert(new_products).execute()
                print(f"✅ {len(new_products)}개 신제품 저장 완료")
                
                # 저장된 제품 출력
                for p in new_products[:5]:
                    print(f"  - {p['title'][:40]}")
                if len(new_products) > 5:
                    print(f"  ... 외 {len(new_products)-5}개")
                
                return len(new_products)
            else:
                print("ℹ️  신규 제품 없음 (모두 기존 제품)")
                return 0
                
        except Exception as e:
            print(f"❌ DB 저장 실패: {e}")
            return 0

def main():
    print("="*60)
    print("🏪 편의점 신제품 크롤러 시작")
    print(f"⏰ 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    try:
        crawler = CUCrawler()
        products = crawler.crawl()
        new_count = crawler.save_to_db(products)
        
        print("\n" + "="*60)
        print(f"✨ 크롤링 완료!")
        print(f"📊 수집: {len(products)}개 | 신규: {new_count}개")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        exit(1)

if __name__ == "__main__":
    main()
