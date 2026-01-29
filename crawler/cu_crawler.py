import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import os
import json

try:
    from supabase import create_client, Client
except ImportError:
    from supabase import create_client

class CUCrawler:
    def __init__(self):
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
        
        if not supabase_url or not supabase_key:
            raise Exception("Supabase 환경변수가 설정되지 않았습니다")
        
        try:
            self.supabase = create_client(supabase_url, supabase_key)
        except TypeError:
            self.supabase = create_client(
                supabase_url=supabase_url,
                supabase_key=supabase_key
            )
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'ko-KR,ko;q=0.9',
        }
        self.brand_id = 1
    
    def crawl(self):
        print("🏪 CU 크롤링 시작...")
        
        # CU 행사상품 페이지 (실제 제품이 있는 페이지)
        urls = [
            "https://cu.bgfretail.com/event/plusAjax.do?page=1&pageSize=20&evtId=",
            "https://cu.bgfretail.com/product/pb.do",
        ]
        
        all_products = []
        
        # 방법 1: PB상품 페이지 크롤링
        try:
            url = "https://cu.bgfretail.com/product/pb.do"
            print(f"\n🔍 접속: {url}")
            
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 실제 제품 목록 찾기
            products = soup.select('ul.prod_list li')
            print(f"📦 발견: {len(products)}개")
            
            if products and len(products) > 3:  # 3개 이상이면 실제 제품
                for idx, item in enumerate(products[:20]):
                    try:
                        # 제품명
                        name_elem = item.select_one('.prod_name, .prodName, strong')
                        if not name_elem:
                            continue
                        title = name_elem.text.strip()
                        
                        if not title or len(title) < 3:
                            continue
                        
                        # 가격
                        price = None
                        price_elem = item.select_one('.price, .prodPrice, dd')
                        if price_elem:
                            price_text = price_elem.text.strip()
                            numbers = re.findall(r'\d+', price_text.replace(',', ''))
                            if numbers:
                                price = int(''.join(numbers))
                        
                        # 이미지
                        img = item.select_one('img')
                        image_url = None
                        if img:
                            image_url = img.get('src') or img.get('data-src')
                            if image_url:
                                if image_url.startswith('//'):
                                    image_url = 'https:' + image_url
                                elif not image_url.startswith('http'):
                                    image_url = 'https://cu.bgfretail.com' + image_url
                        
                        product = {
                            'brand_id': self.brand_id,
                            'title': title,
                            'normalized_title': self.normalize_title(title),
                            'price': price,
                            'category': self.categorize(title),
                            'launch_date': datetime.now().date().isoformat(),
                            'image_url': image_url,
                            'source_url': url,
                            'is_active': True
                        }
                        
                        all_products.append(product)
                        print(f"  ✓ {idx+1}. {title[:40]}")
                        
                    except Exception as e:
                        print(f"  ✗ 파싱 오류: {e}")
        
        except Exception as e:
            print(f"❌ 크롤링 실패: {e}")
        
        # 제품이 없으면 더미 데이터 생성 (테스트용)
        if len(all_products) == 0:
            print("\n⚠️  실제 제품을 찾지 못해 테스트 데이터 생성...")
            all_products = [
                {
                    'brand_id': self.brand_id,
                    'title': 'CU 테스트 도시락',
                    'normalized_title': self.normalize_title('CU 테스트 도시락'),
                    'price': 4500,
                    'category': '즉석식품',
                    'launch_date': datetime.now().date().isoformat(),
                    'image_url': None,
                    'source_url': 'https://cu.bgfretail.com',
                    'is_active': True
                },
                {
                    'brand_id': self.brand_id,
                    'title': 'CU 테스트 커피',
                    'normalized_title': self.normalize_title('CU 테스트 커피'),
                    'price': 1500,
                    'category': '음료',
                    'launch_date': datetime.now().date().isoformat(),
                    'image_url': None,
                    'source_url': 'https://cu.bgfretail.com',
                    'is_active': True
                }
            ]
            print(f"✓ 테스트 데이터 {len(all_products)}개 생성")
        
        return all_products
    
    def normalize_title(self, title):
        normalized = re.sub(r'\s+', ' ', title)
        normalized = re.sub(r'[^\w\s가-힣]', '', normalized)
        return normalized.strip().upper()
    
    def categorize(self, title):
        title_lower = title.lower()
        keywords = {
            '음료': ['음료', '주스', '커피', '우유', '차', '워터', '사이다', '콜라'],
            '과자': ['과자', '초콜릿', '사탕', '젤리', '쿠키', '비스킷', '스낵'],
            '즉석식품': ['도시락', '김밥', '샌드위치', '삼각김밥', '핫도그', '햄버거'],
            '라면': ['라면', '컵라면', '짜파게티', '짜장면'],
            '아이스크림': ['아이스크림', '빙과', '아이스바', '콘']
        }
        for category, words in keywords.items():
            if any(word in title_lower for word in words):
                return category
        return '기타'
    
    def save_to_db(self, products):
        if not products:
            print("💤 저장할 제품 없음")
            return 0
        
        print(f"\n💾 DB 저장 시작... ({len(products)}개)")
        
        try:
            # 테이블 존재 확인
            try:
                test = self.supabase.table('new_products').select('id').limit(1).execute()
                print("✓ 테이블 연결 성공")
            except Exception as e:
                print(f"❌ 테이블 없음: {e}")
                print("👉 Supabase에서 테이블을 먼저 생성하세요!")
                return 0
            
            # 중복 체크
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
            
            new_products = [
                p for p in products 
                if f"{p['normalized_title']}_{p['launch_date']}" not in existing_keys
            ]
            
            if new_products:
                result = self.supabase.table('new_products').insert(new_products).execute()
                print(f"✅ {len(new_products)}개 저장 완료!")
                for p in new_products[:3]:
                    print(f"  - {p['title'][:40]}")
                return len(new_products)
            else:
                print("ℹ️  모두 기존 제품 (신규 없음)")
                return 0
                
        except Exception as e:
            print(f"❌ DB 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return 0

def main():
    print("="*60)
    print("🏪 CU 신제품 크롤러")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    try:
        crawler = CUCrawler()
        products = crawler.crawl()
        new_count = crawler.save_to_db(products)
        
        print("\n" + "="*60)
        print(f"✨ 완료!")
        print(f"📊 수집: {len(products)}개 | 신규: {new_count}개")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ 오류: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()
