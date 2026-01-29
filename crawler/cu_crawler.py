import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import os
import time

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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.brand_id = 1
    
    def crawl(self):
        print("🏪 CU 크롤링 시작...")
        
        # 여러 URL 시도
        urls = [
            "https://cu.bgfretail.com/event/plus.do",  # 기획전
            "https://cu.bgfretail.com/product/product.do?category=product&depth2=6&sf=N",  # 신상품
            "https://cu.bgfretail.com/product/product.do?category=product",  # 전체 상품
        ]
        
        all_products = []
        
        for url in urls:
            try:
                print(f"\n🔍 시도 중: {url}")
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                print(f"✓ 응답 코드: {response.status_code}")
                print(f"✓ 콘텐츠 길이: {len(response.content)} bytes")
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # 여러 가능한 선택자 시도
                selectors = [
                    '.prodListWrap .prod_item',
                    '.prod_list li',
                    '.product-list .product-item',
                    '.list_product li',
                    'ul.prod_list > li',
                    '.prod-list li',
                    'div[class*="prod"] li',
                    'article.prod_item',
                ]
                
                items = []
                for selector in selectors:
                    items = soup.select(selector)
                    if items:
                        print(f"✓ 선택자 성공: {selector}")
                        print(f"📦 발견된 상품: {len(items)}개")
                        break
                
                if not items:
                    print(f"✗ 상품을 찾지 못함")
                    # HTML 구조 힌트 출력
                    print("\n📝 HTML 구조 샘플:")
                    print(soup.prettify()[:1000])
                    continue
                
                products = self.parse_items(items, url)
                all_products.extend(products)
                
                if len(all_products) >= 20:
                    break
                    
            except Exception as e:
                print(f"❌ URL 처리 실패: {e}")
                continue
        
        print(f"\n📊 총 수집: {len(all_products)}개")
        return all_products[:20]  # 최대 20개
    
    def parse_items(self, items, base_url):
        """제품 아이템 파싱"""
        products = []
        
        for idx, item in enumerate(items[:20]):
            try:
                # 여러 가능한 선택자로 시도
                title_selectors = ['.prodName', '.prod_name', '.product-name', 
                                   '.name', 'dt', 'h3', 'h4', '.title', 'strong']
                title = None
                for sel in title_selectors:
                    elem = item.select_one(sel)
                    if elem:
                        title = elem.text.strip()
                        break
                
                if not title:
                    # a 태그의 title 속성 확인
                    a_tag = item.select_one('a')
                    if a_tag and a_tag.get('title'):
                        title = a_tag.get('title').strip()
                
                if not title or len(title) < 2:
                    continue
                
                # 가격
                price_selectors = ['.price', '.prod_price', '.product-price', '.val', 'dd']
                price = None
                for sel in price_selectors:
                    elem = item.select_one(sel)
                    if elem:
                        price_text = elem.text.strip()
                        price_match = re.findall(r'\d+', price_text.replace(',', ''))
                        if price_match:
                            price = int(''.join(price_match))
                            break
                
                # 이미지
                img_elem = item.select_one('img')
                image_url = None
                if img_elem:
                    image_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-original')
                    if image_url:
                        if not image_url.startswith('http'):
                            if image_url.startswith('//'):
                                image_url = 'https:' + image_url
                            elif image_url.startswith('/'):
                                image_url = 'https://cu.bgfretail.com' + image_url
                            else:
                                image_url = 'https://cu.bgfretail.com/' + image_url
                
                # 링크
                link_elem = item.select_one('a')
                source_url = base_url
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
                print(f"  ✓ {idx+1}. {title[:40]}...")
                
            except Exception as e:
                print(f"  ✗ 상품 파싱 실패: {e}")
                continue
        
        return products
    
    def normalize_title(self, title):
        normalized = re.sub(r'\s+', ' ', title)
        normalized = re.sub(r'[^\w\s가-힣]', '', normalized)
        return normalized.strip().upper()
    
    def categorize(self, title):
        title_lower = title.lower()
        keywords = {
            '음료': ['음료', '주스', '커피', '우유', '차', '워터', '사이다', '콜라', '에이드'],
            '과자': ['과자', '초콜릿', '사탕', '젤리', '쿠키', '비스킷', '스낵', '칩'],
            '즉석식품': ['도시락', '김밥', '샌드위치', '삼각김밥', '핫도그', '햄버거', '버거'],
            '라면': ['라면', '컵라면', '짜파게티', '짜장면', '볶음면'],
            '아이스크림': ['아이스크림', '빙과', '아이스바', '콘', '파인트']
        }
        for category, words in keywords.items():
            if any(word in title_lower for word in words):
                return category
        return '기타'
    
    def save_to_db(self, products):
        if not products:
            print("💤 수집된 제품 없음")
            return 0
        
        print(f"\n💾 DB 저장 시작...")
        
        try:
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
                self.supabase.table('new_products').insert(new_products).execute()
                print(f"✅ {len(new_products)}개 신제품 저장 완료")
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
            import traceback
            traceback.print_exc()
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
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()
