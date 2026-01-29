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
        }
        self.brand_id = 1
    
    def crawl(self):
        print("🏪 CU 크롤링 시작...")
        
        url = "https://cu.bgfretail.com/product/product.do?category=product&depth2=6&sf=N"
        
        try:
            print(f"\n🔍 접속 중: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            print(f"✓ 응답 코드: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 선택자 시도
            items = soup.select('div[class*="prod"] li')
            print(f"📦 발견된 상품: {len(items)}개\n")
            
            if items:
                # 첫 번째 상품의 HTML 구조 출력 (디버깅용)
                print("="*60)
                print("🔍 첫 번째 상품 HTML 구조:")
                print("="*60)
                print(items[0].prettify()[:2000])
                print("="*60)
                print()
            
            products = self.parse_items(items, url)
            return products
            
        except Exception as e:
            print(f"❌ 크롤링 실패: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def parse_items(self, items, base_url):
        """제품 아이템 파싱 - 상세 디버깅"""
        products = []
        
        for idx, item in enumerate(items[:5]):  # 처음 5개만
            print(f"\n--- 상품 {idx+1} 파싱 시도 ---")
            
            try:
                # 모든 텍스트 출력
                all_text = item.get_text(strip=True)
                print(f"전체 텍스트: {all_text[:100]}")
                
                # 제품명 찾기 - 다양한 선택자 시도
                title = None
                title_attempts = [
                    ('강', item.select_one('strong')),
                    ('이름', item.select_one('.prodName')),
                    ('이름2', item.select_one('.prod_name')),
                    ('이름3', item.select_one('.name')),
                    ('dt', item.select_one('dt')),
                    ('h3', item.select_one('h3')),
                    ('a.title', item.select_one('a')
.get('title') if item.select_one('a') else None),
                ]
                
                for label, elem in title_attempts:
                    if elem:
                        if isinstance(elem, str):
                            title = elem
                        else:
                            title = elem.text.strip()
                        if title and len(title) > 2:
                            print(f"✓ 제품명 발견 ({label}): {title}")
                            break
                
                if not title:
                    # 텍스트 전체에서 추출 시도
                    lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                    if lines:
                        title = lines[0]
                        print(f"⚠ 제품명 추측: {title}")
                
                # 가격 찾기
                price = None
                price_text = None
                
                price_elems = [
                    item.select_one('.price'),
                    item.select_one('dd'),
                    item.select_one('.val'),
                ]
                
                for elem in price_elems:
                    if elem:
                        price_text = elem.text.strip()
                        break
                
                if not price_text:
                    # 숫자가 포함된 텍스트 찾기
                    import re
                    numbers = re.findall(r'\d{1,3}(?:,\d{3})*', all_text)
                    if numbers:
                        price_text = numbers[0]
                
                if price_text:
                    price_match = re.findall(r'\d+', price_text.replace(',', ''))
                    if price_match:
                        price = int(''.join(price_match))
                        print(f"✓ 가격 발견: {price}원 (원본: {price_text})")
                
                # 이미지 찾기
                img = item.select_one('img')
                image_url = None
                if img:
                    image_url = img.get('src') or img.get('data-src')
                    if image_url and not image_url.startswith('http'):
                        if image_url.startswith('//'):
                            image_url = 'https:' + image_url
                        elif image_url.startswith('/'):
                            image_url = 'https://cu.bgfretail.com' + image_url
                    print(f"✓ 이미지: {image_url[:50]}...")
                
                # 링크 찾기
                link = item.select_one('a')
                source_url = base_url
                if link and link.get('href'):
                    href = link.get('href')
                    if href.startswith('http'):
                        source_url = href
                    elif href.startswith('/'):
                        source_url = 'https://cu.bgfretail.com' + href
                
                # 제품 데이터 생성
                if title and len(title) > 2:
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
                    print(f"✅ 파싱 성공: {title[:40]}")
                else:
                    print(f"❌ 제품명을 찾을 수 없음")
                
            except Exception as e:
                print(f"❌ 파싱 오류: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"\n📊 최종 파싱 성공: {len(products)}개")
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
            '즉석식품': ['도시락', '김밥', '샌드위치', '삼각김밥', '핫도그', '햄버거'],
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
    print("🏪 편의점 신제품 크롤러 시작 (디버그 모드)")
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
