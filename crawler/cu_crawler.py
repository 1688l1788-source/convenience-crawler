import os
import re
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

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
        
        self.brand_id = 1
    
    def setup_driver(self):
        """Selenium 크롬 드라이버 설정"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    
    def crawl(self):
        print("🏪 CU 크롤링 시작...")
        driver = None
        
        try:
            driver = self.setup_driver()
            url = "https://cu.bgfretail.com/product/pb.do"
            
            print(f"\n🔍 접속: {url}")
            driver.get(url)
            
            # 페이지 로딩 대기
            time.sleep(3)
            
            # 제품 목록이 로드될 때까지 대기
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.prod_list li, .prodListWrap li"))
                )
            except:
                print("⚠️ 제품 목록 로딩 타임아웃")
            
            # HTML 파싱
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # 제품 찾기
            products_html = soup.select('ul.prod_list > li')
            if not products_html:
                products_html = soup.select('.prodListWrap li')
            if not products_html:
                products_html = soup.select('li[class*="prod"]')
            
            print(f"📦 발견: {len(products_html)}개")
            
            products = []
            
            for idx, item in enumerate(products_html[:20]):
                try:
                    # 제품명
                    name_elem = item.select_one('.prodName, .prod_name, strong, h3')
                    if not name_elem:
                        continue
                    
                    title = name_elem.text.strip()
                    if not title or len(title) < 3 or '원' in title[:5]:
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
                    
                    # 링크
                    link = item.select_one('a')
                    source_url = url
                    if link and link.get('href'):
                        href = link.get('href')
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
                    print(f"  ✓ {idx+1}. {title[:40]}")
                    
                except Exception as e:
                    print(f"  ✗ 파싱 오류: {e}")
            
            return products
            
        except Exception as e:
            print(f"❌ 크롤링 실패: {e}")
            import traceback
            traceback.print_exc()
            return []
            
        finally:
            if driver:
                driver.quit()
    
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
                self.supabase.table('new_products').insert(new_products).execute()
                print(f"✅ {len(new_products)}개 저장 완료!")
                for p in new_products[:5]:
                    print(f"  - {p['title'][:40]}")
                return len(new_products)
            else:
                print("ℹ️ 모두 기존 제품 (신규 없음)")
                return 0
                
        except Exception as e:
            print(f"❌ DB 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return 0

def main():
    print("="*60)
    print("🏪 CU 신제품 크롤러 (Selenium)")
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
