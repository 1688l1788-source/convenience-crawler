import os
import re
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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
        self.base_url = "https://cu.bgfretail.com"
        
        # 카테고리 URL 매핑
        self.category_urls = [
            ("https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=1", "간편식사"),
            ("https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=2", "과자류"),
            ("https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=3", "아이스크림"),
            ("https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=4", "식품"),
            ("https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=5", "음료"),
        ]
    
    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    
    def crawl_category(self, driver, category_url, category_name):
        print(f"\n📂 {category_name} 크롤링 중...")
        products = []
        
        try:
            driver.get(category_url)
            time.sleep(5)  # 페이지 로딩 대기 증가
            
            # 페이지 HTML 출력 (디버깅용)
            print(f"  🔍 페이지 소스 길이: {len(driver.page_source)}")
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # 다양한 선택자 시도
            selectors = [
                'ul.prodListWrap > li',
                '.prod_list > li',
                'div.prod_list li',
                'li.prod_item',
                '.prodList li',
                'div[class*="prod"] li'
            ]
            
            product_items = []
            for selector in selectors:
                product_items = soup.select(selector)
                if product_items:
                    print(f"  ✅ 선택자 '{selector}' 로 {len(product_items)}개 발견")
                    break
            
            if not product_items:
                print(f"  ❌ 제품 목록을 찾을 수 없습니다")
                # HTML 일부 출력
                print(f"  📄 HTML 샘플: {str(soup)[:500]}")
                return []
            
            # 상위 10개만 처리 (테스트용)
            for idx, item in enumerate(product_items[:10]):
                try:
                    # 링크 찾기
                    link = item.find('a')
                    if not link or not link.get('href'):
                        continue
                    
                    href = link.get('href')
                    
                    # 제품명 찾기 (다양한 방법)
                    title = None
                    if link.get('title'):
                        title = link.get('title').strip()
                    elif item.find(string=True):
                        title = item.get_text().strip()[:50]
                    
                    if not title or len(title) < 3:
                        continue
                    
                    # 가격 찾기 (숫자만)
                    price = None
                    price_text = item.get_text()
                    numbers = re.findall(r'\d+', price_text.replace(',', ''))
                    if numbers:
                        # 가장 큰 숫자를 가격으로 (보통 가격이 가장 큼)
                        price = max(int(n) for n in numbers if len(n) <= 6)
                    
                    # 이미지 URL
                    image_url = None
                    img = item.find('img')
                    if img:
                        image_url = img.get('src') or img.get('data-src')
                        if image_url and not image_url.startswith('http'):
                            image_url = self.base_url + image_url
                    
                    # 상세 URL
                    if href.startswith('http'):
                        source_url = href
                    elif href.startswith('/'):
                        source_url = self.base_url + href
                    else:
                        source_url = self.base_url + '/' + href
                    
                    product = {
                        'brand_id': self.brand_id,
                        'title': title,
                        'normalized_title': self.normalize_title(title),
                        'price': price,
                        'category': category_name,
                        'launch_date': datetime.now().date().isoformat(),
                        'image_url': image_url,
                        'source_url': source_url,
                        'is_active': True
                    }
                    
                    products.append(product)
                    print(f"    ✓ {idx+1}. {title[:30]} ({price}원)")
                    
                except Exception as e:
                    print(f"    ✗ {idx+1}번 파싱 오류: {e}")
            
        except Exception as e:
            print(f"  ❌ {category_name} 크롤링 실패: {e}")
            import traceback
            traceback.print_exc()
        
        return products
    
    def crawl(self):
        print("🏪 CU 신제품 크롤링 시작...")
        driver = None
        all_products = []
        
        try:
            driver = self.setup_driver()
            
            for url, name in self.category_urls:
                products = self.crawl_category(driver, url, name)
                all_products.extend(products)
                time.sleep(3)
            
            print(f"\n✅ 총 {len(all_products)}개 제품 수집 완료")
            return all_products
            
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
    
    def save_to_db(self, products):
        if not products:
            print("💤 저장할 제품 없음")
            return 0
        
        print(f"\n💾 DB 저장 시작... ({len(products)}개)")
        
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
                print(f"✅ {len(new_products)}개 저장 완료!")
                return len(new_products)
            else:
                print("ℹ️ 모두 기존 제품")
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
