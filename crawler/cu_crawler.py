import os
import re
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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
            time.sleep(5)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # 1. 제품 리스트 찾기 (가장 확실한 선택자 사용)
            product_items = soup.select('div[class*="prod"] li')
            if not product_items:
                product_items = soup.select('ul li') # fallback
            
            print(f"  🔍 {len(product_items)}개 아이템 발견 (유효성 검사 전)")
            
            for idx, item in enumerate(product_items[:30]):
                try:
                    # 2. 이미지 추출 (가장 중요)
                    img_tag = item.find('img')
                    if not img_tag:
                        continue
                        
                    image_url = img_tag.get('src')
                    if not image_url or 'blank' in image_url:
                        continue
                        
                    if not image_url.startswith('http'):
                        if image_url.startswith('//'):
                            image_url = 'https:' + image_url
                        else:
                            image_url = self.base_url + image_url
                            
                    title = img_tag.get('alt')
                    
                    # 3. 제목 추출 (이미지 alt가 없으면 텍스트에서)
                    if not title:
                        name_tag = item.select_one('.name, .title, .prod_name, p')
                        if name_tag:
                            title = name_tag.get_text(strip=True)
                    
                    if not title:
                        # 텍스트 전체에서 찾기
                        text = item.get_text(strip=True)
                        if len(text) > 2:
                            title = text.split('원')[0].strip()[-20:] # 추측
                    
                    if not title or len(title) < 2:
                        continue

                    # 4. 가격 추출
                    price = 0
                    price_tag = item.select_one('.price, .cost, .val')
                    price_text = price_tag.get_text() if price_tag else item.get_text()
                    
                    numbers = re.findall(r'\d+', price_text.replace(',', ''))
                    if numbers:
                        # 가장 큰 숫자를 가격으로 간주
                        price = max([int(n) for n in numbers if len(n) < 7])
                    
                    # 5. 링크 추출
                    link_tag = item.find('a')
                    source_url = category_url
                    if link_tag and link_tag.get('href') and 'javascript' not in link_tag.get('href'):
                        href = link_tag.get('href')
                        if href.startswith('http'):
                            source_url = href
                        else:
                            source_url = self.base_url + href
                            
                    # 결과 추가
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
                    print(f"    ✓ {title} ({price}원)")
                    
                except Exception as e:
                    continue
            
        except Exception as e:
            print(f"  ❌ {category_name} 오류: {e}")
        
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
                time.sleep(2)
            
            return all_products
            
        except Exception as e:
            print(f"❌ 전체 오류: {e}")
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
            return 0
        
        print(f"\n💾 DB 저장 시작... ({len(products)}개)")
        try:
            thirty_days_ago = (datetime.now() - timedelta(days=30)).date().isoformat()
            
            # 기존 데이터 확인 안하고 그냥 저장 시도 (중복은 DB에서 처리하거나 무시)
            # 간단하게 하기 위해 최근 데이터만 확인
            
            self.supabase.table('new_products').upsert(products, on_conflict='normalized_title, launch_date').execute()
            print(f"✅ 저장 완료!")
            return len(products)
                
        except Exception as e:
            # upsert 실패 시 개별 insert 시도
            success_count = 0
            for p in products:
                try:
                    self.supabase.table('new_products').insert(p).execute()
                    success_count += 1
                except:
                    pass
            print(f"✅ {success_count}개 저장 완료 (개별)")
            return success_count

def main():
    try:
        crawler = CUCrawler()
        products = crawler.crawl()
        crawler.save_to_db(products)
    except Exception as e:
        print(e)
        exit(1)

if __name__ == "__main__":
    main()
