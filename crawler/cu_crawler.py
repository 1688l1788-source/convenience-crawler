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
            
            # 제품 리스트 찾기 - 더 정확한 선택자 사용
            product_items = soup.select('div.prodListWrap ul.prodList li')
            
            if not product_items:
                product_items = soup.select('ul.prodList > li')
            
            if not product_items:
                product_items = soup.select('div[class*="prod"] li')
            
            print(f"  🔍 {len(product_items)}개 아이템 발견")
            
            for idx, item in enumerate(product_items[:30]):
                try:
                    # 1. 링크 먼저 추출 (가장 중요)
                    link_tag = item.find('a')
                    if not link_tag or not link_tag.get('href'):
                        continue
                    
                    href = link_tag.get('href')
                    
                    # javascript: 링크는 무시 (상세 페이지 아님)
                    if 'javascript' in href or href == '#':
                        continue
                    
                    # 개별 상품 상세 페이지 URL 구성
                    if href.startswith('http'):
                        source_url = href
                    elif href.startswith('/'):
                        source_url = self.base_url + href
                    else:
                        source_url = self.base_url + '/' + href
                    
                    # 상세페이지 링크가 아니면 스킵
                    if 'goodsDetail' not in source_url and 'itemId' not in source_url:
                        continue
                    
                    # 2. 이미지 추출
                    img_tag = item.find('img')
                    if not img_tag:
                        continue
                        
                    image_url = img_tag.get('src')
                    if not image_url or 'blank' in image_url or 'noimage' in image_url:
                        continue
                        
                    if not image_url.startswith('http'):
                        if image_url.startswith('//'):
                            image_url = 'https:' + image_url
                        else:
                            image_url = self.base_url + image_url
                    
                    # 3. 제목 추출
                    title = img_tag.get('alt')
                    
                    if not title or len(title) < 2:
                        # alt가 없으면 name 클래스에서 찾기
                        name_tag = item.select_one('.name, .prodName, .prod_name, p.name')
                        if name_tag:
                            title = name_tag.get_text(strip=True)
                    
                    if not title or len(title) < 2:
                        continue
                    
                    # 파일명이 제목인 경우 스킵 (실제 상품명이 아님)
                    if title.endswith('.jpg') or title.endswith('.png'):
                        # 상세 페이지에서 제목 추출 시도
                        try:
                            driver.get(source_url)
                            time.sleep(2)
                            detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                            title_elem = detail_soup.select_one('.prodTitle, .prod_tit, h3, .title')
                            if title_elem:
                                title = title_elem.get_text(strip=True)
                            driver.back()
                            time.sleep(1)
                        except:
                            continue
                    
                    # 여전히 파일명이면 스킵
                    if title.endswith('.jpg') or title.endswith('.png'):
                        continue

                    # 4. 가격 추출
                    price = 0
                    price_tag = item.select_one('.price, .val, span.val, .cost')
                    
                    if price_tag:
                        price_text = price_tag.get_text()
                        numbers = re.findall(r'\d+', price_text.replace(',', ''))
                        if numbers:
                            price = max([int(n) for n in numbers if 100 <= int(n) < 1000000])
                    
                    # 가격이 없으면 전체 텍스트에서 추출
                    if price == 0:
                        text = item.get_text()
                        numbers = re.findall(r'\d+', text.replace(',', ''))
                        if numbers:
                            # 합리적인 가격 범위만 선택 (100원 ~ 100만원)
                            valid_prices = [int(n) for n in numbers if 100 <= int(n) < 1000000]
                            if valid_prices:
                                price = max(valid_prices)
                    
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
                    print(f"    ✓ {title[:30]} ({price}원)")
                    
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
                time.sleep(3)
            
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
            # 중복 제거 (같은 normalized_title)
            seen = set()
            unique_products = []
            for p in products:
                key = p['normalized_title'] + p['category']
                if key not in seen:
                    seen.add(key)
                    unique_products.append(p)
            
            print(f"  📦 중복 제거 후: {len(unique_products)}개")
            
            # 배치로 저장
            self.supabase.table('new_products').upsert(
                unique_products, 
                on_conflict='normalized_title,launch_date'
            ).execute()
            
            print(f"✅ {len(unique_products)}개 저장 완료!")
            return len(unique_products)
                
        except Exception as e:
            print(f"⚠️ Batch 저장 실패, 개별 저장 시도...")
            success_count = 0
            for p in unique_products:
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
