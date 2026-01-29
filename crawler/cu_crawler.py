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
        
        # 카테고리 URL 매핑 (depth3 = 1~7)
        self.category_urls = [
            "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=1",  # 간편식사
            "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=2",  # 과자류
            "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=3",  # 아이스크림
            "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=4",  # 식품
            "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=5",  # 음료
            "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=6",  # 생활용품
            "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=7",  # 건강/위생용품
        ]
    
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
    
    def extract_product_details(self, driver, product_url):
        """개별 제품 상세 페이지에서 정보 추출"""
        try:
            driver.get(product_url)
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # 제품명
            title = None
            title_elem = soup.select_one('.prodTitle, .prod_title, h3, .name')
            if title_elem:
                title = title_elem.text.strip()
            
            # 가격
            price = None
            price_elem = soup.select_one('.prodPrice, .price, .cost, .val')
            if price_elem:
                price_text = price_elem.text.strip()
                numbers = re.findall(r'\d+', price_text.replace(',', ''))
                if numbers:
                    price = int(''.join(numbers))
            
            # 이미지
            image_url = None
            img = soup.select_one('.prodImg img, .prod_img img, .detail_img img')
            if img:
                image_url = img.get('src') or img.get('data-src')
                if image_url:
                    if image_url.startswith('//'):
                        image_url = 'https:' + image_url
                    elif not image_url.startswith('http'):
                        image_url = self.base_url + image_url
            
            return {
                'title': title,
                'price': price,
                'image_url': image_url,
                'source_url': product_url
            }
            
        except Exception as e:
            print(f"  ⚠️ 상세 페이지 크롤링 실패: {e}")
            return None
    
    def crawl_category(self, driver, category_url, category_name):
        """카테고리별 신제품 크롤링"""
        print(f"\n📂 {category_name} 크롤링 중...")
        products = []
        
        try:
            driver.get(category_url)
            time.sleep(3)
            
            # 제품 목록 대기
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.prodListWrap li, .prod_list li"))
                )
            except:
                print(f"  ⚠️ {category_name} 제품 목록 로딩 실패")
                return []
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # 제품 찾기
            product_items = soup.select('ul.prodListWrap > li, .prod_list > li')
            print(f"  📦 발견: {len(product_items)}개")
            
            # 상위 20개만 처리
            for idx, item in enumerate(product_items[:20]):
                try:
                    # 제품 링크 추출
                    link = item.select_one('a')
                    if not link or not link.get('href'):
                        continue
                    
                    href = link.get('href')
                    
                    # 절대 URL 생성
                    if href.startswith('http'):
                        product_url = href
                    elif href.startswith('/'):
                        product_url = self.base_url + href
                    else:
                        product_url = self.base_url + '/' + href
                    
                    # gdIdx 확인 (실제 제품 상세 페이지인지 확인)
                    if 'gdIdx=' not in product_url:
                        continue
                    
                    # 상세 페이지에서 정보 추출
                    details = self.extract_product_details(driver, product_url)
                    
                    if not details or not details['title']:
                        continue
                    
                    # 제품 정보 구성
                    product = {
                        'brand_id': self.brand_id,
                        'title': details['title'],
                        'normalized_title': self.normalize_title(details['title']),
                        'price': details['price'],
                        'category': self.categorize(details['title']),
                        'launch_date': datetime.now().date().isoformat(),
                        'image_url': details['image_url'],
                        'source_url': details['source_url'],
                        'is_active': True
                    }
                    
                    products.append(product)
                    print(f"    ✓ {idx+1}. {product['title'][:40]}")
                    
                    # 과도한 요청 방지
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"    ✗ {idx+1}번 제품 파싱 오류: {e}")
                    continue
            
        except Exception as e:
            print(f"  ❌ {category_name} 크롤링 실패: {e}")
        
        return products
    
    def crawl(self):
        """전체 카테고리 크롤링"""
        print("🏪 CU 신제품 크롤링 시작...")
        driver = None
        all_products = []
        
        try:
            driver = self.setup_driver()
            
            category_names = [
                "간편식사",
                "과자류", 
                "아이스크림",
                "식품",
                "음료",
                "생활용품",
                "건강/위생용품"
            ]
            
            # 각 카테고리 크롤링
            for url, name in zip(self.category_urls, category_names):
                products = self.crawl_category(driver, url, name)
                all_products.extend(products)
                
                # 카테고리 간 대기
                time.sleep(2)
            
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
        """제목 정규화"""
        normalized = re.sub(r'\s+', ' ', title)
        normalized = re.sub(r'[^\w\s가-힣]', '', normalized)
        return normalized.strip().upper()
    
    def categorize(self, title):
        """카테고리 자동 분류"""
        title_lower = title.lower()
        keywords = {
            '음료': ['음료', '주스', '커피', '우유', '차', '워터', '사이다', '콜라', '라떼', '에이드'],
            '과자': ['과자', '초콜릿', '사탕', '젤리', '쿠키', '비스킷', '스낵', '칩', '팝콘'],
            '즉석식품': ['도시락', '김밥', '샌드위치', '삼각김밥', '핫도그', '햄버거', '컵밥', '덮밥'],
            '라면': ['라면', '컵라면', '짜파게티', '짜장면', '우동'],
            '아이스크림': ['아이스크림', '빙과', '아이스바', '콘', '빙수']
        }
        
        for category, words in keywords.items():
            if any(word in title_lower for word in words):
                return category
        return '기타'
    
    def save_to_db(self, products):
        """Supabase에 저장"""
        if not products:
            print("💤 저장할 제품 없음")
            return 0
        
        print(f"\n💾 DB 저장 시작... ({len(products)}개)")
        
        try:
            # 중복 체크 (최근 30일)
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
                
                # 카테고리별 통계
                from collections import Counter
                categories = Counter(p['category'] for p in new_products)
                for cat, count in categories.items():
                    print(f"  - {cat}: {count}개")
                
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
