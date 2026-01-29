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
    
    def extract_gdidx(self, item):
        """onclick=view(26285) 에서 gdIdx 추출"""
        try:
            # onclick 속성 찾기
            clickable = item.find(attrs={'onclick': True})
            if clickable:
                onclick = clickable.get('onclick', '')
                # view(26285) 패턴에서 숫자 추출
                match = re.search(r'view\((\d+)\)', onclick)
                if match:
                    return match.group(1)
        except:
            pass
        return None
    
    def crawl_category(self, driver, category_url, category_name):
        print(f"\n📂 {category_name} 크롤링 중...")
        products = []
        
        try:
            driver.get(category_url)
            time.sleep(5)
            
            # 페이지 스크롤
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # 제품 영역 찾기
            product_area = soup.select_one('.prodListWrap, .prodArea')
            if not product_area:
                print(f"  ⚠️ 제품 영역을 찾을 수 없습니다")
                return products
            
            # 제품 항목 찾기
            items = product_area.select('li')
            print(f"  🔍 {len(items)}개 항목 발견")
            
            for item in items:
                try:
                    # 1. 제품 이미지 확인
                    img = item.select_one('img[src*="/product/"]')
                    if not img:
                        continue
                    
                    image_url = img.get('src', '')
                    
                    # New 태그나 아이콘 제외
                    if 'icon' in image_url or 'tag_' in image_url or 'blank' in image_url:
                        continue
                    
                    # HTTPS 변환
                    if image_url.startswith('//'):
                        image_url = 'https:' + image_url
                    elif not image_url.startswith('http'):
                        image_url = self.base_url + image_url
                    
                    # 2. 제품명 추출
                    name_tag = item.select_one('p')
                    if not name_tag:
                        continue
                    
                    title = name_tag.get_text(strip=True)
                    
                    # 제품명 검증
                    if not title or len(title) < 2:
                        continue
                    
                    # 파일명이나 New 제외
                    if title.endswith('.jpg') or title.endswith('.png') or title == 'New':
                        continue
                    
                    # 3. gdIdx 추출 (상세 페이지 URL)
                    gdidx = self.extract_gdidx(item)
                    
                    if gdidx:
                        source_url = f"{self.base_url}/product/view.do?gdIdx={gdidx}&category=product"
                    else:
                        source_url = category_url  # gdIdx 없으면 카테고리 페이지
                    
                    # 4. 가격 추출
                    price = 0
                    price_tag = item.select_one('.price, .val, span[class*="price"]')
                    
                    if price_tag:
                        price_text = price_tag.get_text()
                        numbers = re.findall(r'\d+', price_text.replace(',', ''))
                        if numbers:
                            valid = [int(n) for n in numbers if 100 <= int(n) < 1000000]
                            if valid:
                                price = max(valid)
                    
                    # 가격 없으면 전체 텍스트에서
                    if price == 0:
                        all_text = item.get_text()
                        matches = re.findall(r'(\d{1,3}(?:,\d{3})*|\d+)\s*원', all_text)
                        if matches:
                            price = int(matches[0].replace(',', ''))
                    
                    # 5. 카테고리 키워드 검증
                    if not self.validate_category(title, category_name):
                        print(f"    ⚠️ 카테고리 불일치 스킵: {title}")
                        continue
                    
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
                    print(f"    ✓ {title} ({price}원) [{gdidx or 'N/A'}]")
                    
                except Exception as e:
                    continue
            
            print(f"  ✅ {len(products)}개 수집 완료")
            
        except Exception as e:
            print(f"  ❌ {category_name} 오류: {e}")
        
        return products
    
    def validate_category(self, title, category):
        """카테고리별 키워드로 검증"""
        title_lower = title.lower()
        
        keywords = {
            '아이스크림': ['아이스크림', '빙과', '콘', '바', '슬러시', '아이스', 'ice', '소프트', '젤라또', '셔벗', '소르베', '팝콘'],
            '과자류': ['과자', '스낵', '칩', '쿠키', '비스킷', '초콜릿', '사탕', '젤리', '껌', '캔디', '웨하스', '크래커'],
            '음료': ['음료', '주스', '커피', '차', '워터', '탄산', '에너지', '이온', '밀크', '라떼', '에이드', '스무디'],
            '간편식사': ['도시락', '김밥', '샌드위치', '삼각', '주먹밥', '햄버거', '핫도그', '토스트', '롤', '랩'],
            '식품': ['라면', '컵라면', '우유', '빵', '계란', '치즈', '햄', '소시지', '두부', '김', '냉동']
        }
        
        if category in keywords:
            return any(keyword in title_lower for keyword in keywords[category])
        
        return True
    
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
            print("⚠️ 저장할 제품이 없습니다")
            return 0
        
        print(f"\n💾 DB 저장 시작... ({len(products)}개)")
        
        try:
            # 중복 제거
            seen = set()
            unique_products = []
            for p in products:
                key = f"{p['normalized_title']}_{p['category']}"
                if key not in seen:
                    seen.add(key)
                    unique_products.append(p)
            
            print(f"  📦 중복 제거 후: {len(unique_products)}개")
            
            # 배치 저장
            self.supabase.table('new_products').upsert(
                unique_products,
                on_conflict='normalized_title,launch_date'
            ).execute()
            
            print(f"✅ {len(unique_products)}개 저장 완료!")
            return len(unique_products)
            
        except Exception as e:
            print(f"⚠️ Batch 저장 실패: {e}")
            print(f"  개별 저장 시도...")
            
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
        
        if products:
            crawler.save_to_db(products)
        else:
            print("❌ 수집된 제품이 없습니다")
            exit(1)
            
    except Exception as e:
        print(f"❌ 크롤러 오류: {e}")
        exit(1)

if __name__ == "__main__":
    main()
