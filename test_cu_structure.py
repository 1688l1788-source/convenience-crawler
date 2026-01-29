import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

def test_cu_page():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    # 아이스크림 카테고리만 테스트
    url = "https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=3"
    
    print(f"🔍 페이지 접속: {url}\n")
    driver.get(url)
    time.sleep(5)
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    # 여러 선택자 시도
    selectors = [
        'div.prodListWrap ul.prodList li',
        'ul.prodList > li',
        'div.prodList li',
        'li.prod_list',
        'div[class*="prod"] li',
        '.prodArea li',
    ]
    
    print("=" * 60)
    for selector in selectors:
        items = soup.select(selector)
        print(f"선택자: {selector}")
        print(f"  → {len(items)}개 발견")
        
        if items and len(items) > 0:
            print(f"\n  첫 번째 아이템 구조:")
            print(f"  {items[0].prettify()[:500]}")
            print("\n")
    
    print("=" * 60)
    
    # 전체 HTML 구조 확인
    print("\n📄 페이지 주요 구조:")
    main_containers = soup.select('div[class*="prod"], ul[class*="prod"]')
    for container in main_containers[:3]:
        print(f"  - {container.name}.{container.get('class')}")
    
    driver.quit()

if __name__ == "__main__":
    test_cu_page()
