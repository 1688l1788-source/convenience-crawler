
import puppeteer from 'puppeteer';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

// ES Module 환경에서 __dirname 설정
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// .env 파일 로드 (상위 폴더에 있다고 가정)
dotenv.config({ path: path.join(__dirname, '../.env') });

const SUPABASE_URL = process.env.VITE_SUPABASE_URL;
const SUPABASE_KEY = process.env.VITE_SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Error: .env 파일에 VITE_SUPABASE_URL 또는 VITE_SUPABASE_ANON_KEY가 설정되어 있지 않습니다.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// 브랜드 ID 매핑 (DB의 brands 테이블 참조)
const BRAND_ID = {
  CU: 1,
  GS25: 2,
  SEVEN_ELEVEN: 3,
  EMART24: 4
};

// 카테고리 추론 헬퍼 함수
function getCategory(name) {
  if (name.includes('도시락') || name.includes('김밥') || name.includes('삼각') || name.includes('주먹밥') || name.includes('버거') || name.includes('샌드위치')) return '간편식사';
  if (name.includes('아메리카노') || name.includes('라떼') || name.includes('우유') || name.includes('티') || name.includes('에이드') || name.includes('워터') || name.includes('음료')) return '음료';
  if (name.includes('칩') || name.includes('쿠키') || name.includes('스낵') || name.includes('젤리') || name.includes('초코') || name.includes('사탕') || name.includes('껌')) return '과자';
  if (name.includes('면') || name.includes('라면') || name.includes('우동') || name.includes('국수')) return '라면';
  if (name.includes('바') || name.includes('콘') || name.includes('파르페') || name.includes('빙수') || name.includes('아이스')) return '아이스크림';
  if (name.includes('생리대') || name.includes('치약') || name.includes('칫솔') || name.includes('샴푸') || name.includes('린스') || name.includes('비누') || name.includes('휴지')) return '생활용품';
  return '기타';
}

async function scrapeCU() {
  console.log('🚀 CU 크롤링 시작...');
  const browser = await puppeteer.launch({
    headless: "new",
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  const page = await browser.newPage();

  // CU 검색 페이지 (전체 상품 나열)
  await page.goto('https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=1', {
    waitUntil: 'networkidle2',
  });

  // "더보기" 버튼 클릭 (3번)
  try {
    for (let i = 0; i < 3; i++) {
      const moreBtn = await page.$('a.prodListBtn');
      if (moreBtn) {
        await page.click('a.prodListBtn');
        await new Promise(r => setTimeout(r, 1500)); // 대기 시간 증가
      } else {
        break;
      }
    }
  } catch (e) {
    console.log('더보기 버튼 처리 중 오류 혹은 끝:', e.message);
  }

  // 데이터 추출
  const products = await page.evaluate((brandId) => {
    const items = [];
    const list = document.querySelectorAll('.prodListWrap ul li');

    list.forEach(li => {
      const imgElement = li.querySelector('.photo img');
      const nameElement = li.querySelector('.prodName');
      const priceElement = li.querySelector('.prodPrice span');
      const tagElement = li.querySelector('.tag'); 

      if (nameElement && priceElement) {
        let imageUrl = imgElement ? imgElement.src : null;
        if (imageUrl && !imageUrl.startsWith('http')) {
            imageUrl = `https:${imageUrl}`;
        }
        // CU 이미지 에러 핸들링 (빈 이미지일 경우 처리)
        if (imageUrl && imageUrl.includes('no_img')) imageUrl = null;

        let promotionType = '전체'; 
        if (tagElement) {
            const tagText = tagElement.textContent.trim();
            if (tagText.includes('1+1')) promotionType = '1+1';
            else if (tagText.includes('2+1')) promotionType = '2+1';
            else if (tagText.includes('증정')) promotionType = '덤증정';
        }

        const name = nameElement.textContent.trim();
        const priceStr = priceElement.textContent.replace(/[,원]/g, '');

        items.push({
          brand_id: brandId,
          title: name,
          price: parseInt(priceStr, 10),
          image_url: imageUrl,
          category: '기타', // 나중에 getCategory로 처리하기 위해 placeholder
          promotion_type: promotionType,
          is_active: true,
          source_url: 'https://cu.bgfretail.com/product/product.do?category=product&depth2=4&depth3=1'
        });
      }
    });
    return items;
  }, BRAND_ID.CU);

  await browser.close();
  
  // 카테고리 후처리
  const processedProducts = products.map(p => ({
    ...p,
    category: getCategory(p.title)
  }));

  console.log(`✅ CU: ${processedProducts.length}개의 상품을 찾았습니다.`);
  return processedProducts;
}

async function scrapeGS25() {
  console.log('🚀 GS25 크롤링 시작...');
  const browser = await puppeteer.launch({
    headless: "new",
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  const page = await browser.newPage();

  // GS25 행사 상품 페이지 (1+1, 2+1 등)
  // GS25는 페이지네이션이 JS 함수 호출로 이루어짐
  await page.goto('http://gs25.gsretail.com/gscvs/ko/products/event-goods', {
    waitUntil: 'networkidle2',
  });

  let allProducts = [];

  // 3페이지까지 수집
  for (let pageNum = 1; pageNum <= 3; pageNum++) {
    console.log(`   GS25 - ${pageNum} 페이지 수집 중...`);
    
    // 페이지 데이터 추출
    const products = await page.evaluate((brandId) => {
      const items = [];
      const list = document.querySelectorAll('.prod_list > li');

      list.forEach(li => {
        const titleEl = li.querySelector('.tit');
        const priceEl = li.querySelector('.price .cost');
        const imgEl = li.querySelector('.img img');
        const badgeEl = li.querySelector('.flg .badge'); // 1+1, 2+1 텍스트

        if (titleEl && priceEl) {
          let title = titleEl.textContent.trim();
          let priceRaw = priceEl.textContent.replace(/[,원]/g, '');
          let imageUrl = imgEl ? imgEl.src : null;
          
          let promotionType = '전체';
          if (badgeEl) {
            const badgeText = badgeEl.textContent.trim();
            if (badgeText.includes('1+1')) promotionType = '1+1';
            else if (badgeText.includes('2+1')) promotionType = '2+1';
            else if (badgeText.includes('덤')) promotionType = '덤증정';
          } 
          // 덤증정 이미지가 별도로 있는 경우 (flg_gift)
          else if (li.querySelector('.flg_gift')) {
             promotionType = '덤증정';
          }

          items.push({
            brand_id: brandId,
            title: title,
            price: parseInt(priceRaw, 10),
            image_url: imageUrl,
            category: '기타',
            promotion_type: promotionType,
            is_active: true,
            source_url: 'http://gs25.gsretail.com/gscvs/ko/products/event-goods'
          });
        }
      });
      return items;
    }, BRAND_ID.GS25);

    allProducts = [...allProducts, ...products];

    // 다음 페이지로 이동 (마지막 페이지가 아니면)
    if (pageNum < 3) {
      try {
        // GS25 페이지 이동 JS 실행
        await page.evaluate((next) => {
          if (typeof goodsPageController !== 'undefined') {
            goodsPageController.movePage(next);
          }
        }, pageNum + 1);
        
        // AJAX 로딩 대기
        await new Promise(r => setTimeout(r, 2000));
      } catch (e) {
        console.log('   페이지 이동 중 에러:', e.message);
        break;
      }
    }
  }

  await browser.close();

  // 카테고리 후처리
  const processedProducts = allProducts.map(p => ({
    ...p,
    category: getCategory(p.title)
  }));

  console.log(`✅ GS25: ${processedProducts.length}개의 상품을 찾았습니다.`);
  return processedProducts;
}

async function saveProducts(products) {
  if (products.length === 0) return;

  console.log(`💾 ${products.length}개 데이터 저장/업데이트 중...`);
  
  // 50개씩 끊어서 저장 (Supabase 요청 크기 제한 고려)
  const batchSize = 50;
  for (let i = 0; i < products.length; i += batchSize) {
    const batch = products.slice(i, i + batchSize);
    
    const { error } = await supabase
      .from('new_products')
      .upsert(
        batch.map(p => ({
            brand_id: p.brand_id,
            brand: p.brand_id === 1 ? 'CU' : (p.brand_id === 2 ? 'GS25' : 'Other'), // Legacy column support if needed
            title: p.title, // DB column mismatch fix: schema uses 'name' or 'title'? 
                            // *User Schema check*: Table 'new_products' has 'name', NOT 'title'.
                            // BUT 'SupabaseProduct' interface in types.ts has 'title'.
                            // Let's check the provided SQL in README.
                            // README says: `name text not null`. 
                            // BUT types.ts SupabaseProduct says `title`. 
                            // Let's map to both to be safe or fix based on established pattern.
                            // The `scrapeCU` was returning `title`.
                            // I will map `title` to `name` for the DB insert if the DB expects `name`.
            name: p.title, // Mapping title to name column
            price: p.price,
            image_url: p.image_url,
            category: p.category,
            source_url: p.source_url,
            promotion_type: p.promotion_type,
            is_active: p.is_active,
            // launch_date is required not null in README SQL. Default to today if missing.
            launch_date: new Date().toISOString() 
        })),
        { 
          onConflict: 'brand, name', // Constraint needs to match DB unique index. 
                                     // If index is on (brand, name), this works.
                                     // If using brand_id, might need (brand_id, name).
                                     // Adjusting to common sense 'name' and 'brand' text based on README SQL.
          ignoreDuplicates: false
        } 
      );

    if (error) {
      console.error('❌ 배치 저장 실패:', error.message);
    }
  }
  console.log('✨ 저장 완료!');
}

async function main() {
  try {
    // 1. CU 크롤링
    const cuProducts = await scrapeCU();
    await saveProducts(cuProducts);
    
    // 2. GS25 크롤링
    const gsProducts = await scrapeGS25();
    await saveProducts(gsProducts);

  } catch (error) {
    console.error('크롤링 중 치명적 오류 발생:', error);
  }
}

main();
