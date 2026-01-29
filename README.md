## 기능

- 매일 새벽 3시 자동 실행 (GitHub Actions)
- 중복 제품 자동 필터링
- Supabase 데이터베이스 연동

## 환경 변수 설정

GitHub Repository → Settings → Secrets and variables → Actions

다음 Secrets 추가:
- `SUPABASE_URL`: Supabase 프로젝트 URL
- `SUPABASE_SERVICE_KEY`: Supabase Service Role Key

## 수동 실행 방법

1. GitHub Repository → Actions 탭
2. "Daily Convenience Crawler" 선택
3. "Run workflow" 버튼 클릭

