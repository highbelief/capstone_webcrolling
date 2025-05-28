# 🌞 Solar Forecast App for Muan

이 프로젝트는 기상청 웹사이트에서 무안군 지역의 태양광 발전 관련 기상 데이터를 크롤링하여, 웹 페이지에 시각화하고 로컬 데이터베이스에 저장하는 엣지 환경용 자동화 시스템입니다.

## 🔧 주요 기능

* Selenium을 통한 무안군 태양광 발전 예측 정보 크롤링
* 매일 오전 7시에 자동 수집 및 저장 (APScheduler)
* Flask 웹 서버에서 실시간 정보 출력
* MySQL 로컬 데이터베이스에 자동 저장

## 📁 프로젝트 구조

```
solar-forecast-app/
├── app.py              # 메인 Flask 서버 및 스케줄러
├── requirements.txt    # 의존성 목록
├── README.md           # 설명서
```

## ⚙️ 시스템 요구사항

* Python 3.8 이상
* Google Chrome 브라우저 설치 필요 (Selenium용)
* MySQL 서버 (로컬)

## 📦 설치 방법

```bash
# 1. 패키지 설치
pip install -r requirements.txt

# 2. chromedriver 자동 설치
# 첫 실행 시 자동 설치됨 (chromedriver-autoinstaller 사용)
```

## 🛠 MySQL 설정

```sql
-- 데이터베이스 생성
CREATE DATABASE IF NOT EXISTS solar_forecast_muan
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

-- 사용자 생성 및 권한 부여
CREATE USER IF NOT EXISTS 'solar_user'@'localhost' IDENTIFIED BY 'solar_pass_2025';
GRANT ALL PRIVILEGES ON solar_forecast_muan.* TO 'solar_user'@'localhost';
FLUSH PRIVILEGES;
```

## 🗃 테이블 구조

`measurement` 테이블이 실측 및 예보 데이터를 저장합니다.

```sql
CREATE TABLE IF NOT EXISTS measurement (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    measured_at DATETIME NOT NULL,
    power_kw FLOAT,
    cumulative_kwh FLOAT,
    irradiance_wm2 FLOAT,
    temperature_c FLOAT,
    wind_speed_ms FLOAT,
    forecast_irradiance_wm2 FLOAT,
    forecast_temperature_c FLOAT,
    forecast_wind_speed_ms FLOAT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

## 🚀 실행 방법

```bash
python app.py
```

실행 후 `http://localhost:5000` 에 접속하면 최신 태양광 예보 데이터를 확인할 수 있습니다.

## 📅 자동 저장 스케줄

* 매일 오전 7시 (KST) 자동 크롤링 및 DB 저장
* `APScheduler` 라이브러리를 사용한 백그라운드 작업

## 📝 라이선스

본 프로젝트는 사내 연구 및 비영리 목적의 개발용으로 사용됩니다.

## 🙋 문의

* 담당자: 국립목포대학교 공과대학 융합소프트웨어학과 스포트라이트팀 팀장
* 수정 요청 및 이슈는 GitHub Issue로 등록해주세요.
