from flask import Flask, render_template_string, redirect, url_for
from apscheduler.schedulers.background import BackgroundScheduler
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
import pymysql
import pytz

# Flask 앱 및 시간대 설정
app = Flask(__name__)
KST = pytz.timezone("Asia/Seoul")
chromedriver_autoinstaller.install()

# Chrome 드라이버 옵션 설정
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

def parse_or_zero(val):
    try:
        return float(val) if val != '-' else 0.0
    except:
        return 0.0

# 기상청 페이지에서 데이터를 크롤링하여 DataFrame으로 반환
def download_pvsim(now=None):
    if now is None:
        now = datetime.now(KST)

    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(2)
    driver.get("https://bd.kma.go.kr/kma2020/fs/energySelect2.do?menuCd=F050702000")

    driver.execute_script(f"document.getElementById('testYmd').value = '{now.strftime('%Y%m%d')}';")
    driver.execute_script(f"document.getElementById('testTime').value = '{now.strftime('%H%M')}';")

    driver.find_element(By.ID, "txtLat").send_keys('34.910')
    driver.find_element(By.ID, "txtLon").send_keys('126.435')
    driver.find_element(By.ID, "install_cap").clear()
    driver.find_element(By.ID, "install_cap").send_keys('500')  # 500MW 발전소 기준
    driver.find_element(By.ID, "search_btn").send_keys(Keys.RETURN)

    element = driver.find_element(By.ID, 'toEnergy')
    for _ in range(20):
        lines = element.text.strip().split('\n')[12:]
        if lines and len(lines[0].strip()) > 10:
            break
        sleep(1)
    else:
        driver.quit()
        raise TimeoutException("데이터 수신 실패")

    lines = element.text.strip().split('\n')
    today_data, tomorrow_data = [], []
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)

    for line in lines:
        parts = line.split()
        if len(parts) < 11:
            continue
        hour = parts[0][:-1].zfill(2)
        today_time = today + timedelta(hours=int(hour))
        tomorrow_time = tomorrow + timedelta(hours=int(hour))

        today_data.append([
            today_time.strftime("%Y-%m-%d %H:%M"),
            parse_or_zero(parts[1]),
            parse_or_zero(parts[2]),
            parse_or_zero(parts[3]),
            parse_or_zero(parts[4]),
            parse_or_zero(parts[5]),
            0.0, 0.0, 0.0
        ])

        tomorrow_data.append([
            tomorrow_time.strftime("%Y-%m-%d %H:%M"),
            0.0, 0.0, 0.0, 0.0, 0.0,
            parse_or_zero(parts[8]),
            parse_or_zero(parts[9]),
            parse_or_zero(parts[10])
        ])

    columns = [
        "datetime", "powergen", "cumulative", "irradiance", "temperature", "wind",
        "fcst_irradiance", "fcst_temperature", "fcst_wind"
    ]
    df_today = pd.DataFrame(today_data, columns=columns)
    df_tomorrow = pd.DataFrame(tomorrow_data, columns=columns)

    df_today.fillna(0.0, inplace=True)
    df_tomorrow.fillna(0.0, inplace=True)
    driver.quit()
    return df_today.reset_index(drop=True), df_tomorrow.reset_index(drop=True)

# 수집한 데이터를 MySQL에 저장
def save_to_db(df):
    conn = pymysql.connect(
        host='localhost',
        user='solar_user',
        password='solar_pass_2025',
        db='solar_forecast_muan',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                sql = """
                    INSERT INTO measurement (
                        measured_at, power_kw, cumulative_kwh,
                        irradiance_wm2, temperature_c, wind_speed_ms,
                        forecast_irradiance_wm2, forecast_temperature_c, forecast_wind_speed_ms
                    ) VALUES (
                        %(measured_at)s, %(power_kw)s, %(cumulative_kwh)s,
                        %(irradiance_wm2)s, %(temperature_c)s, %(wind_speed_ms)s,
                        %(forecast_irradiance_wm2)s, %(forecast_temperature_c)s, %(forecast_wind_speed_ms)s
                    )
                """
                data = {
                    'measured_at': datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M'),
                    'power_kw': row['powergen'],
                    'cumulative_kwh': row['cumulative'],
                    'irradiance_wm2': row['irradiance'],
                    'temperature_c': row['temperature'],
                    'wind_speed_ms': row['wind'],
                    'forecast_irradiance_wm2': row['fcst_irradiance'],
                    'forecast_temperature_c': row['fcst_temperature'],
                    'forecast_wind_speed_ms': row['fcst_wind']
                }
                cursor.execute(sql, data)
        conn.commit()
    finally:
        conn.close()

# 매일 7시에 실행될 자동 수집 작업
def scheduled_task():
    print(f"[{datetime.now(KST)}] 자동 수집 시작")
    try:
        df_today, _ = download_pvsim()
        save_to_db(df_today)
        print("✅ 저장 완료")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

# 스케줄러 시작 (매일 오전 7시)
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, 'cron', hour=7, minute=0)
scheduler.start()

# 수동 삽입용 라우트 추가
@app.route("/insert")
def manual_insert():
    try:
        df_today, _ = download_pvsim()
        save_to_db(df_today)
        return redirect(url_for('home'))
    except Exception as e:
        return f"<h1>🚨 삽입 실패</h1><p>{e}</p>"

# 웹 페이지 라우트: 실시간 데이터 크롤링 및 시각화
@app.route("/")
def home():
    try:
        df_today, df_tomorrow = download_pvsim()
        df = pd.concat([df_today, df_tomorrow])
    except Exception as e:
        return f"<h1>🚨 데이터 수집 실패</h1><p>{e}</p>"

    template =     template = """
    <!doctype html>
    <html lang="ko">
    <head>
        <meta charset="utf-8">
        <title>무안군 태양광 예보</title>
        <style>
            body { font-family: sans-serif; padding: 30px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ccc; padding: 8px; text-align: center; }
            th { background-color: #f2f2f2; }
            .btn-insert { margin-top: 20px; padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; }
            .btn-insert:hover { background: #0056b3; }
        </style>
    </head>
    <body>
        <h1>☀ 무안군 태양광 발전 예보</h1>
        <p>크롤링 시각: {{ now }}</p>
        <form action="/insert" method="get">
            <button type="submit" class="btn-insert">데이터 수동 삽입</button>
        </form>
        <table>
            <thead>
                <tr>
                    <th rowspan="2">시간</th>
                    <th colspan="5">오늘</th>
                    <th colspan="3">내일</th>
                </tr>
                <tr>
                    <th>발전량 (MW)</th>
                    <th>누적발전량 (MWh)</th>
                    <th>일사량 (W/m²)</th>
                    <th>기온 (℃)</th>
                    <th>풍속 (m/s)</th>
                    <th>일사량 (W/m²)</th>
                    <th>기온 (℃)</th>
                    <th>풍속 (m/s)</th>
                </tr>
            </thead>
            <tbody>
            {% for row in rows %}
            <tr>
                <td>{{ row.datetime }}</td>
                <td>{{ row.powergen }}</td>
                <td>{{ row.cumulative }}</td>
                <td>{{ row.irradiance }}</td>
                <td>{{ row.temperature }}</td>
                <td>{{ row.wind }}</td>
                <td>{{ row.fcst_irradiance }}</td>
                <td>{{ row.fcst_temperature }}</td>
                <td>{{ row.fcst_wind }}</td>
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </body>
    </html>
    """

    return render_template_string(template, rows=df.to_dict(orient='records'), now=datetime.now(KST).strftime("%Y-%m-%d %H:%M"))

# 서버 실행
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
