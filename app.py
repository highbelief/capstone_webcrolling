from flask import Flask, render_template_string
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

app = Flask(__name__)
KST = pytz.timezone("Asia/Seoul")
chromedriver_autoinstaller.install()

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

def download_pvsim(now=None):
    if now is None:
        now = datetime.now(KST)

    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(2)
    driver.get("https://bd.kma.go.kr/kma2020/fs/energySelect2.do?menuCd=F050702000")

    driver.execute_script(f"document.getElementById('testYmd').value = '{now.strftime('%Y%m%d')}';")
    driver.execute_script(f"document.getElementById('testTime').value = '{now.strftime('%H%M')}';")

    driver.find_element(By.ID, "txtLat").send_keys('35.0606')
    driver.find_element(By.ID, "txtLon").send_keys('126.749')
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

        # 오늘 데이터: parts[1:6], 내일 예보: parts[6:9]
        if parts[1] != '-' and parts[6] != '-':
            today_data.append([today_time.strftime("%Y-%m-%d %H:%M")] + parts[1:6] + parts[6:9])
        elif parts[6] != '-':
            tomorrow_data.append([tomorrow_time.strftime("%Y-%m-%d %H:%M")] + ['-'] * 5 + parts[6:9])

    columns = ["datetime", "powergen", "cumulative", "irradiance", "temperature", "wind",
               "fcst_irradiance", "fcst_temperature", "fcst_wind"]
    df = pd.concat([
        pd.DataFrame(today_data, columns=columns),
        pd.DataFrame(tomorrow_data, columns=columns)
    ])

    for col in columns[1:]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    driver.quit()
    return df.reset_index(drop=True)

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

def scheduled_task():
    print(f"[{datetime.now(KST)}] 자동 수집 시작")
    try:
        df = download_pvsim()
        save_to_db(df)
        print("✅ 저장 완료")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, 'cron', hour=7, minute=0)
scheduler.start()

@app.route("/")
def home():
    try:
        df = download_pvsim()
    except Exception as e:
        return f"<h1>🚨 데이터 수집 실패</h1><p>{e}</p>"

    template = """
    <!doctype html>
    <html lang=\"ko\">
    <head>
        <meta charset=\"utf-8\">
        <title>무안군 태양광 예보</title>
        <style>
            body { font-family: sans-serif; padding: 30px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ccc; padding: 8px; text-align: center; }
            th { background-color: #f2f2f2; }
        </style>
    </head>
    <body>
        <h1>☀ 무안군 태양광 발전 예보</h1>
        <p>크롤링 시각: {{ now }}</p>
        <table>
            <tr>
                <th>시간</th>
                <th>발전량 (kW)</th>
                <th>누적 발전량 (kWh)</th>
                <th>일사량 (W/m²)</th>
                <th>기온 (℃)</th>
                <th>풍속 (m/s)</th>
                <th>예보 일사량</th>
                <th>예보 기온</th>
                <th>예보 풍속</th>
            </tr>
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
        </table>
    </body>
    </html>
    """
    return render_template_string(template, rows=df.to_dict(orient='records'), now=datetime.now(KST).strftime("%Y-%m-%d %H:%M"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
