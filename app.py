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
import requests

app = Flask(__name__)
KST = pytz.timezone("Asia/Seoul")
chromedriver_autoinstaller.install()

# -------------------------------
# 공통 설정
# -------------------------------
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

DB_CONFIG = {
    'host': 'localhost',
    'user': 'solar_user',
    'password': 'solar_pass_2025',
    'db': 'solar_forecast_muan',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}
API_URL = "https://galaxy.kr-weathernews.com/api_v2/weather_v5.cgi?loc=4684033000&language=ko&5828907"

# -------------------------------
# 기상청 API 기반 날씨 수집 및 저장
# -------------------------------
def fetch_weather_preview():
    res = requests.get(API_URL)
    res.raise_for_status()
    data = res.json()[0]
    current_wind_raw = data.get("detailinfo", {}).get("wspd", {}).get("value", "-")
    try:
        current_wind = f"{float(current_wind_raw):.1f}"
    except:
        current_wind = "-"

    results = []
    for entry in data.get("daily", [])[:7]:
        date_key = entry["TimeLocal"].split("T")[0]
        wind_list = [float(h["wspd"]) for h in data.get("hourly", []) if h["TimeLocal"].startswith(date_key)]
        avg_wind = f"{sum(wind_list)/len(wind_list):.1f}" if wind_list else current_wind
        results.append({
            "date": date_key,
            "am_temp": entry["mint"],
            "pm_temp": entry["maxt"],
            "am_rain": entry["pop"],
            "pm_rain": entry["pop"],
            "am_wind": avg_wind,
            "pm_wind": avg_wind,
            "am_sky": entry["day_cmt"],
            "pm_sky": entry["night_cmt"]
        })
    return results

def insert_weather_data():
    try:
        rows = fetch_weather_preview()
        print("📦 저장 시도 대상 (날씨 예보):")
        for row in rows:
            print(row)
    except Exception as e:
        print("❌ 날씨 데이터 fetch 실패:", e)
        return

    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            inserted, updated = 0, 0
            for row in rows:
                try:
                    cursor.execute("""
                        INSERT INTO daily_weather_forecast (
                            forecast_date, location,
                            forecast_temperature_am_c, forecast_temperature_pm_c,
                            forecast_precip_prob_am, forecast_precip_prob_pm,
                            forecast_temperature_min_c, forecast_temperature_max_c, forecast_precip_prob,
                            forecast_sky_am, forecast_sky_pm
                        ) VALUES (
                            %(date)s, '전남 무안군 청계면',
                            %(am_temp)s, %(pm_temp)s,
                            %(am_rain)s, %(pm_rain)s,
                            %(am_temp)s, %(pm_temp)s, %(am_rain)s,
                            %(am_sky)s, %(pm_sky)s
                        )
                        ON DUPLICATE KEY UPDATE
                            forecast_temperature_am_c = VALUES(forecast_temperature_am_c),
                            forecast_temperature_pm_c = VALUES(forecast_temperature_pm_c),
                            forecast_precip_prob_am = VALUES(forecast_precip_prob_am),
                            forecast_precip_prob_pm = VALUES(forecast_precip_prob_pm),
                            forecast_temperature_min_c = VALUES(forecast_temperature_min_c),
                            forecast_temperature_max_c = VALUES(forecast_temperature_max_c),
                            forecast_precip_prob = VALUES(forecast_precip_prob),
                            forecast_sky_am = VALUES(forecast_sky_am),
                            forecast_sky_pm = VALUES(forecast_sky_pm)
                    """, row)
                    if cursor.rowcount == 1:
                        inserted += 1
                    elif cursor.rowcount == 2:
                        updated += 1
                except Exception as e:
                    print("❌ 날씨 INSERT 실패:", e)
                    print("🔍 실패한 행:", row)
        conn.commit()
        print(f"✅ 날씨 저장 완료: {inserted}개 삽입, {updated}개 갱신")
    except Exception as e:
        print("❌ 날씨 DB 저장 중 오류:", e)
    finally:
        conn.close()

# -------------------------------
# 발전 실측 크롤링 및 저장
# -------------------------------
def parse_or_zero(val):
    try:
        return float(val) if val != '-' else 0.0
    except:
        return 0.0

def download_pvsim(now=None):
    if now is None:
        now = datetime.now(KST)

    print("📦 크롤링 시각 기준 now:", now.strftime('%Y-%m-%d %H:%M'))

    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(2)
    driver.get("https://bd.kma.go.kr/kma2020/fs/energySelect2.do?menuCd=F050702000")
    driver.execute_script(f"document.getElementById('testYmd').value = '{now.strftime('%Y%m%d')}';")
    driver.execute_script(f"document.getElementById('testTime').value = '{now.strftime('%H%M')}';")
    driver.find_element(By.ID, "txtLat").send_keys('34.910')
    driver.find_element(By.ID, "txtLon").send_keys('126.435')
    driver.find_element(By.ID, "install_cap").clear()
    driver.find_element(By.ID, "install_cap").send_keys('500')
    driver.find_element(By.ID, "search_btn").send_keys(Keys.RETURN)

    element = driver.find_element(By.ID, 'toEnergy')
    for i in range(20):
        lines = element.text.strip().split('\n')[12:]
        if lines and len(lines[0].strip()) > 10:
            break
        sleep(1)
    else:
        driver.quit()
        raise TimeoutException("데이터 수신 실패: 20초 동안 유효한 데이터 미도달")

    print(f"📊 수신된 데이터 라인 수: {len(lines)}")

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
    driver.quit()

    print("✅ 수집된 시간 범위:")
    print("오늘:", df_today['datetime'].iloc[0], "~", df_today['datetime'].iloc[-1])
    print("내일:", df_tomorrow['datetime'].iloc[0], "~", df_tomorrow['datetime'].iloc[-1])

    return df_today.fillna(0.0).reset_index(drop=True), df_tomorrow.fillna(0.0).reset_index(drop=True)

def save_to_db(df):
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            inserted, updated, skipped = 0, 0, 0
            for _, row in df.iterrows():
                try:
                    # 유효성 검사: 측정값이 모두 0이면 저장하지 않음
                    if all(row[col] in [0, None] for col in ['powergen', 'irradiance', 'temperature']):
                        print("⚠️ 무효 측정값 스킵됨:", row['datetime'])
                        skipped += 1
                        continue

                    data = {
                        'measured_at': datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M'),
                        'power_mw': row['powergen'],
                        'cumulative_mwh': row['cumulative'],
                        'irradiance_wm2': row['irradiance'],
                        'temperature_c': row['temperature'],
                        'wind_speed_ms': row['wind'],
                        'forecast_irradiance_wm2': row['fcst_irradiance'],
                        'forecast_temperature_c': row['fcst_temperature'],
                        'forecast_wind_speed_ms': row['fcst_wind']
                    }

                    sql = """
                        INSERT INTO measurement (
                            measured_at, power_mw, cumulative_mwh,
                            irradiance_wm2, temperature_c, wind_speed_ms,
                            forecast_irradiance_wm2, forecast_temperature_c, forecast_wind_speed_ms
                        ) VALUES (
                            %(measured_at)s, %(power_mw)s, %(cumulative_mwh)s,
                            %(irradiance_wm2)s, %(temperature_c)s, %(wind_speed_ms)s,
                            %(forecast_irradiance_wm2)s, %(forecast_temperature_c)s, %(forecast_wind_speed_ms)s
                        )
                        ON DUPLICATE KEY UPDATE
                            power_mw = VALUES(power_mw),
                            cumulative_mwh = VALUES(cumulative_mwh),
                            irradiance_wm2 = VALUES(irradiance_wm2),
                            temperature_c = VALUES(temperature_c),
                            wind_speed_ms = VALUES(wind_speed_ms),
                            forecast_irradiance_wm2 = VALUES(forecast_irradiance_wm2),
                            forecast_temperature_c = VALUES(forecast_temperature_c),
                            forecast_wind_speed_ms = VALUES(forecast_wind_speed_ms)
                    """
                    affected = cursor.execute(sql, data)
                    if affected == 1:
                        inserted += 1
                    elif affected == 2:
                        updated += 1
                except Exception as e:
                    print("❌ INSERT 실패:", e)
                    print("🔍 문제 발생 데이터:", row.to_dict())
        conn.commit()
        print(f"✅ 저장 완료: {inserted}개 삽입, {updated}개 갱신, {skipped}개 스킵")
    except Exception as e:
        print("❌ DB 저장 중 예외 발생:", e)
    finally:
        conn.close()


# -------------------------------
# Flask 라우팅
# -------------------------------
@app.route("/")
def home():
    return """
    <h2>☀ 통합 모니터링 시스템</h2>
    <p><a href='/weather'><button>🌤 날씨 예보</button></a></p>
    <p><a href='/solar'><button>🔆 태양광 발전량 예보</button></a></p>
    """

@app.route("/insert")
def manual_insert():
    try:
        df_today, _ = download_pvsim()
        save_to_db(df_today)
        return redirect(url_for('solar'))
    except Exception as e:
        return f"<h1>🚨 삽입 실패</h1><p>{e}</p>"

@app.route("/insert-weather")
def insert_weather():
    try:
        insert_weather_data()
        return redirect(url_for('weather'))
    except Exception as e:
        return f"<h1>🌧️ 삽입 실패</h1><p>{e}</p>"

@app.route("/weather")
def weather():
    try:
        rows = fetch_weather_preview()
    except Exception as e:
        return f"<h1>🌧️ 날씨 수집 실패</h1><p>{e}</p>"

    template = """
    <html><head><meta charset='utf-8'><title>날씨 예보</title>
    <style>
        body { font-family: sans-serif; padding: 30px; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ccc; padding: 8px; text-align: center; }
        th { background-color: #f2f2f2; }
        .btn-save { margin-top: 20px; padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; }
        .btn-save:hover { background: #0056b3; }
    </style>
    </head><body>
        <h2>🌤 무안군 청계면 날씨 예보</h2>
        <form action="/insert-weather" method="get">
            <button type="submit" class="btn-save">예보 수동 저장</button>
        </form>
        <table>
            <tr>
                <th>날짜</th><th>기온(오전)</th><th>기온(오후)</th><th>강수확률(오전)</th><th>강수확률(오후)</th>
                <th>풍속(오전)</th><th>풍속(오후)</th><th>날씨(오전)</th><th>날씨(오후)</th>
            </tr>
            {% for row in rows %}
            <tr>
                <td>{{ row.date }}</td><td>{{ row.am_temp }}</td><td>{{ row.pm_temp }}</td><td>{{ row.am_rain }}</td><td>{{ row.pm_rain }}</td>
                <td>{{ row.am_wind }}</td><td>{{ row.pm_wind }}</td><td>{{ row.am_sky }}</td><td>{{ row.pm_sky }}</td>
            </tr>
            {% endfor %}
        </table>
    </body></html>
    """
    return render_template_string(template, rows=rows)

@app.route("/solar")
def solar():
    try:
        df_today, df_tomorrow = download_pvsim()
        df = pd.concat([df_today, df_tomorrow])
    except Exception as e:
        return f"<h1>🚨 발전량 수집 실패</h1><p>{e}</p>"

    template = """
    <!doctype html>
    <html lang="ko">
    <head>
        <meta charset="utf-8">
        <title>무안군 태양광 발전 예보</title>
        <style>
            body { font-family: sans-serif; padding: 30px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ccc; padding: 8px; text-align: center; }
            th { background-color: #f2f2f2; }
            .btn-insert { margin-top: 20px; padding: 10px 20px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer; }
            .btn-insert:hover { background: #1c7c34; }
        </style>
    </head>
    <body>
        <h1>🔆 무안군 태양광 발전 예보</h1>
        <p>예보 시각: {{ now }}</p>
        <form action="/insert" method="get">
            <button type="submit" class="btn-insert">발전량 수동 저장</button>
        </form>
        <table>
            <thead>
                <tr>
                    <th rowspan="2">시간</th>
                    <th colspan="5">오늘</th>
                    <th colspan="3">내일</th>
                </tr>
                <tr>
                    <th>발전량 (MW)</th><th>누적 (MWh)</th><th>일사량</th><th>기온</th><th>풍속</th>
                    <th>예보 일사량</th><th>예보 기온</th><th>예보 풍속</th>
                </tr>
            </thead>
            <tbody>
            {% for row in rows %}
            <tr>
                <td>{{ row.datetime }}</td><td>{{ row.powergen }}</td><td>{{ row.cumulative }}</td>
                <td>{{ row.irradiance }}</td><td>{{ row.temperature }}</td><td>{{ row.wind }}</td>
                <td>{{ row.fcst_irradiance }}</td><td>{{ row.fcst_temperature }}</td><td>{{ row.fcst_wind }}</td>
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </body></html>
    """
    return render_template_string(template, rows=df.to_dict(orient='records'), now=datetime.now(KST).strftime("%Y-%m-%d %H:%M"))

# -------------------------------
# 스케줄러 실행
# -------------------------------
scheduler = BackgroundScheduler()
scheduler.add_job(insert_weather_data, 'cron', hour=7, minute=0)
scheduler.add_job(lambda: save_to_db(download_pvsim()[0]), 'cron', hour=7, minute=5)
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
