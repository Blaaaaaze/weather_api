"""
Readme файл не прикреплялся, текст из него написан в комментариях под кодом
"""

import asyncio
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
import aiohttp
import sqlite3
from datetime import datetime, timedelta

DATABASE = "weather.db"
UPDATE_INTERVAL = 15 * 60

app = FastAPI()


def init_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            user_id INTEGER NOT NULL,
            forecast TEXT,
            last_updated TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """)
    conn.commit()
    conn.close()

init_db()

class RegisterUserRequest(BaseModel):
    name: str

class AddCityRequest(BaseModel):
    name: str
    latitude: float
    longitude: float

class WeatherQuery(BaseModel):
    city: str
    time: Optional[str] = None
    parameters: Optional[List[str]] = None

async def fetch_weather(latitude: float, longitude: float):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m,surface_pressure,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,precipitation_probability,precipitation,pressure_msl,surface_pressure,visibility,wind_speed_10m,wind_direction_10m,wind_gusts_10m"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:

            if response.status == 200:
                data = await response.json()
                return data
            else:


                raise HTTPException(status_code=response.status, detail="Error fetching weather data")


@app.post("/register")
async def register_user(user: RegisterUserRequest):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users (name) VALUES (?)", (user.name,))
        conn.commit()
        return {"id": cursor.lastrowid}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="User already exists")
    finally:
        conn.close()

@app.post("/add-city")
async def add_city(city: AddCityRequest, user_id: int):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO cities (name, latitude, longitude, user_id) VALUES (?, ?, ?, ?)",
                       (city.name, city.latitude, city.longitude, user_id))
        conn.commit()
        return {"message": "City added successfully"}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="City already exists for this user")
    finally:
        conn.close()

@app.get("/cities")
async def list_cities(user_id: int):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT name, latitude, longitude FROM cities WHERE user_id = ?", (user_id,))
    cities = cursor.fetchall()
    conn.close()
    return {"cities": [{"name": name, "latitude": lat, "longitude": lon} for name, lat, lon in cities]}

@app.get("/weather")
async def get_weather(latitude: float, longitude: float):
    data = await fetch_weather(latitude, longitude)
    current = data.get("current")
    if not current:
        raise HTTPException(status_code=404, detail="Weather data not available")
    return {
        "temperature": current.get("temperature_2m"),
        "wind_speed": current.get("wind_speed_10m"),
        "pressure": current.get("surface_pressure")
    }

@app.post("/weather-forecast")
async def weather_forecast(query: WeatherQuery, user_id: int):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT latitude, longitude, forecast, last_updated FROM cities WHERE name = ? AND user_id = ?",
                   (query.city, user_id))
    city = cursor.fetchone()
    if not city:
        raise HTTPException(status_code=404, detail="City not found")

    latitude, longitude, forecast, last_updated = city
    now = datetime.utcnow()

    if not forecast or not last_updated or datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S") < now - timedelta(minutes=15):
        data = await fetch_weather(latitude, longitude)
        forecast = data.get("hourly")
        cursor.execute("UPDATE cities SET forecast = ?, last_updated = ? WHERE name = ? AND user_id = ?",
                       (str(forecast), now.strftime("%Y-%m-%d %H:%M:%S"), query.city, user_id))
        conn.commit()

    conn.close()

    if query.time:
        time_index = (datetime.strptime(query.time, "%H:%M").hour)
        hourly_forecast = eval(forecast)
        hourly_data = {key: values[time_index] for key, values in hourly_forecast.items()}
        return {param: hourly_data.get(param) for param in (query.parameters or hourly_data.keys())}

    return forecast

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_forecasts())

async def update_forecasts():
    while True:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute("SELECT id, latitude, longitude FROM cities")
        cities = cursor.fetchall()
        for city_id, latitude, longitude in cities:
            data = await fetch_weather(latitude, longitude)
            forecast = data.get("hourly")
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Updating city {city_id} with new forecast at {now}")
            cursor.execute("UPDATE cities SET forecast = ?, last_updated = ? WHERE id = ?",
                           (str(forecast), now, city_id))
        conn.commit()
        conn.close()
        await asyncio.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)


"""
Описание методов тестового задания

Основные методы: Эти методы доступны по запросу

register_user - метод для доп. задания 1. принимает имя пользователя 
в виде строки (реализован отдельный класс под пользователя), возвращает id пользователя типа int

add_city - принимает в качестве параметров название города и его коордианты, а также id пользователя. 
Эта информация добавляется в базу данных, куда впоследствии добавляется прогноз погоды 
для каждого добавленного города

list_cities - принимает id пользователя, возвращает список доступных (добавленных раннее) городов,
для которых хранится прогноз погоды

get_weather - принимает координаты (долгота и широта), возвращает температуру, давление 
и скорость ветра территории по координатам

weather_forecast - принимает 4 параметра: обязательные - название города и id-пользователя; 
опциональные - время и выборочные опции (возможность выбора параметров погоды). Если время не указано, 
то возвращается результат на нынешнее время, если не указаны опции погоды, то метод отправляет все доступные опции


Вспомогательные методы: Эти методы не принимают запросов, а помогают осуществлять работу основных методов

init_db - инициализаия базы данных. Реализация БД при помощи SQLite3. Содержит 2 таблицы: users, cities

fetch_weather - работает с api open meteo. Получает координаты местности и посылает get запросы

startup_event - запускает задачу по обновлению прогноза погоды для городов из базы данных при запуске сервера
update_forecasts - обновляет данные прогноза погоды каждые 15 минут (в случае их изменения) и 
обновляет поля таблицы cities: forecast и last_updated

Есть 3 специальных класса, которые позволяют удобнее хранить информацию:

RegisterUserRequest - поле, содержащее имя пользователя
AddCityRequest - поля, содержащие название города, долготу и широту
WeatherQuery - поля, содержащие название города, время (необязательно), 
различные варианты погоды (не обязательно)
"""