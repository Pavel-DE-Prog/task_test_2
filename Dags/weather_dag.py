import os
import logging
from datetime import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from tenacity import retry, stop_after_attempt, wait_exponential

API_KEY = ''  # Ваш API ключ для OpenWeatherMap, но лучше использовать переменную окружения

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))

def _fetch_and_process_weather_data(ti):
    """Функция для получения данных о погоде и преобразования их в DataFrame."""
    logging.info("Получение данных о погоде...")
    try:
        response = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q=Minsk&appid={API_KEY}&units=metric')
        response.raise_for_status()  # Проверка на статус ответа

        data = response.json()
        datetime_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Извлечение данных для температуры
        temp_data = {
            "datetime": datetime_now,
            "temp": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "temp_min": data["main"]["temp_min"],
            "temp_max": data["main"]["temp_max"],
            "pressure": data["main"]["pressure"],
        }

        # Извлечение данных о ветре
        wind_data = {
            "datetime": datetime_now,
            "speed": data["wind"]["speed"],
            "deg": data["wind"]["deg"],
            "gust": data["wind"].get("gust", None),
        }

        # Сохраняем данные в XCom
        ti.xcom_push(key='temp_data', value=temp_data)
        ti.xcom_push(key='wind_data', value=wind_data)

        logging.info("Weather data fetched and processed successfully.")

    except Exception as e:
        logging.error(f"Error in fetching and processing data: {e}")
        raise


def _save_data_to_parquet(ti):
    """Функция для сохранения данных о температуре и ветре в формате Parquet."""
    try:
        temp_data = ti.xcom_pull(task_ids="fetch_weather_data", key='temp_data')
        wind_data = ti.xcom_pull(task_ids="fetch_weather_data", key='wind_data')

        if temp_data is None or wind_data is None:
            raise ValueError("No data found in XCom")

        # Создание DataFrame из полученных данных
        temp_df = pd.DataFrame([temp_data])
        wind_df = pd.DataFrame([wind_data])

        # Определяем имя файлов для сохранения в указанной директории
        save_directory = r'C:\Users\Lenovo\The_Weather_in_Minsk\Temp'
        create_directory(save_directory)  # Создание директории, если нужно

        date_str = datetime.now().strftime('%Y_%m_%d')
        temp_filename = os.path.join(save_directory, f'minsk_{date_str}_temp.parquet')
        wind_filename = os.path.join(save_directory, f'minsk_{date_str}_wind.parquet')

        # Сохранение в целевую директорию
        temp_df.to_parquet(temp_filename, index=False)
        wind_df.to_parquet(wind_filename, index=False)

        logging.info(f"Файл с температурой сохранен: {temp_filename}")
        logging.info(f"Файл с ветром сохранен: {wind_filename}")

        # Проверка наличия файлов
        if os.path.exists(temp_filename) and os.path.exists(wind_filename):
            logging.info("Файлы Parquet созданы успешно.")
        else:
            logging.error("Не удалось создать файлы Parquet.")

    except Exception as e:
        logging.error(f"Error in saving data to parquet: {e}")
        raise

# Конфигурация DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 14),
    'retries': 1,
}

# Определяем DAG
with DAG(
        "weather_data_pipeline_dag",
        schedule="@hourly",
        default_args=default_args,
        catchup=False
) as dag:
    fetch_weather_data = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=_fetch_and_process_weather_data
    )

    save_weather_data = PythonOperator(
        task_id="save_weather_data",
        python_callable=_save_data_to_parquet
    )

    # Зависимости задач
    fetch_weather_data >> save_weather_data

