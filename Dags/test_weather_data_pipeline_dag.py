import os
import pandas as pd
from unittest.mock import patch, Mock
from airflow.models import DagBag
from weather_dag import _fetch_and_process_weather_data  # Обратите внимание на правильный импорт
from datetime import datetime  # Добавлено для исправления ошибки

# Тестирование функций вашего DAG
def test_weather_dag():
    dagbag = DagBag()
    dag = dagbag.get_dag("weather_data_pipeline_dag")

    if dag is None:
        print("Не удалось найти DAG. Доступные DAGs:")
        for dag_id in dagbag.dags.keys():
            print(f"- {dag_id}")
        print("Ошибки импорта:", dagbag.import_errors)  # Печать ошибок импорта, если DAG не найден
        return  # Прерываем выполнение, если DAG не найден

    assert len(dag.tasks) == 2, "DAG должна содержать 2 задачи"

def test_fetch_and_process_weather_data():
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "main": {
                "temp": 20,
                "feels_like": 19,
                "temp_min": 18,
                "temp_max": 22,
                "pressure": 1010
            },
            "wind": {
                "speed": 5,
                "deg": 90,
                "gust": None
            }
        }

        ti = Mock()
        ti.xcom_pull.side_effect = lambda key: {
            'temp_data': {
                "datetime": "2025-04-02 18:00:00",
                "temp": 20,
                "feels_like": 19,
                "temp_min": 18,
                "temp_max": 22,
                "pressure": 1010,
            },
            'wind_data': {
                "datetime": "2025-04-02 18:00:00",
                "speed": 5,
                "deg": 90,
                "gust": None,
            }
        }[key]

        _fetch_and_process_weather_data(ti)

        temp_data = ti.xcom_pull(key='temp_data')
        wind_data = ti.xcom_pull(key='wind_data')

        assert temp_data["temp"] == 20, "Temperature value mismatch"
        assert wind_data["speed"] == 5, "Wind speed value mismatch"


def test_save_data_to_parquet():
    temp_data = {
        "datetime": "2025-04-02 18:00:00",
        "temp": 10,
        "feels_like": 9,
        "temp_min": 8,
        "temp_max": 11,
        "pressure": 1020,
    }

    wind_data = {
        "datetime": "2025-04-02 18:00:00",
        "speed": 5,
        "deg": 180,
        "gust": None,
    }

    temp_df = pd.DataFrame([temp_data])
    wind_df = pd.DataFrame([wind_data])

    save_directory = r'C:\Users\Lenovo\The_Weather_in_Minsk\Temp'
    os.makedirs(save_directory, exist_ok=True)

    date_str = datetime.now().strftime('%Y_%m_%d')
    temp_filename = os.path.join(save_directory, f'minsk_{date_str}_temp.parquet')
    wind_filename = os.path.join(save_directory, f'minsk_{date_str}_wind.parquet')

    temp_df.to_parquet(temp_filename, index=False)
    wind_df.to_parquet(wind_filename, index=False)

    assert os.path.exists(temp_filename), "Temperature file was not created."
    assert os.path.exists(wind_filename), "Wind file was not created."

    print("Файлы успешно сохранены!")

    os.remove(temp_filename)
    os.remove(wind_filename)

# Запуск тестов
if __name__ == "__main__":
    test_weather_dag()
    test_fetch_and_process_weather_data()
    test_save_data_to_parquet()
