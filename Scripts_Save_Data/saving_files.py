import pandas as pd

# Получить полный путь к файлам
temp_data = {"datetime": "2025-04-02 18:00:00", "temp": 10}
wind_data = {"datetime": "2025-04-02 18:00:00", "speed": 5}

# Пробую сохранить файлы вручную
try:
    temp_df = pd.DataFrame([temp_data])
    wind_df = pd.DataFrame([wind_data])

    temp_df.to_parquet("C:\\Users\\Lenovo\\The_Weather_in_Minsk\\Temp\\test_temp.parquet", index=False)
    wind_df.to_parquet("C:\\Users\\Lenovo\\The_Weather_in_Minsk\\Temp\\test_wind.parquet", index=False)
    print("Файлы успешно сохранены!")
except Exception as e:
    print(f"Ошибка: {e}")