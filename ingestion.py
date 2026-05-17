import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest

DB_PARAMS = {
    "dbname": "vkr_db", "user": "vkr_user",
    "password": "vkr_password", "host": "localhost", "port": "5432"
}

def init_db():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    # Оставляем таблицу как есть, так как JSONB стерпит любые новые поля
    cur.execute("""
        CREATE TABLE IF NOT EXISTS landing_transactions (
            raw_json JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    return conn, cur

def train_ml_model():
    print("Обучение ML-модели (Isolation Forest) на исторических профилях...")
    # Генерируем "нормальное" поведение пользователей для обучения
    # (Обычно транзакции 100-5000 руб, днем с 8 до 22 часов)
    np.random.seed(42)
    normal_data = pd.DataFrame({
        'amount': np.random.uniform(100, 5000, 1000),
        'hour': np.random.randint(8, 22, 1000),
        'device_code': np.random.choice([0, 1, 2], 1000) # 0: iOS, 1: Android, 2: Web
    })
    
    # contamination=0.05 означает, что мы ожидаем около 5% аномалий
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(normal_data)
    print("Модель успешно обучена и готова к стримингу!")
    return model

def extract_features(tx_data):
    # Превращаем JSON в признаки для модели
    amount = float(tx_data['payload']['financial']['amount'])
    
    timestamp_str = tx_data['metadata']['timestamp']
    hour = datetime.fromisoformat(timestamp_str).hour
    
    device = tx_data['payload']['security']['device_type']
    device_code = 0 if device == 'iOS' else (1 if device == 'Android' else 2)
    
    return pd.DataFrame([[amount, hour, device_code]], columns=['amount', 'hour', 'device_code'])

if __name__ == "__main__":
    conn, cur = init_db()
    
    # 1. Инициализируем ML
    anomaly_model = train_ml_model()
    
    # 2. Подключаемся к Kafka
    consumer = KafkaConsumer(
        'raw_transactions',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

    print("Слушаем Kafka, скорим данные через ML и пишем в PostgreSQL...")
    for message in consumer:
        data = message.value
        
        # --- БЛОК MACHINE LEARNING ---
        try:
            features = extract_features(data)
            # predict возвращает 1 (норма) или -1 (аномалия)
            prediction = anomaly_model.predict(features)[0] 
            
            # decision_function возвращает сырой score. Чем меньше (уходит в минус), тем аномальнее
            raw_score = anomaly_model.decision_function(features)[0]
            
            # Нормализуем score от 0 до 100 (где 100 - это 100% аномалия)
            ml_risk_score = round(max(0, min(100, (0 - raw_score) * 200)), 2)
            
            # ОБОГАЩАЕМ JSON на лету!
            data['metadata']['ml_risk_score'] = ml_risk_score
            data['metadata']['is_ml_anomaly'] = int(prediction == -1)
        except Exception as e:
            # Если данные настолько кривые, что ML упал (например, нет поля amount), 
            # ставим дефолтные значения. dbt потом забракует их регулярными правилами.
            data['metadata']['ml_risk_score'] = 0.0
            data['metadata']['is_ml_anomaly'] = 0
            ml_risk_score = 0.0
            prediction = 1
        # -----------------------------

        cur.execute("INSERT INTO landing_transactions (raw_json) VALUES (%s)", (json.dumps(data),))
        conn.commit()
        
        tx_id = data['metadata']['tx_id']
        flag = "🔴 ML АНОМАЛИЯ!" if prediction == -1 else "🟢 Норма"
        amount_print = data.get('payload', {}).get('financial', {}).get('amount', 'N/A')
        print(f"[{flag}] Записано: {tx_id[:8]} | Сумма: {amount_print} | ML Score: {ml_risk_score}")