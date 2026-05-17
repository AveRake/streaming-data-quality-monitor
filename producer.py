import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'raw_transactions'
CURRENCIES = ['RUB', 'USD', 'EUR', 'KZT']

def generate_transaction():
    is_anomaly = random.random() < 0.3  # Шанс ошибки 30%
    
    # Нормальные данные
    amount = round(random.uniform(100.0, 5000.0), 2)
    currency = random.choice(CURRENCIES)
    ip_addr = f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
    
    # Внесение различных аномалий
    if is_anomaly:
        error_type = random.choice(['negative_amount', 'huge_amount', 'bad_currency', 'no_ip'])
        if error_type == 'negative_amount':
            amount = -150.0
        elif error_type == 'huge_amount':
            amount = 9999999.99 # Подозрительная транзакция (Фрод)
        elif error_type == 'bad_currency':
            currency = 'UNKNOWN_COIN'
        elif error_type == 'no_ip':
            ip_addr = None

    # Сложный вложенный JSON
    tx = {
        "metadata": {
            "tx_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "source_system": "mobile_app_v2.4"
        },
        "payload": {
            "user_id": random.randint(1000, 9999),
            "financial": {
                "amount": amount,
                "currency": currency
            },
            "security": {
                "ip_address": ip_addr,
                "device_type": random.choice(["iOS", "Android", "Web"])
            }
        }
    }
    return tx

if __name__ == "__main__":
    print("Запуск продвинутого генератора транзакций...")
    try:
        while True:
            tx_data = generate_transaction()
            producer.send(TOPIC_NAME, tx_data)
            print(f"Отправлено TX: {tx_data['metadata']['tx_id'][:8]}... | Сумма: {tx_data['payload']['financial']['amount']}")
            time.sleep(1.5)
    except KeyboardInterrupt:
        print("Остановка генератора.")
        producer.close()