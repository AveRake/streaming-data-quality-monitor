import json
import psycopg2
from kafka import KafkaConsumer

DB_PARAMS = {
    "dbname": "vkr_db", "user": "vkr_user",
    "password": "vkr_password", "host": "localhost", "port": "5432"
}

def init_db():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    # Очищаем старые данные, так как структура изменилась
    cur.execute("DROP TABLE IF EXISTS landing_transactions CASCADE;")
    
    # Создаем таблицу заново
    cur.execute("""
        CREATE TABLE landing_transactions (
            raw_json JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    return conn, cur

if __name__ == "__main__":
    conn, cur = init_db()
    consumer = KafkaConsumer(
        'raw_transactions',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest' # Читаем только новые сообщения
    )

    print("Слушаем Kafka и пишем в PostgreSQL...")
    for message in consumer:
        data = message.value
        cur.execute("INSERT INTO landing_transactions (raw_json) VALUES (%s)", (json.dumps(data),))
        conn.commit()
        
        # Достаем ID из вложенного JSON для принта
        tx_id = data['metadata']['tx_id']
        print(f"Записано в БД: {tx_id[:8]}...")