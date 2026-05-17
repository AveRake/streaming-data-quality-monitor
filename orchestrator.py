import subprocess
import time
from datetime import datetime

# Интервал запуска (в секундах)
INTERVAL_SECONDS = 15 

def run_dbt_pipeline():
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{current_time}] 🔄 Запуск dbt пайплайна (слои Bronze -> Silver -> Gold)...")
    
    try:
        # Вызываем dbt как системную команду
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", "."],
            capture_output=True, # Перехватываем вывод, чтобы консоль не засорялась
            text=True
        )
        
        finish_time = datetime.now().strftime('%H:%M:%S')
        
        if result.returncode == 0:
            print(f"[{finish_time}] ✅ Пайплайн успешно завершен! Данные на дашборде обновлены.")
        else:
            print(f"[{finish_time}] ❌ ОШИБКА в dbt (логи ниже):")
            # Если ошибка, выводим последние 15 строк лога dbt
            print('\n'.join(result.stdout.split('\n')[-15:]))
            
    except FileNotFoundError:
        print("🚨 Ошибка: команда 'dbt' не найдена. Убедитесь, что вы находитесь в виртуальном окружении.")
    except Exception as e:
        print(f"🚨 Системная ошибка: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Запуск Enterprise DQ Orchestrator (Micro-batch Streaming)")
    print(f"⏳ Интервал синхронизации: каждые {INTERVAL_SECONDS} секунд.")
    print("=" * 60)
    
    try:
        while True:
            run_dbt_pipeline()
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n🛑 Оркестратор остановлен пользователем.")