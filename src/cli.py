import argparse
from datetime import datetime, date, timedelta

def parse_dates(from_date, to_date):
    """Парсинг и валидация дат"""
    
    # Определение дат
    if not from_date:
        yesterday = date.today() - timedelta(days=1)
        from_date = yesterday.strftime("%Y-%m-%d")
    
    if not to_date:
        to_date = from_date
    
    try:
        start_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(to_date, "%Y-%m-%d").date()
    except ValueError:
        print("❌ Неверный формат даты. Используйте YYYY-MM-DD")
        exit(1)
    
    if start_date > end_date:
        print("❌ Дата начала не может быть позже даты окончания")
        exit(1)
    
    return from_date, to_date

def main():
    parser = argparse.ArgumentParser(description="Парсер контактов из корпоративной почты")
    
    parser.add_argument(
        '--from-date', 
        help='Дата начала в формате YYYY-MM-DD (по умолчанию: вчера)'
    )
    parser.add_argument(
        '--to-date', 
        help='Дата окончания в формате YYYY-MM-DD (по умолчанию: равна from-date)'
    )
    parser.add_argument(
        '--debug', 
        action='store_true', 
        help='Включить подробное логирование'
    )
    
    args = parser.parse_args()
    
    from_date, to_date = parse_dates(args.from_date, args.to_date)
    
    print(f"🚀 Парсинг писем с {from_date} по {to_date}")
    print(f"📊 Режим отладки: {'включен' if args.debug else 'выключен'}")
    print("✅ CLI работает корректно!")

if __name__ == "__main__":
    main()
