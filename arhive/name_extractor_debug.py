#!/usr/bin/env python3
# -*- coding: utf-8 -*-

print("🚀 Запуск отладочной версии...")

try:
    print("📦 Импортируем библиотеки...")
    import re
    print("✅ re импортирован")
    
    import imaplib
    print("✅ imaplib импортирован")
    
    import email
    print("✅ email импортирован")
    
    import ssl
    print("✅ ssl импортирован")
    
    import os
    print("✅ os импортирован")
    
    from dotenv import load_dotenv
    print("✅ dotenv импортирован")
    
    from datetime import datetime, timedelta
    print("✅ datetime импортирован")
    
    from typing import List, Dict
    print("✅ typing импортирован")
    
    import time
    print("✅ time импортирован")
    
except Exception as e:
    print(f"❌ Ошибка импорта: {e}")
    exit(1)

print("🔧 Все библиотеки импортированы успешно")

try:
    print("🔐 Загружаем переменные окружения...")
    load_dotenv()
    print("✅ .env файл загружен")
    
except Exception as e:
    print(f"❌ Ошибка загрузки .env: {e}")
    exit(1)

try:
    print("📝 Создаем лог-файл...")
    log_filename = 'name_extractor_debug_log.txt'
    
    with open(log_filename, 'w', encoding='utf-8') as f:
        f.write("=== ОТЛАДОЧНЫЙ ЛОГ ===\n")
        f.write(f"Запуск: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
        f.write("=" * 50 + "\n\n")
    
    print(f"✅ Лог-файл {log_filename} создан")
    
except Exception as e:
    print(f"❌ Ошибка создания лог-файла: {e}")
    exit(1)

def write_debug_log(message: str):
    """Записывает отладочное сообщение"""
    try:
        with open(log_filename, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
        print(f"DEBUG: {message}")
    except Exception as e:
        print(f"❌ Ошибка записи в лог: {e}")

write_debug_log("Отладочная функция логирования работает")

try:
    print("🔗 Проверяем переменные окружения...")
    
    imap_server = os.environ.get('IMAP_SERVER')
    imap_port = os.environ.get('IMAP_PORT', 143)
    imap_user = os.environ.get('IMAP_USER')
    imap_password = os.environ.get('IMAP_PASSWORD')
    
    write_debug_log(f"IMAP_SERVER: {imap_server}")
    write_debug_log(f"IMAP_PORT: {imap_port}")
    write_debug_log(f"IMAP_USER: {imap_user}")
    write_debug_log(f"IMAP_PASSWORD: {'*' * len(imap_password) if imap_password else 'НЕ ЗАДАН'}")
    
    if not all([imap_server, imap_user, imap_password]):
        raise ValueError("Не все переменные окружения заданы")
    
    print("✅ Переменные окружения проверены")
    
except Exception as e:
    write_debug_log(f"❌ Ошибка переменных окружения: {e}")
    exit(1)

try:
    print("🔌 Тестируем подключение к IMAP...")
    write_debug_log("Начинаем тест подключения к IMAP...")
    
    # Устанавливаем таймаут
    import socket
    socket.setdefaulttimeout(30)
    write_debug_log("Таймаут установлен на 30 секунд")
    
    # Пробуем подключиться
    write_debug_log(f"Подключаемся к {imap_server}:{imap_port}")
    mailbox = imaplib.IMAP4(imap_server, int(imap_port))
    write_debug_log("IMAP4 соединение установлено")
    
    mailbox.starttls(ssl.create_default_context())
    write_debug_log("STARTTLS выполнен")
    
    mailbox.login(imap_user, imap_password)
    write_debug_log("Авторизация успешна")
    
    mailbox.select('INBOX')
    write_debug_log("INBOX выбран")
    
    # Тестовый поиск
    status, data = mailbox.search(None, 'ALL')
    total_emails = len(data[0].split()) if status == 'OK' else 0
    write_debug_log(f"Всего писем в INBOX: {total_emails}")
    
    mailbox.logout()
    write_debug_log("Отключение от IMAP успешно")
    
    print("✅ IMAP подключение работает!")
    
except Exception as e:
    write_debug_log(f"❌ Ошибка IMAP подключения: {e}")
    print(f"❌ Ошибка IMAP подключения: {e}")
    exit(1)

try:
    print("📝 Тестируем экстрактор ФИО...")
    write_debug_log("Начинаем тест экстрактора ФИО...")
    
    # Простой тест паттернов
    test_text = "Руководитель: Иванов И.И., Менеджер: Петрова Анна Сергеевна"
    
    name_patterns = [
        r'\b([А-ЯЁ][а-яёъь]+\s[А-ЯЁ][а-яёъь]+\s[А-ЯЁ][а-яёъь]+)\b',
        r'\b([А-ЯЁ][а-яёъь]+\s[А-ЯЁ]\.\s*[А-ЯЁ]\.)\b',
    ]
    
    found_names = []
    for pattern in name_patterns:
        matches = re.findall(pattern, test_text)
        found_names.extend(matches)
    
    write_debug_log(f"Тестовый текст: {test_text}")
    write_debug_log(f"Найденные ФИО: {found_names}")
    
    print(f"✅ Паттерны ФИО работают! Найдено: {found_names}")
    
except Exception as e:
    write_debug_log(f"❌ Ошибка тестирования ФИО: {e}")
    print(f"❌ Ошибка тестирования ФИО: {e}")
    exit(1)

# Если дошли до сюда - все работает
write_debug_log("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
print(f"📄 Подробности в файле: {log_filename}")

print("\n" + "="*60)
print("🎯 ДИАГНОСТИКА ЗАВЕРШЕНА")
print("Если все тесты прошли успешно, значит проблема")
print("была в основном коде. Попробуй упрощенную версию.")
print("="*60)
