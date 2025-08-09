#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Устойчивый скрипт для выгрузки писем за 7 месяцев с обработкой SSL-ошибок
"""

import os
import re
import ssl
import imaplib
import email
import csv
import calendar
import time
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Настройки подключения
IMAP_SERVER   = os.getenv('IMAP_SERVER')
IMAP_PORT     = int(os.getenv('IMAP_PORT', 143))
IMAP_USER     = os.getenv('IMAP_USER')
IMAP_PASSWORD = os.getenv('IMAP_PASSWORD')

# Настройки устойчивости
MAX_RETRIES = 3
RETRY_DELAY = 5  # секунд между попытками
BATCH_SIZE = 50  # писем за раз перед переподключением
REQUEST_DELAY = 0.5  # пауза между запросами

# Названия месяцев
MONTHS_RU = {
    1: 'january', 2: 'february', 3: 'march', 4: 'april',
    5: 'may', 6: 'june', 7: 'july', 8: 'august',
    9: 'september', 10: 'october', 11: 'november', 12: 'december'
}

class RobustIMAPConnection:
    """Устойчивое IMAP-соединение с автоматическим переподключением"""
    
    def __init__(self, server, port, user, password):
        self.server = server
        self.port = port
        self.user = user
        self.password = password
        self.mail = None
        self.last_connect_time = 0
        
    def connect(self):
        """Подключение к серверу с обработкой ошибок"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                if self.mail:
                    try:
                        self.mail.logout()
                    except:
                        pass
                
                print(f"   🔌 Подключение к серверу (попытка {attempt + 1}/{max_attempts})...")
                
                self.mail = imaplib.IMAP4(self.server, self.port)
                self.mail.starttls(ssl.create_default_context())
                self.mail.login(self.user, self.password)
                self.mail.select('INBOX')
                
                self.last_connect_time = time.time()
                print(f"   ✅ Подключение успешно")
                return True
                
            except Exception as e:
                print(f"   ❌ Ошибка подключения (попытка {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    time.sleep(RETRY_DELAY)
                    
        return False
    
    def reconnect_if_needed(self):
        """Переподключение если прошло много времени или есть проблемы"""
        current_time = time.time()
        
        # Переподключаемся каждые 10 минут для профилактики
        if current_time - self.last_connect_time > 600:
            print(f"   🔄 Профилактическое переподключение...")
            return self.connect()
        
        return True
    
    def safe_fetch(self, msg_id, flags='(RFC822)'):
        """Безопасное получение письма с повторными попытками"""
        
        for attempt in range(MAX_RETRIES):
            try:
                # Небольшая пауза между запросами
                time.sleep(REQUEST_DELAY)
                
                status, data = self.mail.fetch(msg_id, flags)
                if status == 'OK':
                    return data
                else:
                    raise Exception(f"IMAP fetch returned: {status}")
                    
            except (imaplib.IMAP4.abort, ssl.SSLError, OSError, ConnectionError) as e:
                print(f"      ⚠️ SSL ошибка при получении письма (попытка {attempt + 1}): {e}")
                
                if attempt < MAX_RETRIES - 1:
                    print(f"      🔄 Переподключение через {RETRY_DELAY} сек...")
                    time.sleep(RETRY_DELAY)
                    
                    if not self.connect():
                        print(f"      ❌ Не удалось переподключиться")
                        continue
                else:
                    print(f"      ❌ Письмо пропущено после {MAX_RETRIES} попыток")
                    return None
                    
            except Exception as e:
                print(f"      ❌ Другая ошибка: {e}")
                return None
        
        return None
    
    def safe_search(self, criteria):
        """Безопасный поиск писем"""
        
        for attempt in range(MAX_RETRIES):
            try:
                status, data = self.mail.search(None, criteria)
                if status == 'OK':
                    return data[0].split() if data[0] else []
                else:
                    raise Exception(f"IMAP search returned: {status}")
                    
            except Exception as e:
                print(f"      ⚠️ Ошибка поиска (попытка {attempt + 1}): {e}")
                
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    if not self.connect():
                        continue
                else:
                    print(f"      ❌ Поиск не удался")
                    return []
        
        return []
    
    def close(self):
        """Закрытие соединения"""
        if self.mail:
            try:
                self.mail.logout()
            except:
                pass

def generate_7_months() -> list:
    """Генерирует список из 7 месяцев: январь-июль 2025"""
    
    months = []
    for month_num in range(1, 8):
        year = 2025
        start_date = f"{year}-{month_num:02d}-01"
        last_day = calendar.monthrange(year, month_num)[1]
        end_date = f"{year}-{month_num:02d}-{last_day:02d}"
        
        months.append({
            'year': year,
            'month_num': month_num,
            'month_name': MONTHS_RU[month_num],
            'start_date': start_date,
            'end_date': end_date,
            'description': f"{MONTHS_RU[month_num].title()} {year}"
        })
    
    return months

def count_tokens_accurate(text: str) -> dict:
    """Точный подсчет токенов"""
    results = {
        'characters': len(text),
        'russian_estimate': len(text) // 3
    }
    
    try:
        import tiktoken
        enc = tiktoken.encoding_for_model("gpt-4")
        actual_tokens = len(enc.encode(text))
        results.update({
            'actual_tokens': actual_tokens,
            'char_per_token': len(text) / actual_tokens if actual_tokens > 0 else 0
        })
    except:
        pass
    
    return results

def decode_header_value(val: str) -> str:
    """Декодирует MIME-заголовки"""
    from email.header import decode_header, make_header
    return str(make_header(decode_header(val or '')))

def extract_plain_text(msg, keep_forwards: bool = True) -> str:
    """Извлекает весь текст письма"""
    text_parts = []
    max_len = 500_000
    
    try:
        if msg.is_multipart():
            for part in msg.walk():
                ctype = part.get_content_type()
                if ctype in ('text/plain', 'text/html'):
                    try:
                        raw = part.get_payload(decode=True)
                        charset = part.get_content_charset() or 'utf-8'
                        chunk = raw.decode(charset, errors='ignore')
                        
                        if ctype == 'text/html':
                            chunk = re.sub(r'<[^>]+>', '', chunk)
                        
                        text_parts.append(chunk)
                    except:
                        continue
        else:
            try:
                raw = msg.get_payload(decode=True)
                if raw:
                    charset = msg.get_content_charset() or 'utf-8'
                    text_parts.append(raw.decode(charset, errors='ignore'))
            except:
                pass

        full_text = '\n'.join(text_parts)
        return full_text[:max_len].strip()
    
    except:
        return ""

def imap_date_str(dt: datetime) -> str:
    """Переводит datetime в формат IMAP"""
    return dt.strftime('%d-%b-%Y')

def fetch_emails_month_robust(month_info: dict):
    """Устойчивая выгрузка писем за месяц"""
    
    print(f"🎯 ОБРАБАТЫВАЮ: {month_info['description'].upper()}")
    print("-" * 50)
    
    # Создаем устойчивое соединение
    imap_conn = RobustIMAPConnection(IMAP_SERVER, IMAP_PORT, IMAP_USER, IMAP_PASSWORD)
    
    if not imap_conn.connect():
        print(f"❌ Не удалось подключиться для {month_info['description']}")
        return []

    dt_start = datetime.strptime(month_info['start_date'], '%Y-%m-%d')
    dt_end   = datetime.strptime(month_info['end_date'], '%Y-%m-%d')
    
    all_records = []
    current = dt_start
    total_days = (dt_end - dt_start).days + 1
    day_counter = 0
    processed_emails = 0

    print(f"📅 Обрабатываю {month_info['description']}")
    print(f"   Период: {month_info['start_date']} - {month_info['end_date']}")
    
    while current <= dt_end:
        day_counter += 1
        date_imap = imap_date_str(current)
        date_display = current.strftime('%Y-%m-%d')
        
        # Поиск писем за день
        criteria = f'(ON "{date_imap}")'
        ids = imap_conn.safe_search(criteria)
        
        if len(ids) > 0:
            print(f"   📬 День {day_counter}/{total_days} ({date_display}): {len(ids)} писем")
        
        # Обрабатываем письма пакетами
        for i, msg_id in enumerate(ids, 1):
            # Переподключение каждые BATCH_SIZE писем
            if processed_emails > 0 and processed_emails % BATCH_SIZE == 0:
                print(f"      🔄 Профилактическое переподключение после {processed_emails} писем...")
                if not imap_conn.connect():
                    print(f"      ❌ Ошибка переподключения, продолжаем...")
                    continue
            
            # Получаем письмо
            fetch_data = imap_conn.safe_fetch(msg_id)
            if not fetch_data:
                continue
            
            try:
                raw = fetch_data[0][1]
                msg = email.message_from_bytes(raw)
                body = extract_plain_text(msg, keep_forwards=True)
                
                record = {
                    'month': month_info['description'],
                    'date': decode_header_value(msg.get('Date', '')),
                    'from': decode_header_value(msg.get('From', '')),
                    'to': decode_header_value(msg.get('To', '')),
                    'subject': decode_header_value(msg.get('Subject', '')),
                    'char_count': len(body),
                    'body': body
                }
                all_records.append(record)
                processed_emails += 1
                
            except Exception as e:
                print(f"      ❌ Ошибка обработки письма: {e}")
                continue
        
        current += timedelta(days=1)
        
        # Небольшая пауза между днями
        time.sleep(0.1)

    imap_conn.close()
    print(f"   ✅ {month_info['description']} завершен: {len(all_records)} писем")
    return all_records

def save_month_csv(records, month_info: dict):
    """Сохраняет данные месяца в CSV"""
    filename = f"emails_{month_info['year']}_{month_info['month_num']:02d}_{month_info['month_name']}.csv"
    fields = ['month', 'date', 'from', 'to', 'subject', 'char_count', 'body']
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)
    
    return filename

def estimate_month_costs(month_records, month_name: str):
    """Оценка стоимости для месяца"""
    if not month_records:
        print(f"   📊 {month_name}: нет писем")
        return 0
    
    total_chars = sum(r['char_count'] for r in month_records)
    sample_text = '\n'.join([r['body'][:1000] for r in month_records[:5]])
    token_stats = count_tokens_accurate(sample_text)
    
    if 'actual_tokens' in token_stats and token_stats['char_per_token'] > 0:
        est_tokens = int(total_chars / token_stats['char_per_token'])
        method = "точная (tiktoken)"
    else:
        est_tokens = total_chars // 3
        method = "приблизительная для русского"
    
    print(f"   📊 {month_name}:")
    print(f"      📧 Писем: {len(month_records)}")
    print(f"      📝 Символов: {total_chars:,}")
    print(f"      🎯 Токенов: {est_tokens:,} ({method})")
    
    return est_tokens

def main():
    """Главная функция с обработкой ошибок"""
    
    print("🚀 УСТОЙЧИВАЯ ВЫГРУЗКА ЗА 7 МЕСЯЦЕВ")
    print("🛡️ С обработкой SSL-ошибок и автоматическим переподключением")
    print("=" * 70)
    
    months = generate_7_months()
    
    print(f"📅 Запланировано {len(months)} месяцев:")
    for m in months:
        print(f"   • {m['description']}: {m['start_date']} - {m['end_date']}")
    
    print(f"\n⚙️ Настройки устойчивости:")
    print(f"   • Максимум попыток: {MAX_RETRIES}")
    print(f"   • Задержка между попытками: {RETRY_DELAY} сек")
    print(f"   • Переподключение каждые: {BATCH_SIZE} писем")
    print(f"   • Пауза между запросами: {REQUEST_DELAY} сек")
    
    print("\n" + "=" * 70)
    
    monthly_stats = []
    total_emails = 0
    total_tokens = 0
    
    for month_info in months:
        try:
            month_records = fetch_emails_month_robust(month_info)
            
            if month_records:
                filename = save_month_csv(month_records, month_info)
                print(f"   💾 Файл создан: {filename}")
                
                month_tokens = estimate_month_costs(month_records, month_info['description'])
                
                monthly_stats.append({
                    'month': month_info['description'],
                    'filename': filename,
                    'emails': len(month_records),
                    'tokens': month_tokens
                })
                
                total_emails += len(month_records)
                total_tokens += month_tokens
            else:
                print(f"   ⚠️ Нет писем в {month_info['description']}")
                monthly_stats.append({
                    'month': month_info['description'],
                    'filename': 'нет данных',
                    'emails': 0,
                    'tokens': 0
                })
                
        except Exception as e:
            print(f"   ❌ Критическая ошибка {month_info['description']}: {e}")
            continue
        
        print()
    
    # Итоговая статистика
    print("=" * 70)
    print("📊 ИТОГОВАЯ СТАТИСТИКА:")
    print("=" * 70)
    
    for stat in monthly_stats:
        if stat['emails'] > 0:
            print(f"📂 {stat['filename']}")
            print(f"   {stat['month']}: {stat['emails']:,} писем, {stat['tokens']:,} токенов")
        else:
            print(f"📂 {stat['month']}: нет писем")
    
    print(f"\n🎯 ОБЩИЙ ИТОГ:")
    print(f"   📧 Всего писем: {total_emails:,}")
    print(f"   🎯 Всего токенов: {total_tokens:,}")
    
    if total_tokens > 0:
        print(f"\n💰 СТОИМОСТЬ ОБРАБОТКИ:")
        api_costs = {
            'Groq (бесплатно)': 0.0,
            'Qwen-Flash': 0.15,
            'Gemini Flash': 0.19,
            'GPT-4o-mini': 0.28,
            'Gemini Pro': 2.38,
            'GPT-4o': 4.76,
            'Claude Sonnet': 6.6
        }
        
        for model, price_per_1m in api_costs.items():
            cost = (total_tokens / 1_000_000) * price_per_1m
            if cost == 0:
                print(f"   {model}: БЕСПЛАТНО")
            elif cost < 1:
                print(f"   {model}: ${cost:.3f} (~{cost*100:.0f}¢)")
            else:
                print(f"   {model}: ${cost:.2f}")
    
    print("\n✅ УСТОЙЧИВАЯ ВЫГРУЗКА ЗАВЕРШЕНА!")

if __name__ == '__main__':
    main()
