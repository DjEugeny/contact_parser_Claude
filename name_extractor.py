#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import imaplib
import email
import ssl
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List, Dict
import time

load_dotenv()

class NameExtractorFixed:
    """Исправленный экстрактор ФИО со всеми форматами"""
    
    def __init__(self):
        self.imap_server = os.environ.get('IMAP_SERVER')
        self.imap_port = int(os.environ.get('IMAP_PORT', 143))
        self.imap_user = os.environ.get('IMAP_USER')
        self.imap_password = os.environ.get('IMAP_PASSWORD')
        
        # 🔧 ИСПРАВЛЕННЫЕ паттерны для ВСЕХ форматов ФИО
        self.name_patterns = [
            # Полное ФИО (3 слова): Иванов Иван Иванович, Иван Иванович Иванов
            r'\b([А-ЯЁ][а-яёъь]+\s[А-ЯЁ][а-яёъь]+\s[А-ЯЁ][а-яёъь]+)\b',
            
            # Фамилия + 2 инициала: Иванов И.И., Иванов И. И.
            r'\b([А-ЯЁ][а-яёъь]+\s[А-ЯЁ]\.\s*[А-ЯЁ]\.)\b',
            
            # Фамилия + 1 инициал: Иванов И.
            r'\b([А-ЯЁ][а-яёъь]+\s[А-ЯЁ]\.)\b',
            
            # 2 инициала + фамилия: И.И. Иванов, И. И. Иванов
            r'\b([А-ЯЁ]\.\s*[А-ЯЁ]\.\s+[А-ЯЁ][а-яёъь]+)\b',
            
            # 1 инициал + фамилия: И. Иванов
            r'\b([А-ЯЁ]\.\s+[А-ЯЁ][а-яёъь]+)\b',
            
            # Имя + Фамилия: Иван Иванов (только если не попали в полное ФИО)
            r'\b([А-ЯЁ][а-яёъь]+\s[А-ЯЁ][а-яёъь]+)\b'
        ]
        
        # Контекстные индикаторы наличия ФИО
        self.name_indicators = [
            'руководитель', 'директор', 'менеджер', 'специалист',
            'заместитель', 'начальник', 'исполнитель', 'ответственный',
            'контактное лицо', 'представитель', 'координатор',
            'от:', 'с уважением', 'подпись', 'исп.', 'тел.', 'моб.',
            'факс', 'email', 'почта', 'ведущий', 'старший', 'главный'
        ]
        
        # Слова для исключения (не ФИО)
        self.exclusions = [
            'центр', 'отдел', 'департамент', 'управление', 'служба',
            'компания', 'организация', 'предприятие', 'учреждение',
            'фирма', 'корпорация', 'группа', 'холдинг', 'сеть',
            'россия', 'москва', 'санкт-петербург', 'новосибирск',
            'система', 'технология', 'разработка', 'производство',
            'общество', 'товарищество', 'кооператив', 'банк',
            'институт', 'университет', 'академия', 'школа',
            'больница', 'поликлиника', 'клиника', 'аптека'
        ]
        
        print("✅ Исправленный экстрактор ФИО инициализирован")
    
    def extract_names_only(self, text: str) -> List[Dict[str, str]]:
        """Извлекает ФИО из текста с покрытием всех форматов"""
        
        if not text or not isinstance(text, str):
            return []
        
        # Ограничиваем размер текста
        text = text[:15000]
        
        # Предобработка текста
        processed_text = self._preprocess_text(text)
        
        # Извлекаем ФИО по паттернам
        raw_names = self._extract_by_patterns(processed_text)
        
        # Фильтруем по контексту и качеству
        filtered_names = self._filter_names(raw_names, processed_text)
        
        # Нормализуем и убираем дубликаты
        final_names = self._normalize_and_deduplicate(filtered_names)
        
        return final_names
    
    def _preprocess_text(self, text: str) -> str:
        """Предобработка текста для лучшего извлечения ФИО"""
        
        # Нормализуем пробелы и переносы
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('\n', ' ').replace('\r', ' ')
        
        # Исправляем возможные проблемы с кодировкой
        text = text.replace('ё', 'е').replace('Ё', 'Е')
        
        return text.strip()
    
    def _extract_by_patterns(self, text: str) -> List[str]:
        """Извлекает ФИО по regex паттернам"""
        
        found_names = []
        
        for pattern in self.name_patterns:
            matches = re.findall(pattern, text)
            found_names.extend(matches)
        
        return found_names
    
    def _filter_names(self, raw_names: List[str], full_text: str) -> List[str]:
        """Фильтрует ФИО по контексту и исключениям"""
        
        filtered = []
        full_text_lower = full_text.lower()
        
        for name in raw_names:
            # Проверяем на исключения
            if self._is_excluded(name):
                continue
            
            # Проверяем контекст (есть ли рядом индикаторы ФИО)
            if self._has_name_context(name, full_text_lower):
                filtered.append(name)
            # Или если это явно выглядит как ФИО (3 слова или инициалы)
            elif self._looks_like_name(name):
                filtered.append(name)
        
        return filtered
    
    def _is_excluded(self, name: str) -> bool:
        """Проверяет, не содержит ли 'имя' исключаемые слова"""
        
        name_lower = name.lower()
        
        for exclusion in self.exclusions:
            if exclusion in name_lower:
                return True
        
        # Дополнительная проверка: слишком длинные 'имена' (вероятно не ФИО)
        if len(name) > 50:
            return True
            
        # Проверка на цифры (ФИО не должны содержать цифры)
        if re.search(r'\d', name):
            return True
        
        return False
    
    def _has_name_context(self, name: str, full_text_lower: str) -> bool:
        """Проверяет наличие контекстных индикаторов рядом с именем"""
        
        # Ищем имя в тексте и проверяем контекст в радиусе 100 символов
        name_lower = name.lower()
        
        # Находим позицию имени в тексте
        name_pos = full_text_lower.find(name_lower)
        if name_pos == -1:
            return False
        
        # Извлекаем контекст (100 символов до и после)
        context_start = max(0, name_pos - 100)
        context_end = min(len(full_text_lower), name_pos + len(name_lower) + 100)
        context = full_text_lower[context_start:context_end]
        
        # Проверяем наличие индикаторов в контексте
        for indicator in self.name_indicators:
            if indicator in context:
                return True
        
        return False
    
    def _looks_like_name(self, name: str) -> bool:
        """Проверяет, выглядит ли строка как ФИО"""
        
        # Если есть инициалы - скорее всего ФИО
        if re.search(r'[А-ЯЁ]\.', name):
            return True
        
        # Если 3 слова - может быть полное ФИО
        if len(name.split()) == 3:
            return True
        
        # Если 2 коротких слова без цифр - может быть Имя Фамилия
        words = name.split()
        if len(words) == 2 and all(len(word) >= 3 for word in words) and not re.search(r'\d', name):
            # Дополнительная проверка: оба слова должны начинаться с заглавной
            if all(word[0].isupper() for word in words):
                return True
        
        return False
    
    def _normalize_and_deduplicate(self, names: List[str]) -> List[Dict[str, str]]:
        """Нормализует ФИО и убирает дубликаты"""
        
        normalized = []
        seen = set()
        
        for name in names:
            # Нормализуем пробелы в инициалах
            normalized_name = re.sub(r'([А-ЯЁ])\.\s*([А-ЯЁ])\.', r'\1. \2.', name.strip())
            normalized_name = re.sub(r'\s+', ' ', normalized_name)
            
            # Убираем дубликаты
            if normalized_name not in seen:
                normalized.append({
                    'fullname': normalized_name,
                    'type': self._classify_name_type(normalized_name)
                })
                seen.add(normalized_name)
        
        return normalized
    
    def _classify_name_type(self, name: str) -> str:
        """Классифицирует тип ФИО"""
        
        if re.search(r'[А-ЯЁ]\.\s*[А-ЯЁ]\.', name):
            return 'with_two_initials'     # С двумя инициалами
        elif re.search(r'[А-ЯЁ]\.', name):
            return 'with_one_initial'      # С одним инициалом
        elif len(name.split()) == 3:
            return 'full_name'             # Полное ФИО
        elif len(name.split()) == 2:
            return 'name_surname'          # Имя Фамилия
        else:
            return 'unknown'
    
    def _extract_email_body_fast(self, msg) -> str:
        """Быстрое извлечение тела письма"""
        
        body = ""
        max_size = 15000
        
        try:
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        charset = part.get_content_charset() or 'utf-8'
                        try:
                            payload = part.get_payload(decode=True)
                            if payload:
                                content = payload.decode(charset, errors="ignore")
                                body = content[:max_size]
                                break
                        except Exception:
                            continue
            else:
                charset = msg.get_content_charset() or 'utf-8'
                try:
                    payload = msg.get_payload(decode=True)
                    if payload:
                        content = payload.decode(charset, errors="ignore")
                        body = content[:max_size]
                except Exception:
                    body = ""
        except Exception:
            body = ""
        
        return body.strip()
    
    def _decode_header_clean(self, header_str: str) -> str:
        """Декодирует заголовки почты"""
        
        if not header_str:
            return ""
        
        try:
            from email.header import decode_header, make_header
            decoded = str(make_header(decode_header(header_str)))
            return decoded
        except:
            return header_str
    
    def _parse_email_date(self, date_str: str) -> str:
        """Парсит дату письма в +4 часовой пояс"""
        
        if not date_str:
            return "Дата неизвестна"
        
        try:
            dt = email.utils.parsedate_to_datetime(date_str)
            dt_adjusted = dt.replace(tzinfo=None) + timedelta(hours=4)
            return dt_adjusted.strftime('%d.%m.%Y %H:%M')
        except Exception:
            return date_str[:16] if len(date_str) > 16 else date_str
    
    def test_single_date_detailed(self, date_str: str = '2025-07-29', show_details: bool = True):
        """Тестирует извлечение ФИО за одну дату"""
        
        if show_details:
            print(f"\n{'='*80}")
            print(f"📝 ТЕСТ ФИО ЗА {date_str} (ИСПРАВЛЕННАЯ ВЕРСИЯ)")
            print(f"{'='*80}")
        
        try:
            # Подключение
            if show_details:
                print(f"🔌 Подключаюсь к {self.imap_server}...")
            
            mailbox = imaplib.IMAP4(self.imap_server, self.imap_port)
            mailbox.starttls(ssl.create_default_context())
            mailbox.login(self.imap_user, self.imap_password)
            mailbox.select('INBOX')
            
            # Поиск писем
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            imap_date = dt.strftime('%d-%b-%Y')
            criteria = f'(ON "{imap_date}")'
            
            status, data = mailbox.search(None, criteria)
            mail_ids = data[0].split() if status == 'OK' else []
            
            total_emails = len(mail_ids)
            
            if show_details:
                print(f"📬 Всего писем за {date_str}: {total_emails}")
            
            if total_emails == 0:
                if show_details:
                    print("❌ Писем не найдено!")
                mailbox.logout()
                return {
                    'date': date_str,
                    'total_emails': 0,
                    'emails_with_names': 0,
                    'unique_names': 0,
                    'names_list': [],
                    'detailed_results': []
                }
            
            # Обработка писем
            emails_with_names = []
            all_names = []
            
            start_time = time.time()
            
            for i, mail_id in enumerate(mail_ids, 1):
                try:
                    status, msg_data = mailbox.fetch(mail_id, '(RFC822)')
                    if status != 'OK':
                        continue
                    
                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)
                    
                    # Извлекаем заголовки
                    subject_raw = msg.get('Subject', 'Без темы')
                    from_raw = msg.get('From', 'Неизвестно')
                    date_raw = msg.get('Date', '')
                    
                    subject = self._decode_header_clean(subject_raw)
                    from_addr = self._decode_header_clean(from_raw)
                    email_date = self._parse_email_date(date_raw)
                    
                    # Извлекаем тело письма
                    body = self._extract_email_body_fast(msg)
                    
                    # Ищем ФИО
                    names = self.extract_names_only(body)
                    
                    if names:
                        email_info = {
                            'number': i,
                            'subject': subject,
                            'from': from_addr,
                            'date': email_date,
                            'names': names
                        }
                        emails_with_names.append(email_info)
                        all_names.extend(names)
                        
                        # Детальный вывод каждого письма
                        if show_details:
                            print(f"\n📧 Письмо {i}/{total_emails}: {email_date}")
                            print(f"   📝 Тема: {subject}")
                            print(f"   👤 От: {from_addr}")
                            print(f"   📝 ФИО:")
                            for name_info in names:
                                print(f"      ✅ {name_info['fullname']} ({name_info['type']})")
                    
                except Exception as e:
                    if show_details:
                        print(f"❌ Ошибка письма {i}: {e}")
                    continue
            
            mailbox.logout()
            
            # Подсчет уникальных ФИО
            unique_names = []
            seen_names = set()
            for name_info in all_names:
                if name_info['fullname'] not in seen_names:
                    unique_names.append(name_info)
                    seen_names.add(name_info['fullname'])
            
            # Итоговый отчет по дню
            total_time = time.time() - start_time
            
            if show_details:
                print(f"\n{'='*80}")
                print(f"📊 РЕЗУЛЬТАТЫ ФИО ЗА {date_str}")
                print(f"{'='*80}")
                print(f"📬 Всего писем: {total_emails}")
                print(f"📝 Писем с ФИО: {len(emails_with_names)}")
                print(f"🎯 Уникальных ФИО: {len(unique_names)}")
                print(f"⏱️ Время обработки: {total_time:.1f} сек")
                
                if unique_names:
                    print(f"\n📋 ВСЕ УНИКАЛЬНЫЕ ФИО ЗА {date_str}:")
                    for i, name_info in enumerate(sorted(unique_names, key=lambda x: x['fullname']), 1):
                        print(f"   {i:2d}. {name_info['fullname']} ({name_info['type']})")
                
                print(f"\n{'='*80}")
                print(f"✅ ТЕСТ ФИО ЗА {date_str} ЗАВЕРШЕН")
                print(f"{'='*80}")
            
            return {
                'date': date_str,
                'total_emails': total_emails,
                'emails_with_names': len(emails_with_names),
                'unique_names': len(unique_names),
                'names_list': unique_names,
                'detailed_results': emails_with_names
            }
            
        except Exception as e:
            if show_details:
                print(f"❌ Критическая ошибка для {date_str}: {e}")
            return None
    
    def test_date_range_detailed(self, start_date: str, end_date: str):
        """Тестирует диапазон дат с детальными логами"""
        
        print(f"\n{'='*80}")
        print(f"📝 ТЕСТ ФИО ПО ДИАПАЗОНУ ДАТ (ИСПРАВЛЕННЫЙ): {start_date} - {end_date}")
        print(f"{'='*80}")
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_dt
        
        total_emails_all = 0
        total_names_all = []
        all_daily_results = []
        total_days = (end_dt - start_dt).days + 1
        
        # Тестируем каждую дату в диапазоне
        day_counter = 1
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            
            print(f"\n🎯 ДЕНЬ {day_counter}/{total_days}: {date_str}")
            print(f"{'='*50}")
            
            # Тестируем текущую дату с детальным выводом
            daily_results = self.test_single_date_detailed(date_str, show_details=True)
            
            if daily_results:
                total_emails_all += daily_results['total_emails']
                total_names_all.extend(daily_results['names_list'])
                all_daily_results.append(daily_results)
            else:
                print(f"❌ Ошибка обработки даты {date_str}")
            
            current_date += timedelta(days=1)
            day_counter += 1
        
        # Подсчет уникальных ФИО за весь период
        unique_names_all = []
        seen_names_all = set()
        for name_info in total_names_all:
            if name_info['fullname'] not in seen_names_all:
                unique_names_all.append(name_info)
                seen_names_all.add(name_info['fullname'])
        
        # Итоговый отчет по диапазону
        print(f"\n{'='*80}")
        print(f"📊 ИТОГОВЫЙ ОТЧЕТ ФИО ПО ДИАПАЗОНУ {start_date} - {end_date}")
        print(f"{'='*80}")
        print(f"📅 Протестировано дней: {total_days}")
        print(f"📬 Всего писем: {total_emails_all}")
        print(f"🎯 Всего уникальных ФИО: {len(unique_names_all)}")
        
        if unique_names_all:
            print(f"\n📋 ВСЕ ФИО ЗА ПЕРИОД {start_date} - {end_date}:")
            for i, name_info in enumerate(sorted(unique_names_all, key=lambda x: x['fullname']), 1):
                print(f"   {i:3d}. {name_info['fullname']} ({name_info['type']})")
            
            # Статистика по дням
            print(f"\n📊 КРАТКАЯ СТАТИСТИКА ПО ДНЯМ:")
            for day_result in all_daily_results:
                if day_result['emails_with_names'] > 0:
                    print(f"   📅 {day_result['date']}: {day_result['total_emails']} писем, {day_result['emails_with_names']} с ФИО, {day_result['unique_names']} уник.")
                else:
                    print(f"   📅 {day_result['date']}: {day_result['total_emails']} писем, ФИО не найдено")
        
        print(f"\n{'='*80}")
        print(f"✅ ТЕСТ ФИО ПО ДИАПАЗОНУ ЗАВЕРШЕН")
        print(f"{'='*80}")
        
        return {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': total_days,
            'total_emails': total_emails_all,
            'total_unique_names': len(unique_names_all),
            'all_names': unique_names_all,
            'daily_results': all_daily_results
        }


def main():
    """Главная функция для тестирования исправленного ФИО"""
    
    print("🚀 ЗАПУСК ИСПРАВЛЕННОГО ТЕСТЕРА ФИО")
    
    tester = NameExtractorFixed()
    
    # 🎯 НАСТРОЙКА ДАТ - ИЗМЕНИ ЗДЕСЬ! (строки 595-596)
    start_date = '2025-07-29'  # ← НАЧАЛЬНАЯ ДАТА (включительно)
    end_date = '2025-08-04'    # ← КОНЕЧНАЯ ДАТА (включительно)
    
    # Запуск теста диапазона с детальными логами
    results = tester.test_date_range_detailed(start_date, end_date)
    
    if results:
        print(f"\n🎉 ИСПРАВЛЕННЫЙ ТЕСТ ФИО ЗАВЕРШЕН!")
        print(f"📅 Период: {results['start_date']} - {results['end_date']}")
        print(f"📬 Всего писем: {results['total_emails']}")
        print(f"📝 Всего уникальных ФИО: {results['total_unique_names']}")


if __name__ == "__main__":
    main()
