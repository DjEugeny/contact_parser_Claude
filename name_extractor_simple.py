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
import logging
import sys

load_dotenv()

# 📝 НАСТРОЙКА ЛОГИРОВАНИЯ
log_filename = 'name_extractor_no_normalization_log.txt'
if os.path.exists(log_filename):
    os.remove(log_filename)

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('name_extractor_no_normalization')

class NameExtractorNoNormalization:
    """🎯 УПРОЩЕННЫЙ экстрактор ФИО БЕЗ нормализации склонений - собираем как есть!"""
    
    def __init__(self):
        self.imap_server = os.environ.get('IMAP_SERVER')
        self.imap_port = int(os.environ.get('IMAP_PORT', 143))
        self.imap_user = os.environ.get('IMAP_USER')
        self.imap_password = os.environ.get('IMAP_PASSWORD')
        
        # Создаем папку data если её нет
        if not os.path.exists('data'):
            os.makedirs('data')
        
        # 📄 Загружаем паттерны и исключения
        self.name_patterns = self._load_patterns_from_file()
        self.exclusions = self._load_exclusions_from_file()
        
        # 🎯 Контекстные индикаторы
        self.name_indicators = [
            'руководитель', 'директор', 'менеджер', 'специалист',
            'заместитель', 'начальник', 'исполнитель', 'ответственный',
            'контактное лицо', 'представитель', 'координатор',
            'от:', 'с уважением', 'подпись', 'исп.', 'тел.', 'моб.',
            'факс', 'email', 'почта', 'ведущий', 'старший', 'главный'
        ]
        
        logger.info("✅ Упрощенный экстрактор ФИО инициализирован (БЕЗ нормализации)")
        logger.info(f"📄 Загружено паттернов: {len(self.name_patterns)}")
        logger.info(f"🚫 Загружено исключений: {len(self.exclusions)}")
        logger.info("🎯 Стратегия: собираем ФИО как есть, нормализация потом!")
    
    def _load_patterns_from_file(self) -> List[str]:
        """Загружает паттерны из файла"""
        patterns_file = 'data/name_patterns.txt'
        
        if not os.path.exists(patterns_file):
            default_patterns = [
                # Полное ФИО (3 слова)
                r'\b([А-ЯЁ][а-яё]{2,20}\s[А-ЯЁ][а-яё]{2,20}\s[А-ЯЁ][а-яё]{2,20})\b',
                # Имя + Фамилия (2 слова) 
                r'\b([А-ЯЁ][а-яё]{2,20}\s[А-ЯЁ][а-яё]{2,20})\b',
                # Фамилия + 1 инициал
                r'\b([А-ЯЁ][а-яё]{2,20}\s[А-ЯЁ]\.)\b',
                # Фамилия + 2 инициала
                r'\b([А-ЯЁ][а-яё]{2,20}\s[А-ЯЁ]\.\s*[А-ЯЁ]\.)\b',
                # Инициалы + фамилия
                r'\b([А-ЯЁ]\.\s*[А-ЯЁ]\.\s*[А-ЯЁ][а-яё]{2,20})\b',
                r'\b([А-ЯЁ]\.\s*[А-ЯЁ][а-яё]{2,20})\b'
            ]
            with open(patterns_file, 'w', encoding='utf-8') as f:
                f.write('# Паттерны для извлечения ФИО (без нормализации)\n')
                for pattern in default_patterns:
                    f.write(pattern + '\n')
        
        patterns = []
        try:
            with open(patterns_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        patterns.append(line)
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки паттернов: {e}")
            patterns = [r'\b([А-ЯЁ][а-яё]+\s[А-ЯЁ][а-яё]+)\b']
        
        return patterns
    
    def _load_exclusions_from_file(self) -> set:
        """Загружает исключения из файла"""
        exclusions_file = 'data/exclusions.txt'
        exclusions = set()
        
        if not os.path.exists(exclusions_file):
            default_exclusions = [
                '# Исключения для ФИО',
                '# Географические объекты',
                'улица', 'ул.', 'проспект', 'пр.', 'переулок', 'пер.',
                'андриена лежена', 'лежена', 'варшавское шоссе',
                '# Должности',
                'директор', 'менеджер', 'специалист', 'руководитель',
                'ведущий', 'старший', 'главный', 'заместитель',
                '# Компании',
                'ооо', 'зао', 'технология', 'дна-технология', 'днк-технология'
            ]
            
            with open(exclusions_file, 'w', encoding='utf-8') as f:
                for item in default_exclusions:
                    f.write(item + '\n')
        
        try:
            with open(exclusions_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip().lower()
                    if line and not line.startswith('#'):
                        exclusions.add(line)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось загрузить исключения: {e}")
        
        return exclusions
    
    def extract_names_only(self, text: str) -> List[Dict[str, str]]:
        """🎯 ГЛАВНАЯ ФУНКЦИЯ: извлекает ФИО БЕЗ нормализации, как есть"""
        
        if not text or not isinstance(text, str):
            return []
        
        # Ограничиваем размер
        text = text[:15000]
        
        # Минимальная предобработка (только пробелы)
        processed_text = self._minimal_preprocess(text)
        
        # Извлекаем по паттернам
        raw_names = self._extract_by_patterns(processed_text)
        
        # Фильтруем только по исключениям и контексту
        filtered_names = self._filter_names_simple(raw_names, processed_text)
        
        # Простая дедупликация (только точные совпадения)
        final_names = self._simple_deduplicate(filtered_names)
        
        return final_names
    
    def _minimal_preprocess(self, text: str) -> str:
        """Минимальная предобработка - только нормализуем пробелы"""
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('\n', ' ').replace('\r', ' ')
        return text.strip()
    
    def _extract_by_patterns(self, text: str) -> List[str]:
        """Извлекает ФИО по паттернам"""
        found_names = []
        for pattern in self.name_patterns:
            try:
                matches = re.findall(pattern, text)
                found_names.extend(matches)
            except Exception as e:
                logger.warning(f"⚠️ Ошибка в паттерне: {e}")
        return found_names
    
    def _filter_names_simple(self, raw_names: List[str], full_text: str) -> List[str]:
        """Простая фильтрация - только исключения и контекст"""
        
        filtered = []
        full_text_lower = full_text.lower()
        
        for name in raw_names:
            # 🚫 Проверяем исключения
            if self._is_excluded(name):
                continue
            
            # ✅ Проверяем контекст или очевидность ФИО
            if self._has_name_context(name, full_text_lower) or self._looks_like_name(name):
                filtered.append(name)
        
        return filtered
    
    def _is_excluded(self, name: str) -> bool:
        """Проверка исключений"""
        name_lower = name.lower()
        
        # Точное совпадение
        if name_lower in self.exclusions:
            return True
        
        # Частичное совпадение
        for exclusion in self.exclusions:
            if len(exclusion.split()) > 1:
                if exclusion in name_lower:
                    return True
        
        # Проверка слов
        words = name_lower.split()
        for word in words:
            if word in self.exclusions:
                return True
        
        # Базовые проверки
        if len(name) > 100 or re.search(r'\d', name) or re.search(r'[a-zA-Z]', name):
            return True
        
        return False
    
    def _has_name_context(self, name: str, full_text_lower: str) -> bool:
        """Проверяет контекст"""
        name_lower = name.lower()
        name_pos = full_text_lower.find(name_lower)
        if name_pos == -1:
            return False
        
        context_start = max(0, name_pos - 100)
        context_end = min(len(full_text_lower), name_pos + len(name_lower) + 100)
        context = full_text_lower[context_start:context_end]
        
        for indicator in self.name_indicators:
            if indicator in context:
                return True
        return False
    
    def _looks_like_name(self, name: str) -> bool:
        """Проверяет похожесть на ФИО"""
        # Если есть инициалы - скорее всего ФИО
        if re.search(r'[А-ЯЁ]\.', name):
            return True
        
        # Если 3 слова - может быть полное ФИО
        if len(name.split()) == 3:
            return True
        
        # Если 2 слова без цифр и латиницы
        words = name.split()
        if len(words) == 2:
            if all(len(word) >= 2 for word in words):
                if all(word[0].isupper() for word in words):
                    if not re.search(r'[0-9a-zA-Z]', name):
                        return True
        return False
    
    def _simple_deduplicate(self, names: List[str]) -> List[Dict[str, str]]:
        """🎯 ПРОСТАЯ дедупликация - только точные совпадения, БЕЗ нормализации"""
        
        result_names = []
        seen = set()
        
        for name in names:
            # Только убираем лишние пробелы, НЕ меняем падежи
            clean_name = re.sub(r'\s+', ' ', name.strip())
            
            if clean_name not in seen:
                result_names.append({
                    'fullname': clean_name,
                    'type': self._classify_name_type(clean_name)
                })
                seen.add(clean_name)
        
        return result_names
    
    def _classify_name_type(self, name: str) -> str:
        """Классификация типа ФИО"""
        if re.search(r'[А-ЯЁ]\.\s*[А-ЯЁ]\.', name):
            return 'with_two_initials'
        elif re.search(r'[А-ЯЁ]\.', name):
            return 'with_one_initial'
        elif len(name.split()) == 3:
            return 'full_name'
        elif len(name.split()) == 2:
            return 'name_surname'
        else:
            return 'unknown'
    
    # Методы работы с почтой остаются прежними
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
        """Парсит дату письма"""
        if not date_str:
            return "Дата неизвестна"
        try:
            dt = email.utils.parsedate_to_datetime(date_str)
            dt_adjusted = dt.replace(tzinfo=None) + timedelta(hours=4)
            return dt_adjusted.strftime('%d.%m.%Y %H:%M')
        except Exception:
            return date_str[:16] if len(date_str) > 16 else date_str
    
    def test_single_date_detailed(self, date_str: str = '2025-07-29'):
        """Тестирует одну дату БЕЗ нормализации"""
        logger.info("=" * 80)
        logger.info(f"📝 ТЕСТ ФИО БЕЗ НОРМАЛИЗАЦИИ ЗА {date_str}")
        logger.info("=" * 80)
        
        try:
            logger.info(f"🔌 Подключаюсь к {self.imap_server}...")
            
            mailbox = imaplib.IMAP4(self.imap_server, self.imap_port)
            mailbox.starttls(ssl.create_default_context())
            mailbox.login(self.imap_user, self.imap_password)
            mailbox.select('INBOX')
            
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            imap_date = dt.strftime('%d-%b-%Y')
            criteria = f'(ON "{imap_date}")'
            
            status, data = mailbox.search(None, criteria)
            mail_ids = data[0].split() if status == 'OK' else []
            
            total_emails = len(mail_ids)
            logger.info(f"📬 Всего писем за {date_str}: {total_emails}")
            
            if total_emails == 0:
                logger.info("❌ Писем не найдено!")
                mailbox.logout()
                return {
                    'date': date_str,
                    'total_emails': 0,
                    'emails_with_names': 0,
                    'unique_names': 0,
                    'names_list': []
                }
            
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
                    
                    subject = self._decode_header_clean(msg.get('Subject', 'Без темы'))
                    from_addr = self._decode_header_clean(msg.get('From', 'Неизвестно'))
                    email_date = self._parse_email_date(msg.get('Date', ''))
                    
                    body = self._extract_email_body_fast(msg)
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
                        
                        logger.info(f"\n📧 Письмо {i}/{total_emails}: {email_date}")
                        logger.info(f"   📝 Тема: {subject}")
                        logger.info(f"   👤 От: {from_addr}")
                        logger.info("   📝 ФИО (как есть):")
                        for name_info in names:
                            logger.info(f"      ✅ {name_info['fullname']} ({name_info['type']})")
                
                except Exception as e:
                    logger.error(f"❌ Ошибка письма {i}: {e}")
                    continue
            
            mailbox.logout()
            
            # Подсчет уникальных ФИО
            unique_names = []
            seen_names = set()
            for name_info in all_names:
                if name_info['fullname'] not in seen_names:
                    unique_names.append(name_info)
                    seen_names.add(name_info['fullname'])
            
            total_time = time.time() - start_time
            
            logger.info("=" * 80)
            logger.info(f"📊 РЕЗУЛЬТАТЫ БЕЗ НОРМАЛИЗАЦИИ ЗА {date_str}")
            logger.info("=" * 80)
            logger.info(f"📬 Всего писем: {total_emails}")
            logger.info(f"📝 Писем с ФИО: {len(emails_with_names)}")
            logger.info(f"🎯 Уникальных ФИО: {len(unique_names)}")
            logger.info(f"⏱️ Время обработки: {total_time:.1f} сек")
            logger.info("💡 Стратегия: собираем как есть, дубликаты/склонения решаем позже!")
            
            if unique_names:
                logger.info(f"\n📋 ВСЕ ФИО ЗА {date_str} (как есть в письмах):")
                for i, name_info in enumerate(sorted(unique_names, key=lambda x: x['fullname']), 1):
                    logger.info(f"   {i:2d}. {name_info['fullname']} ({name_info['type']})")
            
            logger.info("=" * 80)
            logger.info(f"✅ ТЕСТ БЕЗ НОРМАЛИЗАЦИИ ЗА {date_str} ЗАВЕРШЕН")
            logger.info("=" * 80)
            
            return {
                'date': date_str,
                'total_emails': total_emails,
                'emails_with_names': len(emails_with_names),
                'unique_names': len(unique_names),
                'names_list': unique_names,
                'detailed_results': emails_with_names
            }
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка для {date_str}: {e}")
            return None
    
    def test_date_range_detailed(self, start_date: str, end_date: str):
        """Тестирует диапазон дат БЕЗ нормализации"""
        logger.info("=" * 80)
        logger.info(f"📝 ТЕСТ ФИО БЕЗ НОРМАЛИЗАЦИИ: {start_date} - {end_date}")
        logger.info("💡 Собираем ФИО как есть, нормализация потом на этапе таблицы!")
        logger.info("=" * 80)
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_dt
        
        total_emails_all = 0
        total_names_all = []
        all_daily_results = []
        total_days = (end_dt - start_dt).days + 1
        
        day_counter = 1
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            
            logger.info(f"\n🎯 ДЕНЬ {day_counter}/{total_days}: {date_str}")
            logger.info("=" * 50)
            
            daily_results = self.test_single_date_detailed(date_str)
            
            if daily_results:
                total_emails_all += daily_results['total_emails']
                total_names_all.extend(daily_results['names_list'])
                all_daily_results.append(daily_results)
            else:
                logger.info(f"❌ Ошибка обработки даты {date_str}")
            
            current_date += timedelta(days=1)
            day_counter += 1
        
        # Подсчет уникальных ФИО за весь период
        unique_names_all = []
        seen_names_all = set()
        for name_info in total_names_all:
            if name_info['fullname'] not in seen_names_all:
                unique_names_all.append(name_info)
                seen_names_all.add(name_info['fullname'])
        
        logger.info("=" * 80)
        logger.info(f"📊 ИТОГОВЫЙ ОТЧЕТ БЕЗ НОРМАЛИЗАЦИИ {start_date} - {end_date}")
        logger.info("=" * 80)
        logger.info(f"📅 Протестировано дней: {total_days}")
        logger.info(f"📬 Всего писем: {total_emails_all}")
        logger.info(f"🎯 Всего уникальных ФИО: {len(unique_names_all)}")
        logger.info("💡 Включая все склонения - дедуплицируем потом в таблице!")
        
        if unique_names_all:
            logger.info(f"\n📋 ВСЕ ФИО ЗА ПЕРИОД {start_date} - {end_date} (как есть):")
            for i, name_info in enumerate(sorted(unique_names_all, key=lambda x: x['fullname']), 1):
                logger.info(f"   {i:3d}. {name_info['fullname']} ({name_info['type']})")
            
            logger.info("\n📊 КРАТКАЯ СТАТИСТИКА ПО ДНЯМ:")
            for day_result in all_daily_results:
                if day_result['emails_with_names'] > 0:
                    logger.info(f"   📅 {day_result['date']}: {day_result['total_emails']} писем, {day_result['emails_with_names']} с ФИО, {day_result['unique_names']} уник.")
                else:
                    logger.info(f"   📅 {day_result['date']}: {day_result['total_emails']} писем, ФИО не найдено")
        
        logger.info("=" * 80)
        logger.info(f"✅ ТЕСТ БЕЗ НОРМАЛИЗАЦИИ ЗАВЕРШЕН")
        logger.info("💡 Следующий этап: объединение всех экстракторов + нормализация в таблице")
        logger.info("=" * 80)
        
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
    """Главная функция упрощенного тестера БЕЗ нормализации"""
    
    logger.info("🚀 ЗАПУСК УПРОЩЕННОГО ТЕСТЕРА ФИО (БЕЗ НОРМАЛИЗАЦИИ)")
    logger.info("💡 Стратегия: собираем максимально широко как есть!")
    
    tester = NameExtractorNoNormalization()
    
    # 🎯 НАСТРОЙКА ДАТ - ИЗМЕНИ ЗДЕСЬ!
    start_date = '2025-07-29'  # ← НАЧАЛЬНАЯ ДАТА
    end_date = '2025-08-04'    # ← КОНЕЧНАЯ ДАТА
    
    results = tester.test_date_range_detailed(start_date, end_date)
    
    if results:
        logger.info(f"\n🎉 УПРОЩЕННЫЙ СБОР ФИО ЗАВЕРШЕН!")
        logger.info(f"📅 Период: {results['start_date']} - {results['end_date']}")
        logger.info(f"📬 Всего писем: {results['total_emails']}")
        logger.info(f"📝 Всего уникальных ФИО: {results['total_unique_names']}")
        logger.info(f"💾 Данные для анализа: name_extractor_no_normalization_log.txt")


if __name__ == "__main__":
    main()
