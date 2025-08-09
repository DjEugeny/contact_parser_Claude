#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import os
from datetime import datetime, timedelta
import imaplib
import email
from email.header import decode_header
from dotenv import load_dotenv
import ssl
import logging
import sys
from collections import defaultdict

load_dotenv()

log_filename = 'fixed_position_extractor_log.txt'
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
logger = logging.getLogger('fixed_position_extractor')

class FixedPositionExtractor:
    def __init__(self):
        # Подключение к почте
        self.imap_server = os.environ.get('IMAP_SERVER')
        self.imap_port = int(os.environ.get('IMAP_PORT', 143))
        self.imap_user = os.environ.get('IMAP_USER')
        self.imap_password = os.environ.get('IMAP_PASSWORD')
        
        self.names_data = self._load_names_data()
        
        # Усиленные фильтры мусора
        self.garbage_patterns = [
            r'^[а-яё]{1,15}ич$|^[а-яё]{1,15}на$',  # отчества
            r'@|mailto:|\.ru|\.com|http|www',        # email/web
            r'\d{4}|\+\d{2}:\d{2}|^\d+$',           # даты/числа
            r'^(от|кому|re|fwd)[:,\s]|^>+',         # email префиксы
            r'понедельник|вторник|среда|четверг|пятница|суббота|воскресенье',
            r'января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря'
        ]
        
        logger.info("✅ Исправленный экстрактор должностей инициализирован")
        logger.info(f"👥 Загружено ФИО: {len(self.names_data)}")
        logger.info("🎯 Новая стратегия: максимально полные должности")
    
    def _load_names_data(self):
        """Загружаем ФИО из предыдущего этапа"""
        names = []
        try:
            with open('name_extractor_log.txt', 'r', encoding='utf-8') as f:
                content = f.read()
            
            name_pattern = r'^\s*\d+\.\s+([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)*)\s*\([^)]+\)$'
            
            lines = content.split('\n')
            for line in lines:
                match = re.match(name_pattern, line.strip())
                if match:
                    name = match.group(1).strip()
                    if name not in names:
                        names.append(name)
            
            return names
        except FileNotFoundError:
            logger.warning("⚠️ Файл name_extractor_log.txt не найден")
            return []
    
    def find_complete_positions_for_names(self, text, email_subject="", email_date="", from_addr=""):
        """КАРДИНАЛЬНО НОВЫЙ поиск полных должностей"""
        results = []
        
        for name in self.names_data:
            positions = self._extract_complete_positions_near_name(text, name)
            
            for position_info in positions:
                results.append({
                    'name': name,
                    'position': position_info['position'],
                    'confidence': position_info['confidence'],
                    'method': position_info['method'],
                    'context': position_info['context'][:200],
                    'email_subject': email_subject,
                    'email_date': email_date,
                    'from_addr': from_addr
                })
        
        return results
    
    def _extract_complete_positions_near_name(self, text, name):
        """Извлечение ПОЛНЫХ должностей рядом с ФИО"""
        positions_found = []
        
        # Находим все вхождения имени
        name_lower = name.lower()
        text_lines = text.split('\n')
        
        for i, line in enumerate(text_lines):
            line_lower = line.lower()
            
            if name_lower in line_lower:
                # Анализируем контекст вокруг найденного имени
                context_lines = []
                
                # Берем ±3 строки вокруг имени
                start_idx = max(0, i - 3)
                end_idx = min(len(text_lines), i + 4)
                context_lines = text_lines[start_idx:end_idx]
                context_text = '\n'.join(context_lines)
                
                # Применяем методы поиска полных должностей
                methods = [
                    self._method_signature_block_analysis(context_text, name, i - start_idx),
                    self._method_multiline_job_assembly(context_text, name, i - start_idx),
                    self._method_contextual_expansion(context_text, name, i - start_idx)
                ]
                
                # Берем лучший результат
                for method_result in methods:
                    if method_result and self._is_complete_valid_position(method_result['position']):
                        positions_found.append(method_result)
                        break
        
        return positions_found
    
    def _method_signature_block_analysis(self, context, name, name_line_idx):
        """Анализ блоков подписей с поиском полных должностей"""
        
        lines = context.split('\n')
        name_line = lines[name_line_idx] if name_line_idx < len(lines) else ""
        
        # Паттерн 1: С уважением, ФИО, ДОЛЖНОСТЬ на следующих строках
        if 'с уважением' in context.lower():
            for i in range(name_line_idx + 1, min(len(lines), name_line_idx + 4)):
                if i < len(lines):
                    candidate = lines[i].strip()
                    if candidate and self._looks_like_complete_job_title(candidate):
                        return {
                            'position': self._clean_and_complete_position(candidate),
                            'confidence': 0.95,
                            'method': 'signature_block_post_name',
                            'context': context
                        }
        
        # Паттерн 2: ДОЛЖНОСТЬ перед ФИО
        if name_line_idx > 0:
            for i in range(max(0, name_line_idx - 3), name_line_idx):
                candidate = lines[i].strip()
                if candidate and self._looks_like_complete_job_title(candidate):
                    return {
                        'position': self._clean_and_complete_position(candidate),
                        'confidence': 0.90,
                        'method': 'signature_block_pre_name',
                        'context': context
                    }
        
        # Паттерн 3: ДОЛЖНОСТЬ | Организация (в той же строке что и ФИО)
        if '|' in name_line:
            parts = name_line.split('|')
            for part in parts:
                if name.lower() not in part.lower():
                    candidate = part.strip()
                    if self._looks_like_complete_job_title(candidate):
                        return {
                            'position': self._clean_and_complete_position(candidate),
                            'confidence': 0.85,
                            'method': 'signature_inline_pipe',
                            'context': context
                        }
        
        return None
    
    def _method_multiline_job_assembly(self, context, name, name_line_idx):
        """Сборка многострочных должностей"""
        
        lines = [line.strip() for line in context.split('\n') if line.strip()]
        
        # Ищем строки которые могут быть частями должности
        job_candidates = []
        
        for i, line in enumerate(lines):
            if i != name_line_idx and self._could_be_job_part(line):
                job_candidates.append((i, line))
        
        # Если есть кандидаты, пытаемся собрать полную должность
        if job_candidates:
            # Находим кандидатов близких к имени
            close_candidates = [
                (idx, line) for idx, line in job_candidates 
                if abs(idx - name_line_idx) <= 2
            ]
            
            if close_candidates:
                # Сортируем по близости к имени
                close_candidates.sort(key=lambda x: abs(x[0] - name_line_idx))
                
                # Пытаемся собрать полную должность
                assembled_position = self._assemble_complete_job_title(close_candidates, name_line_idx)
                
                if assembled_position:
                    return {
                        'position': assembled_position,
                        'confidence': 0.80,
                        'method': 'multiline_assembly',
                        'context': context
                    }
        
        return None
    
    def _method_contextual_expansion(self, context, name, name_line_idx):
        """Контекстное расширение коротких должностей до полных"""
        
        # Ищем базовые индикаторы должностей
        job_indicators = [
            'директор', 'руководитель', 'менеджер', 'специалист', 
            'начальник', 'заведующий', 'представитель', 'координатор'
        ]
        
        context_lower = context.lower()
        
        for indicator in job_indicators:
            if indicator in context_lower:
                # Находим полную должность с этим индикатором
                expanded = self._expand_job_contextually(context, indicator, name)
                
                if expanded and len(expanded) > len(indicator) + 5:  # Должность должна быть расширена
                    return {
                        'position': expanded,
                        'confidence': 0.75,
                        'method': 'contextual_expansion',
                        'context': context
                    }
        
        return None
    
    def _expand_job_contextually(self, context, base_indicator, name):
        """Умное расширение базового индикатора до полной должности"""
        
        # Находим позицию индикатора
        context_lower = context.lower()
        base_pos = context_lower.find(base_indicator.lower())
        
        if base_pos == -1:
            return None
        
        # Расширяем должность в обе стороны
        start = base_pos
        end = base_pos + len(base_indicator)
        
        # Расширяем влево (модификаторы)
        left_modifiers = [
            'региональный', 'ведущий', 'старший', 'главный', 'заместитель', 
            'зам.', 'исполняющий', 'первый', 'коммерческий', 'технический'
        ]
        
        expanded_left = self._expand_left(context, start, left_modifiers)
        if expanded_left is not None:
            start = expanded_left
        
        # Расширяем вправо (продолжения)
        right_extensions = [
            'по', 'отдела', 'департамента', 'группы', 'направления', 'снабжения',
            'продаж', 'развития', 'поставок', 'закупок', 'оборудования', 'проектам',
            'тендерам', 'клиентами', 'технологий', 'микробиологии', 'биотехнологий'
        ]
        
        expanded_right = self._expand_right(context, end, right_extensions)
        if expanded_right is not None:
            end = expanded_right
        
        # Извлекаем расширенную должность
        expanded = context[start:end].strip()
        
        # Дополнительная проверка на многострочность
        expanded = self._handle_multiline_in_expansion(context, start, end)
        
        return self._clean_and_complete_position(expanded)
    
    def _expand_left(self, context, start_pos, modifiers):
        """Расширение влево с поиском модификаторов"""
        
        original_start = start_pos
        
        # Ищем модификаторы перед должностью
        for modifier in modifiers:
            # Проверяем есть ли модификатор слева от текущей позиции
            search_start = max(0, start_pos - 50)  # Ищем в радиусе 50 символов
            left_context = context[search_start:start_pos].lower()
            
            modifier_pos = left_context.rfind(modifier)
            if modifier_pos != -1:
                # Нашли модификатор, сдвигаем начало
                new_start = search_start + modifier_pos
                if new_start < original_start:
                    return new_start
        
        return None
    
    def _expand_right(self, context, end_pos, extensions):
        """Расширение вправо с поиском продолжений"""
        
        original_end = end_pos
        current_pos = end_pos
        
        # Продолжаем расширение пока находим связанные слова
        while current_pos < len(context):
            # Пропускаем пробелы
            while current_pos < len(context) and context[current_pos].isspace():
                current_pos += 1
            
            if current_pos >= len(context):
                break
            
            # Ищем следующее слово
            word_start = current_pos
            while current_pos < len(context) and (context[current_pos].isalpha() or context[current_pos] in '-'):
                current_pos += 1
            
            if word_start < current_pos:
                word = context[word_start:current_pos].lower()
                
                if word in extensions:
                    # Нашли продолжение, продолжаем расширение
                    continue
                else:
                    # Не нашли продолжение, останавливаемся
                    break
            else:
                break
        
        return current_pos if current_pos > original_end else None
    
    def _handle_multiline_in_expansion(self, context, start, end):
        """Обработка многострочных должностей при расширении"""
        
        lines = context[start:end].split('\n')
        
        # Если должность многострочная, собираем её правильно
        if len(lines) > 1:
            job_parts = []
            for line in lines:
                cleaned_line = line.strip()
                if cleaned_line and not self._is_garbage_line(cleaned_line):
                    job_parts.append(cleaned_line)
            
            if job_parts:
                return ' '.join(job_parts)
        
        return context[start:end].strip()
    
    def _assemble_complete_job_title(self, candidates, name_line_idx):
        """Сборка полного названия должности из частей"""
        
        # Сортируем кандидатов по позиции относительно имени
        candidates.sort(key=lambda x: x[0])
        
        # Пытаемся собрать логичную должность
        job_parts = []
        
        for idx, line in candidates:
            cleaned = line.strip()
            if cleaned and not self._is_garbage_line(cleaned):
                # Проверяем, логично ли добавить эту часть
                if self._is_logical_job_part(cleaned, job_parts):
                    job_parts.append(cleaned)
        
        if job_parts:
            assembled = ' '.join(job_parts)
            return self._clean_and_complete_position(assembled)
        
        return None
    
    def _could_be_job_part(self, line):
        """Проверяет, может ли строка быть частью должности"""
        if not line or len(line.strip()) < 3:
            return False
        
        line_lower = line.lower().strip()
        
        # Содержит ключевые слова должностей
        job_keywords = [
            'директор', 'руководитель', 'менеджер', 'специалист', 'начальник',
            'заведующий', 'представитель', 'координатор', 'консультант', 'эксперт',
            'по', 'отдела', 'департамента', 'группы', 'снабжения', 'продаж'
        ]
        
        return any(keyword in line_lower for keyword in job_keywords)
    
    def _is_logical_job_part(self, part, existing_parts):
        """Проверяет логичность добавления части должности"""
        
        part_lower = part.lower()
        
        # Если это первая часть, принимаем любую разумную
        if not existing_parts:
            return self._looks_like_complete_job_title(part)
        
        # Если уже есть части, проверяем логичность добавления
        combined_lower = ' '.join(existing_parts).lower()
        
        # Логичные продолжения
        logical_continuations = [
            ('менеджер', ['по']),
            ('специалист', ['по']),
            ('представитель', ['в', 'по']),
            ('руководитель', ['отдела', 'департамента', 'группы']),
            ('начальник', ['отдела'])
        ]
        
        for base, continuations in logical_continuations:
            if base in combined_lower:
                if any(cont in part_lower for cont in continuations):
                    return True
        
        return False
    
    def _looks_like_complete_job_title(self, text):
        """Проверяет, выглядит ли текст как полная должность"""
        if not text or len(text.strip()) < 5:
            return False
        
        text_lower = text.lower().strip()
        
        # Не должно быть мусора
        if self._is_garbage_line(text):
            return False
        
        # Должно содержать индикаторы должности
        job_indicators = [
            'директор', 'руководитель', 'менеджер', 'специалист', 'начальник',
            'заведующий', 'представитель', 'координатор', 'консультант', 'врач'
        ]
        
        has_job_indicator = any(indicator in text_lower for indicator in job_indicators)
        
        # Или содержать характерные связки
        job_patterns = [
            r'по\s+\w+',  # "по продажам", "по закупкам"
            r'отдела\s+\w+',  # "отдела снабжения"
            r'в\s+[А-ЯЁ]{2,4}',  # "в СФО"
        ]
        
        has_job_pattern = any(re.search(pattern, text) for pattern in job_patterns)
        
        return has_job_indicator or has_job_pattern
    
    def _is_complete_valid_position(self, position):
        """Проверяет, является ли должность полной и валидной"""
        if not position or len(position.strip()) < 8:  # Минимум 8 символов для полной должности
            return False
        
        position_lower = position.lower().strip()
        
        # Не должна заканчиваться предлогами (признак обрезанности)
        ending_prepositions = ['по', 'в', 'для', 'от', 'к', 'на', 'с', 'у']
        words = position_lower.split()
        if words and words[-1] in ending_prepositions:
            return False
        
        # Не должна быть мусором
        if self._is_garbage_line(position):
            return False
        
        # Должна содержать осмысленное содержание
        return self._looks_like_complete_job_title(position)
    
    def _is_garbage_line(self, line):
        """Проверяет, является ли строка мусором"""
        if not line:
            return True
        
        line_clean = line.strip().lower()
        
        # Применяем все паттерны мусора
        for pattern in self.garbage_patterns:
            if re.search(pattern, line_clean):
                return True
        
        # Дополнительные проверки
        if len(line_clean) < 3:
            return True
        
        if line_clean in ['с уважением', 'добрый день', 'спасибо']:
            return True
        
        return False
    
    def _clean_and_complete_position(self, raw_position):
        """Очистка и завершение должности"""
        if not raw_position:
            return None
        
        # Базовая очистка
        cleaned = raw_position.strip()
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Убираем лишние символы в начале и конце
        cleaned = re.sub(r'^[^\w\u0400-\u04FF]+', '', cleaned)
        cleaned = re.sub(r'[^\w\u0400-\u04FF\s\-()«»""]+$', '', cleaned)
        
        # Убираем обрамляющие кавычки
        for quote_pair in [('"', '"'), ('«', '»'), ('„', '"')]:
            if cleaned.startswith(quote_pair[0]) and cleaned.endswith(quote_pair[1]):
                cleaned = cleaned[1:-1].strip()
        
        # Capitalize first letter if needed
        if cleaned and cleaned[0].islower():
            cleaned = cleaned[0].upper() + cleaned[1:]
        
        return cleaned.strip()
    
    def smart_deduplicate_results(self, raw_results):
        """Умная дедупликация с сохранением полных должностей"""
        
        # Группируем по нормализованным именам
        name_groups = defaultdict(list)
        
        for result in raw_results:
            normalized_name = self._normalize_name_for_grouping(result['name'])
            name_groups[normalized_name].append(result)
        
        deduplicated = []
        
        for normalized_name, group_results in name_groups.items():
            # Для каждой группы имен находим лучшие должности
            position_groups = defaultdict(list)
            
            for result in group_results:
                # Нормализуем должность для группировки, но сохраняем оригинал
                normalized_pos = self._normalize_position_for_grouping(result['position'])
                position_groups[normalized_pos].append(result)
            
            # Для каждой группы должностей выбираем лучший результат
            for normalized_pos, pos_results in position_groups.items():
                # Выбираем результат с максимальной confidence и самой полной должностью
                best_result = max(pos_results, key=lambda x: (x['confidence'], len(x['position'])))
                
                # Используем самые полные версии имени и должности
                best_result['name'] = max([r['name'] for r in pos_results], key=len)
                best_result['position'] = max([r['position'] for r in pos_results], key=len)
                
                deduplicated.append(best_result)
        
        return sorted(deduplicated, key=lambda x: x['name'])
    
    def _normalize_name_for_grouping(self, name):
        """Нормализация имени для группировки дубликатов"""
        # Приводим к нижнему регистру и убираем лишние пробелы
        normalized = re.sub(r'\s+', ' ', name.lower().strip())
        
        # Для группировки используем первые два слова (фамилия + имя)
        words = normalized.split()
        if len(words) >= 2:
            return f"{words[0]} {words[1]}"
        return normalized
    
    def _normalize_position_for_grouping(self, position):
        """Нормализация должности для группировки дубликатов"""
        normalized = position.lower().strip()
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Стандартизируем сокращения
        replacements = {
            'зам.': 'заместитель',
            'зав.': 'заведующий',
            'сфо': 'СФО',
            'кдл': 'КДЛ'
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        return normalized
    
    def _decode_header_clean(self, header_str):
        """Декодирует заголовки почты"""
        if not header_str:
            return ""
        try:
            from email.header import decode_header, make_header
            decoded = str(make_header(decode_header(header_str)))
            return decoded
        except:
            return header_str
    
    def _parse_email_date(self, date_str):
        """Парсит дату письма"""
        if not date_str:
            return "Дата неизвестна"
        try:
            dt = email.utils.parsedate_to_datetime(date_str)
            dt_adjusted = dt.replace(tzinfo=None) + timedelta(hours=4)
            return dt_adjusted.strftime('%d.%m.%Y %H:%M')
        except Exception:
            return date_str[:16] if len(date_str) > 16 else date_str
    
    def _extract_email_body_fast(self, msg):
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
    
    def test_single_date_detailed(self, date_str):
        """ИСПРАВЛЕННАЯ версия тестирования одной даты"""
        logger.info("=" * 80)
        logger.info(f"📋 ИСПРАВЛЕННЫЙ ПОИСК ДОЛЖНОСТЕЙ ЗА {date_str}")
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
            
            # ИСПРАВЛЕНИЕ: Всегда возвращаем нужную структуру
            if total_emails == 0:
                logger.info("❌ Писем не найдено!")
                mailbox.logout()
                return {
                    'date': date_str,
                    'total_emails': 0,
                    'emails_processed': 0,
                    'final_results': [],  # ← ИСПРАВЛЕНИЕ: добавляем обязательный ключ
                    'raw_count': 0,
                    'deduplicated_count': 0
                }
            
            all_results = []
            emails_processed = 0
            
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
                    
                    # НОВЫЙ УЛУЧШЕННЫЙ алгоритм поиска
                    email_results = self.find_complete_positions_for_names(body, subject, email_date, from_addr)
                    
                    if email_results:
                        all_results.extend(email_results)
                        emails_processed += 1
                        
                        logger.info(f"\n📧 Письмо {i}/{total_emails}: {email_date}")
                        logger.info(f"   📝 Тема: {subject[:60]}...")
                        logger.info(f"   👤 От: {from_addr[:50]}...")
                        logger.info("   📝 Найденные ПОЛНЫЕ должности:")
                        
                        for result in email_results:
                            logger.info(f"   ✅ {result['name']}")
                            logger.info(f"    ▶️ {result['position']}")
                            logger.info(f"       📊 {result['confidence']:.2f} | {result['method']}")
                
                except Exception as e:
                    logger.error(f"❌ Ошибка письма {i}: {e}")
                    continue
            
            mailbox.logout()
            
            # Умная дедупликация
            logger.info(f"\n🔄 Применяем улучшенную дедупликацию...")
            logger.info(f"   📊 Сырых результатов: {len(all_results)}")
            
            final_results = self.smart_deduplicate_results(all_results)
            logger.info(f"   📊 Финальных результатов: {len(final_results)}")
            
            logger.info("=" * 80)
            logger.info(f"📊 РЕЗУЛЬТАТЫ ЗА {date_str}")
            logger.info("=" * 80)
            logger.info(f"📬 Всего писем: {total_emails}")
            logger.info(f"📋 Писем с должностями: {emails_processed}")
            logger.info(f"🎯 Финальных уникальных пар ФИО+Должность: {len(final_results)}")
            
            if final_results:
                logger.info(f"\n📋 ФИНАЛЬНЫЕ ПОЛНЫЕ ДОЛЖНОСТИ ЗА {date_str}:")
                for i, result in enumerate(final_results, 1):
                    logger.info(f"   {i:2d}. {result['name']} — {result['position']}")
                    logger.info(f"       📊 {result['confidence']:.2f} | {result['method']}")
            
            logger.info("=" * 80)
            
            return {
                'date': date_str,
                'total_emails': total_emails,
                'emails_processed': emails_processed,
                'final_results': final_results,  # ← Всегда присутствует
                'raw_count': len(all_results),
                'deduplicated_count': len(final_results)
            }
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка для {date_str}: {e}")
            # ИСПРАВЛЕНИЕ: Даже при ошибке возвращаем правильную структуру
            return {
                'date': date_str,
                'total_emails': 0,
                'emails_processed': 0,
                'final_results': [],
                'raw_count': 0,
                'deduplicated_count': 0
            }
    
    def test_date_range_detailed(self, start_date, end_date):
        """ИСПРАВЛЕННАЯ версия тестирования диапазона дат"""
        logger.info("=" * 80)
        logger.info(f"🚀 ИСПРАВЛЕННЫЙ ПОИСК ПОЛНЫХ ДОЛЖНОСТЕЙ: {start_date} - {end_date}")
        logger.info("💡 Исправления: полные должности + устранение KeyError")
        logger.info("=" * 80)
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_dt
        
        all_final_results = []
        all_daily_results = []
        total_emails_all = 0
        total_days = (end_dt - start_dt).days + 1
        
        day_counter = 1
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            
            logger.info(f"\n🎯 ДЕНЬ {day_counter}/{total_days}: {date_str}")
            logger.info("=" * 50)
            
            daily_results = self.test_single_date_detailed(date_str)
            
            # ИСПРАВЛЕНИЕ: Проверяем daily_results и наличие final_results
            if daily_results:
                total_emails_all += daily_results.get('total_emails', 0)
                final_results = daily_results.get('final_results', [])
                if final_results:  # Только если есть результаты
                    all_final_results.extend(final_results)
                all_daily_results.append(daily_results)
            
            current_date += timedelta(days=1)
            day_counter += 1
        
        # Финальная дедупликация по всему периоду
        if all_final_results:
            logger.info(f"\n🔄 Финальная дедупликация по всему периоду...")
            period_final = self.smart_deduplicate_results(all_final_results)
        else:
            period_final = []
        
        logger.info("=" * 80)
        logger.info(f"📊 ИТОГОВЫЙ ОТЧЕТ {start_date} - {end_date}")
        logger.info("=" * 80)
        logger.info(f"📅 Протестировано дней: {total_days}")
        logger.info(f"📬 Всего писем: {total_emails_all}")
        logger.info(f"🎯 Итоговых уникальных пар ФИО+Должность: {len(period_final)}")
        
        if period_final:
            logger.info(f"\n📋 ИТОГОВЫЕ ПОЛНЫЕ ДОЛЖНОСТИ ЗА ПЕРИОД:")
            for i, result in enumerate(period_final, 1):
                logger.info(f"   {i:3d}. {result['name']} — {result['position']}")
                logger.info(f"        📊 confidence: {result['confidence']:.2f} | method: {result['method']}")
        
        logger.info("\n📊 СТАТИСТИКА ПО ДНЯМ:")
        for day_result in all_daily_results:
            final_count = len(day_result.get('final_results', []))
            logger.info(f"   📅 {day_result['date']}: {day_result.get('total_emails', 0)} писем → {final_count} полных должностей")
        
        logger.info("=" * 80)
        logger.info("✅ ИСПРАВЛЕННЫЙ ПОИСК ПОЛНЫХ ДОЛЖНОСТЕЙ ЗАВЕРШЕН")
        logger.info("=" * 80)
        
        # Сохраняем результаты
        with open('fixed_position_results.json', 'w', encoding='utf-8') as f:
            json.dump({
                'period': f"{start_date} - {end_date}",
                'total_days': total_days,
                'total_emails': total_emails_all,
                'final_unique_pairs': len(period_final),
                'results': period_final,
                'daily_summary': all_daily_results
            }, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info("💾 Результаты сохранены в fixed_position_results.json")
        
        return period_final

def main():
    """Главная функция исправленного экстрактора"""
    
    logger.info("🚀 ЗАПУСК ИСПРАВЛЕННОГО ЭКСТРАКТОРА ПОЛНЫХ ДОЛЖНОСТЕЙ v4.0")
    logger.info("💡 Исправления: KeyError устранен + поиск ПОЛНЫХ должностей!")
    
    extractor = FixedPositionExtractor()
    
    # Настройка дат
    start_date = '2025-07-29'
    end_date = '2025-08-04'
    
    results = extractor.test_date_range_detailed(start_date, end_date)
    
    logger.info(f"\n🎉 ИСПРАВЛЕННЫЙ ПОИСК ЗАВЕРШЕН!")
    logger.info(f"🎯 Найдено полных должностей: {len(results) if results else 0}")

if __name__ == "__main__":
    main()
