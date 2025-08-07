#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import imaplib
import email
import ssl
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List
import phonenumbers
import time

load_dotenv()

class PhoneExtractorPerfect:
    """Идеальный тестер с исправлением всех найденных проблем"""
    
    def __init__(self):
        self.imap_server = os.environ.get('IMAP_SERVER')
        self.imap_port = int(os.environ.get('IMAP_PORT', 143))
        self.imap_user = os.environ.get('IMAP_USER')
        self.imap_password = os.environ.get('IMAP_PASSWORD')
        
        # Мобильные коды России
        self.mobile_codes = set([
            '910', '912', '913', '914', '915', '916', '917', '918', '919',
            '920', '921', '922', '923', '924', '925', '926', '927', '928', '929',
            '930', '931', '932', '933', '934', '936', '937', '938', '939',
            '950', '951', '952', '953', '954', '955', '956', '957', '958', '959',
            '960', '961', '962', '963', '964', '965', '966', '967', '968', '969',
            '980', '981', '982', '983', '984', '985', '986', '987', '988', '989', '999'
        ])
        
        print("✅ Идеальный тестер с исправлением всех проблем инициализирован")
    
    def extract_phones_only(self, text: str) -> List[str]:
        """Извлекает телефоны с исправлением всех 4 проблем"""
        
        if not text or not isinstance(text, str):
            return []
        
        phones = []
        
        # Ограничиваем текст для ускорения
        text = text[:15000]
        text = self._preprocess_text(text)
        
        try:
            # Сначала ищем основные номера
            base_phones = []
            for match in phonenumbers.PhoneNumberMatcher(text, "RU"):
                phone_number = match.number
                
                if phonenumbers.is_valid_number(phone_number):
                    start_pos = match.start
                    end_pos = match.end
                    
                    # Локальный контекст только для этого номера
                    local_context_start = max(0, start_pos - 30)
                    local_context_end = min(len(text), end_pos + 50)
                    local_context = text[local_context_start:local_context_end]
                    
                    # Обрабатываем номер с его локальным контекстом
                    phone_variants = self._process_single_phone(
                        phone_number, local_context, start_pos, end_pos
                    )
                    
                    base_phones.extend(phone_variants)
            
            # 🔧 ИСПРАВЛЕНИЕ 3: Обрабатываем номера через запятую
            comma_phones = self._extract_comma_separated_phones(text)
            base_phones.extend(comma_phones)
            
            # 🔧 КРИТИЧНО: Постобработка мобильных номеров
            phones = self._postprocess_mobile_phones(base_phones)
                        
        except Exception as e:
            print(f"⚠️ Ошибка извлечения телефонов: {e}")
        
        return phones
    
    def _process_single_phone(self, phone_number, local_context: str, start_pos: int, end_pos: int) -> List[str]:
        """Обрабатывает один номер с его локальным контекстом"""
        
        # Форматируем базовый номер
        base_formatted = self._format_phone_russian(phone_number)
        
        phones = []
        
        # 🔧 ИСПРАВЛЕНИЕ 4: Ищем варианты только в непосредственной близости
        variant_in_same_number = self._extract_local_variants(local_context, base_formatted)
        
        # 🔧 ИСПРАВЛЕНИЕ 1,2: Ищем добавочные с расширенными ключевыми словами
        extensions = self._extract_extensions_improved(local_context)
        
        # Добавляем основной номер
        if extensions and not self._is_mobile_phone(base_formatted):
            ext_str = ", ".join(extensions)
            phones.append(f"{base_formatted} ({ext_str})")
        else:
            phones.append(base_formatted)
        
        # Добавляем варианты (только для городских номеров)
        for variant in variant_in_same_number:
            if extensions and not self._is_mobile_phone(variant):
                ext_str = ", ".join(extensions)
                phones.append(f"{variant} ({ext_str})")
            else:
                phones.append(variant)
        
        return phones
    
    def _extract_local_variants(self, local_context: str, base_number: str) -> List[str]:
        """Извлекает варианты только из непосредственного контекста номера"""
        
        variants = []
        
        # Ищем паттерн с вариантами цифр ТОЛЬКО в том же номере
        # Пример: "+7 (495) 933 71 47 (48)" - (48) относится к этому номеру
        variant_pattern = r'\+7\s*\(\d{3}\)\s*\d{3}[-\s]*\d{2}[-\s]*\d{2}\s*\((\d{1,2})\)'
        variant_match = re.search(variant_pattern, local_context)
        
        if variant_match:
            variant_digits = variant_match.group(1)
            
            # Создаем вариант только если это городской номер
            if not self._is_mobile_phone(base_number):
                variant_number = self._create_variant_number(base_number, variant_digits)
                variants.append(variant_number)
        
        return variants
    
    def _extract_comma_separated_phones(self, text: str) -> List[str]:
        """🔧 ИСПРАВЛЕНИЕ 3: Обрабатывает номера вида +7(38822)6-43-63, 6-43-65"""
        
        phones = []
        
        # Ищем паттерн: основной номер + запятая + короткие цифры
        pattern = r'\+7\s*\((\d{3,5})\)\s*(\d{1,3})[-\s]*(\d{2})[-\s]*(\d{2})\s*,\s*(\d{1,3})[-\s]*(\d{2})[-\s]*(\d{2})'
        
        matches = re.finditer(pattern, text)
        
        for match in matches:
            code_raw = match.group(1)
            first_part = match.group(2)
            first_mid = match.group(3)
            first_end = match.group(4)
            second_part = match.group(5)
            second_mid = match.group(6)
            second_end = match.group(7)
            
            # Определяем правильный код (первые 3 цифры из 5-значного кода)
            if len(code_raw) == 5:
                real_code = code_raw[:3]
                first_part = code_raw[3:] + first_part
            else:
                real_code = code_raw
            
            # Формируем оба номера
            first_number = f"+7 ({real_code}) {first_part}-{first_mid}-{first_end}"
            second_number = f"+7 ({real_code}) {second_part}-{second_mid}-{second_end}"
            
            phones.extend([first_number, second_number])
        
        return phones
    
    def _extract_extensions_improved(self, context_text: str) -> List[str]:
        """🔧 ИСПРАВЛЕНИЕ 1,2: Расширенный поиск добавочных номеров"""
        
        extensions = []
        
        # 🔧 РАСШИРЕННЫЕ паттерны для добавочных номеров
        extension_patterns = [
            r'доб\.?\s*(\d{1,5})',
            r'доп\.?\s*(\d{1,5})',           # Добавлено: доп.
            r'добавочный\s+(\d{1,5})',
            r'ext\.?\s*(\d{1,5})',
            r'вн\.?\s*(\d{1,5})',            # Добавлено: вн.
            r'в\.?\s*н\.?\s*(\d{1,5})'       # Добавлено: в.н.
        ]
        
        # Ищем в контексте
        for pattern in extension_patterns:
            matches = re.finditer(pattern, context_text, re.IGNORECASE)
            for match in matches:
                ext_num = match.group(1)
                ext_formatted = f"доб. {ext_num}"
                if ext_formatted not in extensions:
                    extensions.append(ext_formatted)
        
        return extensions
    
    def _postprocess_mobile_phones(self, phones: List[str]) -> List[str]:
        """Убирает добавочные номера с мобильных телефонов"""
        
        processed = []
        seen = set()
        
        for phone in phones:
            if self._is_mobile_phone(phone):
                # Это мобильный - убираем добавочные
                phone_clean = re.sub(r'\s*\([^)]*доб\.[^)]*\)', '', phone)
                if phone_clean not in seen:
                    processed.append(phone_clean)
                    seen.add(phone_clean)
            else:
                # Это городской - оставляем как есть
                if phone not in seen:
                    processed.append(phone)
                    seen.add(phone)
        
        return processed
    
    def _is_mobile_phone(self, phone: str) -> bool:
        """Проверяет, является ли номер мобильным"""
        
        code_match = re.search(r'\+7\s*\((\d{3})\)', phone)
        code = code_match.group(1) if code_match else None
        return code in self.mobile_codes
    
    def _create_variant_number(self, base_number: str, variant_digits: str) -> str:
        """Создает вариант номера с замененными последними цифрами"""
        
        variant_formatted = variant_digits.zfill(2)
        return re.sub(r'-(\d{2})$', f'-{variant_formatted}', base_number)
    
    def _format_phone_russian(self, phone_number) -> str:
        """Форматирует в русский стиль: +7 (XXX) XXX-XX-XX"""
        
        international = phonenumbers.format_number(
            phone_number, 
            phonenumbers.PhoneNumberFormat.INTERNATIONAL
        )
        
        if international.startswith('+7 '):
            digits = re.sub(r'\D', '', international[3:])
            if len(digits) >= 10:
                code = digits[:3]
                num = digits[3:]
                if len(num) >= 7:
                    return f'+7 ({code}) {num[:3]}-{num[3:5]}-{num[5:7]}'
        
        return international
    
    def _preprocess_text(self, text: str) -> str:
        """Предобработка текста"""
        def fix_scientific(match):
            try:
                return str(int(float(match.group(0))))
            except:
                return match.group(0)
        
        text = re.sub(r'\d+\.\d+e[+-]?\d+', fix_scientific, text, flags=re.IGNORECASE)
        return text
    
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
    
    def test_real_email_perfect(self, date_str: str = '2025-07-29'):
        """🎯 ИДЕАЛЬНЫЙ ТЕСТ: Все проблемы исправлены"""
        
        print(f"\n{'='*80}")
        print(f"📞 ИДЕАЛЬНЫЙ ТЕСТ PHONENUMBERS ЗА {date_str}")
        print(f"{'='*80}")
        
        try:
            # Подключение
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
            print(f"📬 Всего писем за {date_str}: {total_emails}")
            
            if total_emails == 0:
                print("❌ Писем не найдено!")
                return
            
            # Обработка писем
            emails_with_phones = []
            all_phones = set()
            
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
                    
                    # Ищем телефоны с идеальной обработкой
                    phones = self.extract_phones_only(body)
                    
                    if phones:
                        emails_with_phones.append({
                            'number': i,
                            'subject': subject,
                            'from': from_addr,
                            'date': email_date,
                            'phones': phones
                        })
                        
                        all_phones.update(phones)
                    
                except Exception as e:
                    print(f"❌ Ошибка письма {i}: {e}")
                    continue
            
            mailbox.logout()
            
            # Итоговый отчет
            total_time = time.time() - start_time
            
            print(f"\n{'='*80}")
            print(f"📊 ИДЕАЛЬНЫЕ РЕЗУЛЬТАТЫ")
            print(f"{'='*80}")
            print(f"📬 Всего писем: {total_emails}")
            print(f"📞 Писем с телефонами: {len(emails_with_phones)}")
            print(f"🎯 Уникальных телефонов: {len(all_phones)}")
            print(f"⏱️ Время обработки: {total_time:.1f} сек")
            
            if emails_with_phones:
                print(f"\n{'='*80}")
                print(f"📞 ПИСЬМА С ТЕЛЕФОНАМИ:")
                print(f"{'='*80}")
                
                for email_info in emails_with_phones:
                    print(f"\n📧 Письмо {email_info['number']}/{total_emails}: {email_info['date']}")
                    print(f"   📝 Тема: {email_info['subject']}")
                    print(f"   👤 От: {email_info['from']}")
                    print(f"   📞 Телефоны:")
                    for phone in email_info['phones']:
                        print(f"      ✅ {phone}")
                
                print(f"\n{'='*80}")
                print(f"📋 ВСЕ УНИКАЛЬНЫЕ ТЕЛЕФОНЫ:")
                print(f"{'='*80}")
                for i, phone in enumerate(sorted(all_phones), 1):
                    print(f"   {i:2d}. {phone}")
            
            else:
                print(f"\n❌ Телефонов не найдено!")
            
            print(f"\n{'='*80}")
            print(f"✅ ИДЕАЛЬНЫЙ ТЕСТ ЗАВЕРШЕН")
            print(f"{'='*80}")
            
            return {
                'total_emails': total_emails,
                'emails_with_phones': len(emails_with_phones),
                'unique_phones': len(all_phones),
                'phones_list': list(all_phones)
            }
            
        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
            return None


def main():
    """Главная функция"""
    
    print("🚀 ЗАПУСК ИДЕАЛЬНОГО ТЕСТА")
    
    tester = PhoneExtractorPerfect()
    
    # 🎯 ИДЕАЛЬНЫЙ ТЕСТ за 29.07.2025 - 04.08.2025
    results = tester.test_date_range('2025-07-29', '2025-08-04')
    
    if results:
        print(f"\n🎉 ИДЕАЛЬНЫЙ тест завершен!")
        print(f"📞 Найдено {results['emails_with_phones']} писем с телефонами")
        print(f"🎯 Всего уникальных номеров: {results['unique_phones']}")


if __name__ == "__main__":
    main()
