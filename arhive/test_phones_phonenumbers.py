#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import imaplib
import email
import ssl
import os
from dotenv import load_dotenv
from datetime import datetime
from typing import List
import phonenumbers
from phonenumbers import NumberParseException

load_dotenv()

class PhoneExtractorPhoneNumbers:
    """Тестер телефонов с использованием библиотеки phonenumbers"""
    
    def __init__(self):
        self.imap_server = os.environ.get('IMAP_SERVER')
        self.imap_port = int(os.environ.get('IMAP_PORT', 143))
        self.imap_user = os.environ.get('IMAP_USER')
        self.imap_password = os.environ.get('IMAP_PASSWORD')
        
        print("✅ Тестер с библиотекой phonenumbers инициализирован")
    
    def extract_phones_only(self, text: str) -> List[str]:
        """ГЛАВНАЯ ФУНКЦИЯ: Извлекает телефоны с помощью phonenumbers"""
        
        if not text or not isinstance(text, str):
            return []
        
        phones = []
        
        # Предобработка: исправляем научную нотацию
        text = self._preprocess_text(text)
        
        try:
            # Используем PhoneNumberMatcher для России
            for match in phonenumbers.PhoneNumberMatcher(text, "RU"):
                phone_number = match.number
                
                # Проверяем, что это валидный номер
                if phonenumbers.is_valid_number(phone_number):
                    # Форматируем в нужный вид
                    formatted_phone = self._format_phone(phone_number, text)
                    
                    # Проверяем, что это не ИНН/КПП (дополнительная защита)
                    if self._is_real_phone(formatted_phone, text) and formatted_phone not in phones:
                        phones.append(formatted_phone)
                        
        except Exception as e:
            print(f"⚠️ Ошибка извлечения телефонов: {e}")
        
        return phones
    
    def _preprocess_text(self, text: str) -> str:
        """Предобработка текста"""
        # Исправляем научную нотацию
        def fix_scientific(match):
            try:
                return str(int(float(match.group(0))))
            except:
                return match.group(0)
        
        text = re.sub(r'\d+\.\d+e[+-]?\d+', fix_scientific, text, flags=re.IGNORECASE)
        return text
    
    def _format_phone(self, phone_number, original_text: str) -> str:
        """Форматирует телефон в нужный вид: +7 (XXX) XXX-XX-XX"""
        
        # Базовый формат
        formatted = phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        
        # Приводим к нужному формату +7 (XXX) XXX-XX-XX
        if formatted.startswith('+7 '):
            # Извлекаем цифры после +7
            digits = re.sub(r'\D', '', formatted[3:])
            if len(digits) >= 10:
                code = digits[:3]
                num = digits[3:]
                if len(num) >= 7:
                    base_formatted = f'+7 ({code}) {num[:3]}-{num[3:5]}-{num[5:7]}'
                    
                    # Ищем добавочный номер в оригинальном тексте
                    extension = self._extract_extension(original_text, formatted)
                    return base_formatted + extension
        
        return formatted
    
    def _extract_extension(self, original_text: str, phone_in_text: str) -> str:
        """Извлекает добавочный номер из оригинального текста"""
        
        # Ищем паттерны добавочных номеров рядом с телефоном
        extension_patterns = [
            r'доб\.?\s*(\d+)',
            r'ext\.?\s*(\d+)',
            r'добавочный\s+(\d+)'
        ]
        
        for pattern in extension_patterns:
            matches = re.finditer(pattern, original_text, re.IGNORECASE)
            for match in matches:
                return f' (доб. {match.group(1)})'
        
        return ""
    
    def _is_real_phone(self, phone: str, original_text: str) -> bool:
        """Дополнительная проверка на ИНН/КПП"""
        
        # Извлекаем цифры
        digits = re.sub(r'\D', '', phone)
        
        # Если длина соответствует ИНН/КПП, проверяем контекст
        if len(digits) in [9, 10, 12]:
            # Ищем телефонные индикаторы в тексте
            phone_indicators = ['тел', 'моб', 'факс', '+7', '8 (', 'телефон', 'т.']
            text_lower = original_text.lower()
            has_phone_context = any(indicator in text_lower for indicator in phone_indicators)
            
            # Если нет телефонного контекста, это может быть ИНН
            if not has_phone_context:
                return False
        
        return True
    
    def _extract_email_body(self, msg) -> str:
        """Извлекает тело письма"""
        body = ""
        
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        body = part.get_payload(decode=True).decode(charset, errors="ignore")
                        break
                    except Exception:
                        continue
        else:
            charset = msg.get_content_charset() or 'utf-8'
            try:
                body = msg.get_payload(decode=True).decode(charset, errors="ignore")
            except Exception:
                body = ""
        
        return body.strip()
    
    def test_phones_for_date(self, date_str: str, max_emails: int = 10):
        """Тестирует извлечение телефонов за конкретную дату"""
        
        print(f"\n{'='*60}")
        print(f"📞 ТЕСТ PHONENUMBERS ЗА {date_str}")
        print(f"{'='*60}")
        
        try:
            mailbox = imaplib.IMAP4(self.imap_server, self.imap_port)
            mailbox.starttls(ssl.create_default_context())
            mailbox.login(self.imap_user, self.imap_password)
            mailbox.select('INBOX')
            
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            imap_date = dt.strftime('%d-%b-%Y')
            criteria = f'(ON "{imap_date}")'
            
            status, data = mailbox.search(None, criteria)
            mail_ids = data[0].split() if status == 'OK' else []
            
            print(f"📬 Найдено писем: {len(mail_ids)}")
            
            all_phones = []
            processed_emails = 0
            
            for i, mail_id in enumerate(mail_ids):
                if processed_emails >= max_emails:
                    break
                
                try:
                    status, msg_data = mailbox.fetch(mail_id, '(RFC822)')
                    if status != 'OK':
                        continue
                    
                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)
                    
                    subject = msg.get('Subject', 'Без темы')
                    from_addr = msg.get('From', 'Неизвестно')
                    body = self._extract_email_body(msg)
                    
                    phones = self.extract_phones_only(body)
                    
                    if phones:
                        print(f"\n📧 Письмо {i+1}: {subject[:50]}...")
                        print(f"👤 От: {from_addr}")
                        print(f"📞 Найденные телефоны:")
                        for phone in phones:
                            print(f"   ✅ {phone}")
                            if phone not in all_phones:
                                all_phones.append(phone)
                    else:
                        print(f"\n📧 Письмо {i+1}: {subject[:50]}... (БЕЗ ТЕЛЕФОНОВ)")
                    
                    processed_emails += 1
                    
                except Exception as e:
                    print(f"❌ Ошибка обработки письма {i+1}: {e}")
                    continue
            
            mailbox.logout()
            
            print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА ЗА {date_str}:")
            print(f"   📬 Обработано писем: {processed_emails}")
            print(f"   📞 Уникальных телефонов: {len(all_phones)}")
            
            if all_phones:
                print(f"\n📋 ВСЕ НАЙДЕННЫЕ ТЕЛЕФОНЫ:")
                for i, phone in enumerate(all_phones, 1):
                    print(f"   {i:2d}. {phone}")
            else:
                print(f"\n❌ Телефоны не найдены!")
            
            return all_phones
            
        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
            return []
    
    def test_sample_text(self, text: str, description: str = "Тестовый текст"):
        """Тестирует функцию на образце текста"""
        
        print(f"\n{'='*60}")
        print(f"🧪 ТЕСТ НА ОБРАЗЦЕ: {description}")
        print(f"{'='*60}")
        
        print(f"📝 Исходный текст:")
        print(f"   {text}")
        
        phones = self.extract_phones_only(text)
        
        print(f"\n📞 Найденные телефоны: {len(phones)}")
        for phone in phones:
            print(f"   ✅ {phone}")
        
        return phones


def main():
    """Основная функция для тестирования phonenumbers"""
    
    print("=== 📞 ТЕСТЕР С БИБЛИОТЕКОЙ PHONENUMBERS ===")
    
    tester = PhoneExtractorPhoneNumbers()
    
    # 🎯 Все тестовые образцы
    test_samples = [
        ("Телефон: +7-913-399-32-72", "Дефисный формат из логов"),
        ("8 800-770-71-21, доб.1315", "8-ка с добавочным"),
        ("Тел. +7 (495) 933 71 47 (48), доб.171", "Сложный формат с доб."),
        ("ИНН 5408287373, тел: 8 (3852) 291-295", "С ИНН"),
        ("КПП 540801001, ОГРН 1115476045871, моб. +7 (913) 930-03-26", "С КПП и ОГРН"),
        ("Руководитель ОМТС: Бабиченко Иван Сергеевич\n☎ Телефон: +7-913-399-32-72", "Из реальной подписи"),
        ("тел.: +7 (913) 928-12-94\nТелефон.: +7 (905) 952-20-20, +7 (383) 380-21-04", "Множественные телефоны"),
        ("т. +7-923-101 7014", "т. с пробелами"),
        ("8 995 101-76-30", "8 с пробелами без скобок"),
        ("5408287373", "Чистый ИНН (должен игнорироваться)"),
        ("540801001", "Чистый КПП (должен игнорироваться)"),
        ("1115476045871", "Чистый ОГРН (должен игнорироваться)")
    ]
    
    success_count = 0
    total_count = len(test_samples)
    
    for text, desc in test_samples:
        print(f"\n{'='*60}")
        print(f"🧪 ТЕСТ: {desc}")
        print(f"{'='*60}")
        
        phones = tester.test_sample_text(text, desc)
        
        # Простая оценка: если нашел хотя бы один телефон и не должен игнорировать
        should_find = not ("игнорироваться" in desc)
        found_phones = len(phones) > 0
        
        if should_find and found_phones:
            print(f"   ✅ ТЕСТ ПРОЙДЕН: Нашел телефоны")
            success_count += 1
        elif not should_find and not found_phones:
            print(f"   ✅ ТЕСТ ПРОЙДЕН: Правильно проигнорировал")
            success_count += 1
        elif should_find and not found_phones:
            print(f"   ❌ ТЕСТ ПРОВАЛЕН: Не нашел телефоны")
        else:
            print(f"   ⚠️ ТЕСТ ЧАСТИЧНЫЙ: Неожиданный результат")
    
    print(f"\n🎯 ИТОГОВАЯ ОЦЕНКА: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")
    
    if success_count >= total_count * 0.9:  # 90% успеха
        print(f"🎉 PHONENUMBERS ТЕСТИРОВАНИЕ УСПЕШНО! Можно переходить к реальной почте!")
        return True
    else:
        print(f"❌ ТЕСТИРОВАНИЕ ТРЕБУЕТ ДОРАБОТКИ.")
        return False


if __name__ == "__main__":
    success = main()
    
    if success:
        print(f"\n🚀 СЛЕДУЮЩИЙ ШАГ: Тестирование на реальной почте")
        print(f"Запуск: tester.test_phones_for_date('2025-07-29', max_emails=5)")
