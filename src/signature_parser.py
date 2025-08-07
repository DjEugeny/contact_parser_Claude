import re
import phonenumbers
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ContactInfo:
    """Структура для хранения извлечённых контактных данных"""
    fio: str = ""
    position: str = ""
    company: str = ""
    email: str = ""
    phones: List[str] = None
    address: str = ""
    city: str = ""
    inn: str = ""
    
    def __post_init__(self):
        if self.phones is None:
            self.phones = []


class SignatureParser:
    """Парсер подписей из деловых писем"""
    
    def __init__(self):
        # Паттерны для поиска границ подписи
        self.signature_separators = [
            r'С уважением,?\s*',
            r'Best regards,?\s*',
            r'Всего доброго,?\s*',
            r'С наилучшими пожеланиями,?\s*',
            r'---+',
            r'___+', 
            r'–{2,}',
            r'={3,}',
            r'Отправлено с\s+',
            r'Sent from\s+',
        ]
        
        # ИСПРАВЛЕННЫЕ паттерны для телефонов с точным форматированием
        self.phone_patterns = [
            # ИСПРАВЛЕНО: Тел. +7 (495) 640-17-71 (доб. 2036)
            {
                'pattern': r'[Тт]ел\.?\s*(?P<country>\+7|8)?\s*\((?P<code>\d{3,5})\)\s*(?P<num1>\d{2,3})-(?P<num2>\d{2})-(?P<num3>\d{2})(?:\s*\(доб\.?\s*(?P<ext>\d+)\))?',
                'type': 'city_with_prefix'
            },
            
            # Тел.(3952)78-25-79, доб. 121
            {
                'pattern': r'[Тт]ел\.?\s*\((?P<code>\d{2,5})\)\s*(?P<num1>\d{2,3})-?(?P<num2>\d{2,3})-?(?P<num3>\d{2,3})(?:,?\s*доб\.?\s*(?P<ext>\d+))?',
                'type': 'city'
            },
            
            # 8-3852-50-40-38
            {
                'pattern': r'8-(?P<code>\d{3,4})-(?P<num1>\d{2,3})-(?P<num2>\d{2,3})-(?P<num3>\d{2,3})',
                'type': 'city'
            },
            
            # +7 (495) 640-17-71 (доб. 2036) БЕЗ префикса "Тел"
            {
                'pattern': r'(?P<country>\+7|8)[\s\-]*\((?P<code>\d{3})\)[\s\-]*(?P<num1>\d{3})[\s\-]*(?P<num2>\d{2})[\s\-]*(?P<num3>\d{2})(?:[\s\(]*доб\.?\s*(?P<ext>\d+)\)?)?',
                'type': 'mobile'
            },
            
            # +79778842779 (слитно мобильный)
            {
                'pattern': r'(?P<country>\+7)(?P<code>\d{3})(?P<num1>\d{3})(?P<num2>\d{2})(?P<num3>\d{2})',
                'type': 'mobile'
            },
            
            # 8 (385-2) 29-81-12 (городской с дефисом в коде)
            {
                'pattern': r'8\s*\((?P<code1>\d{2,3})-(?P<code2>\d{1,3})\)\s*(?P<num1>\d{2,3})-(?P<num2>\d{2,3})-(?P<num3>\d{2,3})',
                'type': 'city_complex'
            },
            
            # +7 913 245 50 71 (с пробелами)
            {
                'pattern': r'(?P<country>\+7|8)[\s]+(?P<code>\d{3})[\s]+(?P<num1>\d{3})[\s]+(?P<num2>\d{2})[\s]+(?P<num3>\d{2})',
                'type': 'mobile'
            },
        ]
        
        # УЛУЧШЕННЫЕ паттерны для ИНН
        self.inn_patterns = [
            r'ИНН:?\s*(\d{10}|\d{12})',
            r'инн:?\s*(\d{10}|\d{12})',
            r'\bИНН\b.*?(\d{10}|\d{12})',
        ]
        
        # Улучшенный паттерн для email
        self.email_pattern = r'[\w\.\-]+@[\w\.\-]+\.[a-zA-Z]{2,}'


    def extract_signature_block(self, email_body: str) -> str:
        """Извлекает блок подписи из письма"""
        
        # Ищем разделители подписи
        for separator in self.signature_separators:
            match = re.search(separator, email_body, re.IGNORECASE | re.MULTILINE)
            if match:
                signature_start = match.end()
                signature_block = email_body[signature_start:].strip()
                if signature_block:
                    return signature_block
        
        # Если разделители не найдены, ищем контактные строки
        lines = email_body.strip().split('\n')
        if len(lines) > 5:
            contact_lines = []
            for line in lines:
                line_lower = line.lower()
                if any(marker in line_lower for marker in ['тел', 'email', 'моб', '+7', '8-', 'инн']):
                    contact_lines.append(line)
            
            if contact_lines:
                first_contact_idx = max(0, lines.index(contact_lines[0]) - 2)
                signature_block = '\n'.join(lines[first_contact_idx:])
                return signature_block
            
            signature_block = '\n'.join(lines[-8:])
            return signature_block
        
        return email_body


    def extract_phones(self, text: str) -> List[str]:
        """ИСПРАВЛЕННАЯ: Извлекает телефоны с дедупликацией"""
        phones = []
        seen_base_numbers = set()  # Для отслеживания базовых номеров
        
        for phone_config in self.phone_patterns:
           pattern = phone_config['pattern']
           phone_type = phone_config['type']
        
           matches = re.finditer(pattern, text, re.IGNORECASE)
           for match in matches:
                try:
                    formatted_phone = self._format_phone_match(match, phone_type)
                    if formatted_phone:
                        # НОВОЕ: Извлекаем базовый номер без добавочного для сравнения
                        base_number = self._extract_base_number(formatted_phone)

                        if base_number not in seen_base_numbers:
                            phones.append(formatted_phone)
                            seen_base_numbers.add(base_number)
                        else:
                             # Если базовый номер уже есть, заменяем на версию с добавочным
                            if '(доб.' in formatted_phone:
                                # Удаляем старый номер без добавочного
                                phones = [p for p in phones if self._extract_base_number(p) != base_number]
                                phones.append(formatted_phone)

                except Exception as e:
                    continue

        return phones
    
    def _extract_base_number(self, phone: str) -> str:
        """НОВАЯ ФУНКЦИЯ: Извлекает базовый номер без добавочного"""
        # Убираем добавочный номер
        base_phone = phone.split(' (доб.')[0]
        # Оставляем только цифры и +
        return ''.join(c for c in base_phone if c.isdigit() or c == '+')


    def _format_phone_match(self, match, phone_type: str) -> Optional[str]:
        """ИСПРАВЛЕННАЯ ФУНКЦИЯ: Форматирует извлечённый телефон в красивый вид"""
        
        groups = match.groupdict()
        
        if phone_type == 'city_with_prefix':
            # НОВЫЙ ТИП: Тел. +7 (495) 640-17-71 (доб. 2036)
            country = groups.get('country', '')
            code = groups.get('code', '')
            num1 = groups.get('num1', '')
            num2 = groups.get('num2', '')
            num3 = groups.get('num3', '')
            ext = groups.get('ext', '')
            
            if code and num1 and num2 and num3:
                formatted = f'+7 ({code}) {num1}-{num2}-{num3}'
                if ext:
                    formatted += f' (доб. {ext})'
                return formatted
        
        elif phone_type == 'mobile':
            # Мобильный: +7 (913) 708-12-98
            code = groups.get('code', '')
            num1 = groups.get('num1', '')
            num2 = groups.get('num2', '')
            num3 = groups.get('num3', '')
            ext = groups.get('ext', '')
            
            if code and num1 and num2 and num3:
                formatted = f'+7 ({code}) {num1}-{num2}-{num3}'
                if ext:
                    formatted += f' (доб. {ext})'
                return formatted
        
        elif phone_type == 'city':
            # Городской: +7 (495) 640-17-71 или +7 (3952) 50-40-38
            code = groups.get('code', '')
            num1 = groups.get('num1', '')
            num2 = groups.get('num2', '')
            num3 = groups.get('num3', '')
            ext = groups.get('ext', '')
            
            if code and num1 and num2 and num3:
                formatted = f'+7 ({code}) {num1}-{num2}-{num3}'
                if ext:
                    formatted += f' (доб. {ext})'
                return formatted
        
        elif phone_type == 'city_complex':
            # Городской с составным кодом: +7 (3852) 29-81-12
            code1 = groups.get('code1', '')
            code2 = groups.get('code2', '')
            num1 = groups.get('num1', '')
            num2 = groups.get('num2', '')
            num3 = groups.get('num3', '')
            
            if code1 and code2 and num1 and num2 and num3:
                full_code = code1 + code2
                formatted = f'+7 ({full_code}) {num1}-{num2}-{num3}'
                return formatted
        
        return None


    def extract_inn(self, text: str) -> str:
        """Извлекает ИНН из текста с улучшенными паттернами"""
        
        for pattern in self.inn_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                inn = matches[0]
                if len(inn) in [10, 12] and inn.isdigit():
                    return inn
        
        # Ищем 10 или 12 цифр, НЕ являющиеся телефонами
        digital_sequences = re.findall(r'\b(\d{10}|\d{12})\b', text)
        
        for seq in digital_sequences:
            # Исключаем мобильные номера
            if not (seq.startswith('79') or seq.startswith('89') or seq.startswith('77') or seq.startswith('78')):
                inn_context = re.search(rf'.{{0,20}}\bИНН\b.{{0,20}}{re.escape(seq)}.{{0,20}}', text, re.IGNORECASE)
                if inn_context:
                    return seq
        
        return ""


    def extract_emails(self, text: str) -> List[str]:
        """Извлекает email адреса с улучшенной валидацией"""
        emails = []
        
        potential_emails = re.findall(self.email_pattern, text, re.IGNORECASE)
        
        for email in potential_emails:
            email = email.lower().strip()
            if self._is_valid_email(email):
                emails.append(email)
        
        return emails

    def _is_valid_email(self, email: str) -> bool:
        """Проверяет валидность email"""
        
        if not email or '@' not in email:
            return False
        
        parts = email.split('@')
        if len(parts) != 2:
            return False
        
        local, domain = parts
        
        if len(local) == 0 or len(local) > 64:
            return False
        
        if len(domain) == 0 or len(domain) > 255:
            return False
        
        if '.' not in domain:
            return False
        
        if domain.startswith('.') or domain.endswith('.'):
            return False
        
        return True


    def parse_signature(self, email_body: str) -> ContactInfo:
        """Основной метод парсинга подписи"""
        
        signature_block = self.extract_signature_block(email_body)
        
        contact = ContactInfo()
        
        contact.phones = self.extract_phones(signature_block)
        contact.inn = self.extract_inn(signature_block) 
        emails = self.extract_emails(signature_block)
        contact.email = emails[0] if emails else ""
        
        return contact


# Тестовая функция с информативным выводом
def test_signature_parser():
    """Тестирование парсера на реальных примерах"""
    
    print("=== 📞 ТЕСТ ИСПРАВЛЕННОГО ПАРСЕРА ПОДПИСЕЙ (ФИНАЛЬНАЯ ВЕРСИЯ) ===")
    print("🔄 Инициализация парсера...")
    
    parser = SignatureParser()
    
    # Тесты с проблемными форматами
    test_emails = [
        {
            'name': '🔬 СибЛабСервис (исправленный)',
            'text': """
            Здравствуйте!
            
            Спасибо за ваше предложение.
            
            С уважением,
            Клебанова Ирина
            Специалист по развитию бизнеса
            Тел.(3952)78-25-79, доб. 121
            ООО «СибЛабСервис»
            Г. Иркутск, ул. Ленана, д.7, оф. 104
            E-mail: kiwi@siblabservice.ru
            """
        },
        {
            'name': '⚕️ Алтайский центр (исправленный)',
            'text': """
            Письмо по поводу заявки
            
            ---
            Харлова Олеся Анатольевна
            Врач по общей гигиене отделения метрологии и стандартизации
            ФБУЗ «Центр гигиены и эпидемиологии в Алтайском крае»
            8-3852-50-40-38
            Email: olgmironenko@mail.ru
            """
        },
        {
            'name': '💼 ДНК-Технология (ИСПРАВЛЕНО)',
            'text': """
            Добрый день, коллеги
            
            Фролова Мария Борисовна
            Специалист по тендерам | ООО «ДНК-Технология»
            Тел. +7 (495) 640-17-71 (доб. 2036)
            Email: torgi@dna-technology.ru
            ИНН: 5407123456
            Москва, Варшавское шоссе, дом 125Ж, корпус 5
            """
        },
        {
            'name': '📱 Мобильные номера (исправленный)',
            'text': """
            С уважением,
            Воронина Елена Николаевна
            ООО "БИОСИНТЕК"
            Тел.: +79778842779
            Моб.: 8 913 245 50 71
            E-mail: voronina_l@mail.ru
            ИНН: 5407987654
            """
        },
        {
            'name': '🏛️ Сложный городской номер',
            'text': """
            Ольга Николаевна Мироненко
            Директор центра
            Тел.: 8 (385-2) 29-81-12, +7 913 245 50 71
            Email: test@example.ru
            ИНН: 2209092324
            """
        }
    ]
    
    for test in test_emails:
        print(f"\n{'='*60}")
        print(f"📧 ТЕСТ: {test['name']}")
        print(f"{'='*60}")
        print(f"📝 Текст письма:{test['text']}")
        
        contact = parser.parse_signature(test['text'])
        
        print(f"\n📊 РЕЗУЛЬТАТЫ ПАРСИНГА:")
        print(f"📧 Email: {contact.email if contact.email else 'НЕ НАЙДЕН'}")
        print(f"📞 Телефоны: {contact.phones if contact.phones else 'НЕ НАЙДЕНЫ'}")
        print(f"🏦 ИНН: {contact.inn if contact.inn else 'НЕ НАЙДЕН'}")
        
        phones_count = len(contact.phones)
        has_email = "✅" if contact.email else "❌"
        has_inn = "✅" if contact.inn else "❌"
        
        print(f"📈 Статистика: {phones_count} телефонов, Email: {has_email}, ИНН: {has_inn}")


if __name__ == "__main__":
    test_signature_parser()
