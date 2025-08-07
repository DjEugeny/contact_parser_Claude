import re
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from ner_extractor import RussianNERExtractor, NERResult
from signature_parser import SignatureParser, ContactInfo

logger = logging.getLogger(__name__)


@dataclass
class FullContactInfo:
    """Полная информация о контакте"""
    fio: str = ""
    position: str = ""
    company: str = ""
    email: str = ""
    phones: List[str] = field(default_factory=list)
    address: str = ""
    city: str = ""
    inn: str = ""
    confidence_score: float = 0.0
    issues: List[str] = field(default_factory=list)
    source: str = ""
    email_date: str = ""
    email_subject: str = ""


class ContactProcessor:
    """Высококачественный процессор контактной информации"""
    
    def __init__(self, debug: bool = False):
        """Инициализация с загрузкой всех паттернов из файлов"""
        
        self.debug = debug
        self.ner_extractor = RussianNERExtractor()
        self.signature_parser = SignatureParser()
        
        # Загружаем все конфигурационные файлы
        self.internal_domains = self._load_list_from_file('data/internal_domains.txt')
        self.blacklist_emails = self._load_list_from_file('data/blacklist.txt')
        self.stop_words_person = self._load_list_from_file('data/stop_words_person.txt')
        self.stop_words_org = self._load_list_from_file('data/stop_words_org.txt')
        self.phone_patterns = self._load_list_from_file('data/phone_patterns.txt')
        self.position_patterns = self._load_list_from_file('data/position_patterns.txt')
        self.company_blacklist = self._load_list_from_file('data/company_blacklist.txt')
        
        # Компилируем регексы для телефонов
        self.phone_regexes = []
        for pattern in self.phone_patterns:
            if pattern and not pattern.startswith('#'):
                try:
                    self.phone_regexes.append(re.compile(pattern))
                except:
                    if self.debug:
                        logger.debug(f"⚠️ Неправильный regex паттерн: {pattern}")
        
        # Критичные внутренние маркеры DNA-Technology
        self.critical_internal_markers = [
            'telegram: @dna_tech_rus',
            '@dna_tech_rus',  
            'dna-technology.ru',
            'варшавское шоссе, дом 125ж',
            'корпус 5',
            '+7 (495) 640-17-71',
            'доб. 2030',
            'от гоголева',
            'от фролова', 
            'светлана воронова',
            'мария гоголева',
            'мария фролова',
            'сучкова наталья',
            'youtube | rutube',
            '>youtube'
        ]
        
        # Статистика
        self.stats = {
            'processed': 0,
            'with_issues': 0,
            'high_confidence': 0,
            'low_confidence': 0,
            'successful_extractions': 0,
            'failed_extractions': 0
        }
        
        logger.info("✅ ContactProcessor с улучшенными паттернами инициализирован")


    def _load_list_from_file(self, filename: str) -> set:
        """Загружает список из файла"""
        try:
            with open(filename, encoding='utf-8') as f:
                items = set()
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        items.add(line.lower())
                if self.debug:
                    logger.debug(f"📁 {filename}: {len(items)} записей")
                return items
        except FileNotFoundError:
            logger.warning(f'⚠️ Файл {filename} не найден')
            return set()


    def process_email_signature(self, email_body: str, email_subject: str, 
                               email_date: str, external_emails: List[str]) -> List[FullContactInfo]:
        """ГЛАВНАЯ ФУНКЦИЯ: Обработка подписей с улучшенными паттернами"""
        
        contacts = []
        
        try:
            # 🔧 ИСПРАВЛЕНИЕ: Проверяем типы данных
            if not isinstance(email_body, str):
                logger.error(f"❌ email_body должен быть строкой, получен: {type(email_body)}")
                return []
            
            if not isinstance(external_emails, list):
                logger.error(f"❌ external_emails должен быть списком, получен: {type(external_emails)}")
                return []
            
            # Фильтруем только реально внешние email
            truly_external_emails = []
            for email in external_emails:
                if isinstance(email, str) and not self._is_internal_email(email):
                    truly_external_emails.append(email)
            
            if not truly_external_emails:
                if self.debug:
                    logger.debug("📧 Нет внешних email")
                return []
            
            # Извлекаем подписи с улучшенной очисткой
            signature_blocks = self._extract_clean_signatures(email_body)
            
            # Обрабатываем каждый блок подписи
            for signature_block in signature_blocks:
                if isinstance(signature_block, str) and len(signature_block.strip()) > 15:
                    contact = self._process_signature_block(
                        signature_block, 
                        truly_external_emails[0],  # 🔧 ИСПРАВЛЕНИЕ: Передаем первый email как строку
                        email_subject, 
                        email_date
                    )
                    if contact:
                        contacts.append(contact)
            
            # Если подписи не найдены, пробуем из всего письма
            if not contacts:
                clean_body = self._deep_clean_email_body(email_body)
                if isinstance(clean_body, str) and len(clean_body.strip()) > 30:
                    contact = self._process_signature_block(
                        clean_body, 
                        truly_external_emails[0],  # 🔧 ИСПРАВЛЕНИЕ: Передаем первый email как строку
                        email_subject, 
                        email_date
                    )
                    if contact:
                        contacts.append(contact)
            
            # Обновляем статистику и качество
            for contact in contacts:
                contact.confidence_score, contact.issues = self._analyze_contact_quality(contact)
                self.stats['processed'] += 1
                
                if contact.issues:
                    self.stats['with_issues'] += 1
                
                if contact.confidence_score >= 0.5:
                    self.stats['high_confidence'] += 1
                else:
                    self.stats['low_confidence'] += 1
            
            return contacts
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки подписи: {e}")
            self.stats['failed_extractions'] += 1
            return []


    def _is_internal_email(self, email: str) -> bool:
        """УЛУЧШЕННАЯ проверка внутренних email"""
        if not email or not isinstance(email, str):
            return True
            
        email_lower = email.lower().strip()
        
        # Blacklist из файла
        if email_lower in self.blacklist_emails:
            return True
        
        # Внутренние домены из файла
        for domain in self.internal_domains:
            if email_lower.endswith('@' + domain):
                return True
        
        return False


    def _extract_clean_signatures(self, email_body: str) -> List[str]:
        """УЛУЧШЕННАЯ функция извлечения подписей"""
        
        # 🔧 ИСПРАВЛЕНИЕ: Проверка типа данных
        if not isinstance(email_body, str):
            logger.error(f"❌ email_body должен быть строкой, получен: {type(email_body)}")
            return []
        
        signature_markers = [
            'с уважением',
            'best regards', 
            'всего доброго',
            'с наилучшими пожеланиями',
            '---',
            '____',
            '====',
            '--'
        ]
        
        lines = email_body.split('\n')
        signature_blocks = []
        
        # Ищем подписи по маркерам
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            
            for marker in signature_markers:
                if marker in line_lower and len(line_lower) < 80:
                    start_idx = i
                    end_idx = min(len(lines), i + 20)  # Увеличили до 20 строк
                    signature_block = '\n'.join(lines[start_idx:end_idx])
                    
                    # Глубокая очистка
                    clean_block = self._deep_filter_internal_markers(signature_block)
                    if isinstance(clean_block, str) and len(clean_block.strip()) > 15:
                        signature_blocks.append(clean_block)
                    break
        
        # Если не найдено по маркерам, берём последние строки
        if not signature_blocks and len(lines) > 8:
            last_block = '\n'.join(lines[-15:])  # Увеличили до 15 строк
            clean_block = self._deep_filter_internal_markers(last_block)
            if isinstance(clean_block, str) and len(clean_block.strip()) > 20:
                signature_blocks.append(clean_block)
        
        return signature_blocks


    def _deep_filter_internal_markers(self, text: str) -> str:
        """ГЛУБОКАЯ фильтрация внутренних маркеров"""
        
        # 🔧 ИСПРАВЛЕНИЕ: Проверка типа данных
        if not isinstance(text, str):
            logger.error(f"❌ text должен быть строкой, получен: {type(text)}")
            return ""
        
        if not text:
            return ""
        
        lines = text.split('\n')
        filtered_lines = []
        
        for line in lines:
            line_lower = line.lower().strip()
            
            if not line_lower:
                continue
            
            # Проверяем критичные внутренние маркеры
            is_internal = False
            for marker in self.critical_internal_markers:
                if marker in line_lower:
                    is_internal = True
                    if self.debug:
                        logger.debug(f"🚫 Отфильтрован маркер: {marker}")
                    break
            
            # Проверяем blacklist компаний
            if not is_internal:
                for blacklist_item in self.company_blacklist:
                    if blacklist_item in line_lower:
                        is_internal = True
                        if self.debug:
                            logger.debug(f"🚫 Отфильтрован blacklist: {blacklist_item}")
                        break
            
            # Проверяем технические заголовки
            if not is_internal:
                # Временные метки
                if re.match(r'^\d{1,2}:\d{2}, \d{1,2} [а-я]+ \d{4}', line_lower):
                    is_internal = True
                # Заголовки писем
                elif re.match(r'^(от кого|отправлено|subject|from|sent):', line_lower):
                    is_internal = True
                # Цитирование
                elif line_lower.startswith('>>>') or line_lower.startswith('>>'):
                    is_internal = True
            
            if not is_internal:
                filtered_lines.append(line)
        
        return '\n'.join(filtered_lines)


    def _deep_clean_email_body(self, email_body: str) -> str:
        """ГЛУБОКАЯ очистка всего тела письма"""
        
        # 🔧 ИСПРАВЛЕНИЕ: Проверка типа данных
        if not isinstance(email_body, str):
            logger.error(f"❌ email_body должен быть строкой, получен: {type(email_body)}")
            return ""
        
        lines = email_body.split('\n')
        clean_lines = []
        
        for line in lines:
            line_lower = line.lower().strip()
            
            if not line_lower:
                continue
            
            # Пропускаем заголовки цепочек
            chain_headers = [
                'от кого:', 'от:', 'from:', 'sent:', 'отправлено:',
                'subject:', 'тема:', 'cc:', 'копия:', 'bcc:'
            ]
            
            is_junk = False
            for header in chain_headers:
                if line_lower.startswith(header):
                    is_junk = True
                    break
            
            # Пропускаем технические строки
            if not is_junk:
                tech_patterns = [
                    r'^\d{1,2}:\d{2}, \d{1,2}',  # Временные метки
                    r'^\d{4}-\d{2}-\d{2}',       # Даты
                    r'^on \d{1,2}/',             # "On 29/07/2025" 
                    r'^в \d{1,2}:\d{2}',         # "В 10:15"
                    r'^\d{2}\.\d{2}\.\d{4}.*пишет', # "28.07.2025 пишет"
                ]
                
                for pattern in tech_patterns:
                    if re.match(pattern, line_lower):
                        is_junk = True
                        break
            
            # Пропускаем внутренние маркеры
            if not is_junk:
                for marker in self.critical_internal_markers:
                    if marker in line_lower:
                        is_junk = True
                        break
            
            if not is_junk:
                clean_lines.append(line)
        
        return '\n'.join(clean_lines)


    def _process_signature_block(self, signature_block: str, external_email: str,
                                subject: str, date: str) -> Optional[FullContactInfo]:
        """УЛУЧШЕННАЯ обработка одного блока подписи"""
        
        try:
            # 🔧 ИСПРАВЛЕНИЕ: Строгая проверка типов данных
            if not isinstance(signature_block, str):
                logger.error(f"❌ signature_block должен быть строкой, получен: {type(signature_block)}")
                return None
            
            if not isinstance(external_email, str):
                logger.error(f"❌ external_email должен быть строкой, получен: {type(external_email)}")
                return None
            
            # Извлекаем данные с помощью NER
            ner_result = self.ner_extractor.extract_entities(signature_block)
            
            # Извлекаем данные с помощью парсера подписей
            signature_data = self.signature_parser.parse_signature(signature_block)
            
            contact = FullContactInfo()
            
            # ФИО с улучшенной валидацией
            if hasattr(ner_result, 'persons') and ner_result.persons:
                persons_str = str(ner_result.persons) if not isinstance(ner_result.persons, str) else ner_result.persons
                if self._is_valid_person_name(persons_str):
                    contact.fio = persons_str
            
            # Должность с улучшенными паттернами
            if hasattr(ner_result, 'positions') and ner_result.positions:
                positions_str = str(ner_result.positions) if not isinstance(ner_result.positions, str) else ner_result.positions
                position = self._clean_position(positions_str)
                if position and self._is_valid_position(position):
                    contact.position = position
            
            # Компания с фильтрацией мусора
            if hasattr(ner_result, 'organizations') and ner_result.organizations:
                organizations_str = str(ner_result.organizations) if not isinstance(ner_result.organizations, str) else ner_result.organizations
                company = self._clean_company(organizations_str)
                if company and self._is_valid_company(company):
                    contact.company = company
            
            # Адрес с проверкой
            if hasattr(ner_result, 'locations') and ner_result.locations:
                locations_str = str(ner_result.locations) if not isinstance(ner_result.locations, str) else ner_result.locations
                if self._is_valid_address(locations_str):
                    contact.address = locations_str
            
            # Email - предпочитаем внешний
            if hasattr(signature_data, 'email') and signature_data.email and not self._is_internal_email(signature_data.email):
                contact.email = str(signature_data.email)
            else:
                contact.email = external_email  # 🔧 ИСПРАВЛЕНИЕ: Используем переданный email как строку
            
            # Телефоны - улучшенный парсинг
            contact.phones = self._extract_phones_improved(signature_block)
            if hasattr(signature_data, 'phones') and signature_data.phones:
                # Добавляем телефоны из парсера, избегая дублей
                phones_from_parser = signature_data.phones if isinstance(signature_data.phones, list) else [str(signature_data.phones)]
                for phone in phones_from_parser:
                    phone_str = str(phone) if not isinstance(phone, str) else phone
                    if phone_str not in contact.phones:
                        contact.phones.append(phone_str)
            
            # ИНН
            if hasattr(signature_data, 'inn') and signature_data.inn:
                contact.inn = str(signature_data.inn)
            
            # Город
            if contact.address:
                contact.city = self._extract_city_from_address(contact.address)
            
            # Корректируем время (+4 часа как просил пользователь)
            contact.email_date = self._correct_email_time(date)
            contact.email_subject = str(subject) if subject else ""
            contact.source = "email_signature"
            
            # Строгая валидация
            if self._is_high_quality_contact(contact):
                return contact
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки блока подписи: {e}")
        
        return None


    def _extract_phones_improved(self, text: str) -> List[str]:
        """УЛУЧШЕННОЕ извлечение телефонов с паттернами из файла"""
        
        # 🔧 ИСПРАВЛЕНИЕ: Проверка типа данных
        if not isinstance(text, str):
            logger.error(f"❌ text должен быть строкой, получен: {type(text)}")
            return []
        
        phones = []
        
        # Используем регексы из файла
        for regex in self.phone_regexes:
            try:
                matches = regex.findall(text)
                for match in matches:
                    if isinstance(match, tuple):
                        phone = ''.join(match)
                    else:
                        phone = str(match)
                    
                    # Нормализуем формат
                    phone = self._normalize_phone(phone)
                    if phone and phone not in phones:
                        phones.append(phone)
            except Exception as e:
                if self.debug:
                    logger.debug(f"⚠️ Ошибка regex телефона: {e}")
        
        # Дополнительные паттерны для конкретных форматов из логов
        additional_patterns = [
            r'\+7-(\d{3})-(\d{3})-(\d{2})-(\d{2})',  # +7-913-399-32-72
            r'8\s+\((\d{3,4})\)\s+(\d{3})-(\d{3})',  # 8 (3852) 291-295
            r'\+7\s+\((\d{3})\)\s+(\d{3})\s+(\d{2})\s+(\d{2})\s+\((\d{2})\)', # +7 (495) 933 71 47 (48)
            r'8\s+(\d{3})-(\d{3})-(\d{2})-(\d{2}),?\s*доб\.(\d+)', # 8 800-770-71-21, доб.1315
        ]
        
        for pattern in additional_patterns:
            try:
                matches = re.findall(pattern, text)
                for match in matches:
                    if isinstance(match, tuple):
                        # Форматируем телефон
                        if len(match) == 4:  # +7-XXX-XXX-XX-XX
                            phone = f"+7-{match[0]}-{match[1]}-{match[2]}-{match[3]}"
                        elif len(match) == 3:  # 8 (XXXX) XXX-XXX
                            phone = f"8 ({match[0]}) {match[1]}-{match[2]}"
                        elif len(match) == 5:  # +7 (495) 933 71 47 (48)
                            phone = f"+7 ({match[0]}) {match[1]} {match[2]} {match[3]} ({match[4]})"
                        else:
                            phone = ''.join(match)
                    else:
                        phone = str(match)
                    
                    phone = self._normalize_phone(phone)
                    if phone and phone not in phones:
                        phones.append(phone)
            except Exception as e:
                if self.debug:
                    logger.debug(f"⚠️ Ошибка дополнительного regex: {e}")
        
        return phones


    def _normalize_phone(self, phone: str) -> str:
        """Нормализует формат телефона"""
        if not phone or not isinstance(phone, str):
            return ""
        
        # Убираем лишние пробелы
        phone = re.sub(r'\s+', ' ', phone.strip())
        
        # Проверяем, что это похоже на телефон
        if len(phone) < 7:
            return ""
        
        return phone


    def _clean_position(self, position: str) -> str:
        """Очищает должность от мусора"""
        if not position or not isinstance(position, str):
            return ""
        
        # Убираем повторяющиеся имена из должности
        words = position.split()
        cleaned_words = []
        
        # Ищем где заканчивается должность и начинается имя
        for i, word in enumerate(words):
            # Если слово начинается с заглавной буквы и похоже на имя
            if (re.match(r'^[А-ЯЁ][а-яё]+$', word) and 
                i > 0 and 
                len(word) > 3 and
                word.lower() not in ['отдела', 'отделом', 'группы', 'департамента']):
                # Проверяем следующее слово - если тоже имя, значит здесь начинается ФИО
                if i + 1 < len(words) and re.match(r'^[А-ЯЁ][а-яё]+$', words[i + 1]):
                    break
            cleaned_words.append(word)
        
        return ' '.join(cleaned_words).strip()


    def _clean_company(self, company: str) -> str:
        """Очищает название компании от мусора"""
        if not company or not isinstance(company, str):
            return ""
        
        # Убираем префиксы мусора
        prefixes_to_remove = [
            'заказчик: ',
            'конечный пользователь: ',
            'клиент: '
        ]
        
        company_lower = company.lower()
        for prefix in prefixes_to_remove:
            if company_lower.startswith(prefix):
                company = company[len(prefix):]
                break
        
        # Убираем суффиксы с ИНН, КПП и адресами
        patterns_to_cut = [
            r',\s*инн\s+\d+.*',
            r',\s*кпп\s+\d+.*', 
            r',\s*\d{6},.*',  # Почтовые индексы
            r'\s*\d{6},.*'    # Почтовые индексы без запятой
        ]
        
        for pattern in patterns_to_cut:
            company = re.sub(pattern, '', company, flags=re.IGNORECASE)
        
        return company.strip()


    def _is_valid_person_name(self, name: str) -> bool:
        """УЛУЧШЕННАЯ валидация имен персон"""
        if not name or not isinstance(name, str) or len(name.strip()) < 3:
            return False
        
        name_lower = name.lower()
        
        # Проверяем стоп-слова из файла
        if name_lower in self.stop_words_person:
            if self.debug:
                logger.debug(f"🚫 Имя в стоп-листе: {name}")
            return False
        
        # Дополнительные проверки
        invalid_patterns = [
            'центр лабораторной',
            'telegram:',
            'subject:',
            'от кого',
            'компания',
            'youtube',
            'rutube'
        ]
        
        for invalid in invalid_patterns:
            if invalid in name_lower:
                return False
        
        # Проверяем, что это реальное ФИО
        words = name.split()
        if len(words) < 2 or len(words) > 4:
            return False
        
        # Каждое слово должно быть правильно капитализированным
        for word in words:
            if not re.match(r'^[А-ЯЁA-Z][а-яёa-z]+$', word):
                return False
        
        return True


    def _is_valid_position(self, position: str) -> bool:
        """УЛУЧШЕННАЯ валидация должностей"""
        if not position or not isinstance(position, str) or len(position.strip()) < 3:
            return False
        
        position_lower = position.lower()
        
        # Исключаем явно неправильные должности
        invalid_indicators = [
            'subject:',
            'от кого',
            'telegram:',
            'отправлено',
            'youtube',
            'rutube',
            'при запросе на счет'
        ]
        
        for invalid in invalid_indicators:
            if invalid in position_lower:
                return False
        
        # Проверяем, что должность не начинается с цифр или технических символов
        if re.match(r'^[\d\>\<\@\#\$\%]', position_lower):
            return False
        
        return True


    def _is_valid_company(self, company: str) -> bool:
        """УЛУЧШЕННАЯ валидация компаний"""
        if not company or not isinstance(company, str) or len(company.strip()) < 3:
            return False
        
        company_lower = company.lower()
        
        # Проверяем blacklist компаний из файла
        for blacklist_item in self.company_blacklist:
            if blacklist_item in company_lower:
                if self.debug:
                    logger.debug(f"🚫 Компания в blacklist: {blacklist_item}")
                return False
        
        return True


    def _is_valid_address(self, address: str) -> bool:
        """УЛУЧШЕННАЯ валидация адресов"""
        if not address or not isinstance(address, str) or len(address.strip()) < 5:
            return False
        
        address_lower = address.lower()
        
        # Исключаем мусорные данные в адресах
        junk_indicators = [
            '>>>',
            'от кого:',
            'subject:',
            'отправлено:',
            'оплату №',
            'содержат конфиденциальную',
            'настоящим уведомляем',
            'в марте выставляли',
            'прошу вас',
            'закупка планируется',
            'кп я только что',
            '19:00, 18 июня 2025'
        ]
        
        for junk in junk_indicators:
            if junk in address_lower:
                return False
        
        return True


    def _extract_city_from_address(self, address: str) -> str:
        """УЛУЧШЕННОЕ извлечение города"""
        
        if not address or not isinstance(address, str):
            return ""
        
        city_patterns = [
            r'[гГ]\.?\s*([А-ЯЁ][а-яё\-]+)',
            r'город\s+([А-ЯЁ][а-яё\-]+)',
        ]
        
        for pattern in city_patterns:
            match = re.search(pattern, address)
            if match:
                city = match.group(1)
                # Дополнительная валидация
                if len(city) > 2 and city.lower() not in ['настоящим', 'просим', 'закупка']:
                    return city
        
        # Ищем первое подходящее слово
        parts = address.split(',')
        if parts:
            first_part = parts[0].strip()
            words = first_part.split()
            for word in words:
                if (re.match(r'^[А-ЯЁ][а-яё\-]+$', word) and 
                    len(word) > 3 and 
                    word.lower() not in ['просим', 'настоящим', 'содержат', 'заказчик']):
                    return word
        
        return ""


    def _correct_email_time(self, date_str: str) -> str:
        """Корректирует время письма (+4 часа)"""
        if not date_str or not isinstance(date_str, str):
            return ""
        
        try:
            # Парсим дату в формате "29.07.2025 10:10"
            dt = datetime.strptime(date_str, "%d.%m.%Y %H:%M")
            # Добавляем 4 часа
            corrected_dt = dt + timedelta(hours=4)
            return corrected_dt.strftime("%d.%m.%Y %H:%M")
        except:
            return date_str


    def _is_high_quality_contact(self, contact: FullContactInfo) -> bool:
        """УЛУЧШЕННАЯ валидация высококачественного контакта"""
        
        # Проверяем, что email не внутренний
        if contact.email and self._is_internal_email(contact.email):
            return False
        
        # Контакт должен иметь значимые поля
        has_name = bool(contact.fio and contact.fio.strip())
        has_company = bool(contact.company and contact.company.strip())
        has_email = bool(contact.email and contact.email.strip() and '@' in contact.email)
        has_phone = bool(contact.phones)
        has_address = bool(contact.address and contact.address.strip())
        
        # Для высокого качества нужно минимум:
        # (Имя ИЛИ Компания) И (Email ИЛИ Телефон)
        has_identity = has_name or has_company
        has_contact_method = has_email or has_phone
        
        return has_identity and has_contact_method


    def _analyze_contact_quality(self, contact: FullContactInfo) -> tuple[float, List[str]]:
        """УЛУЧШЕННЫЙ анализ качества контакта"""
        
        issues = []
        score = 0.0
        
        # ФИО (30%)
        if contact.fio and len(contact.fio.split()) >= 2:
            score += 0.3
        else:
            issues.append("ФИО неполное или отсутствует")
        
        # Должность (10%)
        if contact.position:
            score += 0.1
        else:
            issues.append("Должность не определена")
        
        # Компания (20%)
        if contact.company:
            score += 0.2
        else:
            issues.append("Компания не найдена")
        
        # Телефоны (20%)
        if contact.phones:
            score += 0.2
        else:
            issues.append("Телефоны не найдены")
        
        # Email (10%)
        if contact.email and '@' in contact.email:
            score += 0.1
        
        # Адрес (10%)
        if contact.address:
            score += 0.1
        else:
            issues.append("Адрес не найден")
        
        return round(score, 2), issues


    def deduplicate_contacts(self, contacts: List[FullContactInfo]) -> List[FullContactInfo]:
        """ИСПРАВЛЕННАЯ дедупликация контактов"""
        
        if not contacts:
            return contacts
        
        unique_contacts = []
        seen_keys = set()
        
        for contact in contacts:
            # Создаём более строгий ключ дедупликации
            key_parts = []
            
            # Основной ключ - email (нормализованный)
            if contact.email:
                email_normalized = contact.email.lower().strip()
                key_parts.append(f"email:{email_normalized}")
            
            # Дополнительный ключ - ФИО (нормализованное)
            if contact.fio:
                fio_normalized = re.sub(r'\s+', ' ', contact.fio.lower().strip())
                key_parts.append(f"fio:{fio_normalized}")
            
            # Если есть ключевые данные
            if key_parts:
                key = '|'.join(sorted(key_parts))  # Сортируем для консистентности
                
                if key not in seen_keys:
                    unique_contacts.append(contact)
                    seen_keys.add(key)
                    if self.debug:
                        logger.debug(f"✅ Добавлен уникальный контакт: {contact.fio or contact.email}")
                else:
                    # Если дубль найден, выбираем лучший по качеству
                    existing_idx = None
                    for idx, existing in enumerate(unique_contacts):
                        existing_key_parts = []
                        if existing.email:
                            existing_key_parts.append(f"email:{existing.email.lower().strip()}")
                        if existing.fio:
                            existing_key_parts.append(f"fio:{re.sub(r's+', ' ', existing.fio.lower().strip())}")
                        
                        if key_parts and '|'.join(sorted(existing_key_parts)) == key:
                            existing_idx = idx
                            break
                    
                    if existing_idx is not None and contact.confidence_score > unique_contacts[existing_idx].confidence_score:
                        if self.debug:
                            logger.debug(f"🔄 Заменен контакт на лучший: {contact.fio or contact.email}")
                        unique_contacts[existing_idx] = contact
                    else:
                        if self.debug:
                            logger.debug(f"🗑️ Отброшен дубль: {contact.fio or contact.email}")
            else:
                # Если нет ключевых данных, добавляем как есть
                unique_contacts.append(contact)
        
        if self.debug and len(contacts) != len(unique_contacts):
            logger.debug(f"🗑️ Итого дедупликация: {len(contacts)} → {len(unique_contacts)}")
        
        return unique_contacts


    def get_processing_stats(self) -> Dict:
        """Возвращает статистику обработки"""
        
        stats = self.stats.copy()
        
        if stats['processed'] > 0:
            stats['high_confidence_percent'] = round(stats['high_confidence'] / stats['processed'] * 100, 1)
            stats['issues_percent'] = round(stats['with_issues'] / stats['processed'] * 100, 1)
        
        return stats
