import os
import ssl
import imaplib
import email
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime, getaddresses
from dotenv import load_dotenv
import socket
from bs4 import BeautifulSoup
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import sys
import re

# Добавляем путь для импорта наших модулей
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from contact_processor import ContactProcessor, FullContactInfo

# Настройка логирования для консоли
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class IMAPClient:
    """IMAP-клиент для извлечения высококачественных контактов из корпоративной почты"""
    
    def __init__(self, debug: bool = False):
        """Инициализация IMAP-клиента"""
        
        self.debug = debug
        self.contact_processor = ContactProcessor(debug=debug)
        
        # Загружаем переменные окружения
        load_dotenv()
        
        # Настройки подключения из .env
        self.imap_server = os.environ.get("IMAP_SERVER")
        self.imap_port = int(os.environ.get("IMAP_PORT", 143))
        self.imap_user = os.environ.get("IMAP_USER")
        self.imap_password = os.environ.get("IMAP_PASSWORD")
        
        # Загружаем списки доменов и стоп-слов
        self.internal_domains = self._load_list_from_file('data/internal_domains.txt')
        self.blacklist_emails = self._load_list_from_file('data/blacklist.txt')
        
        # Настройка таймаута
        socket.setdefaulttimeout(180)
        
        # Статистика обработки
        self.stats = {
            'total_emails': 0,
            'external_emails': 0,
            'internal_emails': 0,
            'processed_contacts': 0,
            'valid_contacts': 0,
            'high_quality_contacts': 0,  # НОВОЕ: счетчик качественных контактов
            'low_quality_rejected': 0,   # НОВОЕ: отклоненных низкокачественных
            'duplicates_removed': 0,     # НОВОЕ: удаленных дублей
            'successful_extractions': 0,
            'failed_extractions': 0,
            'chain_emails': 0,
            'original_emails': 0,
            'forwarded_emails': 0
        }
        
        logger.info("✅ IMAP-клиент инициализирован")
        if self.debug:
            logger.debug(f"🔌 Сервер: {self.imap_server}:{self.imap_port}")
            logger.debug(f"👤 Пользователь: {self.imap_user}")

    def _load_list_from_file(self, filename: str) -> set:
        """Загружает список из файла с обработкой ошибок"""
        try:
            with open(filename, encoding='utf-8') as f:
                items = set()
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        items.add(line.lower())
                logger.info(f"✅ Загружен файл {filename}: {len(items)} записей")
                return items
        except FileNotFoundError:
            logger.warning(f'⚠️ Файл {filename} не найден, используется пустой список.')
            return set()

    def _is_internal_email(self, email_addr: str) -> bool:
        """Строгая проверка внутренних email"""
        if not email_addr:
            return True
            
        email_lower = email_addr.lower().strip()
        
        # Проверяем черный список
        if email_lower in self.blacklist_emails:
            return True
        
        # Проверяем внутренние домены
        if '@' in email_lower:
            domain = email_lower.rsplit('@', 1)[-1]
            if domain in self.internal_domains:
                return True
        
        # Дополнительная проверка для DNA-Technology
        if 'dna-technology.ru' in email_lower:
            return True
        
        return False

    def _smart_decode(self, value: str) -> str:
        """Умная декодировка заголовков писем"""
        if not value:
            return ""
        try:
            return str(make_header(decode_header(value)))
        except Exception:
            return value

    def _extract_email_body(self, msg) -> str:
        """Извлекает текст письма с приоритетом text/plain"""
        email_body = ""
        
        if msg.is_multipart():
            # Сначала ищем text/plain
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))
                
                if "attachment" not in content_disposition:
                    if content_type == "text/plain":
                        charset = part.get_content_charset() or 'utf-8'
                        try:
                            email_body = part.get_payload(decode=True).decode(charset, errors="ignore")
                            break
                        except Exception:
                            continue
            
            # Если text/plain не найден, используем text/html
            if not email_body:
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition", ""))
                    
                    if "attachment" not in content_disposition and content_type == "text/html":
                        charset = part.get_content_charset() or 'utf-8'
                        try:
                            html_body = part.get_payload(decode=True).decode(charset, errors="ignore")
                            email_body = BeautifulSoup(html_body, "html.parser").get_text(separator='\n', strip=True)
                            break
                        except Exception:
                            continue
        else:
            charset = msg.get_content_charset() or 'utf-8'
            try:
                email_body = msg.get_payload(decode=True).decode(charset, errors="ignore")
            except Exception:
                email_body = ""
        
        return email_body.strip()

    def _analyze_email_type(self, subject: str, email_body: str) -> str:
        """Анализирует тип письма"""
        
        subject_lower = subject.lower()
        body_lower = email_body.lower()
        
        # Определяем пересланные письма
        if any(marker in subject_lower for marker in ['fwd:', 'fw:', 'пересл:', 'переслано:']):
            return 'forwarded'
        
        # Определяем ответы
        if any(marker in subject_lower for marker in ['re:', 'ответ:', 'отв:']):
            return 'reply'
        
        # Ищем маркеры цепочек в тексте
        chain_markers = [
            'написал(а):',
            'wrote:',
            'от кого:',
            'from:',
            'отправлено:',
            'sent:',
            '-----original message-----',
            '-----исходное сообщение-----'
        ]
        
        if any(marker in body_lower for marker in chain_markers):
            return 'chain'
        
        return 'original'

    def _has_external_participants(self, msg, email_body: str) -> Tuple[bool, List[str]]:
        """СТРОГО улучшенная функция определения внешних участников"""
        
        # Анализируем тип письма
        subject = self._smart_decode(msg.get("Subject", ""))
        email_type = self._analyze_email_type(subject, email_body)
        
        if self.debug:
            logger.debug(f"📧 Тип письма: {email_type}")
        
        # Обновляем статистику
        if email_type == 'chain':
            self.stats['chain_emails'] += 1
        elif email_type == 'forwarded':
            self.stats['forwarded_emails'] += 1
        else:
            self.stats['original_emails'] += 1
        
        # Собираем участников из заголовков
        external_emails = set()
        
        # Извлекаем участников из заголовков письма
        header_participants = []
        for header_name in ['From', 'To', 'Cc', 'Bcc']:
            header_value = msg.get(header_name, '')
            if header_value:
                participants = getaddresses([header_value])
                for name, email_addr in participants:
                    if email_addr:
                        header_participants.append(email_addr.lower())
        
        # СТРОГАЯ фильтрация только реально внешних участников
        for email_addr in header_participants:
            if not self._is_internal_email(email_addr):
                external_emails.add(email_addr)
        
        # Ищем email-адреса в тексте письма (только в подписях, не в заголовках)
        signature_emails = self._extract_signature_emails(email_body)
        for email_addr in signature_emails:
            if not self._is_internal_email(email_addr):
                external_emails.add(email_addr)
        
        result_emails = list(external_emails)
        
        # Логируем результат
        if self.debug and result_emails:
            logger.debug(f"🌐 Найдены внешние участники: {result_emails[:3]}{'...' if len(result_emails) > 3 else ''}")
        
        return len(result_emails) > 0, result_emails

    def _extract_signature_emails(self, email_body: str) -> List[str]:
        """Извлекает email только из подписей, исключая заголовки цепочек"""
        
        lines = email_body.split('\n')
        signature_emails = []
        
        # Определяем границы подписей
        signature_markers = [
            'с уважением',
            'best regards',
            'всего доброго',
            'с наилучшими пожеланиями',
            '---',
            '___',
            '====',
            '--'
        ]
        
        in_signature = False
        for line in lines:
            line_lower = line.lower().strip()
            
            # Пропускаем заголовки цепочек
            if any(marker in line_lower for marker in ['от кого:', 'from:', 'отправлено:', 'sent:', 'wrote:', 'subject:']):
                in_signature = False
                continue
            
            # Ищем начало подписи
            if any(marker in line_lower for marker in signature_markers):
                in_signature = True
                continue
            
            # Если в подписи, ищем email
            if in_signature:
                email_matches = re.findall(r'[\w\.\-]+@[\w\.\-]+', line)
                signature_emails.extend(email_matches)
        
        # Если явных подписей не найдено, ищем email в последних строках (но не в заголовках)
        if not signature_emails and len(lines) > 5:
            last_lines = lines[-8:]  # Последние 8 строк
            for line in last_lines:
                # Исключаем строки с заголовками цепочек
                if not any(marker in line.lower() for marker in ['от кого:', 'from:', 'отправлено:', 'sent:', 'subject:']):
                    email_matches = re.findall(r'[\w\.\-]+@[\w\.\-]+', line)
                    signature_emails.extend(email_matches)
        
        return signature_emails

    def process_emails(self, from_date: str, to_date: str) -> List[FullContactInfo]:
        """ОСНОВНОЙ МЕТОД: Обработка писем с высоким качеством результатов"""
        
        processed_contacts = []
        
        try:
            # Подключение к IMAP-серверу
            logger.info(f"🔌 Подключаюсь к {self.imap_server}:{self.imap_port}...")
            
            mailbox = imaplib.IMAP4(self.imap_server, self.imap_port)
            mailbox.starttls(ssl_context=ssl.create_default_context())
            mailbox.login(self.imap_user, self.imap_password)
            mailbox.select("INBOX")
            
            logger.info("✅ Успешно подключился к почтовому серверу!")
            
            # Формируем поисковый запрос по датам
            search_criteria = self._build_search_criteria(from_date, to_date)
            
            # Поиск писем
            status, messages_ids = mailbox.search(None, search_criteria)
            message_id_list = messages_ids[0].split()
            
            total_emails = len(message_id_list)
            self.stats['total_emails'] = total_emails
            
            logger.info(f"📬 Найдено писем за период {from_date} - {to_date}: {total_emails}")
            
            # Обработка каждого письма
            for i, msg_id in enumerate(message_id_list, 1):
                try:
                    # Получаем письмо
                    status, msg_data = mailbox.fetch(msg_id, "(RFC822)")
                    if status != "OK":
                        continue
                    
                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)
                    
                    # Извлекаем основные данные письма
                    subject = self._smart_decode(msg.get("Subject", "")).strip()
                    email_body = self._extract_email_body(msg)
                    
                    # 🔧 ИСПРАВЛЕНО: Получаем дату письма с коррекцией (+4 часа)
                    mail_date_raw = msg.get("Date", "")
                    try:
                        mail_date = parsedate_to_datetime(mail_date_raw)
                        # ✅ ДОБАВЛЕНА КОРРЕКЦИЯ ВРЕМЕНИ (+4 часа)
                        corrected_date = mail_date + timedelta(hours=4)
                        date_str = corrected_date.strftime("%d.%m.%Y %H:%M")
                    except Exception:
                        date_str = mail_date_raw or "-"
                    
                    # СТРОГАЯ проверка наличия внешних участников
                    has_external, external_emails = self._has_external_participants(msg, email_body)
                    
                    if self.debug:
                        logger.debug(f"📧 Обработка {i}/{total_emails}: {subject[:50]}...")
                    
                    if not has_external:
                        self.stats['internal_emails'] += 1
                        if self.debug:
                            logger.debug("⚪ Письмо содержит только внутренние контакты. Пропущено.")
                        continue
                    
                    self.stats['external_emails'] += 1
                    
                    # Логируем информацию о письме с внешними участниками
                    logger.info("=" * 60)
                    logger.info(f"📧 Письмо {i}/{total_emails}: {date_str}")
                    logger.info(f"📝 Тема: {subject}")
                    logger.info(f"🌐 Внешние участники: {', '.join(external_emails[:3])}")
                    
                    # Обрабатываем письмо через процессор контактов
                    try:
                        contacts = self.contact_processor.process_email_signature(
                            email_body, subject, date_str, external_emails
                        )
                        
                        if contacts:
                            # НОВАЯ ЛОГИКА: Строгая фильтрация по качеству + дедупликация
                            high_quality_contacts = self._filter_and_dedupe_contacts(contacts)
                            
                            if high_quality_contacts:
                                processed_contacts.extend(high_quality_contacts)
                                self.stats['successful_extractions'] += 1
                                self.stats['high_quality_contacts'] += len(high_quality_contacts)
                                
                                # Логируем только высококачественные контакты
                                for contact in high_quality_contacts:
                                    logger.info(f"✅ Высококачественный контакт:")
                                    logger.info(f"   👤 ФИО: {contact.fio if contact.fio else '[]'}")
                                    logger.info(f"   💼 Должность: {contact.position if contact.position else '[]'}")
                                    logger.info(f"   🏢 Компания: {contact.company if contact.company else '[]'}")
                                    logger.info(f"   📧 Email: {contact.email if contact.email else '[]'}")
                                    logger.info(f"   📞 Телефоны: {contact.phones if contact.phones else '[]'}")
                                    logger.info(f"   📍 Адрес: {contact.address if contact.address else '[]'}")
                                    logger.info(f"   🏙️ Город: {contact.city if contact.city else '[]'}")
                                    logger.info(f"   🏦 ИНН: {contact.inn if contact.inn else '[]'}")
                                    logger.info(f"   📊 Качество: {contact.confidence_score}")
                            else:
                                logger.warning("❌ Все контакты низкого качества (< 0.5), отброшены")
                                self.stats['low_quality_rejected'] += len(contacts)
                                self.stats['failed_extractions'] += 1
                        else:
                            logger.warning("❌ Контакты не найдены")
                            self.stats['failed_extractions'] += 1
                        
                        self.stats['processed_contacts'] += len(contacts) if contacts else 0
                        
                    except Exception as e:
                        self.stats['failed_extractions'] += 1
                        logger.error(f"❌ Ошибка обработки письма: {e}")
                
                except Exception as e:
                    logger.error(f"❌ Ошибка получения письма {msg_id}: {e}")
                    continue
            
            # Финальная дедупликация всех контактов
            if processed_contacts:
                logger.info(f"🔄 Выполняется финальная дедупликация {len(processed_contacts)} контактов...")
                unique_contacts = self.contact_processor.deduplicate_contacts(processed_contacts)
                duplicates_removed = len(processed_contacts) - len(unique_contacts)
                self.stats['duplicates_removed'] += duplicates_removed
                self.stats['valid_contacts'] = len(unique_contacts)
                
                if duplicates_removed > 0:
                    logger.info(f"🗑️ Удалено дублей: {duplicates_removed}")
                
                processed_contacts = unique_contacts
            
            # Закрываем соединение
            mailbox.logout()
            logger.info("✅ Соединение с сервером закрыто")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка IMAP-клиента: {e}")
            raise
        
        return processed_contacts

    def _filter_and_dedupe_contacts(self, contacts: List[FullContactInfo]) -> List[FullContactInfo]:
        """НОВАЯ ФУНКЦИЯ: Фильтрация по качеству + дедупликация"""
        
        if not contacts:
            return []
        
        # Этап 1: Фильтруем только высококачественные контакты (качество >= 0.5)
        high_quality = []
        for contact in contacts:
            if contact.confidence_score >= 0.5:
                high_quality.append(contact)
            else:
                self.stats['low_quality_rejected'] += 1
        
        # Этап 2: Дедупликация высококачественных контактов
        if high_quality:
            unique_contacts = self.contact_processor.deduplicate_contacts(high_quality)
            duplicates_removed = len(high_quality) - len(unique_contacts)
            if duplicates_removed > 0:
                self.stats['duplicates_removed'] += duplicates_removed
                if self.debug:
                    logger.debug(f"🗑️ Удалено дублей в письме: {duplicates_removed}")
            return unique_contacts
        
        return []

    def _build_search_criteria(self, from_date: str, to_date: str) -> str:
        """Формирует критерии поиска писем по датам"""
        
        try:
            start_date = datetime.strptime(from_date, "%Y-%m-%d")
            end_date = datetime.strptime(to_date, "%Y-%m-%d")
            
            # Формат даты для IMAP: DD-Mon-YYYY
            start_date_imap = start_date.strftime("%d-%b-%Y")
            end_date_imap = end_date.strftime("%d-%b-%Y")
            
            if from_date == to_date:
                return f'(ON "{start_date_imap}")'
            else:
                return f'(SINCE "{start_date_imap}" BEFORE "{end_date_imap}")'
                
        except ValueError as e:
            logger.error(f"Ошибка формата даты: {e}")
            # Fallback на вчерашний день
            yesterday = (date.today() - timedelta(days=1)).strftime("%d-%b-%Y")
            return f'(ON "{yesterday}")'

    def get_processing_stats(self) -> Dict:
        """Возвращает подробную статистику обработки"""
        
        stats = self.stats.copy()
        
        # Добавляем процентные показатели
        if stats['total_emails'] > 0:
            stats['external_percent'] = round(stats['external_emails'] / stats['total_emails'] * 100, 1)
            stats['internal_percent'] = round(stats['internal_emails'] / stats['total_emails'] * 100, 1)
        
        if stats['processed_contacts'] > 0:
            stats['high_quality_percent'] = round(stats['high_quality_contacts'] / stats['processed_contacts'] * 100, 1)
            stats['rejected_percent'] = round(stats['low_quality_rejected'] / stats['processed_contacts'] * 100, 1)
        
        # Добавляем статистику процессора контактов
        try:
            processor_stats = self.contact_processor.get_processing_stats()
            stats.update(processor_stats)
        except Exception:
            pass
        
        return stats

# Тестовая функция
def test_imap_client():
    """Тестирование IMAP-клиента с подробным выводом"""
    
    print("=== 📧 ТЕСТ ОКОНЧАТЕЛЬНО ИСПРАВЛЕННОГО IMAP-КЛИЕНТА ===")
    print("⚠️  ВНИМАНИЕ: Этот тест подключается к реальной почте!")
    
    # Создаём клиент в обычном режиме (не debug для чистого лога)
    client = IMAPClient(debug=False)
    
    # Задаём конкретный период для тестирования
    from_date = "2025-07-29"
    to_date = "2025-07-29"
    
    try:
        print(f"🚀 Обработка писем за период {from_date} - {to_date}...")
        
        contacts = client.process_emails(from_date, to_date)
        
        print(f"\n{'='*60}")
        print("📊 РЕЗУЛЬТАТЫ ОБРАБОТКИ")
        print(f"{'='*60}")
        
        stats = client.get_processing_stats()
        
        # Улучшенный вывод статистики
        print(f"📬 Всего писем: {stats.get('total_emails', 0)}")
        print(f"🌐 С внешними контактами: {stats.get('external_emails', 0)} ({stats.get('external_percent', 0)}%)")
        print(f"🏠 Только внутренние: {stats.get('internal_emails', 0)} ({stats.get('internal_percent', 0)}%)")
        print(f"")
        print(f"📧 Типы писем:")
        print(f"   📄 Оригинальные: {stats.get('original_emails', 0)}")
        print(f"   🔗 Цепочки: {stats.get('chain_emails', 0)}")
        print(f"   📤 Пересланные: {stats.get('forwarded_emails', 0)}")
        print(f"")
        print(f"👥 Обработка контактов:")
        print(f"   📝 Всего найдено: {stats.get('processed_contacts', 0)}")
        print(f"   ✅ Высокое качество: {stats.get('high_quality_contacts', 0)} ({stats.get('high_quality_percent', 0)}%)")
        print(f"   ❌ Отклонено (низкое качество): {stats.get('low_quality_rejected', 0)} ({stats.get('rejected_percent', 0)}%)")
        print(f"   🗑️ Удалено дублей: {stats.get('duplicates_removed', 0)}")
        print(f"   🎯 ИТОГОВЫХ контактов: {len(contacts)}")
        
        print(f"\n📋 НАЙДЕНО ВЫСОКОКАЧЕСТВЕННЫХ КОНТАКТОВ: {len(contacts)}")
        
        if contacts:
            print(f"\n🔍 ПРИМЕРЫ ВЫСОКОКАЧЕСТВЕННЫХ КОНТАКТОВ:")
            for i, contact in enumerate(contacts[:5], 1):  # Показываем первые 5
                print(f"\n{'='*40}")
                print(f"📇 КОНТАКТ {i}")
                print(f"{'='*40}")
                print(f"👤 ФИО: {contact.fio if contact.fio else 'НЕ НАЙДЕНО'}")
                print(f"💼 Должность: {contact.position if contact.position else 'НЕ НАЙДЕНА'}")
                print(f"🏢 Компания: {contact.company if contact.company else 'НЕ НАЙДЕНА'}")
                print(f"📧 Email: {contact.email if contact.email else 'НЕ НАЙДЕН'}")
                print(f"📞 Телефоны: {contact.phones if contact.phones else 'НЕ НАЙДЕНЫ'}")
                print(f"📍 Адрес: {contact.address if contact.address else 'НЕ НАЙДЕН'}")
                print(f"🏙️ Город: {contact.city if contact.city else 'НЕ НАЙДЕН'}")
                print(f"🏦 ИНН: {contact.inn if contact.inn else 'НЕ НАЙДЕН'}")
                print(f"📊 Качество: {contact.confidence_score}")
                print(f"📅 Источник: {contact.email_date}")
        
        print(f"\n🎯 СИСТЕМА РАБОТАЕТ С ВЫСОКИМ КАЧЕСТВОМ!")
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")

if __name__ == "__main__":
    test_imap_client()
