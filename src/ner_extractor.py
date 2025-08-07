from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsSyntaxParser,
    NewsNERTagger,
    PER,
    ORG,
    LOC,
    Doc
)
import re
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class NERResult:
    """Результат извлечения именованных сущностей"""
    persons: List[str] = None
    organizations: List[str] = None
    locations: List[str] = None
    positions: List[str] = None
    
    def __post_init__(self):
        if self.persons is None:
            self.persons = []
        if self.organizations is None:
            self.organizations = []
        if self.locations is None:
            self.locations = []
        if self.positions is None:
            self.positions = []

class RussianNERExtractor:
    """Извлечение именованных сущностей для русского языка с Natasha"""
    
    def __init__(self):
        # Инициализация компонентов Natasha
        self.segmenter = Segmenter()
        self.morph_vocab = MorphVocab()
        
        self.emb = NewsEmbedding()
        self.morph_tagger = NewsMorphTagger(self.emb)
        self.syntax_parser = NewsSyntaxParser(self.emb)
        self.ner_tagger = NewsNERTagger(self.emb)
        
        # Загружаем стоп-слова
        self.stop_words_person = self._load_stop_words('data/stop_words_person.txt')
        self.stop_words_org = self._load_stop_words('data/stop_words_org.txt')
        
        # Ключевые слова должностей (для фильтрации из организаций)
        self.position_keywords = [
            'директор', 'директриса', 'менеджер', 'специалист', 'врач', 'инженер',
            'руководитель', 'начальник', 'начальница', 'заместитель', 'заведующий', 'координатор'
        ]
        
        # ИСПРАВЛЕННЫЕ паттерны для должностей
        self.position_patterns = [
            # Директор с различными вариантами
            r'Директор(?:\s+[А-ЯЁ][а-яё]+)*(?:\s+центра|\s+отдела|\s+департамента|\s+управления|\s+службы|\s+филиала)*(?:\s+[а-яё\s]+)?',
            # Менеджер с "по" и без
            r'Менеджер(?:\s+[а-яё]+)*(?:\s+по\s+[а-яё\s]+)?',
            # Специалист с "по" и без  
            r'Специалист(?:\s+[а-яё]+)*(?:\s+по\s+[а-яё\s]+)?',
            # Врач с различными вариантами
            r'Врач(?:\s+по\s+[а-яё\s]+)?(?:\s+отделения\s+[а-яё\s]+)?(?:\s+метрологии)?(?:\s+и)?(?:\s+стандартизации)?',
            # Остальные должности
            r'Инженер(?:\s+[а-яё\s]+)?',
            r'Руководитель(?:\s+[а-яё\s]+)?',
            r'Начальник(?:\s+[а-яё\s]+)?',
            r'Заведующий(?:\s+[а-яё\s]+)?',
            r'Заместитель(?:\s+[а-яё\s]+)?'
        ]

    def _load_stop_words(self, filename: str) -> set:
        """Загружает стоп-слова из файла"""
        try:
            with open(filename, encoding='utf-8') as f:
                stop_words = set()
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        stop_words.add(line.lower())
                return stop_words
        except FileNotFoundError:
            print(f"⚠️  Файл {filename} не найден")
            return set()

    def _clean_fio_text(self, fio_text: str) -> str:
        """Тщательная очистка ФИО от мусора"""
        if not fio_text:
            return ""
        
        # Убираем переносы строк и управляющие символы
        cleaned = re.sub(r'[\r\n\t]+', ' ', fio_text)
        
        # Убираем множественные пробелы
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Убираем подозрительные слова из ФИО
        suspicious_patterns = [
            r'\b(тел|телефон|моб|мобильный|факс|email|e-mail)\b',
            r'\b(директор|менеджер|специалист|врач)\b',
            r'\b(ооо|зао|ао|ип)\b'
        ]
        
        for pattern in suspicious_patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        # Убираем лишние символы в начале и конце
        cleaned = re.sub(r'^[^\w]+|[^\w]+$', '', cleaned)
        
        # Убираем множественные пробелы снова
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Проверяем, что результат похож на ФИО (2-3 слова, каждое с заглавной)
        words = cleaned.split()
        if len(words) < 2 or len(words) > 4:
            return ""
        
        # Проверяем, что каждое слово начинается с заглавной буквы
        for word in words:
            if not re.match(r'^[А-ЯЁ][а-яё]+$', word):
                return ""
        
        return cleaned

    def _is_stop_word_person(self, text: str) -> bool:
        """Проверяет, является ли текст стоп-словом для персон"""
        text_lower = text.lower().strip()
        
        if text_lower in self.stop_words_person:
            return True
        
        for stop_word in self.stop_words_person:
            if len(stop_word) > 3 and stop_word in text_lower:
                return True
        
        return False

    def _is_stop_word_org(self, text: str) -> bool:
        """Проверяет, является ли текст стоп-словом для организаций"""
        text_lower = text.lower().strip()
        
        if text_lower in self.stop_words_org:
            return True
        
        for stop_word in self.stop_words_org:
            if len(stop_word) > 3 and stop_word in text_lower:
                return True
        
        return False

    def _is_position_not_organization(self, text: str) -> bool:
        """Проверяет, является ли текст должностью (НЕ организацией)"""
        text_lower = text.lower().strip()
        
        # Если начинается с ключевого слова должности - это должность
        for keyword in self.position_keywords:
            if text_lower.startswith(keyword):
                return True
        
        # Если содержит паттерны должностей
        for pattern in self.position_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        return False

    def _extend_org_with_region(self, org: str, line: str) -> str:
        """Расширяет организацию с региональной информацией"""
        # Расширенные паттерны для разных регионов
        regional_patterns = [
            # "в [Название] крае"
            rf"{re.escape(org)}\s+в\s+[А-ЯЁ][а-яё\-]+(?:\s+[а-яё\-]+)?\s+крае",
            # "в [Название] области"
            rf"{re.escape(org)}\s+в\s+[А-ЯЁ][а-яё\-]+(?:\s+[а-яё\-]+)?\s+области",
            # "в [Название] обл."
            rf"{re.escape(org)}\s+в\s+[А-ЯЁ][а-яё\-]+(?:\s+[а-яё\-]+)?\s+обл\.",
            # "в Республике [Название]"
            rf"{re.escape(org)}\s+в\s+Республике\s+[А-ЯЁ][а-яё\-]+",
            # "в [Название] республике"
            rf"{re.escape(org)}\s+в\s+[А-ЯЁ][а-яё\-]+\s+республике",
        ]
        
        for pattern in regional_patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                return match.group(0).strip()
        
        return line.strip()

    def clean_organization_text(self, text: str) -> str:
        """Сохраняет географию, удаляет отдельные города и должности"""
        cleaned = re.sub(r'\s+', ' ', text).strip()
        
        # Проверяем, не является ли это должностью
        if self._is_position_not_organization(cleaned):
            return ""  # Возвращаем пустую строку для должностей
        
        # Отрезаем ТОЛЬКО города в формате " Г. Город" (отдельные от названия)
        separate_city_patterns = [
            r'\s+[ГгGg]\.?\s+[А-ЯЁ][а-яё\-]+(?:\s*,.*)?$',  # " Г. Иркутск" в конце строки
            r'\s+город\s+[А-ЯЁ][а-яё\-]+(?:\s*,.*)?$',      # " город Барнаул" в конце строки
        ]
        
        for pattern in separate_city_patterns:
            match = re.search(pattern, cleaned)
            if match:
                # Отрезаем только если это отдельный город в конце
                cleaned = cleaned[:match.start()].strip()
                break
        
        # Удаляем ТОЛЬКО телефонные сокращения и номера (НЕ географию!)
        cleaned = re.sub(r'\s+(Тел\.?|Факс|Email|E-mail|Моб\.?)(\s|$)', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\d{1,2}-\d{4}-\d{2}-\d{2}', '', cleaned)
        
        # Проверка стоп-слов
        if self._is_stop_word_org(cleaned):
            return ""
        
        return cleaned.strip()

    def merge_person_fragments(self, person_fragments: List[str], text: str) -> List[str]:
        """Объединяет фрагменты ФИО с улучшенной очисткой"""
        merged_names = []
        
        # Улучшенные паттерны для ФИО
        fio_patterns = [
            r'([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',  # Фамилия Имя Отчество
            r'([А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.)',            # Фамилия И. О.
        ]
        
        for pattern in fio_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Тщательная очистка ФИО
                full_name = self._clean_fio_text(match.strip())
                if full_name and not self._is_stop_word_person(full_name):
                    merged_names.append(full_name)
        
        # Если паттерны не сработали, обрабатываем фрагменты
        if not merged_names and len(person_fragments) >= 1:
            for fragment in person_fragments:
                clean_fragment = self._clean_fio_text(fragment)
                if clean_fragment and not self._is_stop_word_person(clean_fragment):
                    merged_names.append(clean_fragment)
        
        return list(set(merged_names))

    def extract_full_addresses(self, text: str) -> List[str]:
        """Извлекает адреса и города"""
        addresses = []
        
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Ищем любые строки с адресными маркерами
            address_markers = ['г.', 'ул.', 'дом', 'д.', 'улица', 'проспект', 'пр.', 'корпус', 'кор.', 'офис', 'оф.', 'квартира', 'кв.']
            
            if any(marker in line.lower() for marker in address_markers):
                # Очищаем от лишних символов в начале/конце
                cleaned_line = re.sub(r'^\W+|\W+$', '', line)
                if len(cleaned_line) > 8:  # Минимальная длина адреса
                    addresses.append(cleaned_line)
            
            # Отдельный поиск городов только если нет полных адресов
            elif re.match(r'^[ГгGg]\.?\s*([А-ЯЁ][а-яё\-]+)$', line):
                # Только строки типа "Г. Иркутск" на отдельной строке
                city_match = re.search(r'[ГгGg]\.?\s*([А-ЯЁ][а-яё\-]+)', line)
                if city_match:
                    addresses.append(f"г. {city_match.group(1)}")
        
        # Если не нашли адреса, ищем простые названия городов
        if not addresses:
            for line in lines:
                line = line.strip()
                if re.match(r'^[А-ЯЁ][а-яё\-]+$', line) and len(line) > 3:
                    # Проверяем контекст - не является ли это частью должности
                    line_idx = lines.index(line)
                    is_city = True
                    
                    # Проверяем соседние строки
                    for check_idx in [line_idx - 1, line_idx + 1]:
                        if 0 <= check_idx < len(lines):
                            check_line = lines[check_idx].strip()
                            if any(word in check_line.lower() for word in ['врач', 'директор', 'менеджер', 'отделения', 'метрологии']):
                                is_city = False
                                break
                    
                    if is_city:
                        addresses.append(line)
        
        # Убираем дубликаты и сортируем по длине (полные адреса важнее)
        if addresses:
            addresses = sorted(set(addresses), key=len, reverse=True)
        
        return addresses

    def extract_clean_positions(self, text: str) -> List[str]:
        """ИСПРАВЛЕННАЯ: Извлекает должности с упрощенной логикой"""
        positions = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Пропускаем пустые строки и строки с ФИО
            if not line or re.match(r'^[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s*[А-ЯЁ]*[а-яё]*\s*$', line):
                continue

            # Пропускаем строки с организациями
            if re.search(r'\b(ООО|АО|ЗАО|ИП|ФГБОУ|ФБУЗ)\b', line, re.IGNORECASE):
                continue

            # Пропускаем строки с адресами и контактами
            if re.search(r'\b(г\.|ул\.|тел\.?|email|факс|\+7|\d{1,2}-\d{4})\b', line, re.IGNORECASE):
                continue
            
            # КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Ищем строки, которые начинаются с должностных слов
            line_lower = line.lower()

            # Проверяем, начинается ли строка с ключевого слова должности
            starts_with_position = False
            for keyword in self.position_keywords:
                if line_lower.startswith(keyword):
                    starts_with_position = True
                    break
            if starts_with_position:
                # Проверяем разумную длину (не слишком длинная для должности)
                if len(line) < 150:  # Увеличили лимит для длинных должностей
                    clean_position = self.clean_position_text(line, line)
                    if clean_position and clean_position not in positions:
                        positions.append(clean_position)
    
        return positions

    def clean_position_text(self, position: str, full_line: str = "") -> str:
        """ИСПРАВЛЕННАЯ: Очищает текст должности без потерь"""
        position = re.sub(r'\s+', ' ', position).strip()
        
        if not position:
            return ""
        
        # ИСПРАВЛЕНИЕ: Более мягкая очистка - сохраняем больше информации
        
        # Удаляем только телефонные номера и сокращения
        position = re.sub(r'\d{1,2}-\d{4}-\d{2}-\d{2}', '', position)
        position = re.sub(r'\b(Тел\.?|Факс|Email|E-mail)\b.*', '', position, flags=re.IGNORECASE)
        position = re.sub(r'^[^\w]+|[^\w]+$', '', position)
        position = re.sub(r'\s+', ' ', position).strip()
        
        # Проверяем, что содержит ключевые слова должностей
        if not any(keyword in position.lower() for keyword in self.position_keywords):
            return ""
        
        # ИСПРАВЛЕНИЕ: Капитализируем и возвращаем полную должность
        words = position.split()
        if words:
            # Первое слово с заглавной, остальные как есть
            result = words[0].capitalize()
            if len(words) > 1:
                result += ' ' + ' '.join(words[1:])
            return result
        
        return position.strip()

    def extract_entities(self, text: str) -> NERResult:
        """Основной метод извлечения сущностей с фильтрацией стоп-слов"""
        
        result = NERResult()
        
        # Создаём документ Natasha
        doc = Doc(text)
        doc.segment(self.segmenter)
        doc.tag_morph(self.morph_tagger)
        doc.parse_syntax(self.syntax_parser)
        doc.tag_ner(self.ner_tagger)
        
        # Собираем сырые данные от Natasha
        raw_persons = []
        raw_orgs = []
        raw_locations = []
        
        for span in doc.spans:
            span.normalize(self.morph_vocab)
            
            if span.type == PER:
                raw_persons.append(span.text)  # Используем span.text вместо span.normal
            elif span.type == ORG:
                raw_orgs.append(span.text)     # Используем span.text вместо span.normal
            elif span.type == LOC:
                raw_locations.append(span.text)
        
        # Постобработка с фильтрацией стоп-слов
        result.persons = self.merge_person_fragments(raw_persons, text)
        
        # Обработка организаций с расширенной региональной логикой
        filtered_orgs = []
        for org_text in raw_orgs:
            org_str = org_text.strip()
            
            # Удаляем ведущие слова должностей из организации
            words = org_str.split()
            if words and words[0].lower() in self.position_keywords:
                org_str = ' '.join(words[1:])
            
            if not org_str:
                continue
            
            # Найти строку, содержащую эту организацию для расширения региональной информацией
            line = next((l for l in text.split('\n') if org_str in l), org_str)
            full_org = self._extend_org_with_region(org_str, line)
            
            # Очищаем организацию (включая проверку на должности)
            clean_org = self.clean_organization_text(full_org)
            if clean_org and clean_org not in filtered_orgs:
                filtered_orgs.append(clean_org)
        
        result.organizations = filtered_orgs
        
        # Используем функции извлечения адресов и должностей
        result.locations = self.extract_full_addresses(text)
        result.positions = self.extract_clean_positions(text)
        
        return result

    def extract_city_from_address(self, text: str) -> Optional[str]:
        """Извлекает город из адреса"""
        
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Ищем города в любых строках с адресными маркерами
            if any(marker in line.lower() for marker in ['г.', 'город']):
                city_patterns = [
                    r'[ГгGg]\.?\s*([А-ЯЁ][а-яё\-]+)',  # г. Барнаул
                    r'город\s+([А-ЯЁ][а-яё\-]+)',      # город Барнаул
                ]
                
                for pattern in city_patterns:
                    match = re.search(pattern, line)
                    if match:
                        return match.group(1)
        
        # Поиск городов в отдельных строках
        for line in lines:
            line = line.strip()
            if re.match(r'^[ГгGg]\.?\s*[А-ЯЁ][а-яё\-]+$', line):
                city_match = re.search(r'[ГгGg]\.?\s*([А-ЯЁ][а-яё\-]+)', line)
                if city_match:
                    return city_match.group(1)
            elif re.match(r'^[А-ЯЁ][а-яё\-]+$', line) and len(line) > 3:
                # Проверяем контекст - не является ли это частью должности
                line_idx = lines.index(line)
                is_city = True
                
                # Проверяем соседние строки
                for check_idx in [line_idx - 1, line_idx + 1]:
                    if 0 <= check_idx < len(lines):
                        check_line = lines[check_idx].strip()
                        if any(word in check_line.lower() for word in ['врач', 'директор', 'менеджер', 'отделения', 'метрологии']):
                            is_city = False
                            break
                
                if is_city:
                    return line
        
        # Извлекаем из локаций, если есть
        result = self.extract_entities(text)
        if result.locations:
            first_address = result.locations[0]
            city_match = re.search(r'[ГгGg]\.?\s*([А-ЯЁ][а-яё\-]+)', first_address)
            if city_match:
                return city_match.group(1)
            return first_address.split(',')[0].strip()
        
        return None

# Тестовая функция с информативным логом
def test_ner_extractor():
    """Тестирование исправленного NER-экстрактора"""
    
    print("=== 🧪 ТЕСТ ИСПРАВЛЕННОГО NER ЭКСТРАКТОРА ===")
    print("🔄 Загружаем модель Natasha...")
    
    extractor = RussianNERExtractor()
    
    # Все тесты включая новые
    test_signatures = [
        {
            'name': '🔬 СибЛабСервис',
            'text': """С уважением,
Клебанова Ирина Дмитриевна
Специалист по развитию бизнеса
Тел.(3952)78-25-79, доп. 121
ЗАО «СибЛабСервис»
Г. Иркутск, ул. Сталина, д.7, кв. 104"""
        },
        {
            'name': '⚕️ Алтайский центр',
            'text': """С уважением,
Харлова Олеся Анатольевна
Директориса Алтайского центра прикладной биотехнологии
ФБУЗ «Центр гигиены и эпидемиологии в Алтайском крае»
г. Барнаул, Семкина, 1а, кор. 2, оф. 310
8-8182-23-45-38"""
        },
        {
            'name': '🏛️ Республика Татарстан',
            'text': """С уважением,
Иванов Иван Иванович
Менеджер проектов развития биотехнологий в р. Татарстан
ООО "Рога и Копыта" в Республике Татарстан
г. Казань, Кремлёвская ул., д.2, оф.5
+7 (843) 123-45-67"""
        },
        {
            'name': '🌲 Архангельская обл.',
            'text': """С уважением,
Петров Петр Петрович
Директор по найму
АО "Северная Звезда" в Архангельской обл.
г. Архангельск, ул. Ленина, д.10, кв.20
8-8182-23-45-67"""
        }
    ]
    
    for test in test_signatures:
        print(f"\n{'='*60}")
        print(f"📧 ТЕСТ: {test['name']}")
        print(f"{'='*60}")
        print(f"📝 Тестовая подпись:\n{test['text']}")
        
        result = extractor.extract_entities(test['text'])
        city = extractor.extract_city_from_address(test['text'])
        
        print(f"\n📊 РЕЗУЛЬТАТЫ:")
        print(f"👤 Персоны: {result.persons}")
        print(f"🏢 Организации: {result.organizations}")
        print(f"💼 Должности: {result.positions}")
        print(f"📍 Локации (Адреса): {result.locations}")
        print(f"🏙️ Город: {city}")

if __name__ == "__main__":
    test_ner_extractor()
