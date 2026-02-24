import re
from datetime import datetime

from loguru import logger


def replace_str(text: str, src: str, dst: str) -> str:
    try:
        result = re.sub(
            rf'(?<!\w){re.escape(src)}(?!\w)',
            dst,
            text,
            flags=re.IGNORECASE
        )
    except Exception as e:
        logger.error(f"Failed to replace {src} -> {dst} in {text}")
        result = text
    return result


def replace_str_punc(text: str, src: str, dst: str) -> str:
    try:
        result = re.sub(
            rf'(?<!\w){re.escape(src)}(?!\w)',
            dst,
            text,
            flags=re.IGNORECASE
        )
    except Exception as e:
        logger.error(f"Failed to replace {src} -> {dst} in {text}")
        result = text
    return result


def find_str_punc(text: str, term: str):
    try:
        matches = re.findall(
            rf'(?<!\w){re.escape(term)}(?!\w)',
            text,
            flags=re.IGNORECASE
        )
        if len(matches) > 0:
            return matches[0]
    except Exception as e:
        logger.error(f"Failed to find {term} in {text}: {e}")
    return None


class StringUtils:
    def __init__(self):
        import spacy
        self.nlp = spacy.load("en_core_web_sm")

    def replace_str_lemma(self, text: str, src: str, dst: str) -> str:
        src_lemmas = [token.lemma_ for token in self.nlp(src)]
        doc = self.nlp(text)
        new_tokens = []
        i = 0
        while i < len(doc):
            if [token.lemma_ for token in doc[i:i + len(src_lemmas)]] == src_lemmas:
                logger.info("LEMMA REPLACEMENT")
                new_tokens.append(dst)
                i += len(src_lemmas)
            else:
                new_tokens.append(doc[i].text)
                i += 1
        new_text = " ".join(new_tokens)
        print("BEFORE: ", text)
        print("AFTER: ", new_text)
        print("\n----------------\n")
        return new_text


def check_str_punc(text: str, src: str) -> bool:
    try:
        pattern = r'(?<![\w.]){}(?!\w)'.format(re.escape(src))
        if re.search(pattern, text, flags=re.IGNORECASE):
            return True
    except Exception as e:
        logger.error(f"Failed to search {src} in {text}")
    return False


class Timer:
    start_time: datetime

    def __init__(self):
        self.start_time = datetime.now()

    @staticmethod
    def start():
        return Timer()

    def lap(self) -> float:
        return (datetime.now() - self.start_time).total_seconds()
