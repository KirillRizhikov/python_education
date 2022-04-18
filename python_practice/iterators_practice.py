"""Нужно реализовать класс контейнер Sentence и итератор SentenceIterator к нему.
Класс Sentence:
- принимает только строки, иначе рейзит ошибку TypeError
- принимает только законченные предложения (. ... ! ?), иначе - ValueError
- принимает только одно предложение, иначе - пользовательский MultipleSentencesError

- пример работы: __repr__() >>> <Sentence(words=13, other_chars=7)>
- метод Sentence()._words должен возвращать ленивый итератор. Cмысл тут
 в том, что мы не хотим хранить в объекте список слов, потому что
 предложения могут быть очень большими и занимать много памяти, по
 этому мы будем генерировать его по необходимости и отдавать пользователю
пример работы: Sentence('Hello word!')._words() >>> <generator object
Sentence._words at 0x7f4e8cb065f0> next(Sentence('Hello word!')._words()) >>> 'Hello'
- имеет свойство Sentence().words, которое возвращает список всех
слов в предложении (*напоминаю, что мы не хотим хранить все эти слова в нашем объекте)
- имеет свойство Sentence().other_chars, которое возвращает список
всех не слов в предложении
- умеет отдавать слово по индексу пример работы: Sentence('Hello world!')[0] >>> 'Hello'
- умеет отдавать срез по словам пример работы: Sentence('Hello world!')[:] >>> 'Hello world'
- может быть использован в цикле for
пример работы:
for word in Sentence('Hello world!'):
    print(word)
    >>> 'Hello'
    >>> 'world'
- при передаче в качестве аргумента в функцию iter() возвращает SentenceIterator


Класс SentenceIterator:
- реализует Iterator Protocol
- при передаче в качестве аргумента в функцию next() по очереди возвращает
слова из порождающего его объекта Sentence"""

import re


class MultipleSentencesError(Exception):
    """Multisentense error class"""

    def __init__(self):
        self.message = f"MultipleSentencesError"

    def __str__(self):
        return self.message


class Sentence():
    """Sentence class"""

    def __init__(self, sentence):
        _pattern_nonletter = '[^a-zA-Z]'
        self._length = len([n for n in SentenceIterator(sentence)])
        self.sentence = sentence
        self.other_charts = len(
            [n for n in sentence if re.search(_pattern_nonletter, n)])
        if type(sentence) != str:
            raise TypeError

        if not re.search(r".+[?|!|\.\.\.|\.]", sentence):
            raise ValueError

        if re.search(r".+[?|!|\.\.\.|\.].+\w[?|!|\.\.\.|\.]", sentence):
            raise MultipleSentencesError()

    def _word(self):
        return SentenceIterator(self.sentence)

    def __iter__(self):
        return SentenceIterator(self.sentence)

    def __len__(self):
        return self._length

    def __getitem__(self, index):
        if type(index) == int:
            if index > (self.__len__() - 1):
                raise IndexError()
        return [n for n in SentenceIterator(self.sentence)][index]

    @property
    def words(self):
        """returns words"""
        return list(SentenceIterator(self.sentence))

    @property
    def other_chars(self):
        """returns characters"""
        return [char for char in self.sentence if re.search("\W", char)]

    def __repr__(self):
        return f"Sentence(words={self._length}, other_chars={self.other_charts})"


class SentenceIterator():
    """Sentence iterator class"""

    def __init__(self, sentence):
        self.sentence = sentence
        self._counter = -1
        self.next_word = ['', 0]
        self.start_char = 0

    def find_word(self, start_char):
        """finds word"""
        zipp = zip(self.sentence[start_char:] +
                   "x", "x" + self.sentence[start_char:])
        pattern_letter = '[a-zA-Z]'
        pattern_nonletter = '[^a-zA-Z]'
        end_word = 0
        find_word = ''
        char = ''
        for i in zipp:
            if re.search(pattern_letter, i[0]) and re.search(pattern_letter, i[1]):
                find_word += i[0]
            if re.search(pattern_nonletter, i[0]):
                char += i[0]
            if re.search(pattern_letter, i[0]) and re.search(pattern_nonletter, i[1]):
                break
            end_word += 1
        return find_word, end_word + start_char

    def gen_item(self):
        """Generates next iten"""
        if self.next_word[1] >= len(self.sentence):
            raise StopIteration
        self.next_word = self.find_word(self.start_char)
        self.start_char = self.next_word[1]
        return self.next_word[0]

    def __next__(self):
        return self.gen_item()

    def __iter__(self):
        return self


sentence = Sentence("hello, Kharkiv  and World...")
for word in sentence:
    print(word)
print(sentence._word())
for i in sentence:
    print(i)
print(len(sentence))
print(sentence[1])
print(sentence[0])
print(sentence[3])
print(sentence.words)
print(sentence[:])
print(sentence[:2])
print(repr(sentence))
print(sentence.other_chars)
