#! /usr/bin/python2
# -*- coding: utf8 -*-

import pykka
import time
import nltk as nl
import re, requests, string
from bs4 import BeautifulSoup
# from html.parser import HTMLParseError # python 3
from HTMLParser import HTMLParseError # python 2
from collections import Iterable

class Parser(pykka.ThreadingActor):
    """ Superclass, tools for text cleaning and stemming """
    def __init__(self):
        super(Parser, self).__init__()
        self.stemFR = nl.stem.snowball.FrenchStemmer(ignore_stopwords=False)
        self.stemEN = nl.stem.snowball.EnglishStemmer(ignore_stopwords=False)
        self.punct_filter = re.compile('[0-9%s]' % re.escape(string.punctuation))
        
    def on_receive(self, message):
        return None
        
    def remove_punct(self, text):
        # return text.translate(self.table, self.exclude)
        return self.punct_filter.sub('', text)
        
    def stemmer(self, tokens, lang):
        result = None
        if lang == 'en':
            result = [self.stemEN.stem(word) for word in tokens]
        elif lang == 'fr':
            result = [self.stemFR.stem(word) for word in tokens]
        return(result)

class Tw_parser(Parser):
    """ parse list of tweets and create
    "   the document representing the user
    """
    def __init__(self):
        super(Tw_parser, self).__init__()

    def parse_tweets(self, texts, texts_lang):
        """ Assume message is a tweet collection """
        document = []
        [document.extend(self.text_parser(txt, lang)) for txt, lang in zip(texts, texts_lang)]
        out_msg = " ".join(document)
        return out_msg

    def text_parser(self, text, lang):
        result = []
        ## Clean text 
        text = re.sub(r"http\S+", "", text)
        text = self.remove_punct(text)
        ## Tokenize text    
        tokens = nl.word_tokenize(text)
        ## Stem text
        tokens = self.stemmer(tokens, lang)
        if isinstance(tokens, Iterable):
            result.extend(tokens)
        return result

class Url_parser(Parser):
    """ Fetch metadata of Url, parse it and send it back """
    def __init__(self):
        super(Url_parser, self).__init__()

    def parse_url(self, url):
        try :
            meta = self.get_meta(url)
            document = self.meta_parser(meta)
            out_msg = " ".join(document)
        except:
            out_msg = ""
        return out_msg

    def meta_parser(self, meta):
        result = []
        lang = meta['lang']
        title = unicode(meta['title']) ## These unicode() not useful in python3
        description = unicode(meta['description'])
        # if isinstance(title, str): python3
        if isinstance(title, unicode): 
            title = nl.word_tokenize(self.remove_punct(title))
            title_tokens = self.stemmer(title, lang)
            if isinstance(title_tokens, Iterable):
                result.extend(title_tokens)
        # if isinstance(description, str): # python3
        if isinstance(description, unicode):
            description = nl.word_tokenize(self.remove_punct(description))
            description_tokens = self.stemmer(description, lang)
            if isinstance(description_tokens, Iterable):
                result.extend(description_tokens)
        return result

    def get_meta(self, url):
        '''
            Get meta data of a website from url
        '''
        data = {}
        data["canonical"] = None
        data["title"] = ""
        data["description"] = ""
        data["lang"] = "en" # Default language: english
        data["errstatus"] = None
        try:
            response = requests.get(url, timeout=3)
            if response.status_code == 200: 
                soup = BeautifulSoup(response.text)
                page_attr = soup.find('html').attrs
                # get lang
                if 'lang' in page_attr:
                    data['lang'] = page_attr['lang']

                # get canonical
                canonical = soup.find("link", rel="canonical")
                if canonical:
                    data["canonical"] = canonical["href"]
                # get title
                if soup.title:
                    data["title"] = soup.title.string
                # get description
                if soup.find('meta', attrs={'name':'description'}):
                    description = soup.find('meta', attrs={'name':'description'})
                    if description.has_key("content"):
                        data["description"] = description["content"]
                # Complete metadata with facebook open graph data
                if soup.findAll('meta', {"property":re.compile("^og")}):
                    for tag in soup.findAll('meta', {"property":re.compile("^og")}):
                        tag_type = tag['property']
                        if tag_type == "og:title" and data["title"] is None:
                            data["title"] = tag["content"]
                        if tag_type == "og:description" and data["description"] is None:
                            data["description"] = tag["content"]
                        if tag_type == "og:url" and data["canonical"] is None:
                            data["canonical"] = tag["content"]
                        if tag_type == "og:locale" :
                            data["lang"] = tag["content"][0:2] # Get lang, no use of country
                # Ensure canonical is not empty
                if not data['canonical'] or len(data['canonical']) == 0:
                    data['canonical'] = url
            else:
                data["canonical"] = url
                data["errstatus"] = "URL returned status "+str(response.status_code)
        except HTMLParseError:
            data["canonical"] = url
            data["errstatus"] = "Error parsing page data"
        return data