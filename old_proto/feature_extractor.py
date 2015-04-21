import nltk as nl
import re, requests, string, json
from bs4 import BeautifulSoup
from html.parser import HTMLParseError
from urllib.parse import urlparse
from urllib.parse import urljoin
from collections import Iterable

'''
    Requires nltk data
'''

class feature_extractor(object):
    """ Extract features from tweet and urls metadata"""
    def __init__(self):
    # def __init__(self, arg):
        # super(feature_extractor, self).__init__()
        # self.arg = arg
        self.stemFR = nl.stem.snowball.FrenchStemmer(ignore_stopwords=False)
        self.stemEN = nl.stem.snowball.EnglishStemmer(ignore_stopwords=False)
        # self.exclude = set(string.punctuation)
        # self.table = string.maketrans("","")
        self.filter = re.compile('[0-9%s]' % re.escape(string.punctuation))

    def remove_punct(self, text):
        # return text.translate(self.table, self.exclude)
        return self.filter.sub('', text)

    def stemmer(self, tokens, lang):
        result = None
        if lang == 'en':
            result = [self.stemEN.stem(word) for word in tokens]
        elif lang == 'fr':
            result = [self.stemFR.stem(word) for word in tokens]
        return(result)

    ## IMPROVE ABSTRACTION FOR PARSERS, CODE REDUNDANCY
    def text_parser(self, text, lang):
        result = []
        ## Clean text 
        text = re.sub(r"http\S+", "", text)
        # text = re.sub(r"(RT)|[#@]", "", text) # remove #, @ and RT
        text = self.remove_punct(text)
        ## Tokenize text    
        tokens = nl.word_tokenize(text)
        tokens = self.stemmer(tokens, lang)
        if isinstance(tokens, Iterable):
            result.extend(tokens)
        return(result)

    def meta_parser(self, meta):
        result = []
        lang = meta['lang']
        title = meta['title']
        description = meta['description']
        if isinstance(title, str):
            title = nl.word_tokenize(self.remove_punct(title))
            title_tokens = self.stemmer(title, lang)
            if isinstance(title_tokens, Iterable):
                result.extend(title_tokens)
        if isinstance(description, str):
            description = nl.word_tokenize(self.remove_punct(description))
            description_tokens = self.stemmer(description, lang)
            if isinstance(description_tokens, Iterable):
                result.extend(description_tokens)
        return(result)

    def transform(self, tweet):
        result = []
        lang = tweet['lang'][0:2]
        if len(lang) != 2:
            lang = "en"
        result.extend(self.text_parser(tweet["text"], lang))
        # url_contents = []
        # urls = tweet["entities"]["urls"]
        # if len(urls) > 0:
        #     for url in urls:
        #         meta = self.get_meta(url["expanded_url"])
        #         result.extend(self.meta_parser(meta))
        return(result)

    def get_meta(self, url):
        '''
            Get meta data of a website from url
        '''
        data = {}
        data["canonical"] = None
        data["title"] = ""
        data["description"] = ""
        data["lang"] = "en" # Default language: english
        # data["opengraph"] = {}
        data["errstatus"] = None
        try:
            response = requests.get(url, timeout=1) #timeout: 3
            if response.status_code == 200: 
                soup = BeautifulSoup(response.text)

                #get canonical
                canonical = soup.find("link", rel="canonical")
                if canonical:
                    data["canonical"] = canonical["href"]
                #get title
                # if soup.title.string:
                if soup.title:
                    data["title"] = soup.title.string
                #get description
                if soup.find('meta', attrs={'name':'description'}):
                    description = soup.find('meta', attrs={'name':'description'})
                    if description.has_key("content"):
                        data["description"] = description["content"]
                # Complete metadata with facebook open graph data
                if soup.findAll('meta', {"property":re.compile("^og")}):
                    for tag in soup.findAll('meta', {"property":re.compile("^og")}):
                        tag_type = tag['property']
                        # data["opengraph"][tag_type] = tag['content']
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
        return(data)

