import xml.sax
import gevent
import re, sys, os
from string import printable
from nltk.stem import PorterStemmer

# Flush this dictionary after reading 1000 documents
# Syntax => {category: {word: [docid, count]...}...,...}
complete_index = {"T": {}, "B": {}, "C": {}, "R": {}, "E": {}, "I": {}}

# Directory name of the output index
index_dir = "index/"

# The output index directory names
index_mappings = {"T": "title/",
                  "B": "body/",
                  "C": "category/",
                  "R": "references/",
                  "E": "links/",
                  "I": "infobox/"}

# Used to seperate the index in different files
documents_scanned = 0

# Total documents scanned
N = 0

# Index file Index
indexfile_no = 1

# Safely remove these words from the body
f = open("stopwords.txt")
STOP_WORDS = f.readlines()
f.close()

# Page ID to title mapping
pageid_to_title = None

# Strip with these punctuation symbols while tokenization
PUNCTUATION = [u".", u",", u"|", u"-", u":", u";", u"?", u"(", u")", u"*", u"\"", u"\'", u"=", u'\\', u"&", u'/', u"<", u">", u"[", u"]", u"{", u"}", u"#", u"!", u"%"]

# For Punctuation removal from token
p = PorterStemmer()

# Hopefully these will speed up the code ! :p
references_re = re.compile(u"={2,3} ?references ?={2,3}.*?={2,3}", re.DOTALL)
external_links_re = re.compile(u"={2,3} ?external links?={2,3}.*?\n\n", re.DOTALL)
category_re = re.compile(u"\[\[category:.*?\]\]", re.DOTALL)
infobox_re = re.compile(u"\{\{infobox.*?\}\}", re.DOTALL)
url_re = re.compile(u"((http[s]?:\/\/)|(www\.))(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+")
number_re = re.compile(u"(\d+\,?)+(\.)?\d+")

# Extra stuff in references
UNNECESSARY = ["access", "first", "title", "url", "date", "publisher", "last", "location", "cite", "web", "book", "article", "author", "year", "isbn", "ref", "editor", "volume", "issue"]

# Remove == and other unnecessary tokens
REFERENCES_SUB = "(={2,3} ?[rR]eferences ?={2,3})|(={2,3})|" + "|".join(UNNECESSARY)

# Extra noise
NOISE = u".,|+-@~`:;?()*\"'=\\&/<>[]{}#!%^$ "

# Noise filter | Making this dictionary to improve time
NOISE_FILTER = {}
filters = STOP_WORDS + PUNCTUATION + UNNECESSARY
for stop_word in filters:
    word = stop_word.strip()
    NOISE_FILTER[word] = 1

# ==============================================================================
class Document(object):
    """
        Document class which handles
        tokenization, indexing, etc
    """

    # --------------------------------------------------------------------------
    def __init__(self, page_id, title, text):

        self.page_id = page_id
        self.title = title.lower()
        self.refernces = ""
        self.external_links = ""
        self.categories = []
        self.text = text.lower()
        self.category_size = {"R": 0, "T": 0, "B": 0, "C": 0, "E": 0}
        self.split_sections(self.text)

    # --------------------------------------------------------------------------
    def split_sections(self, text):
        """
            Section flags --> R => References,
                              T => Title,
                              B => Body,
                              C => Category,
                              E => External links
        """

        text = text.encode("ascii", "ignore")
        new_text = text

        # Different threads for parallel processing
        threads = []

        # Tokenize title
        title_tokens = self.title.split()
        self.category_size["T"] = len(title_tokens)
        threads.append(gevent.spawn(self.tokenize, title_tokens, "T"))

        # Remove citation text
        new_text = re.sub(r"\{\{[c|C]ite.*?\}\}", u"", new_text)

        # Get the references section
        references = ""
        try:
            references = references_re.search(text).group()
        except AttributeError:
            pass

        # Remove the extra words which should not be counted for index
        if references != "":
            references = re.sub(REFERENCES_SUB, u"", references)
            new_text = re.sub(references_re, u"==", new_text)
            references_tokens = references.split()
            self.category_size["R"] = len(references_tokens)
            threads.append(gevent.spawn(self.tokenize, references_tokens, "R"))

        # Get Categories
        categories = re.findall(category_re, text)
        new_text = re.sub(category_re, u"", new_text)
        category_tokens = []
        if categories != []:
            for ctg in categories:
                category_tokens.extend(ctg[11:-2].split())
            # Design decision - Space seperated categories
            # are considered seperately.
            self.category_size["C"] = len(category_tokens)
            threads.append(gevent.spawn(self.tokenize, category_tokens, "C"))

        # Get External links
        links = None
        try:
            links = external_links_re.search(text).group()
            new_text = re.sub(external_links_re,
                              u"",
                              new_text).encode("ascii", "ignore")
        except AttributeError:
            pass

        if links:
            links = links.split("\n")
            for link in links:
                if link == "":
                    break
                links_tokens = link.split()
                self.category_size["E"] += len(links_tokens)
                threads.append(gevent.spawn(self.tokenize, links_tokens, "E"))

        try:
            infobox_tokens = infobox_re.search(new_text).group()
            infobox_tokens = infobox_tokens.split()
            if infobox_tokens != []:
                threads.append(gevent.spawn(self.tokenize,
                                            infobox_tokens,
                                            "I"))
                new_text = re.sub(infobox_re, "", new_text)
        except AttributeError:
            pass

        body_tokens = new_text.split()
        self.category_size["B"] = len(new_text)
        threads.append(gevent.spawn(self.tokenize, body_tokens, "B"))

        # Start parallel processing
        gevent.joinall(threads)

    # --------------------------------------------------------------------------
    def update_index(self, section, token):

        page_id = self.page_id
        category = complete_index[section]

        if token not in category:
            category[token] = {}
        if page_id not in category[token]:
            category[token][page_id] = 0

        category[token][page_id] += 1
        # @Todo: Add the following to normalize
        #.0 / self.category_size[section]

    # --------------------------------------------------------------------------
    def is_url(self, token):
        try:
            tmp = url_re.match(token).group()
            return True
        except AttributeError:
            return False

    # --------------------------------------------------------------------------
    def is_number(self, token):
        try:
            number_re.match(token).group()
            return True
        except:
            return False

    # --------------------------------------------------------------------------
    def tokenize(self, tokens, section):

        for token in tokens:
            splitted = re.split(u"(\|)|\-|\:|\;|\?|\(|\)|\*|\=|\\|\&|\/|\<|\>|\[|\]|\{|\}|\#|\+|\&|\%20|\_|\&nbsp|(\')|(\")", token)
            for i in splitted:
                if i is not None and len(i) > 1:
                    tmpi = i.strip(NOISE)
                    if tmpi not in NOISE_FILTER:
                        tmpi = p.stem_word(tmpi)
                        tmpi = tmpi.strip(NOISE)
                        tmpi = filter(lambda x: x in printable, tmpi)
                        self.update_index(section, tmpi)

# ------------------------------------------------------------------------------
def write_to_index(section):

    category_index = complete_index[section]
    filename = index_dir + index_mappings[section] + "file" + str(indexfile_no)
    # Store current sys.stdout
    prev_stdout = sys.stdout
    sys.stdout = open(filename, "w")

    for token in sorted(category_index.items()):
        line = token[0] + "#"
        for doc_id in token[1]:
            line += hex(int(doc_id))[2:] + "-" + \
                    str(category_index[token[0]][doc_id]) + ";"
        print line[:-1].encode("utf-8")

    # Restore sys.stdout
    sys.stdout = prev_stdout

# ------------------------------------------------------------------------------
def create_index():
    """
        Write the complete_index to a file with a different format
    """

    # @Todo: Yet to sort by tf in each file

    threads = []
    for section in complete_index:
        threads.append(gevent.spawn(write_to_index, section))
    gevent.joinall(threads)

# ------------------------------------------------------------------------------
def dump_index():

    global documents_scanned, indexfile_no, complete_index
    create_index()
    complete_index = {"T": {}, "B": {}, "C": {}, "R": {}, "E": {}, "I": {}}
    indexfile_no += 1
    documents_scanned = 0

# ==============================================================================
class PageHandler(xml.sax.ContentHandler):

    # --------------------------------------------------------------------------
    def __init__(self):
       self.complete_data = ""
       self.tag = ""
       self.title = ""
       self.page_id = ""
       self.page_flag = False
       self.text = ""

    # --------------------------------------------------------------------------
    def startElement(self, tag, attributes):
        self.tag = tag

    # --------------------------------------------------------------------------
    def endElement(self, tag):

        global N, documents_scanned, pageid_to_title
        if tag == "page":
            N += 1
            d = Document(self.page_id, self.title, self.text)

            pageid_to_title += str(self.page_id) + "," + self.title + "\n"
            self.complete_data = ""
            self.tag = ""
            self.title = ""
            self.text = ""
            self.page_flag = False
            documents_scanned += 1
            if documents_scanned == 1000:
                dump_index()
                documents_scanned = 0

    # --------------------------------------------------------------------------
    def characters(self, content):

        if self.tag == "id":
            if self.page_flag is False:
                self.page_id = content
                self.page_flag = True
        elif self.tag == "title":
            if self.title == "" and self.title != content:
                self.title = content
        elif self.tag == "text":
            self.text += content


# ------------------------------------------------------------------------------
if __name__ == "__main__":

    pageid_to_title = ""
    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    handler = PageHandler()
    parser.setContentHandler(handler)
    parser.parse("main-dump.xml")
    if documents_scanned > 0:
        dump_index()

    f = open("sorted/N", "w")
    f.write(str(N))
    f.close()

    pageid_to_title = filter(lambda x: x in printable, pageid_to_title)
    f = open("sorted/pagetitle", "w")
    f.write(pageid_to_title)
    f.close()
