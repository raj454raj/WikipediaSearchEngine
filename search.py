import re, os, subprocess
import linecache
from operator import itemgetter
from string import printable
from nltk.stem import PorterStemmer
from math import log10

# Safely remove these words from the body
f = open("stopwords.txt")
STOP_WORDS = f.readlines()
f.close()

# Extra stuff in references
UNNECESSARY = ["access", "first", "title", "url", "date", "publisher", "last", "location", "cite", "web", "book", "article", "author", "year", "isbn", "ref", "editor", "volume", "issue"]

# Strip with these punctuation symbols while tokenization
PUNCTUATION = [u".", u",", u"|", u"-", u":", u";", u"?", u"(", u")", u"*", u"\"", u"\'", u"=", u'\\', u"&", u'/', u"<", u">", u"[", u"]", u"{", u"}", u"#", u"!", u"%"]

p = PorterStemmer()

# Extra noise
NOISE = u".,|+-@~`:;?()*\"'=\\&/<>[]{}#!%^$ "

# Noise filter | Making this dictionary to improve time
NOISE_FILTER = {}
filters = STOP_WORDS + PUNCTUATION + UNNECESSARY
for stop_word in filters:
    word = stop_word.strip()
    NOISE_FILTER[word] = 1

section_mapping = {"B": "body/",
                   "R": "references/",
                   "T": "title/",
                   "I": "infobox/",
                   "E": "links/",
                   "C": "category/"}

priority = {"B": 10,
            "R": 30,
            "T": 200,
            "I": 50,
            "E": 20,
            "C": 40}

# doc id and their tf-idf scores
all_mapping = {}

# Get title from page id
idtotitle = {}

N = None
f.close()

# --------------------------------------------------------------------------
def tokenize(tokens):

    new_tokens = []
    for token in tokens:
        splitted = re.split(u"(\|)|\-|\:|\;|\?|\(|\)|\*|\=|\\|\&|\/|\<|\>|\[|\]|\{|\}|\#|\+|\&|\%20|\_|\&nbsp|(\')|(\")", token)
        for i in splitted:
            if i is not None and len(i) > 1:
                tmpi = i.strip(NOISE)
                if tmpi not in NOISE_FILTER:
                    tmpi = p.stem_word(tmpi)
                    tmpi = tmpi.strip(NOISE)
                    tmpi = filter(lambda x: x in printable, tmpi)
                    new_tokens.append(tmpi.lower())
    return new_tokens

# -----------------------------------------------------------------------------
def parse_query(query):
    query_dict = {"B": None,
                  "R": None,
                  "T": None,
                  "I": None,
                  "E": None,
                  "C": None}
    categories = ["b: ", "r: ", "t: ", "i: ", "e: ", "c: "]
    field_query = False
    for category in categories:
        if category in query:
            field_query = True
            break
    if field_query:
        category_re = "(b\:\s)|(r\:\s)|(t\:\s)|(i\:\s)|(e\:\s)|(c\:\s)"
        tmpquery = re.split(category_re, query)
        tmpquery = filter(lambda x: x != None and x != "", tmpquery)
        for i in xrange(len(tmpquery)):
            if tmpquery[i] in categories:
                query_dict[tmpquery[i][0].upper()] = tokenize(tmpquery[i + 1].split())
    else:
        query_tokens = tokenize(query.split())
        for section in query_dict:
            query_dict[section] = query_tokens

    return query_dict

# -----------------------------------------------------------------------------
def lower_bound(lo, hi, x, filename):
    if lo > hi:
        return hi

    mid = (lo + hi) >> 1;
    midline = linecache.getline(filename, mid).strip()

    if midline == x:
        return lower_bound(lo, mid - 1, x, filename)
    elif midline > x:
        return lower_bound(lo, mid - 1, x, filename)
    else:
        return lower_bound(mid + 1, hi, x, filename)

# -----------------------------------------------------------------------------
def binary_search(lo, hi, x, filename):

    if lo > hi:
        return -1

    mid = (lo + hi) >> 1;
    row = linecache.getline(filename, mid).split("#")
    midline = row[0]
    if midline == x:
        return row[1]
    elif midline > x:
        return binary_search(lo, mid - 1, x, filename)
    else:
        return binary_search(mid + 1, hi, x, filename)

# -----------------------------------------------------------------------------
def get_postings_list(token, section):

    global N

    filename = "secondaryIndex/" + section + "index"
    command = "wc -l " + filename
    result = subprocess.check_output(command, shell=True)
    lenfile = int(result.split()[0])

    # Get file_number from secondary index
    file_number = lower_bound(1, lenfile, token, filename) + 1

    filename = "sorted/" + section + "file" + str(file_number)
    command = "wc -l " + filename
    result = subprocess.check_output(command, shell=True)
    lenfile = int(result.split()[0])

    # Get the postings row
    postings_list = binary_search(1, lenfile, token, filename)
    if postings_list == -1:
        # Word not found
        return ""

    postings_list = postings_list.split(";")
    postings_list = postings_list[:-1]
    return postings_list[:500]

# -----------------------------------------------------------------------------
def union_tfidfs(postings_list, section):

    length_of_list = len(postings_list)

    # Take union of tf-idf of query terms by adding docids to all_mapping
    for doc in postings_list:
        tmp = doc.split("-")
        doc_id = tmp[0]
        tf = log10(1 + int(tmp[1]))
        idf = log10(N * 1.0 / length_of_list)
        if all_mapping.has_key(doc_id):
            all_mapping[doc_id] += priority[section] * tf * idf
        else:
            all_mapping[doc_id] = priority[section] * tf * idf
    f.close()

# -----------------------------------------------------------------------------
if __name__ == "__main__":


    f = open("sorted/N")
    N = int(f.read())
    f.close()

    f = open("sorted/pagetitle")
    lines = f.readlines()
    f.close()
    for line in lines:
        tmp = line.split(",")
        idtotitle[tmp[0]] = ",".join(tmp[1:]).strip()

    query = raw_input("Enter search query: ")
    query_dict = parse_query(query)

    while query != "quit":
        for section in query_dict:
            if query_dict[section]:
                for token in query_dict[section]:
                    postings_list = get_postings_list(token,
                                                      section_mapping[section])
                    union_tfidfs(postings_list, section)

        relevant = sorted(all_mapping.items(), key=itemgetter(1), reverse=True)
        for doc in relevant[:10]:
            print int(doc[0], 16), idtotitle[doc[0]]

        print "\n\n*****************************\n\n"
        all_mapping = {}
        query = raw_input("Enter search query: ")
        query_dict = parse_query(query)
