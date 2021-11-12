import logging
import re
import codecs
from bs4 import BeautifulSoup
import lxml.html
from lxml import html, etree
from io import StringIO
import urllib.request
import urllib.error
from urllib.request import Request, urlopen
from urllib.parse import urlparse
from urllib.parse import urljoin
from collections import Counter
from collections import defaultdict

archive = {}
subdomain = defaultdict(set)
stopWords = ["a","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","be","because",
"been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did",
"didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't",
"has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself",
"his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't",
"my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours",
"ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such",
"than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll",
"they're","they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're",
"we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why",
"why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves"] 
tokens = defaultdict(int)
longest = 0
longestLink = "" 

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

    def start_crawling(self):

        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        # max = 0
        # maxLink = ""
        # global longestLink
         
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))            
            url_data = self.corpus.fetch_url(url)
          
            # current = 0   
                             
            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    #current+=1
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
            
            #Max Link analytics
            # if current > max:
            #     max = current
            #     maxLink = url_data['url'] if not url_data['is_redirected'] else url_data['final_url']
        
        #print statements for output.txt
        # with codecs.open("output.txt", "w", 'UTF-8') as file:
        #     file.write("Part One:\n")
        #     for link in (subdomain): 
        #         file.write("Subdomain: " + str(link) + "\tURLs processed: " + str(len(subdomain[link]))+ "\n")
        #     file.write("\nPart Two:\nPage with most valid outlinks: " + maxLink + "\nLink Count:" + str(max) + "\n")
        #     file.write("\nPart Three:\nList of URLs:\n")
        #     for elements in archive:
        #         file.write(str(elements)+"\n")
                
        #     file.write("Traps Found:\n")

        #     for elements in archive:
        #         if archive[elements] > 600:
        #             file.write(str(elements)+ " "+ str(archive[elements]) + "\n")
           
        #     file.write("\nPart Four:\n")
        #     file.write("Longest Page: " + str(longestLink) + "\n")
        #     file.write("\nPart Five:\n")
        #     for key in sorted(tokens.items(), key=lambda item: item[1], reverse = True)[:50]:
        #         file.write(str(key) + "\n")
       



    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        global stopWords
        global subdomain
        global tokens
        global longest
        global longestLink
        
        #check for corpus size and http code
        if url_data['size'] == 0 or url_data['http_code'] == '404':
            return []
        urlContent = url_data['content']
        #check for redirection
        if url_data['is_redirected'] is True:
            url = url_data['final_url']
        else:
            url = url_data['url']


        #storing subdomain
        parsedUrl = urlparse(url)
       
        outputLinks = []

        soup = BeautifulSoup(urlContent, 'lxml')

        #remove html tag and tokenize
        text = soup.get_text()

        #determine longest page
        currentLength = len(text)
        if currentLength > longest:
            longest = currentLength
            longestLink = url

        splitText = text.split()
        resultText = [word for word in splitText if word.lower() not in stopWords]
        joinedText = ' '.join(resultText)
        #tokenize(joinedText)

        s = ''
        for char in joinedText:
            if char.isalnum():
                s += char.lower()
            else:
                if not (s.isspace() or len(s) == 0):
                    tokens[s] +=1
                    s = ''

        #print(joinedText)
        for tag in soup.find_all('a', href=True):
            outputLinks.append(urljoin(url, tag.get('href')))

        #subdomain analytics
        subdomainName = parsedUrl.hostname.split('/')[0]
        subdomain[subdomainName].update(set(outputLinks))

        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        global archive
        global max
        global maxLink
        
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        #filter out urls that are too long
        if len(parsed.geturl()) > 85:
            return False 
                
        #check for continuous repeating directories
        split = url.split('/')
        url_elements = Counter(split) 
        for element in url_elements: 
            if url_elements[element] > 30:
                return False 

        #history trap: check to see how many times we've encountered the same dynamic link
        if '?' in url:
            querySplit = url.split('?')
            link = querySplit[0]

            if (link in archive): 
                if archive[link] > 600:
                    archive[link] += 1
                    return False
                else:
                    archive[link] += 1
            else: 
                archive[link] = 1

            
        #skip unneccessary files, 1/25/20 added file type gctx, npy, py
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf|gctx|npy|py)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False
    
    def tokenize(text): 
        global tokens
        s = ' '
        for char in text:
            if char.isalnum():
                s += char.lower()
            else:
                tokens.append(s)
                s = ''
        return 
    

        
        



