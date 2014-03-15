import urllib2
from bs4 import BeautifulSoup
import re

def wrapped_open_url(url):
    req = urllib2.Request("http://tv.sohu.com")
    req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:27.0) Gecko/20100101 Firefox/27.0")
    req.add_header("Accept","text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    return urllib2.urlopen(req)

href_pattern = re.compile(r'^http')

class HtmlParser(object):
    def __init__(self,url):
        self._url = url
        self.conn = wrapped_open_url(url)
        self.code = self.conn.getcode()
        self.htmlobj = self._content_read(self.conn)
        
    def get_code(self):
        return self.code
    
    def get_all_href(self):
        links = self.htmlobj.findAll(href=href_pattern)
        return links
    
    def get_useful_mess(self):
        mess_dict = {
            'title':self.htmlobj.title.string,
            'description':self.htmlobj.find("meta",attrs={"name":"description"}),
            'keyword':self.htmlobj.find("meta",attrs={"name":"keywords"}),
            'url':self.conn.geturl(),
        }
        return mess_dict
        
    def _content_read(self,source):
        try:
            return BeautifulSoup(source.read())
        except:
            return None
        
    