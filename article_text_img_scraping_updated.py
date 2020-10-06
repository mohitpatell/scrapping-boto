import requests
import re
import time
import pandas as pd
import numpy as np
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

import os
import shortuuid
import json
import boto3
s3 = boto3.client('s3')

# from boto.s3.connection import S3Connection
# s3 = S3Connection(os.environ['S3_KEY'], os.environ['S3_SECRET'])

def is_English(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

def clean_string(a_string):
    new_string = re.sub(r'‘|’|"|—|“|”|,', '', a_string).strip()
    new_string = re.sub(r'–', ' ', new_string)
    #new_string = re.sub(r'é', 'e', new_string)
    return new_string

def filter_string(a_string):
    if a_string == '.':
        return False
    elif a_string == '':
        return False
    else:
        return True

def get_uuid(url):

    # GENERATE A UNIQUE ID BASED ON THE URL

    unique_id = shortuuid.uuid(name=url)

    return unique_id

class medium_article_scrapping():
    """
    CLASS DEFINITION TO SCRAPE ALL DATA FOR AN ARTICLE AND RETURN JSON OUTPUT.
    """

    def __init__(self):
        self.scraper_class = 'medium_article_scrapping'



    def get_title(self, soup):
        try:
            title = soup.find('h1').text
            return title

        except:
            print("Couldn't get title from article.")
            return 'None'

    def get_subtitle(self, soup):
        try:
            subtitle_soup = soup.find('h1').parent.parent.next_sibling.find('h2')
            subtitle = clean_string(subtitle_soup.text)
            return subtitle

        except:
            print("Couldn't (or didn't) get subtitle from article.")
            return 'None'

    def get_tags(self, soup):
        try:
            tags = []
            tags = [x.text for x in soup.find_all("a", href=re.compile(".*tag.*"))]
            if len(tags) == 0:
                return 'None'
            else:
                return tags

        except:
            print("Couldn't get article tags.")

    def get_tags_link(self, soup):
        try:
            tags_link = []
            tags = [x.text for x in soup.find_all("a", href=re.compile(".*tag.*"))]
            for tag in tags:
                tags_link.append("https://medium.com/tag/" + tag + "/archive/" )
            if len(tags_link) == 0:
                return 'None'
            else:
                return tags_link

        except:
            print("Couldn't get article tag links.")

    def get_author(self, soup):
        try:
            author = soup.find('div',style=re.compile(r'flex:.*')).find('a').text
            return author
        except:
            print("Couldn't get author from article.")

    def get_h1_headers(self, soup):
        try:
            article_soup = soup.find('article')
            h1_header_soups = article_soup.find_all('h1')

            if len(h1_header_soups) == 1:
                return 'None'
            else:
                h1_headers = [clean_string(x.text) for x in h1_header_soups[1:]]
                return h1_headers
        except:
            print("Couldn't get h1 headers.")


    def get_h2_headers(self, soup):
        try:
            h2_headers = []

            article_soup = soup.find('article')
            h2_header_soups = article_soup.find_all('h2')

            for header_soup in h2_header_soups:
                if header_soup.text == "Dive in. We'll learn what you like along the way.":
                    continue
                else:
                    h2_headers.append(clean_string(header_soup.text))

            if len(h2_headers) == 0:
                return 'None'
            else:
                return h2_headers

        except:
            print("Couldn't get h2 headers.")

    def get_paragraphs(self, soup):
        try:
            article_soup = soup.find('article')
            paragraphs = [clean_string(x.text) for x in article_soup.find_all('p')]
            return paragraphs
        except:
            print("Couldn't scrape paragraphs.")

    def get_blockquotes(self, soup):
        try:
            article_soup = soup.find('article')
            blockquotes = [x.text for x in article_soup.find_all('blockquote')]
            return blockquotes
        except:
            print("Couldn't (or didn't) find blockquotes.")
            return 'None'

    def get_bolded(self,soup):
        try:
            article_soup = soup.find('article')

            bolded = [x.text.strip() for x in article_soup.find_all('strong')]
            bolded = [clean_string(x) for x in bolded if filter_string(x)]
            bolded = [x for x in bolded if x]

            if len(bolded) == 0:
                return 'None'
            else:
                return bolded
        except:
            print("Couldn't get bolded text.")

    def get_italics(self, soup):
        try:
            article_soup = soup.find('article')
            italics = [x.text.strip() for x in article_soup.find_all('em')]
            italics = [clean_string(x) for x in italics if filter_string(x)]

            if len(italics) == 0:
                return 'None'
            else:
                return italics
        except:
            print("Couldnt get italicized text.")

    def count_bullet_lists(self, soup):
        try:
            article_soup = soup.find('article')
            return len(article_soup.find_all('ul'))

        except:
            print("Couldn't count bullet lists.")

    def count_numbered_lists(self, soup):
        try:
            article_soup = soup.find('article')
            return len(article_soup.find_all('ol'))

        except:
            print("Couldn't count numbered lists.")

    def figures(self, soup):
        try:
            article_soup = soup.find('article')
            figures = article_soup.find_all('figure')
            img_info = []
            for f in figures:
                img_tag = f.findChildren("img")
                img_split = (img_tag[0]["src"]).split('?')[0]
                img_replace = img_split.replace('60','1024')
                img_info.append((img_replace, img_tag[0]["alt"]))
            return img_info

        except:
            print("Couldn't find images.")

    def count_gists(self, soup):
        try:
            gists = []
            article_soup = soup.find('article')
            for fig in article_soup.find_all('figure'):
                gist_soup = fig.find('iframe', title=re.compile('.*\.py'))
                if gist_soup == None:
                    continue
                else:
                    gists.append(gist_soup)

            return len(gists)

        except:
            print("Couldn't count gists.")

    def count_code_chunks(self, soup):
        try:
            article_soup = soup.find('article')
            code_chunk_soups = article_soup.find_all('pre')
            return len(code_chunk_soups)

        except:
            print("Couldn't count code chunks.")

    def count_vids(self, soup):
        try:
            yt_vids = []
            article_soup = soup.find('article')
            for figure in article_soup.find_all('figure'):
                yt_soup = figure.find('iframe', src=re.compile('.*youtube.*'))
                if yt_soup == None:
                    continue
                else:
                    yt_vids.append(yt_soup)

            return len(yt_vids)

        except:
            print("Couldn't get YouTube videos.")

    def links(self, soup):
        try:
            article_soup = soup.find('article')
            link_soups = article_soup.find_all('a', {'target': '_blank'})
            links = []
            for link in link_soups:
                links.append(link.get('href'))
            return links

        except:
            print("Couldn't find links.")

    def ext_links(self, soup):
        try:
            article_soup = soup.find('article')
            ext_link_soups = article_soup.find_all('a', attrs={'href': re.compile("http[s]://")})
            ext_links = []
            for ext_link in ext_link_soups:
                ext_links.append(ext_link.get('href'))
            return ext_links

        except:
            print("Couldn't find external links.")

    def scrape(self, url,soup):
        article_data = {

            "UUID" : get_uuid(url),
            "url" : url,
            "title": self.get_title(soup),
            "subtitle": self.get_subtitle(soup),
            "tags": self.get_tags(soup),
            "tags_link": self.get_tags_link(soup),
            "author": self.get_author(soup),
            "h1_headers": self.get_h1_headers(soup),
            "h2_headers": self.get_h2_headers(soup),
            "paragraphs": self.get_paragraphs(soup),
            "blockquotes": self.get_blockquotes(soup),
            "bold_text": self.get_bolded(soup),
            "italic_text": self.get_italics(soup),
            "figures": self.figures(soup),
            "links": self.links(soup),
            "external_links": self.ext_links(soup),
           # "n_bullet_lists": self.count_bullet_lists(soup),
           # "n_numbered_lists": self.count_numbered_lists(soup),
           # "n_gists": self.count_gists(soup),
           # "n_code_chunks": self.count_code_chunks(soup),
           # "n_vids": self.count_vids(soup),
           # "n_links": self.count_links(soup),
        }

        # if subtitle exists, remove it from h2_headers list
        subtitle = article_data['subtitle']
        if subtitle != 'None':
            if article_data['h2_headers'] != 'None':
                article_data['h2_headers'].remove(subtitle)

            if len(article_data['h2_headers']) == 0:
                article_data['h2_headers'] = 'None'

        return(article_data)

# LOAD CSV AND FETCH ARTICLE URLS TO BE SCRAPPED
url_file_name = "AI_5000"
url_file = url_file_name + ".csv"
urls_df = pd.read_csv(url_file)
# urls_df.head()
# N = no of articles to be scrapped
N = 2
#N = len(urls_df)
# MAKE LISTS
urls = urls_df.url.to_list()
#topics = urls_df.Tag.to_list()
#scrape_urls = urls[:N]

# scrape_urls

# SCRAPE TEXT DATA FROM INDIVIDUAL URL/ARTICLE
# page_url = urls[3]
# driver = webdriver.Chrome()
# driver.get(page_url)
# time.sleep(3)
# soup = BeautifulSoup(driver.page_source, 'lxml')

content = medium_article_scrapping()
# data = content.scrape(soup)
# content_df = pd.DataFrame(columns = list(data.keys()))
# content_df = return_df.append(data, ignore_index=True)
# driver.close()

# SCRAPE FOR MULTIPLE/ALL URLS
content_df = pd.DataFrame(columns = ['UUID', 'url','title', 'subtitle','tags','tags_link', 'author', 'h1_headers', 'h2_headers', 'paragraphs',
                                     'blockquotes', 'bold_text', 'italic_text',
                                     'figures', 'links', 'external_links'
                                     #'n_bullet_lists',
                                     #'n_numbered_lists',
                                     #'n_gists',
                                     #'n_code_chunks',
                                     #'n_vids',
                                     #'n_links'
                                     ])


options = Options()
options.add_argument("--headless")
options.add_argument("window-size=1400,1500")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("start-maximized")
options.add_argument("enable-automation")
options.add_argument("--disable-infobars")
options.add_argument("--disable-dev-shm-usage")

start = datetime.now()

for article_i in range(N):

    url = urls[article_i]
    #topic = topics[article_i]
    print("Scrapping : ", url)
    #print(topic)
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(url)
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')

        data_scrape = content.scrape(url, soup)
        content_df = content_df.append(data_scrape, ignore_index=True,  sort=False)

        soup.decompose()

        driver.close()

        print('Scraping done!')


        # SAVE SCAPED DATA TO CSV

        output_file = 'contents_op_' + url_file
        content_df.to_csv(output_file, index=False)

        # Convert to JSON
        json_file = url_file_name + ".json"
        print("writing into..",json_file)
        content_df.to_json(json_file,orient="records",force_ascii=False)
        print("Completed!")
        #parsed_content = json.loads(content_df_json)
        #op_json = json.dumps(parsed_content,indent=4)
        #print("JSON....")

        #print(op_json)


        end = datetime.now()
        
        '''
        UPLOADING TO S3
        '''
        # try:
        
            # first_bucket_name = 'youtern-user-files'
            # first_file_name = output_file
            # s3.
            # s3.upload_file(json_file, 'youtern-user-files', 'json_file11.json')
            # s3.upload_file(output_file, 'youtern-user-files', 'json_file22.csv')
        
            # first_bucket = s3_resource.Bucket(name=first_bucket_name)   
            # first_object = s3_resource.Object(bucket_name=first_bucket_name, key=first_file_name)
        
            # s3_resource.Object(first_bucket_name, first_file_name).upload_file(Filename=first_file_name)
        
        # except Exception as e:
        #     print(str(e))
        # total time taken
        print(f"Total time taken for scraping: {end - start}")
        print(f"Total numer of URL scrapped: {N}")

    except Exception as e:
        print ('article not found')
        print(str(e))
