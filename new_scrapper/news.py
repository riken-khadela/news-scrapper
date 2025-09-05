import requests
requests.packages.urllib3.disable_warnings()
from bs4 import BeautifulSoup
import time
import new_scrapper.settings as cf
from logger import CustomLogger
logger = CustomLogger(log_file_path="log/new_scrapping.log")

class NEWS(object):
    def __init__(self):
        self.News = {}

    def get_request(self,search):
        print('searching for : '+search)
        # proxies=cf.proxies()
        proxies = cf.getProxies()
        c = 0
        while True:
            try:
                ### Old Code ###
                # getHeaders = {
                #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
                # }
                # res = requests.get(search, headers=getHeaders, timeout=30,proxies=proxies)
                
                ### New Code with Scrapedo proxy ####
                
                res = requests.get(
                                    url=search,
                                    proxies=proxies,
                                    verify=False
                                )
                if res.status_code== 200:
                    #Return Response with status True
                    return True, res
                print(res.status_code)
                
            except Exception as e:
                print(e)
            time.sleep(0.5)
            print("Retrying again for:" + str(search)+"Checking try again:" + str(c))
            proxies = cf.getProxies()
            c = c + 1
            if c > 5: break
        return False, False

    def news_section(self, information, main_data, old_dict={}):
        full_table = old_dict.get('news',{})
       
        all_news = information.find_all('div',{'class':'list-item'})
        news_list = []
        news_title_list = []
        try :
            for news in  all_news:
                tmp = {}
                date = news.find('span',{'class':'date-header'}).text.strip()
                date = ','.join(date.split(',')[1:]).strip()
                
                news_source = news.find_all('div', {'class': 'content-block'})
                for news_inside in news_source :
                    news_source_tag = news_inside.find('div', {'class': 'article-details'})
                    news_anchor_tag = news_inside.find('a', {'class': 'accent'})

                    if not news_source_tag or not news_anchor_tag:
                        continue

                    news_source = news_source_tag.text.replace(date, '').strip()
                    news_title = news_anchor_tag.text.strip()
                    news_link = news_anchor_tag.get('href')

                    if news_link and news_title and news_source and date:
                        tmp['date'] = date
                        tmp['label'] = news_title
                        tmp['link'] = news_link
                        tmp['category'] = "News"

                    if tmp:
                        news_title_list.append(news_title)
                        news_list.append(tmp)
                    
            full_table_news_title = [ value['label'] for key, value in full_table.items()]
            new_appendable_news = [i for i in news_list if i['label'] not in full_table_news_title]
            if new_appendable_news :
                next_start_number = len(full_table)
                for new_appendable in new_appendable_news :
                    next_start_number += 1
                    full_table[str(next_start_number)] = new_appendable
        except Exception as e: 
            logger.error(f"ERROR in News news_section : {e}")

                
        return full_table

    def news_process_logic(self, url, dict = {}):
        old_dict = dict.get('news', {})
        self.News = {
            "news" : {}
        }
        try :
            news_detail = {}
            search_url = url+'/timeline'
            session_id, cookies = cf.load_session()
            is_loaded, res = cf.get_scrpido_requests(url, session_id, cookies)
            
            unique_news = {}
            seen_titles = []
            new_key = 0
            
            if is_loaded:
                #  removedublicates
                for _, item in old_dict.items(): 
                    label = item.get('label')
                    if label and label not in seen_titles:
                        seen_titles.append(label)
                        unique_news[new_key] = item
                        new_key += 1

                old_dict = unique_news
                
                data = BeautifulSoup(res.text, 'lxml')
                sections = data.find('news-detailed-list' )
                self.News = self.news_section(sections, data,old_dict)

                news_detail = {
                    "news": self.News
                }
        except Exception as e: 
            logger.error(f"ERROR in News news_process_logic : {e}")
    
        return news_detail