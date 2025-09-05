import requests
requests.packages.urllib3.disable_warnings()
from bs4 import BeautifulSoup
import time
import update_scrapper.settings as cf
from logger import CustomLogger

logger = CustomLogger(log_file_path="log/update.log")

class NEWS(object):
    def __init__(self):
        self.News = {}

    def news_section(self, information, main_data,old_dict):
       
        full_table = old_dict.get('news',{})
        
        # new_full_table={}
        # if len(news_data) > 0:
        #     for i in range(len(news_data)):
        #         category = ""
        #         label = ""
        #         link = ""
        #         date = ""
        #         data = news_data[i]

        #         if data.find('div', {'class': 'activity-title'}):
        #             category = data.find('div', {'class': 'activity-title'}).find('span').text.strip()
                
        #         if data.find('field-formatter'):
        #             date = data.find('field-formatter').find('span').text.strip()
                    
        #         if category == "News":
        #             div_element = data.find('div', {'class': 'activity-details'})
        #             if div_element:
        #                 if div_element.find('press-reference'):
        #                     label_element = div_element.find('press-reference').find('div').find('a')
        #                     if label_element:
        #                         label = label_element.text.strip()

        #                 if div_element.find('press-reference'):
        #                     link_element = div_element.find('press-reference').find('div').find('a')
        #                     if link_element:
        #                         link = link_element.get('href').strip()
                    
        #         if category == 'Funding Round':
        #             div_element = data.find('div', {'class': 'activity-details'})
        #             if div_element:
        #                label = div_element.find('funding-round').text.strip()

        #         if category == 'Acquisition':
        #             div_element = data.find('div', {'class': 'activity-details'})
        #             if div_element:
        #                label = div_element.find('acquisition').text.strip()
                
        #         if category == 'Fund':
        #             div_element = data.find('div', {'class': 'activity-details'})
        #             if div_element:
        #                label = div_element.find('fund-raise').text.strip()
 
        #         new_full_table[str(i + 1)] = {
        #             "category": category,
        #             "label": label,
        #             "link": link,
        #             "date": date
        #         }
        #     # Combine new data and existing data while preserving the original order
        #     combined_table = {}
        #     non_blank_index = 1
        #     for key, value in new_full_table.items():
        #         if value not in combined_table.values():
        #             combined_table[str(non_blank_index)] = value
        #             non_blank_index += 1
        #     for key, value in full_table.items():
        #         if value not in combined_table.values():
        #             combined_table[str(non_blank_index)] = value
        #             non_blank_index += 1

        #     # Update the table with the combined data
        #     full_table= combined_table
        try :
            all_news = information.find_all('div',{'class':'list-item'})
            news_list = []
            news_title_list = []
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
            
            unique_news = {}
            seen_titles = set()
            new_key = 0

            for _, item in full_table.items():
                label = item.get('label')
                if label and label not in seen_titles:
                    seen_titles.add(label)
                    unique_news[str(new_key)] = item
                    new_key += 1

            full_table = unique_news
        except Exception as e: logger.error(f"ERROR in News news_section : {e}")
    
        
        return full_table

    def news_process_logic(self, url,dict):
       
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
                        unique_news[str(new_key)] = item
                        new_key += 1

                old_dict = unique_news
                
                data = BeautifulSoup(res.text, 'lxml')
                sections = data.find('news-detailed-list' )
                self.News = self.news_section(sections, data,old_dict)

                news_detail = {
                    "news": self.News
                }
        except Exception as e: logger.error(f"ERROR in News news_process_logic : {e}")
        
        return news_detail

