import requests
requests.packages.urllib3.disable_warnings()
from bs4 import BeautifulSoup
import time
import new_scrapper.settings as cf

class INVESTMENT(object):
    def __init__(self):
        self.funding_round={}
        self.investors={}
        self.acquisition={}
        self.ipoandstock={}
        self.investments={}
        self.diversity_investments={}
        self.exist={}
        self.fund_raised={}
        
        
    def get_request(self,search):
        print('searching for : '+search)
        # proxies=cf.proxies()
        proxies = cf.getProxies()
        c = 0
        while True:
            try:
                #### Old Code ###
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
    
    def funding_round_section(self,information,main_data):
        funding_round={}
        number_of_funding_rounds=''
        total_funding_amount=''
        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Funding Rounds" in data_validator:
                    number_of_funding_rounds = detail_data.find('field-formatter').find('a').text.strip()
                if "Total amount raised across all funding rounds" in data_validator:
                        total_funding_amount = main_data.find('phrase-list-card').find_all('field-formatter')[1].find('span').text.strip()
        except:pass
        fulltable = {}
        non_blank_index = 1

        try:
            table_data = information.find('list-card')
            if table_data:
                rows = table_data.find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    if columns:
                        row_values = [column.text.strip() for column in columns]
                        # Check if all values in row_values are empty
                        if any(row_values):
                            fulltable[str(non_blank_index)] = {
                            "announced_date": row_values[0],
                            "transaction_name": row_values[1],
                            "number_of_investors": row_values[2],
                            "money_raised": row_values[3],
                            "lead_investors": row_values[4]
                        }
                        non_blank_index += 1
            
        except:pass
        funding_round={
            "number_of_funding_rounds": number_of_funding_rounds,
            "total_funding_amount": total_funding_amount,
            "table": fulltable
            }
        return funding_round
    def investors_section(self,information,main_data):
        investors={}
        number_of_lead_investors=''
        number_of_investors=''
        
        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of lead investment firms and individual investors" in data_validator:
                    number_of_lead_investors = detail_data.find('field-formatter').find('a').text.strip()
                if "Total number of investment firms and individual investors" in data_validator:
                        number_of_investors = detail_data.find('field-formatter').find('a').text.strip()
        except:pass
        fulltable = {}
        non_blank_index = 1

        try:
            table_data = information.find('list-card')
            if table_data:
                rows = table_data.find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    if columns:
                        row_values = [column.text.strip() for column in columns]
                        # Check if all values in row_values are empty
                        if any(row_values):
                            fulltable[str(non_blank_index)] = {
                                "investor_name": row_values[0],
                                "lead_investor": row_values[1],
                                "funding_round": row_values[2],
                                "partners": row_values[3],
                            }
                            non_blank_index += 1
                            
                
        except:pass
        investors={
            "number_of_lead_investors": number_of_lead_investors,
            "number_of_investors": number_of_investors,
            "table": fulltable
            }
        
        return investors
    
    def acquisitions_section(self,information,main_data):
        acquisition={}
        number_of_acquisition=''
        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Acquisitions" in data_validator:
                    number_of_acquisition = detail_data.find('field-formatter').find('a').text.strip()
        except:pass
        fulltable = {}
        non_blank_index = 1

        try:
            table_data = information.find('list-card')
            if table_data:
                rows = table_data.find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    if columns:
                        row_values = [column.text.strip() for column in columns]
                        # Check if all values in row_values are empty
                        if any(row_values):
                            fulltable[str(non_blank_index)] = {
                                "acquiree_name": row_values[0],
                                "announced_date": row_values[1],
                                "price": row_values[2],
                                "transaction_name": row_values[3]
                            }
                            non_blank_index += 1
                
        except:pass
        acquisition={
            "number_of_acquisition": number_of_acquisition,
            "table": fulltable
            }
        return acquisition
    
    
    def investments_section(self,information,main_data):
        investments={}
        number_of_investments=""
        number_of_lead_investments=""
        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Investments made" in data_validator:
                    number_of_investments = detail_data.find('field-formatter').find('a').text.strip()
                if "Total number of Lead Investments made" in data_validator:
                    number_of_lead_investments = detail_data.find('field-formatter').find('a').text.strip()
        except:pass
        fulltable = {}
        non_blank_index = 1

        try:
            table_data = information.find('list-card')
            if table_data:
                rows = table_data.find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    if columns:
                        row_values = [column.text.strip() for column in columns]
                        # Check if all values in row_values are empty
                        if any(row_values):
                            fulltable[str(non_blank_index)] = {
                                "announced_date": row_values[0],
                                "organization_name": row_values[1],
                                "lead_investor": row_values[2],
                                "funding_round": row_values[3],
                                "money_raised":row_values[4]

                            }
                            non_blank_index += 1
                
        except:pass
        investments={
            "number_of_investments": number_of_investments,
            "number_of_lead_investments": number_of_lead_investments,
            "table": fulltable}
        
        return investments
           
    def diversity_investments_section(self,information,main_data):
        diversity_investments={}
        number_of_diversity_investment=""
        
        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                
                if "Total number of diversity investments made by an investor" in data_validator:
                    number_of_diversity_investment = detail_data.find('field-formatter').find('a').text.strip()
        except:pass
        fulltable = {}
        non_blank_index = 1

        try:
            table_data = information.find('list-card')
            if table_data:
                rows = table_data.find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    if columns:
                        row_values = [column.text.strip() for column in columns]
                        # Check if all values in row_values are empty
                        if any(row_values):
                            fulltable[str(non_blank_index)] = {
                                "announced_date": row_values[0],
                                "organization_name": row_values[1],
                                "diversity spotlight": row_values[2],
                                "funding_round": row_values[3],
                                "money_raised":row_values[4]

                            }
                            non_blank_index += 1
                
        except:pass
        diversity_investments={
            "number_of_diversity_investment": number_of_diversity_investment,
            "table": fulltable}
        return diversity_investments
    
    def exist_section(self,information,main_data):
        exist={}
        number_of_exits=""
        total_number_of_exits=''
        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Exits" in data_validator:
                    number_of_exits = information.find_all('field-formatter')
                    for total_exits in number_of_exits:
                        if 'num_exits' in total_exits.find('a').get('href'):
                            total_number_of_exits =total_exits.text.strip()
                    # number_of_exits = detail_data.find('field-formatter').find('a').text.strip()
        except:pass
        fulltable = {}
        non_blank_index = 1
        try:
            table_data = information.find('image-list-card')
            if table_data:
                table_data = table_data.find('ul',{'class':'two-column ng-star-inserted'}).find_all('li')
                for i in range(len(table_data)):
                    column = table_data[i].find('a')
                    if column:
                        fulltable[str(non_blank_index)] = {
                            "organization name": column.text.strip(),
                            "link": "https://www.crunchbase.com"+column.get('href').strip(),
                        }
                        non_blank_index += 1
                
        except:pass
        exist={
            "number_of_exits": total_number_of_exits,
            "table": fulltable}
        return exist
    
    def fund_raised_section(self,information,main_data):
        fund_raised={}
        number_of_funds=""
        total_fund_raised=""
        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Funds raised" in data_validator:
                    number_of_funds = detail_data.find('field-formatter').find('a').text.strip()
                if "Total funding amount raised across all Fund Raises" in data_validator:
                    total_fund_raised = detail_data.find('field-formatter').find('a').text.strip()
        except:pass
        fulltable = {}
        non_blank_index = 1

        try:
            table_data = information.find('list-card')
            if table_data:
                rows = table_data.find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    if columns:
                        row_values = [column.text.strip() for column in columns]
                        # Check if all values in row_values are empty
                        if any(row_values):
                            fulltable[str(non_blank_index)] = {
                                "announced_date": row_values[0],
                                "fund_name": row_values[1],
                                "money_raised": row_values[2]

                            }
                            non_blank_index += 1
                
        except:pass
        fund_raised={
            "number_of_funds": number_of_funds,
            "total_fund_raised": total_fund_raised,
            "table":fulltable
            }

        return fund_raised
        
    def investment_process_logic(self,url):
        print("new investment")
        investments_detail={}
        self.funding_round={}
        self.investors={}
        self.acquisition={}
        self.investments={}
        self.diversity_investments={}
        self.exist={}
        self.fund_raised={}
        search_url=url
        isloaded, res = self.get_request(search_url)
        if isloaded:
            financialdata = BeautifulSoup(res.text, 'lxml')
            sections=financialdata.find_all('span',{'class':'anchor-target'})
            for section in sections:
                check_section= section.get('id')
                if 'funding_rounds' in check_section:
                    self.funding_round = self.funding_round_section(section.parent,financialdata)
                if 'investors' in check_section:
                    self.investors = self.investors_section(section.parent,financialdata)
                if 'acquisitions' in check_section:
                    self.acquisition = self.acquisitions_section(section.parent,financialdata)
                if str(check_section).strip()=="investments":
                    self.investments = self.investments_section(section.parent,financialdata)
                if 'diversity_spotlight_investments' in check_section:
                    self.diversity_investments=self.diversity_investments_section(section.parent,financialdata)
                if str(check_section).strip().lower( )=="exits":
    
                    self.exist=self.exist_section(section.parent,financialdata)
                if str(check_section).strip()=="funds":
                    self.fund_raised = self.fund_raised_section(section.parent,financialdata)
            
            investments_detail={
                    "investment":{
                        "i_funding_round":self.funding_round,
                        "i_investors":self.investors,
                        "i_acquisitions":self.acquisition,
                        "i_investments":self.investments,
                        "i_diversity_investments":self.diversity_investments,
                        "i_exits":self.exist,
                        "i_fund_raised":self.fund_raised
                    }
                    }
        return investments_detail