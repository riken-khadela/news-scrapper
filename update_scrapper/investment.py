import requests
requests.packages.urllib3.disable_warnings()
from bs4 import BeautifulSoup
import time
import update_scrapper.settings as cf
from logger import CustomLogger

logger = CustomLogger(log_file_path="log/update.log")

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
        self.olddict={}
        
        
    def get_request(self,search):
        logger.log('searching for : '+search)
        #print('searching for : '+search)
        # proxies=cf.proxies()
        proxies = cf.getProxies()
        isData=True
        while isData:
            try:
                # getHeaders = {
                #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
                # }
                
                # #Google request to get html body of the search
                # res = requests.get(search, headers=getHeaders, timeout=10,proxies=proxies)
                logger.log(f"############# Searching: {search} #########")
                res = requests.get(
                                    url=search,
                                    proxies=proxies,
                                    verify=False
                                )
                if res.status_code==404:
                    isData=False
                    logger.log(f"Failed - {search} with result code {res.status_code}")
                if res.status_code== 200:
                    #Return Response with status True
                    logger.log(f"Success - {search}")
                    return True, res
                logger.log(res.status_code)
                
            except Exception as e:
                pass
            time.sleep(1)
            logger.log("Retrying again for:" + str(search))
    
        return False, False
    
    def funding_round_section(self,information,main_data,old_dict):
        # Check if 'i_funding_round' key exists in old_dict
        old_i_funding_round_dict = old_dict.get('i_funding_round', {})

        funding_round = {
            "number_of_funding_rounds": old_i_funding_round_dict.get('number_of_funding_rounds', ''),
            "total_funding_amount": old_i_funding_round_dict.get('total_funding_amount', ''),
            "table": old_i_funding_round_dict.get('table', {})
        }


        try:
            details_data = information.find('big-values-card').find_all('div', {'class': 'ng-star-inserted'})
            for detail_data in details_data:
                data_finder = detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator = main_data.find('div', {'id': str(data_finder)}).text

                if "Total number of Funding Rounds" in data_validator:
                    new_number_of_funding_rounds = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_funding_rounds and new_number_of_funding_rounds != funding_round['number_of_funding_rounds']:
                        funding_round['number_of_funding_rounds'] = new_number_of_funding_rounds

                if "Total amount raised across all funding rounds" in data_validator:
                    new_total_funding_amount = main_data.find('phrase-list-card').find_all('field-formatter')[1].find('span').text.strip()
                    if new_total_funding_amount and new_total_funding_amount != funding_round['total_funding_amount']:
                        funding_round['total_funding_amount'] = new_total_funding_amount

        except:
            pass

        new_fulltable = {}
        non_blank_index = 1

        try:
            table_data = information.find('list-card')
            if table_data:
                rows = table_data.find_all('tr')
                for row in rows:
                  columns = row.find_all('td')
                  if columns:
                      row_values = [column.text.strip() for column in columns]
                      # Check if any value in row_values is not empty
                      if any(row_values):
                          new_fulltable[str(non_blank_index)] = {
                              "announced_date": row_values[0],
                              "transaction_name": row_values[1],
                              "number_of_investors": row_values[2],
                              "money_raised": row_values[3],
                              "lead_investors": row_values[4]
                          }
                          non_blank_index += 1

                # Combine new data and existing data while preserving the original order
                combined_table = {}
                non_blank_index = 1
                for key, value in new_fulltable.items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1
                for key, value in funding_round['table'].items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1

                # Update the table with the combined data
                funding_round['table'] = combined_table

        except:
            pass

        return funding_round
    
    def investors_section(self,information,main_data,old_dict):
        # Check if 'i_investors' key exists in old_dict
        old_i_investors_dict = old_dict.get('i_investors', {})

        investors = {
            "number_of_lead_investors": old_i_investors_dict.get('number_of_lead_investors', ''),
            "number_of_investors": old_i_investors_dict.get('number_of_investors', ''),
            "table": old_i_investors_dict.get('table', {})
        }

        
        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text.strip()
                if "Total number of lead investment firms and individual investors" in data_validator:
                    new_number_of_lead_investors = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_lead_investors and new_number_of_lead_investors != investors['number_of_lead_investors']:
                        investors['number_of_lead_investors'] = new_number_of_lead_investors
                if "Total number of investment firms and individual investors" in data_validator:
                        new_number_of_investors = detail_data.find('field-formatter').find('a').text.strip()
                        if new_number_of_investors and new_number_of_investors != investors['number_of_investors']:
                            investors['number_of_investors'] = new_number_of_investors
        except:pass
        new_fulltable = {}
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
                          new_fulltable[str(non_blank_index)] = {
                              "investor_name": row_values[0],
                              "lead_investor": row_values[1],
                              "funding_round": row_values[2],
                              "partners": row_values[3],
                          }
                          non_blank_index += 1
                            
                # Combine new data and existing data while preserving the original order
                combined_table = {}
                non_blank_index = 1
                for key, value in new_fulltable.items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1
                for key, value in investors['table'].items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1

                # Update the table with the combined data
                investors['table'] = combined_table

        except Exception as e:
            logger.warning(f"Error in extracting investor table data: {e}")
            pass

        
        return investors
    
    def acquisitions_section(self, information, main_data,old_dict):
        
        # Check if 'i_acquisitions' key exists in old_dict
        old_i_acquisitions_dict = old_dict.get('i_acquisitions', {})

        acquisition = {
            "number_of_acquisition": old_i_acquisitions_dict.get('number_of_acquisition', ''),
            "table": old_i_acquisitions_dict.get('table', {})
        }

        
        try:
            details_data = information.find('big-values-card').find_all('div', {'class': 'ng-star-inserted'})
            for detail_data in details_data:
                data_finder = detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator = main_data.find('div', {'id': str(data_finder)}).text
                if "Total number of Acquisitions" in data_validator:
                    new_number_of_acquisition = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_acquisition and new_number_of_acquisition != acquisition['number_of_acquisition']:
                        acquisition['number_of_acquisition'] = new_number_of_acquisition
        except:
            pass
        
        new_fulltable = {}
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
                          new_fulltable[str(non_blank_index)] = {
                              "acquiree_name": row_values[0],
                              "announced_date": row_values[1],
                              "price": row_values[2],
                              "transaction_name": row_values[3]
                          }
                          non_blank_index += 1
                            
                # Combine new data and existing data while preserving the original order
                combined_table = {}
                non_blank_index = 1
                for key, value in new_fulltable.items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1
                for key, value in acquisition['table'].items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1

                # Update the table with the combined data
                acquisition['table'] = combined_table
                     
                
        except:pass
        
        return acquisition
    
    
    def investments_section(self,information,main_data,old_dict):
        # Check if 'i_investments' key exists in old_dict
        old_i_investments_dict = old_dict.get('i_investments', {})

        investments = {
            "number_of_investments": old_i_investments_dict.get('number_of_investments', ''),
            "number_of_lead_investments": old_i_investments_dict.get('number_of_lead_investments', ''),
            "table": old_i_investments_dict.get('table', {})
        }

        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Investments made" in data_validator:
                    new_number_of_investments = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_investments and new_number_of_investments != investments['number_of_investments']:
                        investments['number_of_investments'] =new_number_of_investments
                if "Total number of Lead Investments made" in data_validator:
                    new_number_of_lead_investments = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_lead_investments and new_number_of_lead_investments != investments['number_of_lead_investments']:
                        investments['number_of_lead_investments'] = new_number_of_lead_investments
        except:pass
        new_fulltable = {}
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
                          new_fulltable[str(non_blank_index)] = {
                              "announced_date": row_values[0],
                              "organization_name": row_values[1],
                              "lead_investor": row_values[2],
                              "funding_round": row_values[3],
                              "money_raised":row_values[4]

                          }
                          non_blank_index += 1
                # Combine new data and existing data while preserving the original order
                combined_table = {}
                non_blank_index = 1
                for key, value in new_fulltable.items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1
                for key, value in investments['table'].items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1

                # Update the table with the combined data
                investments['table'] = combined_table
                
        except:pass
        
        return investments
           
    def diversity_investments_section(self,information,main_data,old_dict):
        # Check if 'i_diversity_investments' key exists in old_dict
        old_i_diversity_investments_dict = old_dict.get('i_diversity_investments', {})

        diversity_investments = {
            "number_of_diversity_investment": old_i_diversity_investments_dict.get('number_of_diversity_investment', ''),
            "table": old_i_diversity_investments_dict.get('table', {})
        }

        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of diversity investments made by an investor" in data_validator:
                    new_number_of_diversity_investment = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_diversity_investment and new_number_of_diversity_investment != diversity_investments['number_of_diversity_investment']:
                        diversity_investments['number_of_diversity_investment'] = new_number_of_diversity_investment
        except:pass
        new_fulltable = {}
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
                            new_fulltable[str(non_blank_index)] = {
                                "announced_date": row_values[0],
                                "organization_name": row_values[1],
                                "diversity spotlight": row_values[2],
                                "funding_round": row_values[3],
                                "money_raised":row_values[4]

                            }
                            non_blank_index += 1
                # Combine new data and existing data while preserving the original order
                combined_table = {}
                non_blank_index = 1
                for key, value in new_fulltable.items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1
                for key, value in diversity_investments['table'].items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1

                # Update the table with the combined data
                diversity_investments['table'] = combined_table
                
        except:pass
        
        return diversity_investments
    
    def exist_section(self,information,main_data,old_dict):

        # Check if 'i_exits' key exists in old_dict
        old_i_exits_dict = old_dict.get('i_exits', {})

        exist = {
            "number_of_exits": old_i_exits_dict.get('number_of_exits', ''),
            "table": old_i_exits_dict.get('table', {})
        }

        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Exits" in data_validator:
                    new_number_of_exits = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_exits and new_number_of_exits != exist['number_of_exits']:
                        exist['number_of_exits'] = new_number_of_exits
        except:pass
        
        new_fulltable = {}
        non_blank_index = 1
        try:
            table_data = information.find('image-list-card')
            if table_data:
                table_data = table_data.find('ul',{'class':'two-column ng-star-inserted'}).find_all('li')
                for i in range(len(table_data)):
                    column = table_data[i].find('a')
                    if column:
                        new_fulltable[str(non_blank_index)] = {
                            "organization name": column.text.strip(),
                            "link": "https://www.crunchbase.com"+column.get('href').strip(),
                        }
                        non_blank_index += 1
                # Combine new data and existing data while preserving the original order
                combined_table = {}
                non_blank_index = 1
                for key, value in new_fulltable.items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1
                for key, value in exist['table'].items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1

                # Update the table with the combined data
                exist['table'] = combined_table
            
        except:pass
       
        return exist
    
    def fund_raised_section(self,information,main_data,old_dict):
        # Check if 'i_fund_raised' key exists in old_dict
        old_i_fund_raised_dict = old_dict.get('i_fund_raised', {})

        fund_raised = {
            "number_of_funds": old_i_fund_raised_dict.get('number_of_funds', ''),
            "total_fund_raised": old_i_fund_raised_dict.get('total_fund_raised', ''),
            "table": old_i_fund_raised_dict.get('table', {})
        }

        try:
            details_data=information.find('big-values-card').find_all('div',{'class':'ng-star-inserted'})
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Funds raised" in data_validator:
                    new_number_of_funds = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_funds and new_number_of_funds != fund_raised['number_of_funds']:
                        fund_raised['number_of_funds'] = new_number_of_funds
                if "Total funding amount raised across all Fund Raises" in data_validator:
                    new_total_fund_raised = detail_data.find('field-formatter').find('a').text.strip()
                    if new_total_fund_raised and new_total_fund_raised != fund_raised['total_fund_raised']:
                        fund_raised['total_fund_raised'] = new_total_fund_raised
        except:pass
        new_fulltable = {}
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
                            new_fulltable[str(non_blank_index)] = {
                                "announced_date": row_values[0],
                                "fund_name": row_values[1],
                                "money_raised": row_values[2]

                            }
                            non_blank_index += 1
                # Combine new data and existing data while preserving the original order
                combined_table = {}
                non_blank_index = 1
                for key, value in new_fulltable.items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1
                for key, value in fund_raised['table'].items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1

                # Update the table with the combined data
                fund_raised['table'] = combined_table
                
        except:pass
        
        return fund_raised
        

        return fund_raised
        
    def investment_process_logic(self,url,dict):
        old_dict = dict.get('investment', {})
        investments_detail={}
        self.funding_round={}
        self.investors={}
        self.acquisition={}
        self.investments={}
        self.diversity_investments={}
        self.exist={}
        self.fund_raised={}
        search_url=url
        session_id, cookies = cf.load_session()
        isloaded, res = cf.get_scrpido_requests(url, session_id, cookies)
        if isloaded:
            financialdata = BeautifulSoup(res.text, 'lxml')
            sections=financialdata.find_all('span',{'class':'anchor-target'})
            for section in sections:
                check_section= section.get('id')
                if 'funding_rounds' in check_section:
                    self.funding_round = self.funding_round_section(section.parent,financialdata,old_dict)
                if 'investors' in check_section:
                    self.investors = self.investors_section(section.parent,financialdata,old_dict)
                if 'acquisitions' in check_section:
                    self.acquisition = self.acquisitions_section(section.parent,financialdata,old_dict)
                if str(check_section).strip()=="investments":
                    self.investments = self.investments_section(section.parent,financialdata,old_dict)
                if 'diversity_spotlight_investments' in check_section:
                    self.diversity_investments=self.diversity_investments_section(section.parent,financialdata,old_dict)
                if str(check_section).strip()=="exits":
                    self.exist=self.exist_section(section.parent,financialdata,old_dict)
                if str(check_section).strip()=="funds":
                    self.fund_raised = self.fund_raised_section(section.parent,financialdata,old_dict)
            
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
    