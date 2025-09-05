import requests
requests.packages.urllib3.disable_warnings()
from bs4 import BeautifulSoup
import time
import new_scrapper.settings as cf
from logger import CustomLogger
logger = CustomLogger(log_file_path="log/new_scrapping.log")

class FINANCIAL(object):
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
                ### Old Code ####
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
    
    def funding_round_section(self,information,main_data, old_dict = {}):
        old_funding_round_dict = old_dict.get('funding_round', {})
        funding_round = {
            "number_of_funding_rounds": old_funding_round_dict.get('number_of_funding_rounds', ''),
            "total_funding_amount": old_funding_round_dict.get('total_funding_amount', ''),
            "table": old_funding_round_dict.get('table', {})
        }

        try:
            details_data = information.find_all('tile-field')
            if details_data:
                for detail_data in details_data:
                    data_finder = detail_data.find('label-with-info').find('icon').get('aria-describedby')
                    data_validator = main_data.find('div', {'id': str(data_finder)}).text

                    if "Total number of Funding Rounds" in data_validator:
                        new_number_of_funding_rounds=''
                        new_number_of_funding_rounds = detail_data.find('field-formatter')
                        if new_number_of_funding_rounds and new_number_of_funding_rounds.text.strip() != funding_round['number_of_funding_rounds']:
                            funding_round['number_of_funding_rounds'] = new_number_of_funding_rounds.text.strip()

                    if "Total amount raised across all funding rounds" in data_validator:
                        new_total_funding_amount=''
                        new_total_funding_amount = detail_data.find('field-formatter')
                        if new_total_funding_amount and new_total_funding_amount.text.strip() != funding_round['total_funding_amount']:
                            funding_round['total_funding_amount'] = new_total_funding_amount.text.strip()

        except Exception as e: logger.error(f"ERROR in financial funding_round_section : {e}")


        new_fulltable = {}
        non_blank_index = 1

        try:
            table_data = information.find('table')
            if table_data:
                rows = table_data.find_all('tr')
                for row in rows:
                  columns = row.find_all('td')
                  if columns:
                      row_values = [column.text.strip() for column in columns]
                      if any(row_values):
                          new_fulltable[str(non_blank_index)] = {
                              "announced_date": row_values[0],
                              "transaction_name": row_values[1],
                              "number_of_investors": row_values[2],
                              "money_raised": row_values[3],
                              "lead_investors": row_values[4]
                          }
                          non_blank_index += 1

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

                funding_round['table'] = combined_table

        except Exception as e: logger.error(f"ERROR in financial funding_round_section : {e}")


        return funding_round
    
    def investors_section(self, information, main_data,old_dict = {}):
        old_investors_dict = old_dict.get('investors', {})

        investors = {
            "number_of_lead_investors": old_investors_dict.get('number_of_lead_investors', ''),
            "number_of_investors": old_investors_dict.get('number_of_investors', ''),
            "table": old_investors_dict.get('table', {})
        }

        try:
            details_data = information.find_all('tile-field')
            for detail_data in details_data: 
                data_finder = detail_data.find('label-with-info')
                data_validator = data_finder.text
                if "Number of Lead Investors" in data_validator:
                    new_number_of_lead_investors=''
                    new_number_of_lead_investors = detail_data.find('field-formatter')
                    if new_number_of_lead_investors and new_number_of_lead_investors.text.strip() != investors['number_of_lead_investors']:
                        investors['number_of_lead_investors'] = new_number_of_lead_investors.text.strip()
                            
                if "Number of Investors" in data_validator:
                    new_number_of_investors=''
                    new_number_of_investors = detail_data.find('field-formatter')
                    if new_number_of_investors and new_number_of_investors.text.strip() != investors['number_of_investors']:
                        investors['number_of_investors'] = new_number_of_investors.text.strip()
                        
        except Exception as e: logger.error(f"ERROR in financial investors_section : {e}")

        new_fulltable = {}
        non_blank_index = 1

        try:
            table = information.find('table')
            headers = [header.text.strip() for header in table.find_all('th')]

            for row in table.find_all('tr'): 
                columns = row.find_all('td')
                row_data = {headers[i]: columns[i].text.strip() for i in range(len(columns))}
                if row_data :
                    if any(row_data):
                        new_fulltable[str(non_blank_index)] = {
                            "investor_name": row_data.get('Investor Name',""),
                            "lead_investor": row_data.get('Lead Investor',""),
                            "funding_round": row_data.get('Funding Round',""),
                            "partners": row_data.get('Partners',""),
                        }
                        non_blank_index += 1
            
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

                investors['table'] = combined_table

        except Exception as e: logger.error(f"ERROR in financial investors_section : {e}")
        return investors

    
    def acquisitions_section(self,information,main_data,old_dict = {}):
        # Check if 'acquisitions' key exists in old_dict
        old_acquisitions_dict = old_dict.get('acquisitions', {})

        acquisition = {
            "number_of_acquisition": old_acquisitions_dict.get('number_of_acquisition', ''),
            "table": old_acquisitions_dict.get('table', {})
        }
        
        try:
            details_data = information.find_all('tile-field')
            for detail_data in details_data: 
                data_finder = detail_data.find('label-with-info')
                data_validator = data_finder.text
                if "Number" in data_validator:
                    new_number_of_lead_investors=''
                    new_number_of_lead_investors = detail_data.find('field-formatter')
                    if new_number_of_lead_investors and new_number_of_lead_investors.text.strip() != acquisition['number_of_acquisition']:
                        acquisition['number_of_acquisition'] = new_number_of_lead_investors.text.strip()
                        
        except Exception as e: logger.error(f"ERROR in financial acquisitions_section : {e}")

        new_fulltable = {}
        non_blank_index = 1
        new_fulltable = {}
        non_blank_index = 1

        try:
            table = information.find('table')
            for row in table.find_all('tr'): 
                columns = row.find_all('td')
                row_data = [column.text.strip() for column in columns]
                if row_data :
                    if any(row_data):
                        new_fulltable[str(non_blank_index)] = {
                            "acquiree_name": row_data[0],
                              "announced_date": row_data[1],
                              "price": row_data[2],
                              "transaction_name": row_data[3]
                        }
                        non_blank_index += 1
            
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
        except Exception as e: logger.error(f"ERROR in financial acquisitions_section : {e}")

            

        return acquisition
    
    def ipoandstock_section(self,information,main_data, old_dict = {}):
        old_ipo_and_stock_dict = old_dict.get('ipo_&_stock', {})

        ipoandstock = {
            "stock_symbol": old_ipo_and_stock_dict.get('stock_symbol', ''),
            "ipo_date": old_ipo_and_stock_dict.get('ipo_date', ''),
            "ipo_share_price": old_ipo_and_stock_dict.get('ipo_share_price', ''),
            "valuation_at_ipo": old_ipo_and_stock_dict.get('valuation_at_ipo', ''),
            "money_raise_at_ipo": old_ipo_and_stock_dict.get('money_raise_at_ipo', '')
        }

        try:
            details_data = information.find_all('tile-field')
            for detail_data in details_data: 
                data_finder = detail_data.find('label-with-info')
                data_validator = data_finder.text.lower()
                if "symbol" in data_validator:
                    new_number_of_funding_rounds=''
                    new_number_of_funding_rounds = detail_data.find('field-formatter')
                    if new_number_of_funding_rounds and new_number_of_funding_rounds.text.strip() != ipoandstock['stock_symbol']:
                        ipoandstock['stock_symbol'] = new_number_of_funding_rounds.text.strip()
                        
                if "date" in data_validator:
                    new_number_of_funding_rounds=''
                    new_number_of_funding_rounds = detail_data.find('field-formatter')
                    if new_number_of_funding_rounds and new_number_of_funding_rounds.text.strip() != ipoandstock['ipo_date']:
                        ipoandstock['ipo_date'] = new_number_of_funding_rounds.text.strip()
            
                if "price" in data_validator:
                    new_number_of_funding_rounds=''
                    new_number_of_funding_rounds = detail_data.find('field-formatter')
                    if new_number_of_funding_rounds and new_number_of_funding_rounds.text.strip() != ipoandstock['ipo_share_price']:
                        ipoandstock['ipo_share_price'] = new_number_of_funding_rounds.text.strip()
            
                if "Valu" in data_validator:
                    new_number_of_funding_rounds=''
                    new_number_of_funding_rounds = detail_data.find('field-formatter')
                    if new_number_of_funding_rounds and new_number_of_funding_rounds.text.strip() != ipoandstock['valuation_at_ipo']:
                        ipoandstock['valuation_at_ipo'] = new_number_of_funding_rounds.text.strip()
            
                if "amount" in data_validator or "raise" in data_validator :
                    new_number_of_funding_rounds=''
                    new_number_of_funding_rounds = detail_data.find('field-formatter')
                    if new_number_of_funding_rounds and new_number_of_funding_rounds.text.strip() != ipoandstock['money_raise_at_ipo']:
                        ipoandstock['money_raise_at_ipo'] = new_number_of_funding_rounds.text.strip()
            
            
        except Exception as e: logger.error(f"ERROR in financial ipoandstock_section : {e}")
        return ipoandstock
    
    def investments_section(self,information,main_data, old_dict={}):
        old_investments_dict = old_dict.get('investments', {})

        investments = {
            "number_of_investments": old_investments_dict.get('number_of_investments', ''),
            "number_of_lead_investments": old_investments_dict.get('number_of_lead_investments', ''),
            "exits": old_investments_dict.get('exits', ''),
            "number_of_funding_rounds": old_investments_dict.get('number_of_funding_rounds', ''),
            "total_funding_amount": old_investments_dict.get('total_funding_amount', ''),
            "table": old_investments_dict.get('table', {})
        }
            
        try :
            details_data = main_data.find('financial-highlights').find_all('tile-highlight')
            for detail in details_data :
                data_validator =detail.find('span').text.strip()
                    
                if "Lead Investments" == data_validator :
                    new_number_of_lead_investors = detail.find('field-formatter').text.strip()
                    if new_number_of_lead_investors and new_number_of_lead_investors != investments['number_of_lead_investments']:
                        investments['number_of_lead_investments'] = new_number_of_lead_investors
                    
                if "Investments" == data_validator :
                    new_number_of_lead_investors = detail.find('field-formatter').text.strip()
                    if new_number_of_lead_investors and new_number_of_lead_investors != investments['number_of_investments']:
                        investments['number_of_investments'] = new_number_of_lead_investors
                    
                if "Exits" in data_validator :
                    new_number_of_lead_investors = detail.find('field-formatter').text.strip()
                    if new_number_of_lead_investors and new_number_of_lead_investors != investments['exits']:
                        investments['exits'] = new_number_of_lead_investors
                    
                if "Funding Rounds" in data_validator :
                    new_number_of_lead_investors = detail.find('field-formatter').text.strip()
                    if new_number_of_lead_investors and new_number_of_lead_investors != investments['number_of_funding_rounds']:
                        investments['number_of_funding_rounds'] = new_number_of_lead_investors
                    
                if "Total Funding Amount" in data_validator :
                    new_number_of_lead_investors = detail.find('field-formatter').text.strip()
                    if new_number_of_lead_investors and new_number_of_lead_investors != investments['total_funding_amount']:
                        investments['total_funding_amount'] = new_number_of_lead_investors
                    
                
        except Exception as e: logger.error(f"ERROR in financial investments_section : {e}")
        
        new_fulltable = {}
        non_blank_index = 1
        try:
            table_data = information.find('table')
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
                
        except Exception as e: logger.error(f"ERROR in financial investments_section : {e}")

        
        return investments
           
    def diversity_investments_section(self,information,main_data, old_dict = {}):
        old_diversity_investments_dict = old_dict.get('diversity_investments', {})
        diversity_investments = {
            "number_of_diversity_investment": old_diversity_investments_dict.get('number_of_diversity_investment', ''),
            "table": old_diversity_investments_dict.get('table', {})
        }
        try:
            details_data=information.find_all('tile-field')
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                
                if "Total number of diversity investments made by an investor" in data_validator:
                    new_number_of_diversity_investment = detail_data.find('field-formatter').find('a').text.strip()
                    if new_number_of_diversity_investment and new_number_of_diversity_investment != diversity_investments['number_of_diversity_investment']:
                        diversity_investments['number_of_diversity_investment'] = new_number_of_diversity_investment
        except Exception as e:
            logger.error(f"Error in extracting ipoandstock details: {e}")

        new_fulltable = {}
        non_blank_index = 1
        try:
            table_data = information.find('table')
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
                for key, value in diversity_investments['table'].items():
                    if value not in combined_table.values():
                        combined_table[str(non_blank_index)] = value
                        non_blank_index += 1

                # Update the table with the combined data
                diversity_investments['table'] = combined_table
                
        except Exception as e:
            logger.error(f"Error in extracting diversity_investments_section details: {e}")
            ...
        
        return diversity_investments
    
    def exist_section(self,information : BeautifulSoup ,main_data : BeautifulSoup , old_dict = {}):
        old_exits_dict = old_dict.get('exits', {})

        exist = {
            "number_of_exits": old_exits_dict.get('number_of_exits', ''),
            "table": old_exits_dict.get('table', {})
        }

        try:
            details_data=information.find_all('tile-field')
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info')
                if data_finder :
                    data_finder = data_finder.find('icon').get('aria-describedby')
                    data_validator=main_data.find('div',{'id':str(data_finder)})
                    if data_validator : 
                        data_validator = data_validator.text
                        if "Total number of Exits" in data_validator:
                            number_of_exits = information.find_all('field-formatter')
                            for total_exits in number_of_exits:
                                if not total_exits.find('a') : continue
                                if 'num_exits' in total_exits.find('a').get('href'):
                                    new_total_number_of_exits =total_exits.text.strip()
                                    if new_total_number_of_exits and new_total_number_of_exits != exist['number_of_exits']:
                                        exist['number_of_exits'] = new_total_number_of_exits
                                    break
                    
        except Exception as e:
            logger.error(f"Error in extracting exist_section details: {e}")
            ...
        new_fulltable = {}
        non_blank_index = 1
        try:
            table_data = information.find_all('div',{'class':'exit-list-item ng-star-inserted'})
            if table_data :
                for data in table_data : 
                    column = data.find('a')
                    if column :
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
                
        except Exception as e:
            logger.error(f"Error in extracting exist_section details: {e}")
            ...
       
        return exist
    
    def fund_raised_section(self,information,main_data, old_dict = {}):
        old_fund_raised_dict = old_dict.get('fund_raised', {})

        fund_raised = {
            "number_of_funds": old_fund_raised_dict.get('number_of_funds', ''),
            "total_fund_raised": old_fund_raised_dict.get('total_fund_raised', ''),
            "table": old_fund_raised_dict.get('table', {})
        }
        try:
            details_data=information.find_all('tile-field')
            for detail_data in details_data:
                data_finder=detail_data.find('label-with-info').find('icon').get('aria-describedby')
                data_validator=main_data.find('div',{'id':str(data_finder)}).text
                if "Total number of Funds raised" in data_validator:
                    new_number_of_funds = detail_data.find('field-formatter').text.strip()
                    if new_number_of_funds and new_number_of_funds != fund_raised['number_of_funds']:
                        fund_raised['number_of_funds'] = new_number_of_funds
                        
                if "Total funding amount raised across all Fund Raises" in data_validator:
                  new_total_fund_raised = detail_data.find('field-formatter').text.strip()
                  if new_total_fund_raised and new_total_fund_raised != fund_raised['total_fund_raised']:
                    fund_raised['total_fund_raised'] = new_total_fund_raised
                            
        except Exception as e:
            logger.error(f"Error in extracting fund_raised_section details: {e}")
            ...
        new_fulltable = {}
        non_blank_index = 1
        try:
            table_data = information.find('table')
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
                
        except Exception as e:
            logger.error(f"Error in extracting fund_raised_section details: {e}")
            ...
        
        return fund_raised
        
    def financial_process_logic(self,url, dict = {}):
        financial_detail={}
        self.funding_round={}
        self.investors={}
        self.acquisition={}
        self.ipoandstock={}
        self.investments={}
        self.diversity_investments={}
        self.exist={}
        self.fund_raised={}
        session_id, cookies = cf.load_session()
        isloaded, res = cf.get_scrpido_requests(url, session_id, cookies)
        if isloaded:
            try :
                financialdata = BeautifulSoup(res.text, 'lxml')
                
                funding_section = financialdata.find('mat-card',{"id":"funding_rounds"})
                if funding_section :
                    self.funding_round = self.funding_round_section(funding_section,financialdata)
                    
                investors_section = financialdata.find('mat-card',{"id":"investors"})
                if investors_section :
                    self.investors = self.investors_section(investors_section,financialdata)
                    
                acquisitions_section = financialdata.find('mat-card',{"id":"acquisitions"})
                if acquisitions_section :
                    self.acquisition = self.acquisitions_section(acquisitions_section,financialdata)
                        
                ipoandstock_section = financialdata.find('mat-card',{"id":"ipo_and_stock_price"})
                if ipoandstock_section :
                    self.ipoandstock = self.ipoandstock_section(ipoandstock_section,financialdata)

                investments_section = financialdata.find('mat-card',{"id":"investments"}) 
                if investments_section :
                    self.investments = self.investments_section(investments_section,financialdata)
                        
                diversity_section = financialdata.find('mat-card',{"id":"diversity_spotlight_investments"})
                if diversity_section :
                    self.diversity_investments = self.diversity_investments_section(diversity_section,financialdata)
                        
                exist_section = financialdata.find('mat-card',{"id":"exits"})
                if exist_section :
                    self.exist = self.exist_section(exist_section,financialdata)
                        
                fund_raised_section = financialdata.find('mat-card',{"id":"funds"})
                if fund_raised_section :
                    self.fund_raised = self.fund_raised_section(fund_raised_section,financialdata)
                
                financial_detail={
                        "financial":{
                            "funding_round":self.funding_round,
                            "investors":self.investors,
                            "acquisitions":self.acquisition,
                            "ipo_&_stock": self.ipoandstock,
                            "investments":self.investments,
                            "diversity_investments":self.diversity_investments,
                            "exits":self.exist,
                            "fund_raised":self.fund_raised
                        }
                        }
            except Exception as e: 
                logger.error(f"ERROR in financial financial_process_logic : {e}")
        return financial_detail