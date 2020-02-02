import numpy as np
import pandas as pd
import re
from bs4 import BeautifulSoup
import requests
import urllib.request
import time
import json


def get_item(from_page, to_page, base_url='https://www.desocialekaart.be/zoek?page={}'):
    
    """ 
        The function to get the URLs from the target website in order to scrape. 
  
        Parameters: 
            base_url : The main part of the https link of the website that will be scraped.
            from_page : The starting range parameter to get the specific link if the website has many pages or sub_pages
            to_page : The end_range parameter to get the specific link if the website has many pages or sub_pages
          
        Returns: 
            links: A list of URLs 
    """
    
    links = []
    for page_number in range(from_page,to_page):
        page_is_not_processed = True
        while page_is_not_processed:

            try:
                page = requests.get(base_url.format(page_number)+"&where=")
                soup = BeautifulSoup(page.text, 'html.parser')
                name_links = soup.find(id='search-results')
                links_list = name_links.find_all("a")
                for element in links_list:
                    links.append('https://www.desocialekaart.be' + element.get('href'))
                
                page_is_not_processed = False

            except:
                print("I got an error while parsing page {}.. I'll retry soon.".format(page_number))
                time.sleep(0.5)

    return links


def get_ngo_info(ngo_page_url):
    
    """ 
        The function to get information in text format from the target website (a single page) scraping by Beatifulsoap object.
        
  
        Parameters: 
            ngo_page_url : An URL
          
        Returns: 
            results: A dictionary of required data(each key of this dictionary is to be a column)
                    and source link(to catch the errors) 
    """
    
    #page_is_not_processed = True
    results={}
    results["source_page"] = ngo_page_url
    
    res = requests.get(ngo_page_url, verify=False)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    if str(res.status_code) == "200" : 

        try:
            temp_infos = [i.text for i in soup.find(class_='fiche-public-name')]
            results["partner_name"] = temp_infos[0]
        except:
            results["partner_name"] ="null"

        try:
            temp_infos = [i.text for i in soup.find_all('div', {'class':['street', 'number', 'postcode-city']})]

            final_adress = ""
            for element in temp_infos:
                final_adress += " " + element

            results["adress"] = final_adress
        except:
            results["adress"] = "null"

        try:
            temp_infos = [i.text.strip() for i in soup.find_all(class_='field-collection-view clearfix view-mode-full field-collection-view-final')]
            results["phone"] = temp_infos[0]
        except:
            results["phone"] = "null"

        try:
            temp_infos = [i for i in soup.find(class_="fiche-online").find_next('a')]
            results["web_adress"] = temp_infos[0]
        except:
            results["web_adress"] = "null"

        try:
            temp_infos = [i for i in soup.find(class_='fiche-email').find_next('a')]
            results["email_adress"] = temp_infos[0] 
        except:
            results["email_adress"] = "null"  

        try:
            temp_infos = [i.text for i in soup.find_all(class_='fiche-working')]
            results["doelgroup"] = temp_infos[0] 
        except:
             results["doelgroup"] = "null"
    
    else :
        
        results["partner_name"] ="error"
        results["adress"] = "error"
        results["phone"] = "error"
        results["web_adress"] = "error"
        results["email_adress"] = "error" 
        results["doelgroup"] = "error"
    
    return results


def get_ngos_dataframe(ngo_page_urls):
    
    """ 
        The function to convert scraped data into a pandas DataFrame after looping for all pages and cleansing the captured data. 
  
        Parameters: 
            ngo_page_urls : A list of URLs 
          
        Returns: 
            df: A data frame containing features that has been asked for. 
    """
    
    partner_name=[]
    adress=[]
    phone=[]
    web_adress=[]
    email_adress=[]
    doelgroup=[]
    source_pages=[]
    
    for ngo_url in ngo_page_urls:
        
        infos = get_ngo_info(ngo_url)
        partner_name.append(infos["partner_name"])
        adress.append(infos["adress"])
        phone.append(infos["phone"])
        web_adress.append(infos["web_adress"])
        email_adress.append(infos["email_adress"])
        doelgroup.append(infos["doelgroup"])
        source_pages.append(infos["source_page"])
        
    df = pd.DataFrame({"source_page" : source_pages,
                       "partner_name" : partner_name,
                       "adress" : adress,
                       "phone" : phone,
                       "web_adress" : web_adress,
                       "email_adress" : email_adress,
                       "doelgroup": doelgroup})
    
    return df


companies_urls_to_scrape = get_item(from_page=3, to_page=4)
print('first func is ok')
df = get_ngos_dataframe(companies_urls_to_scrape)
print('second func is ok')
df_done = df[df.partner_name!="error"]
df_error = df[df.partner_name=="error"]

df_done.to_excel(r"..\Desktop\companies_well_scrapedtest.xlsx", header=True, index=False)
df_error.to_excel(r"..\Desktop\companies_error.xlsx", header=True, index=False)

