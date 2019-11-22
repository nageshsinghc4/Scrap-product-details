# -*- coding: utf-8 -*-
"""
Description : Search through google for any part code, get top 10 Google search results and scrape product details and Product images from each of them (if available). 

@author: Nagesh Singh Chauhan
"""
from bs4 import BeautifulSoup
import requests
import pandas as pd
from urllib2 import urlopen,urlparse, Request,HTTPError
import urllib2
import numpy as np
from pandas import ExcelWriter
import glob
import os
from httplib import BadStatusLine
import json

USER_AGENT = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

#get result from google search
def fetch_results(search_term, number_results, language_code, language):
    assert isinstance(search_term, str), 'Search term must be a string'
    assert isinstance(number_results, int), 'Number of results must be an integer'
    escaped_search_term = search_term.replace(' ', '+')
    google_url = 'http://www.google.co.in/search?q={}&num={}&hl={}&lr={}'.format(escaped_search_term, number_results, language_code, language)
    response = requests.get(google_url, headers=USER_AGENT)
    response.raise_for_status()
    return search_term, response.text

#parse the results
def parse_results(html, keyword):
    soup = BeautifulSoup(html, 'html.parser')
    found_results = []
    rank = 1
    result_block = soup.find_all('div', attrs={'class': 'g'})
    for result in result_block:
        link = result.find('a', href=True)
        title = result.find('h3', attrs={'class': 'r'})
        if link and title:
            link = link['href']
            if link != '#':
                found_results.append({'rank': rank, 'link': link})
                rank += 1
    return found_results

#return the result
def scrape_google(search_term, number_results, language_code, language):
    try:
        keyword, html = fetch_results(search_term, number_results, language_code, language)
        results = parse_results(html, keyword)

        return results
    except AssertionError:
        raise Exception("Incorrect arguments parsed to function")
    except requests.HTTPError:
        raise Exception("You appear to have been blocked by Google")
    except requests.RequestException:
        raise Exception("Appears to be an issue with your connection")
    
    
#extract product information and images from the urls
def urlScrap(url_list = [], *args):
    print("\n loading ....\n")
    for k,x in enumerate(url_list):
        try:
            url = x['link']
            urls = url.replace('https://','http://') # replace https with http , it solves :::: HTTPSConnectionPool(host='www.eurooptic.com', port=443): Max retries exceeded with url:
            print "\n"+urls+"\n"
            html = requests.get(urls).content.replace("<br>", '\n')
            df_list = pd.read_html(html) #match = match, match the specific string, domt give header parameter
            for i,dfx in enumerate(df_list): #enumerate : to iterate over a list like object
                print dfx
                dfx.to_csv('D:/KJ/Nagesh/Downloads/OpenCatalogue/test/test{}_{}.csv'.format(i,k), index=False, header = True, encoding='utf-8')

        except Exception as e:
            print(e)   

#Extract images from the google Image search
def imageScrap(userInput_PC):
    ActualImages=[]# contains the link for Large original images, type of  image
    image_type="ActiOn"
    userInput_PC= userInput_PC.split()
    userInput_PC='+'.join(userInput_PC)
    url="http://www.google.co.in/search?q="+userInput_PC+"&source=lnms&tbm=isch"
    DIR="Pictures"   #add the directory for your image here
    header={'User-Agent':"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36"}
    soup = BeautifulSoup(urllib2.urlopen(urllib2.Request(url,headers=header)),'html.parser')
    
    for a in soup.find_all("div",{"class":"rg_meta"}):
        link , Type =json.loads(a.text)["ou"]  ,json.loads(a.text)["ity"]
        ActualImages.append((link,Type))          
        
    if not os.path.exists(DIR):
                os.mkdir(DIR)
    DIR = os.path.join(DIR, userInput_PC.split()[0])
    if not os.path.exists(DIR):
                os.mkdir(DIR)    
    print("\n Image results : \n")
    for i , (img , Type) in enumerate( ActualImages[:5]): #set the number of iterations to 5
        try:
            req = urllib2.Request(img, headers={'User-Agent' : header})
            raw_img = urllib2.urlopen(req).read()

            cntr = len([i for i in os.listdir(DIR) if image_type in i]) + 1
           
            print cntr
            if len(Type)==0:
                f = open(os.path.join(DIR , image_type + "_"+ str(cntr)+".jpg"), 'wb')
            else :
                 f = open(os.path.join(DIR , image_type + "_"+ str(cntr)+"."+Type), 'wb')
                        
            f.write(raw_img)
            f.close()
        except Exception as e:
            print "could not load : "+img
            print e    

#Remove all the files after the execution
def removeTempFiles():
    folder = 'D:/KJ/Nagesh/Downloads/OpenCatalogue/test/'
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)        
        except Exception as e:
            print(e)

#main method
if __name__ == '__main__':
    data = [] #store url and rank of the url
    userInput_vendor = raw_input("Choose the vendor name :")
    if(userInput_vendor == 'HP'):
        userInput_PC = raw_input("Enter the PartCode of the HP vendor :") 
    try:
        results = scrape_google(userInput_PC, 10, "en","lang_en")
        for result in results:
            data.append(result)
    except Exception as e:
        print(e)
    print("\nTop 10 Google Search results for given input part code ==>> \n")
    print(data)
    print '\n'

    urlScrap(data, userInput_PC) 
    imageScrap(userInput_PC) 
   
    #Manipulation of extracted data
    try:
        files = glob.glob('D:/KJ/Nagesh/Downloads/OpenCatalogue/test/*.csv')
        frame = pd.DataFrame()
        list_ = []
        for file_ in files:
            df = pd.read_csv(file_,index_col=False) #read only 2 columns 
            list_.append(df)
        frame = pd.concat(list_)
        frame.dropna(subset=['0','1'], inplace=True) #remove empty cells from both columns 
        frame = frame[frame['0'].str.contains('^Height|^Type|^Item|^Length|^Mfg|^Width|^Weight|^Dimensions|^Sku|^SKU|^Brand|^Model|^Color|^Product|^Description|^HSN|^Part|^Category|^BRAND|^MODEL|^COLOR|^DESCRIPTION|^PART|^CATEGORY|^UPC|^PRODUCT|^Department|^Stock|^Manufacturer|^MANUFACTURER|^MPN|^EAN', na = False)]
        frame.drop_duplicates(subset='0', keep="first", inplace= True)
        frame.to_csv('SecondLast_output.csv', index=False,columns=['0', '1'], encoding = 'utf-8')
        Rename_df = pd.DataFrame()
        Rename_df = pd.read_csv('SecondLast_output.csv', index_col = None, header = None, skiprows = 1)
        Rename_df.columns = ['Key', 'Value'] #change column names
   #     Rename_df[Rename_df['Key'].str.isalpha()] #check if the record is alphanumeric or not 
        patternDel="\b[A-Z]+[0-9]+\b" # Regex for removing records of above regex type AGSJST5437SQ 
        row_delete = Rename_df['Key'].str.contains(patternDel)
        Reg_df = Rename_df[~row_delete] 
        Reg_df = Reg_df[~Reg_df['Key'].str.contains('^Copyright|^Customer|^Sold|^Ship|^Best|^Price|^M.R.P|^Enter|^You|^Our|^eBay')]

        pattern_reg = "(\w+\s+){4}" # Regex for removing records having more than 4 words
        row_del = Reg_df['Key'].str.contains(pattern_reg)
        Reg_df_1 = Reg_df[~row_del] # removing records of above regex type
        Reg_df_1['Key'] = Reg_df_1['Key'].str.replace(r'[^\w\s]+', '')
        Reg_df_1.drop_duplicates(subset='Key', keep='first',inplace=True) #remove duplicates 
        Reg_df_1.to_csv('Last_output.csv', index=False, encoding = 'utf-8')
      
        removeTempFiles() #delete all the temporary files created
        print('\nAll the temperory files are deleted\n')
    except Exception as e:
        print(str(e))