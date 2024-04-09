'Module to access historical data from Investing.com and create formatted csv to analyze'

import re, pandas as pd, decimal,json,bs4,os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, ElementNotVisibleException, TimeoutException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



def toDecimalConversion(number,quant=6):
    'Convert to Decimal for accuracy lost in float-types'
    quant = decimal.Decimal(str(quant))
    quant = 1*10**(-quant)
    decimal_return = decimal.Decimal(str(number)).quantize(quant)
    return decimal_return

def event_sources():
    '''Retrieve a list of event details from json file
    \nreturns a tuple:
    \n(country,event_name,link)'''
    with open(r'events_sources.json', 'r') as json_file:
        sources_json=json.load(json_file)
    links=[]
    for k,v in sources_json.items():
            for k2,v2 in v.items():
                links.append((k,k2,v2))
    return links



def data_check(source:str,date_check=None)->bool:
    'check if last entry has consensus after clicking "see more" or if the data is newer than our last update'
    if date_check:
        date_check = pd.to_datetime(date_check)
    soup = bs4.BeautifulSoup(source,'html.parser')
    source = soup.prettify()
    info = re.findall('<tr.*?historicEvent.*?</tr>',source,re.DOTALL)
    tries=0
    for i in info[::-1]:#reverse order
        if date_check:
            month_re = re.search('<td.*?\n(.*?)\n.*?</td>',i,re.DOTALL).group(1)
            month_re = month_re.strip()
            try:
                month_re = month_re[:month_re.index('(')-1]
            except:
                pass
            month_re = pd.to_datetime(month_re)
            if month_re<date_check:
                return True
            else:
                return False
        else:
            consensus_re = re.search('.*?</td>.*?</td>.*?</td>.*?<td.*?\n(.*?)\n.*?</td>',i,re.DOTALL).group(1)
            consensus_re = re.sub('[^0-9.]','',consensus_re)
            try:
                toDecimalConversion(consensus_re)
                return False
            except decimal.InvalidOperation :
                tries+=1
            if tries<16:
                continue
            else:
                return True




def extract_from_html(source:str,country,event_name):
    '''Function that takes the source html and converts it to a csv for analysis on the graphing file
    \nIs called when the parser determines there are no longer any releases with a consensus value, or no more new data
    \nsource: is passed the driver.page_source information with is html string
    \ncountry & event_name: are determined by the event_sources.json file to assist in adding files to Data/
    '''

    extractDF = pd.DataFrame(columns=['Date','Time','Actual','Consensus'])#create empty dataframe
    soup = bs4.BeautifulSoup(source,'html.parser')
    source = soup.prettify()#facilitate parsing with formatted html

    #parse html and retrieve relevant data adding to the end of the dataframe one row at a time
    info = re.findall('<tr.*?historicEvent.*?</tr>',source,re.DOTALL)
    for i in info:
        month_re = re.search('<td.*?\n(.*?)\n.*?</td>',i,re.DOTALL)
        time_re = re.search('.*?</td>.*?<td.*?\n(.*?)\n.*?</td>',i,re.DOTALL)
        actual_re = re.search('.*?</td>.*?</td>.*?<td.*?\n.*?\n(.*?)\n.*?</td>',i,re.DOTALL).group(1)
        consensus_re = re.search('.*?</td>.*?</td>.*?</td>.*?<td.*?\n(.*?)\n.*?</td>',i,re.DOTALL).group(1)
        actual_re = re.sub('[^0-9.]','',actual_re)
        consensus_re  = re.sub('[^0-9.]','',consensus_re)
        try:
            actual_re = toDecimalConversion(actual_re,6)
            consensus_re = toDecimalConversion(consensus_re,6)
        except:
            continue
        time_re = time_re.group(1).strip()
        row = {'Date':month_re.group(1),'Time':time_re,'Actual':actual_re,'Consensus':consensus_re}
        extractDF.loc[len(extractDF)] = row
        
    #create a difference
    extractDF['Difference'] = extractDF['Actual'] - extractDF['Consensus']
    extractDF['Actual'] = extractDF['Actual'].apply(pd.to_numeric)
    extractDF['Consensus'] = pd.to_numeric(extractDF['Consensus'],errors='coerce')

    #handling daylight savings time and converting the times to gmt+2 for MetaTrader time/ forex time
    dst_str = { #dict of time changes for each year to our oldest point of having consensus data, 2008
        2024:('2024-03-10','2024-11-03'),
        2023:('2023-03-12','2023-11-05'),
        2022:('2022-03-13','2022-11-06'),
        2021:('2021-03-14','2021-11-07'),
        2020:('2020-03-08','2020-11-01'),
        2019:('2019-03-10','2019-11-03'),
        2018:('2018-03-11','2018-11-04'),
        2017:('2017-03-12','2017-11-05'),
        2016:('2016-03-13','2016-11-06'),
        2015:('2015-03-08','2015-11-01'),
        2014:('2014-03-09','2014-11-02'),
        2013:('2013-03-10','2013-11-03'),
        2012:('2012-03-11','2012-11-04'),
        2011:('2011-03-13','2011-11-06'),
        2010:('2010-03-14','2010-11-07'),
        2009:('2009-03-08','2009-11-01'),
        2008:('2008-03-09','2008-11-02'),
        }
    #converting the values into timestamps 
    dst = {}
    for i in dst_str:
        temptup = []
        for j in dst_str[i]:
            j = pd.to_datetime(j)
            temptup.append(j)
        dst[i] = temptup
    extractDFDates = extractDF.copy()

    #handling data rows with different formatting and adding the times 
    for i,r in extractDF.iterrows():
        if '(' in r["Date"]:
            date = re.search('(.*?)\(.*',r['Date'])
            newdate = date.group(1).strip()
        else:
            newdate = r['Date']
        newdate = f'{newdate} {r["Time"]}'
        newdate = pd.to_datetime(newdate)

        #adding the hours to the base date depending on the daylight savings time status
        if newdate>dst[newdate.year][0] and newdate<dst[newdate.year][1]:
            newdate = newdate + pd.to_timedelta('8 hours')
        else:
            newdate = newdate + pd.to_timedelta('7 hours')
        extractDFDates['Date'].at[i] = newdate
    extractDFDates.drop(columns=['Time'],inplace=True) 


    try:
        os.makedirs('Data')
    except:
        pass
    try:
        os.makedirs(f'Data\\{country}')
    except:
        if os.path.isfile(f'Data\\{country}\\{event_name}.csv'):
            old_df = pd.read_csv(f'Data\\{country}\\{event_name}.csv',index_col=0,parse_dates=['Date'])
            extractDFDates= pd.concat([extractDFDates,old_df]).drop_duplicates(subset=['Date'],ignore_index=True)
            extractDFDates['Difference'] = extractDFDates['Difference'].apply(toDecimalConversion)
    extractDFDates.to_csv(f'Data\\{country}\\{event_name}.csv')






def gather_data(event:int=0,clicks:int=0):
    'function to access website, and retrieve the html source code, passing on to extract_from_html()'
    
    def x_button_find():
        'function to close pop-up when scraping'
        x_button = WebDriverWait(driver, 0.4).until(
            EC.element_to_be_clickable((By.XPATH, '//i[@class="popupCloseIcon largeBannerCloser"]')))
        if x_button:
            x_button.click()
        return 1
    
    #access event information and sources from json file
    links = event_sources()
    events_to_get = len(links)
    link:str = links[event][2]#third tuple item is the link
    link_num = link[link.index('-',-4)+1:]#extract ID number of page to locate the see more button as it is ID specific 

    #check to see if there is already data, and if there is retrieve the latest date of that file
    newest_event_date =None
    if os.path.isfile(f'Data\\{links[event][0]}\\{links[event][1]}.csv'):
        with open(f'Data\\{links[event][0]}\\{links[event][1]}.csv','r') as data_file:
            for idx,line in enumerate(data_file):
                if idx == 1:
                    items = line.split(',')
                    newest_event_date = items[1]
                    break
    #open webpage and load for 6 seconds before beginning scraping
    driver = webdriver.Chrome()
    driver.set_page_load_timeout(6)
    try:
        driver.get(link)
    except TimeoutException:
        pass
    #beginning of scraping the current link
    try:
        while True:
            if clicks==0:#check if clicked the pop-up 
                try:
                    clicks = x_button_find()
                except:
                    pass
            see_more_button = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, f'//div[@id="showMoreHistory{link_num}"]')))
            if see_more_button:
                try:
                    see_more_button.click()
                except ElementClickInterceptedException:#if popup blocks see more button
                    clicks = x_button_find()
                    continue
                if data_check(driver.page_source,newest_event_date if newest_event_date else None): #preform check on end of data if the last 15 table rows have consensus data
                    html = driver.page_source
                    driver.quit()
                    extract_from_html(html,links[event][0],links[event][1])#format html file into a dataframe and then create csv
                    if event <events_to_get-1:#if there are still links to check run the function again 
                        gather_data(event=(event+1))
                        break
                    else: break
                        
    except (NoSuchElementException, ElementNotVisibleException, TimeoutException) as e:
        pass