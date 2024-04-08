import re, pandas as pd, decimal,json,bs4,os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, ElementNotVisibleException, TimeoutException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



def toDecimalConversion(number,quant=20):
    quant = decimal.Decimal(str(quant))
    quant = 1*10**(-quant)
    decimal_return = decimal.Decimal(str(number)).quantize(quant)
    return decimal_return

def event_sources():
    with open(r'events_sources.json', 'r') as json_file:
        sources_json=json.load(json_file)
    links=[]
    for k,v in sources_json.items():
            for k2,v2 in v.items():
                links.append((k,k2,v2))


def check_end_of_consensus(source:str)->bool:
    'check if last entry has consensus after clicking "see more"'
    soup = bs4.BeautifulSoup(source,'html.parser')
    source = soup.prettify()
    info = re.findall('<tr.*?historicEvent.*?</tr>',source,re.DOTALL)
    tries=0
    for i in info[::-1]:#reverse order
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
    extractDF = pd.DataFrame(columns=['Date','Time','Actual','Consensus'])
    soup = bs4.BeautifulSoup(source,'html.parser')
    source = soup.prettify()
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
        
    extractDF['Difference'] = extractDF['Actual'] - extractDF['Consensus']
    
    extractDF['Actual'] = extractDF['Actual'].apply(pd.to_numeric)

    extractDF['Consensus'] = pd.to_numeric(extractDF['Consensus'],errors='coerce')

    

    dst_str = {
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
    dst = {}
    for i in dst_str:
        temptup = []
        for j in dst_str[i]:
            j = pd.to_datetime(j)
            temptup.append(j)
        dst[i] = temptup
    extractDFDates = extractDF.copy()
    for i,r in extractDF.iterrows():
        if '(' in r["Date"]:
            date = re.search('(.*?)\(.*',r['Date'])
            newdate = date.group(1).strip()
        else:
            newdate = r['Date']
        
        newdate = f'{newdate} {r["Time"]}'
        newdate = pd.to_datetime(newdate)
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
        pass
    extractDFDates.to_csv(f'Data\\{country}\\{event_name}.csv')


def gather_data(links:list,event:int=0,clicks:int=0):
    def x_button_find():
        x_button = WebDriverWait(driver, 0.4).until(
            EC.element_to_be_clickable((By.XPATH, '//i[@class="popupCloseIcon largeBannerCloser"]')))
        if x_button:
            x_button.click()
        return 1
    events_to_get = len(links)
    link:str = links[event][2]
    link_num = link[link.index('-',-4)+1:]
    driver = webdriver.Chrome()
    driver.set_page_load_timeout(6)
    try:
        driver.get(link)
    except TimeoutException:
        print('bypass load')
    try:
        while True:
            if clicks==0:
                try:
                    clicks = x_button_find()
                except:
                    pass
            see_more_button = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, f'//div[@id="showMoreHistory{link_num}"]')))
            if see_more_button:
                try:
                    see_more_button.click()
                except ElementClickInterceptedException:
                    clicks = x_button_find()
                    continue
                if check_end_of_consensus(driver.page_source):
                    html = driver.page_source
                    driver.quit()
                    extract_from_html(html,links[event][0],links[event][1])
                    if event <events_to_get-1:
                        gather_data(links,event+1)
                        break
                    else: break
                        
    except (NoSuchElementException, ElementNotVisibleException, TimeoutException) as e:
        pass