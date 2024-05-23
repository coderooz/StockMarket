from custom_functions.Requester import Requester
from custom_functions.DataHandlers import to_unix_timestamp, decode_json, dlist_dict
from datetime import datetime

sess = None
reto = Requester(
    ref='https://www.nseindia.com',
    set_ref=True,
    agent_file= 'F:/Code Works/Python_works/storage/others/user-agent.txt',      
    set_header=True)

def reqto(url,  ref=None, session=None, param=None):
    global reto, sess
    hedr = {'Connection': 'keep-alive','Cache-Control': 'max-age=0','DNT': '1','Upgrade-Insecure-Requests': '1','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36','Sec-Fetch-User': '?1','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9','Sec-Fetch-Site': 'none','Sec-Fetch-Mode': 'navigate','Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',}
    data, sess = reto.requestSessions(url, ref=ref, header=hedr, params=param, timeout=60, sessions=session, pre_request=True)
    return data

def getInsider(date1:str, date2:str, indexes:str='equities'):
    '''This method is to get insider deals from the nse website.'''
    idx = ['equities']
    if indexes in idx:
        url = f'https://www.nseindia.com/api/corporates-pit?index={indexes}&from_date={date1}&to_date={date2}'
        data = reqto(url)
        if data.status_code == 200:
            return data.json()['data']
        return data
    else:
        t = ','.join(idx)
        raise ValueError(f'The data passed in the indexes is invalid. The valid data points are {t}.')

def getDeals(date1:str, date2:str, catg:str, timeout:int=60):
    """This funciton is to fetch or get the data for block deals, sort sellings as well as bulk deals from nse."""
    if catg == 'short-selling' or catg == 'short':
        url = 'https://www.nseindia.com/api/historical/short-selling'
    elif catg == 'bulk' or catg == 'bulk-deals':
        url = 'https://www.nseindia.com/api/historical/bulk-deals'
    elif catg == 'block' or catg == 'block-deals':
        url = 'https://www.nseindia.com/api/historical/block-deals'
    else: raise ValueError('The cvalue passed in the catg parameter is invalid. please re-check the value passed in the catg parameter in the funtion.This function catg paramerter only takes on of three values, short-shelling, bulk and block.')
    url = url + f'?from={date1}&to={date2}'
    return reqto(url).json()


def getIndexsData():
    url = 'https://www.nseindia.com/api/allIndices'
    data = reqto(url).json()
    return data

def getArchives(symbol:str, date1:str, date2:str):
   url = f'https://www.nseindia.com/api/historical/securityArchives?from={date1}&to={date2}&symbol={symbol}&dataType=priceVolume&series=ALL'
   return reqto(url).json()['data']

def get_FIIDII(date:str, dataCatg:str='all', format:str='%d-%m-%Y', timeout:int=60):
    '''
        This method is to get the FII & DII data from NSE website of any perticular date.
        Paramters:
        ----------
            - date(str): This parameter takes the date of thich te data is to fetched.
            - dataCatg(str): This parameter is to get the values of either oi or vol, ot it be set to both  ith the all parameter.
            - format(str): This parameter takes in which format the date is passed. the default is alreaady set to %d-%m-%Y.
            - timeout(int): This parameter is to set the timeout of the request function after which the request is cancelled.
    '''
    dcatg = ['oi', 'vol' ]
    if dataCatg == 'all':
        data = []
        for i in dcatg:
            ret = get_FIIDII(date, dataCatg=i, format=format, timeout=timeout)
            data += ret if ret!=False and ret!=None else []
        return data
    elif dataCatg in dcatg:
        url = f'https://archives.nseindia.com/content/nsccl/fao_participant_{dataCatg}'
        dt = datetime.strptime(date, format)
        dt2 = dt.strftime("%d%m%Y")
        if dt.weekday() not in (5,6):
            fd = reto.request(f'{url}_{dt2}.csv', timeout=timeout)
            if fd.status_code == 200:
                to = fd.text.replace('\r', '').replace('\t','').split("\n")[1:-1]
                headers = [i.replace(' ','_').lower() for i in to[0].split(',')]
                new_data = []
                for i in to[1:]:
                    d = {'date':date, 'catg':dataCatg}
                    t = i.split(',')
                    for n in range(len(headers)):
                        d[headers[n]] = t[n]
                    new_data.append(d)
                return new_data
        else: return False
    else: raise ValueError('The values passed in the dataCatg parameter is invalid. Please choose either one of the oi, vol or all.')

def get_dta(symb:str, period:list=[], catg:int=1, rang:str='1d', idexed:bool=True, indexName='index', research:bool=False, processed:bool=False):
    '''
        This fucntion is to fetch data from the yahoo website.

        ** Parameters **
        - symb (str): This parameter takes in the symbol or the name of the asset that is to be searched like [Nifty 50] or [Amazon] or the symbol of the asset like [AMZN] or [MSFT].
        - period (list): This parameter is a non by default but takes in two values in a list which is the date of to different dates in 5y-%m-%Y format.
        - catg (int):
        - idexed (bool): This parameter is to certify is the values given are to be index or not. It is [TRUE] by default.
        - indexName (str): This parameter takes in the key value that s to be use while adding index number.
        - research (bool): This parameter uses the YahooSearch Function to search for the correct name of the search like the symbol is given [TRUE] else it is [False] by default.
    '''
    url_data = {"base_url": "https://query2.finance.yahoo.com","endpoint": ["/v8/finance/chart", "/v10/finance/quoteSummary"],"ticker": f'/{symb}',"params":[{'interval':'1m','range':'1d','includePrePost':'true','useYfid':'true','includeAdjestedClose':'true'},{"formatted": "true","language": "en-US","region": "US","modules": ["summaryProfile","financialData","recommendationTrend","upgradeDowngradeHistory","earnings","defaultKeyStatistics","calendarEvents"],"cors_domain": "finance.yahoo.com"}]}
    url = url_data['base_url'] + url_data['endpoint'][0] + url_data['ticker'] # making primary url value
    params = url_data['params'][0].copy()
    if period!=[] and len(period) == 2:
        del params['range']
        p1 = int(to_unix_timestamp(period[0],"%d-%m-%Y"))
        p2 = int(to_unix_timestamp(period[1], "%d-%m-%Y"))
        if p1 < p2:
            params['from'],params['to'] = p1, p2
        else:
            params['from'],params['to'] = p2, p1

    if catg == 1:
        url = reto.request(url=url, params=params, ref='https://finance.yahoo.com/')
        price_data = decode_json(url.text)
        if price_data['chart']['result']!=None:
            prices = price_data['chart']['result'][0]
            quotes = prices['indicators']['quote'][0]
            quotes['timestamp'] = prices['timestamp']
            if processed == True:
                into = {'dataGranularity':prices['meta']['dataGranularity'],'range':prices['meta']['range']}
                cdInfo = candleInfo(dlist_dict(quotes), index=idexed, indexName=indexName)
                k = [] 
                for ci in cdInfo:
                    d = into.copy()
                    d.update(ci)
                    k.append(d)
                return k
            else: return url

        if research:
            symb = yahooSearch(query=symb, count=1, falseAll=False)
            if symb: return get_dta(symb['symbol'], period, catg, idexed, indexName)
            else: return False
        else: return price_data

    elif catg == 2:
        profile_url =url_data['base_url'] + url_data['endpoint'][1] + url_data['ticker']
        params = url_data['params'][1]
        params['modules'] = ','.join(params['modules'])
        return decode_json(Requester().request(url=profile_url, params=params).text)['quoteSummary']['result'][0]
    else: raise ValueError('The `catg` param only takes [1,2] as it\'s values.\nGiven param value = {catg}')

def yahooSearch(query, count:int=10, falseAll:bool=True, exchange:str=None, quote:str=None):
    global reto
    url = f'https://query2.finance.yahoo.com/v1/finance/search?q={query}&quotesCount={count}&newsCount=0&listsCount=2&enableFuzzyQuery=false&quotesQueryId=tss_match_phrase_query&multiQuoteQueryId=multi_quote_single_token_query&newsQueryId=news_cie_vespa&enableCb=true&enableNavLinks=true&enableEnhancedTrivialQuery=true&enableResearchReports=true&enableCulturalAssets=true&enableLogoUrl=true&researchReportsCount=2'
    data = reto.request(url=url, ref='https://finance.yahoo.com/').json()['quotes']
    
    if falseAll: return data
    
    if exchange is not None:
        data = [d for d in data if('exchDisp' in d.keys() and exchange.lower() == d['exchDisp'].lower())]
    
    if quote is not None:
        data = [d for d in data if ('exchDisp' in d.keys() and quote.lower() == d['typeDisp'].lower())]

    for i in data:
        if 'shortname' in i.keys() and (i['shortname'] == query.upper() or i['symbol'] == query.upper()): return i
        
    return False
