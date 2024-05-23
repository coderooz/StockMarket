from DataHandlers import get_unique,valreplace, setNum, flatten_dict, list_dlist, dlist_dict, dict_filter, equalizer_dict, space_remover, generate_time_intervals, get_previous_day, dateFormat, json_parser
from Requester import Requester
from datetime import datetime, time
from .MoneyControlHandler import MoneyControlHandler 
import re, plotly.graph_objects as go
from .utils.utils import setup_db, _handle_csv, connect_db, local_runner
import logging

logging.basicConfig(filename='./error/NSEHandler.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class NSEHandler():
    """
        NSEHandler
        ----------
        
        Functions List:
        - get_stocks: For fetching data:
        - 
    """
    def __init__(self, runLocal:bool=True, writeLogs:bool=True):

        """
            This class is used to handle the data related to National Stock Exchange of India (NSE).
            
            :param str db_file_name: The name of database file which stores
        """
        self.sessions, self.header = None,{'Connection': 'keep-alive','Cache-Control': 'max-age=0','DNT': '1','Upgrade-Insecure-Requests': '1','user-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36','Sec-Fetch-User': '?1','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9','Sec-Fetch-Site': 'none','Sec-Fetch-Mode': 'navigate','Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',}
        self.request = Requester(ref=['https://www.nseindia.com'], set_agent=True, set_header=True, set_ref=True)
        self.current_date = datetime.now()
        self.subscribed = {}
        self.writeLogs = writeLogs
        self.quoteCatg, self.index_list, self.stock_list = None,None,None
        self.dbConn = connect_db('NSE.db', './app/database')
        self.intia = {'moneyControl': None, 'yahoo': None, 'dhan': None}
        if runLocal: local_runner(self)
    
    def get_equityMasters(self, checkIndex:str='', fetchNew:bool=False)->(bool|list[dict]):
        """"
            get_equityMasters()
            -------------------
            Returns the category.

            Parametes:
            - `checkIndex` str: for checking if the category exists or not.
            
            Return:
            - (bool | list[dict]): The method will return a bool if the assets is exists in the list or else will return the date in a list of dictonary where each dict where each dict contains the data like this ```{'quote': 'Permitted to Trade', 'category': 'Others'}...```
        """
        data = []

        if fetchNew:
            data = self.requester('https://www.nseindia.com/api/equity-master')
            if data != None:
                data = [{'quote':i, 'category':k} for k,v in data.items() for i in v]
        else:
            data = self.dbConn.fetch('equityMaster')
            if len(data) == 0:
                data = self.get_equityMasters(fetchNew=True)
                self.dbConn.json_insert('equityMaster', data, True)
        if checkIndex!='': return (checkIndex in [i['quote'] for i in data]) 
        return data

    def get_indexNames(self, fetchNew:bool=False)->list[str]:
        """
            get_indexNames()
            ----------------
            This methods fetches the names of the indexes from the internet.

            Return:
            - list(str): This will return a list of string cointaining the names of the indexes.
        """
        data:list
        if fetchNew:
            data =  get_unique([n[0] for i in self.requester('https://www.nseindia.com/api/index-names').values() for n in i])  #type: ignore
        else:
            data = [i['name'] for i in self.dbConn.fetch('listings', 'name', query="assetType='index'")]
            if len(data) == 0:
                data = self.get_indexNames(True)
                self.dbConn.json_insert('listings', [{'name':i, 'symbol':i, 'assetType': 'index'} for i in data], True)
        return data

    def _get_equity_indices(self, catg:str='', fetchNew:bool=False)->list[dict]:
        """
            get_equity_indices()
            --------------------
            This method is to fetch the stocks names form that are under the indexes. 
            Parameters:
            - catg str: Takes the index name as the category. default is `''`, which will fetch all stock names from all the indexes.
            - fetchNew bool: fro fresh/raw searching.

            Result:
            - 
        """
        data = []
        if fetchNew:
            if catg == '':
                for n in [i['quote'].upper() for i in self.get_equityMasters()]:
                    data += self._get_equity_indices(catg=n)
                data2:dict = {}
                for d in data:
                    if d['symbol'] in data2.keys():
                        data2[d['symbol']]['quote'] = data2[d['symbol']]['quote'] + ',' + d['quote']
                    else:
                        data2[d['symbol']] = d

                data = list(data2.values())
            elif self.get_equityMasters(checkIndex=catg):

                data = self.requester('https://www.nseindia.com/api/equity-stockIndices', params={'index':catg.upper(), 'nometa':('true' if catg!='Securities in F&O' else None)})
                if data is not None and data is not False:
                    if catg!='Securities in F&O':
                        data = [{'symbol':i['symbol'], 'identifier':i['identifier'], 'quote':catg, 'assetType':('index' if i['priority']==1 else 'stock')} for i in setNum(space_remover(data['data']))]  
                    else :
                        pathway = {'name': 'meta > companyName','identifier':'identifier','__pathway__': {'path': 'meta','data':['series','activeSeries','debtSeries','isCASec','isFNOSec','isSLBSec','isDebtSec','isSuspended','isETFSec','isin','industrySymbol','isDelisted','isMunicipalBond','industry','tempSuspendedSeries','slb','faceValue','classOfShare','derivatives','issuedSize']}}
                        data = [{**self.stringer(json_parser(i, pathway)), 'quote':catg, 'assetType':'stock'} for i in setNum(space_remover(data['data']))]
        else:
            query = [('' if catg=='' else f"quote='{catg}'"), "assetType='index'"]
            data = [i['name'] for i in self.dbConn.fetch('listings', 'name', query=query)]
            if len(data)==0:
                data = self._get_equity_indices(catg=catg, fetchNew=True)
                self.dbConn.json_insert('listings', data, True)
        self._updateLogs('listings', 'index_list' , 1)
        return data

    def get_stocks(self, listings:str='', stock:str='', fetchNew:bool=False):
        """
            get_stocks()
            ------------
            This method returns a dictionary containing all stock details.

            Parameters:
                - `listings` str: Default is `''`. 
                - `stock` str: Default is `''`. 
                - `fetchNew` bool: Fetches from the web if `True` else from the database incase its `False`. Default is `False`. 
        """
        data = []
        if fetchNew:
            data = get_unique([i for i in self._get_equity_indices(listings) if i['assetType']=='stock'])
        else:
            query = ["assetType='stock'",('' if listings=='' else f"quote='{listings}'"),('' if  stock=='' else f"symbol='{stock}' OR name='{stock}'")] 
            data = self.dbConn.fetch('listings', query=query)
            if len(data) == 0 or data[0]['name'] =='' or data[0]['name']=='none':
                for i in [i['symbol'] for i in self.dbConn.fetch('listings', 'symbol', query=f"assetType='stock' AND name='' OR 'none'")]:
                    d = self._get_info(i, 'stock')
                    inf, met, sec = d['info'], d['metadata'], d['securityInfo']
                    nd = self.stringer({'name':inf['companyName'],'activeSeries':inf['activeSeries'],'debtSeries':inf['debtSeries'],'isCASec': inf['isCASec'],'isCASec': inf['isCASec'],'isFNOSec':inf['isFNOSec'],'isSLBSec':inf['isSLBSec'],'isDebtSec':inf['isDebtSec'],'isSuspended': inf['isSuspended'],'isin':inf['isin'],'isETFSec': inf['isETFSec'],'industrySymbol':inf['industry'],'isDelisted':inf['isDelisted'] ,'isMunicipalBond': inf['isMunicipalBond'],'series':met['series'],'industry':met['industry'], 'tempSuspendedSeries':','.join(inf['tempSuspendedSeries']), 'slb' : sec['slb'],'faceValue' : sec['faceValue'],'classOfShare' : sec['classOfShare'],'derivatives':sec['derivatives'], 'issuedSize': sec['issuedSize']})
                    self.dbConn.update('listings', nd, f"symbol='{i}' AND assetType='stock'")
                    data.append(nd)
        return data

    def get_asset_info(self, asset:str, catg:str='stock', fetchNew:bool=False):
        """
            get_asset_info()
            ----------------
            This method is responsible for fetching the information of the asset. Mostly for the equity type assets. 
        """

        if fetchNew:
            d = self._get_info(asset, catg)
            inf = d['info']
            data = {'name':inf['companyName'], "symbol": inf['symbol'], "identifier": inf['identifier'],'activeSeries':','.join(inf['activeSeries']) if len(inf['activeSeries']) > 1 else inf['activeSeries'][0] if len(inf['activeSeries']) == 1 else '','debtSeries':','.join(inf['debtSeries']) if len(inf['debtSeries']) > 1 else inf['debtSeries'][0] if len(inf['debtSeries']) == 1 else '', 'isFNOSec': 'true' if inf['isFNOSec'] else 'false', 'isCASec': 'true' if inf['isCASec'] else 'false','isSLBSec':'true' if inf['isSLBSec'] else 'false', 'isDebtSec':'true' if inf['isDebtSec'] else 'false', 'isSuspended': 'true' if inf['isSuspended'] else 'false'}
            if 'industry' in inf.keys():
                met, sec =  d['metadata'], d['securityInfo']
                data = {**data, 'isin':inf['isin'],'isETFSec': 'true' if inf['isETFSec'] else 'false','industrySymbol':inf['industry'],'isDelisted':'true' if inf['isDelisted'] else 'false','isMunicipalBond': 'true' if inf['isMunicipalBond'] else 'false','series':met['series'],'industry':met['industry'], 'tempSuspendedSeries':','.join(inf['tempSuspendedSeries']) if len(inf['tempSuspendedSeries']) > 1 else inf['tempSuspendedSeries'][0] if len(inf['tempSuspendedSeries']) == 1 else '', 'slb' : sec['slb'],'faceValue' : sec['faceValue'],'classOfShare' : sec['classOfShare'],'derivatives':sec['derivatives'], 'issuedSize': sec['issuedSize']}
        else: 
            data = self.dbConn.fetch('listings', query=f"symbol='{asset}' OR identifier='{asset}' OR name='{asset}'")
            if len(data) == 0:
                data = self.get_asset_info(asset, fetchNew=True)
                self.dbConn.json_insert('listings', data)
        return data

    def get_asset_List(self, assetType:str='')->list:
        """
            getList()
            ---------
            Desc: Returns the list of the given asset type.
            Method Type: Database

            Parameters:
                - `assetType` str: Takes the type of asset that is to be fetched. Default is `''`, hich signifies all the asset presents. Values permited are:
                    - `index`: For the indexs.
                    - `stock`: For the indexs.
            
            Returns: List of asset available of the given category or all.

        """
        data:list[str] = []
        if assetType!='' and assetType not in ['', 'index', 'stock']: raise ValueError(f'The value "{assetType}" is invalid.')
        query = '' if assetType=='' else f"assetType='{assetType}'"
        data = [i['name'] for i in self.dbConn.fetch('listings', 'name', query=query)]
        print('fetching asset')
        if len(data) == 0:
            self.get_stocks()
            data = self.get_asset_List(assetType)
        return data 
    
    def getCalenderEvents(self, index:str='', symbol:str='', issuer:str='', catg:int=0, fetchNew:bool=False)->list:
        """
            getCalenderEvents()
            -------------------
            This method is responsible for feting the events calennder from the NSEIndia website.

            Parameters:
            - `index` str: Takes in the which catg the value is to be fetched. Default value is `''`. The values that it excepts are:
                - `''`: ... 
                - `sme`: ... 
                - `equities`: ... 
            - `symbol` (str, optional): 
            - `issuer` (str, optional): 
            - `catg` int: This value decides what type of data is to be fetched. Default value is `0`. The values it accepts are:
                - `0`: Setting this value fetches data from `https://www.nseindia.com/api/event-calendar`. 
                - `1`: Setting this value fetches data from `https://www.nseindia.com/api/eventCalender-sme`.
            - `fetchNew` bool: Default value is `False`.

            Supports Database: Yes
        """
        data:list = []
        if fetchNew:
            if catg==0:
                params = {}
                if index=='': pass
                elif index=='equities':
                    d:str = self.get_asset_info((symbol if symbol!='' else issuer if issuer!='' else ''))
                    if len(d) >= 1:
                        params = {'index':'equities','symbol':d['symbol'],'issuer':d['name']}
                    else: raise ValueError('Asset given is doen\'t exist')
                elif index == 'sme':
                    params = {'index':'sme'}
                data = setNum(space_remover(self.requester('https://www.nseindia.com/api/event-calendar', params=params)))
            elif catg==1:
                data = self.requester('https://www.nseindia.com/api/eventCalender-sme')
            else: raise ValueError('The index value is invalid.')
        else:
            params = []
            if catg==0:
                table = 'calenderEvents'
                if symbol!='': params.append(f"symbol='{symbol}'")
                if issuer!='': params.append(f"company='{issuer}'")
            elif catg==1:
                table = 'eventsList'
            data = self.dbConn.fetch(table, query=params)
            if len(data)==0:
                data = self.getCalenderEvents(index, symbol, issuer, catg, True)
                self.dbConn.json_insert(table, data)                   

        return data

    def getFII_DII(self, date:str='', dataCatg:str='all', format:str='%d-%m-%Y', fetchNew:bool=False)->(list|bool):
        """
            getFII_DII
            ----------

            This method is to get the data FII & DII data from NSE website.

            Parameter:
            - param str date:  Date in 'dd-mm-yyyy' format. If not provided it will take today's
            - param  str dataCatg: Category of Data ('all','latest','historical')
                                all   -> Returns both latest and historical data.
                                latest-> Returns only latest data.
                                historical-> Returns only historical data.

            Returns:
            - list[dict] : List of Dictionary containing the data as per category passed.
        """
        
        def check_time(date):
            current_date = datetime.now()
            if current_date.date() > date.date(): return True
            elif date.date() == current_date.date(): return current_date.hour >= 20
            else: return False
            
        date = self.current_date.strftime(format) if date == '' else date
        dcatg, ret = ['oi','vol'], 0
        data:list = []
        if fetchNew:
            if check_time(dateFormat(date, format))==False: return False

            if dataCatg == 'all':
                for i in dcatg:
                    ret = self.getFII_DII(date, dataCatg=i, format=format, fetchNew=True)
                    data += ret if ret!=False and ret!=None else []
                return data
            elif dataCatg in dcatg:
                try:
                    dt = dateFormat(date, format,"%d%m%Y") 
                    po = self.requester(url=f'https://archives.nseindia.com/content/nsccl/fao_participant_{dataCatg}_{dt}.csv')
                    if po is not None or po is not False:
                        to = po.replace('\r', '').replace('\t','').split("\n")[1:-1]
                        headers = [i.replace(' ','_').lower() for i in to[0].split(',')]
                        date = dateFormat(date, format,"%Y-%m-%d")
                        data = setNum([{'date': date, 'catg': dataCatg, **dict(zip(headers, line.split(',')))} for line in to[1:]])
                        ret = 1
                except:
                    data = []
            else: raise ValueError('The values passed in the dataCatg parameter is invalid. Please choose either one of the oi, vol or all.')
        else:
            table = 'FII_DII'          
            query = f"DATE(date) = DATE('{date}')"
            if dataCatg != '' and dataCatg != 'all' and dataCatg in dcatg:
                query += f" AND catg='{dataCatg}'"
            else: ValueError('The value passed in dataCatg parameter is invalid.')
            
            data = self.dbConn.fetch(table, query=query)
            if len(data) == 0:
                data = self.getFII_DII(date, dataCatg, format, True)
                if data is not None and data is not False and data != []:
                    try:
                        self.dbConn.json_insert(table, data, True)
                        ret = 1 
                    except:
                        pass
        self._updateLogs('FII_DII',f'{date}_{dataCatg}', ret)
        return data

    def get_ticker(self, method:str='moneyControl', params:dict={}):
        """
            get_ticker()
            ------------
            
            This method is responsible for getting the ticker data.

            Parameter:
            - symbol str: takes the symbol or the asset name in for which the ticker is to be fetched.    
            - catg str: This parameter suggest where the data is to be fetched. Default is moneyControl. The accepted values are.
                - `local`: fetches data form the local `nseindia` site. 
                - `yahoo`: fetches data form the `yahoo finance` site.
                - `moneycontrol`: fetches data form the `MoneyControl` site.
        
            - `params` dict: Takes the parameters that are to be passed.
                - `local` : ...
                - `yahoo` : ...
                - `moneyControl` : 
                    - `asset` str: Takes the name of the asset.
                    - `resolution` int: Takes the resolution or the timeframe in which the data is to be fetched. Default is 1. The values permitted are:
                        - `1`: 1 minute timeframe   
                        - `3`: 3 minute timeframe   
                        - `5`: 5 minute timeframe   
                        - `1`: 15 minute timeframe   
                        - `1`: 30 minute timeframe   
                        - `1`: 60 minute timeframe   
                        - `D`: 1 Day timeframe   
                        - `W`: 1 Week/7 days timeframe   
                        - `M`: 1 Month/30 Days timeframe   
                        - `45`: 45 Days timeframe   
                        - `120`: 120 Days timeframe   
                        - `240`: 240 Days timeframe   
                    - `dt_from` (str, optional): This takes the date from when the data is to fetched. Default is ''.
                    - `dt_to` (str, optional): This takes the date till when the data is to fetched. Default is ''.
                    - `countback` (int, optional): This takes the amount of data that is to be fetched. Default is 300.
                    - `currencyCode`(str, optional)=This takes the currency value in which the data is to be fetched.Default is 'INR'.
                    - `dt_format` str: This prameter takes the format of the date that is passed int the method. Default is '%Y-%m-%d'.
                    - `convert_human` bool: This takes a boolen as the value and signifies wether the timestamp is to be convert into human readable format or not. Default is True.
                    - `fetchNew` bool: This takes a boolen as a value and dictates wheter the data is to be searched first or requeseted directly through the net. Default is False.
                - `dhan` : ...

        """
        data = []       
        if method == 'local':
            pass
        elif method == 'yahoo':
            pass
        elif method == 'moneyControl':
            if self.intia['moneyControl'] == None:
                self.intia['moneyControl'] = MoneyControlHandler(localRunner=False)
            data = self.intia['moneyControl'].get_ticker(**params)        
        else: raise ValueError('method mentioned is inalid.')
        return data

    def get_most_active(self, catg:str=''):
        catgList = ['etf', 'securities', 'sme', 'underlying', '52Week', 'variations', 'gainers', 'pb', 'emerge']
        if catg=='':
            data = []
            for i in catgList:
                data.append(self.get_most_active(i))
            return data
        elif catg in catgList:
            catg = '52Week-hlsummary' if catg == '52Week' else catg
            arg = 'most-active-'+ catg if catg!='variations' else 'variations'
            arg = arg if catg!='gainers' else 'volumne-gainers'
            arg = arg if catg!='pb' else 'price-band-hitter'
            arg = arg if catg!='emerge' else 'emerge'
            
            url = f'https://www.nseindia.com/api/live-analysis-{arg}' 
            return self.requester(url)
        else:
            raise ValueError('Please revaluate the values passed in the catg parameter.')

    def get_press_release(self, dt_from:str='', dt_to:str='', format='%d-%m-%Y', fetchNew:bool=False):
        """
        
        """
        if fetchNew:
            url = 'https://www.nseindia.com/api/press-release'
            dt_to = dateFormat(dt_to, format, '%d-%m-%Y') if dt_to!='' else self.current_date.strftime('%d-%m-%Y')
            dt_from = dateFormat(dt_to, format, '%d-%m-%Y') if dt_from!='' else dt_to
            data = self.requester(url, params={'fromDate':dt_from, 'toDate':dt_to})
            if data != None:
                # data = [flatten_dict(d) for d in setNum(data)]
                data = [{**d, 'date_changed':dateFormat(d['changed'], '%a, %m/%d/%Y - %H:%M', '%Y-%m-%d')} for d in data]
        else:
            form = '%Y-%m-%d'
            dt_to = dateFormat(dt_to, format, form) if dt_to!='' else self.current_date.strftime(form)
            y_date = get_previous_day(dt_to, form) if dt_from=='' else dt_from
            query = f"date_changed BETWEEN DATE('{y_date}') AND DATE('{dt_to}')"
            data = self.dbConn.fetch('pressrelease',query=query)
            if data == []:
                data = self.get_press_release(y_date, dt_to, form, True)
                self.dbConn.json_insert('pressrelease', data)
        return data

    def getFinancial(self, index:str='', period:str='Quarterly', asset:str='', fetchNew:bool=False):
        """
            getFinancial()
            --------------
            Gets the financial data for a particular stock or all stocks from NSE.
        
            Parameters:
            - `index` str: 
            - `period` str: 
            - `asset` str: 
            - `fetchNew` bool: 
        """
        data:list = []
        indexList = ['equities', 'sme', 'debt', 'insurance']
        periodList = ['Quarterly', 'Annual']
        if period not in periodList: raise ValueError('Period provided is invalid.')

        if fetchNew:
            if index=='':
                for ind in indexList:
                    data += self.getFinancial(ind, period, asset, True)
            elif index in indexList:
                param:dict = {'index':index,'period':period} 
                if asset!='':param['asset']=asset
                data = self.requester('https://www.nseindia.com/api/corporates-financial-results', params=param)
                if data is not None and data is not False and len(data) > 0:
                    quater = ['First Quarter','Second Quarter', 'Third Quarter', 'Fourth Quarter']
                    data = [{**d, 'indexType':index, 'filingDate': dateFormat(d['filingDate'], '%d-%b-%Y %H:%M','%Y-%m-%d %H:%M'), 'fromDate':dateFormat(d['fromDate'], '%d-%b-%Y', '%Y-%m-%d'), 'toDate': dateFormat(d['toDate'], '%d-%b-%Y', '%Y-%m-%d'), 'exchdisstime':dateFormat(d['exchdisstime'], '%d-%b-%Y %H:%M','%Y-%m-%d %H:%M'),'broadCastDate':dateFormat(d['broadCastDate'], '%d-%b-%Y %H:%M:%S','%Y-%m-%d %H:%M:%S')} for d in data]
        else:
            params = []
            data = self.dbConn.fetch('fianace_data', query=params)
            if len(data) == 0:
                data = self.getFinancial(index, period, asset, True)
                self.dbConn.json_insert('fianace_data', data)
        return data
        
    def shareHolderPattern(self, index:str='', asset:str ='', fetchNew:bool=False):
        """
            shareHolderPattern()
            --------------------
            Fetches Share Holder Pattern data from nse. 

            Parameter:
            - `index` str: This takes the type of index.The values of accepted are:
                - `equities`:...
                - `sme`:...
        """
        data:list = []
        indexList = ['equities', 'sme']
        if fetchNew:
            if index=='':
                for ind in indexList:
                    data += self.shareHolderPattern(ind, asset, True)
            elif index in indexList:
                params:dict = {'index':index,'asset':(asset if asset != '' else None)}
                data = self.requester('https://www.nseindia.com/api/corporate-share-holdings-master', params=params)
                if data is not  None and data is not False and len(data) > 0:
                    data = [{**i, 'date':dateFormat(i['date'], '%d-%b-%Y', '%Y-%m-%d'), 'submissionDate':dateFormat(i['submissionDate'], '%d-%b-%Y', '%Y-%m-%d'), 'broadcastDate':dateFormat(i['broadcastDate'], '%d-%b-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'),'systemDate':dateFormat(i['systemDate'], '%d-%b-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'),'indexType':index} for i in data]
        else:
            query = [(f"indexType'{index}'" if index=='' else ''), ("symbol='{asset}'" if asset=='' else '')]
            data = self.dbConn.fetch('shareHolderPattern', query=query)
            if len(data) == 0:
                data = self.shareHolderPattern(index, asset, True)
                self.dbConn.json_insert('shareHolderPattern', data)
        return data
        
    def creditInfo(self, isin:str='', issuer:str='', from_date:str='', to_date:str='', dt_format:str='', fetchNew:bool=False):
        """
            creditInfo()
            ------------
        """
        data:list = []
        if fetchNew:
            params = {'issuer':(issuer if issuer!='' else None),'isin':(isin if isin!='' else None),'from_date':(dateFormat(from_date, dt_format, '%d-%m-%Y') if from_date!='' else None),'to_date':(dateFormat(to_date, dt_format, '%d-%m-%Y') if to_date!='' else None)}
            data = self.requester('https://www.nseindia.com/api/corporate-credit-rating', params=params) 
            if data is not None and data is not False and len(data) > 0:
                data = [{**i, 'DateofCR': dateFormat(i['DateofCR'], '%d-%m-%Y', '%Y-%m-%d'),'BroadcastDateTime': dateFormat(i['BroadcastDateTime'], '%d-%b-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'),'TimeStamp': dateFormat(i['TimeStamp'], '%d-%b-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'),'DateOfVer':dateFormat(i['DateOfVer'], '%d-%m-%Y', '%Y-%m-%d'),'DateOfCREarlier': dateFormat(i['DateOfCREarlier'], '%d-%m-%Y', '%Y-%m-%d'),'TemDate':dateFormat(i['TemDate'], '%d-%m-%Y', '%Y-%m-%d')} for i in data]
        else:
            query = [(f"CompanyName='{issuer}'" if issuer!='' else ''),(f"ISIN='{isin}'" if isin!='' else '')]
            data = self.dbConn.fetch('creditRating', query=query)
            if len(data) == 0:
                data = self.creditInfo(isin, issuer, from_date, to_date, dt_format, True)
                self.dbConn.json_insert('creditRating', data)
        return data

    # Background data fetching functions. #
    def monthly_active_securities(self):
        ret = 0
        url = 'https://www.nseindia.com/api/historical/most-active-securities-monthly?json=true'
        if (data:=self.requester(url))!=None:
            data = valreplace(data['data'], '_id', '_keyid', True)
            ret = 1
        self._updateLogs('Most Active Secutities', '', ret)
        return data

    def getAdvDec(self, year:int=0, month:int=0, fetchNew:bool=False):
        """
            Retrives the advances and declines data from the web.
        """
        if fetchNew:
            url = 'https://www.nseindia.com/api/historical/advances-decline-monthly'
            if year==0:
                param = {'year':self.current_date.strftime('%Y')} 
            elif 2000 <= year <= int(self.current_date.strftime('%Y')):
                param = {'year':year} 
            else:
                raise ValueError('The year passed is invalid and can be less than 2000 and greater than the current year.')
            
            if month!=0: 
                if 0 < month < 13:
                    url = 'https://www.nseindia.com/api/historical/advances-decline-daily'
                    param = {'timeframe':f'{month}-{year}'} 
                else:
                    raise ValueError('The month passed is invalid and can be less than 1 and greater than the current month.')
            data = self.requester(url, param)
        else:
            query = ''
        if data!=None:
            ret = 1 
            data = valreplace(data['data'], '_id', '_keyid', True)
        else: 
            ret = 0
        self._updateLogs('getAdvDec', str(param.keys()[0]), ret)    
        return data
 
    def get_holidays(self, ty:str='', date:str='', dt_format:str='%d-%m-%Y', fetchNew:bool=False):
        '''
            This method is for the use for fetching the holiday list from the NSE website.
            @param string ty: This parameter ty None by default. It can only take the values clering an trading as its parameter or else the method will return a runttime error.
            @return list: This returns the data in a list format.
        '''
        catg = ['clearing', 'trading']
        if fetchNew:
            if ty in catg:
                data = self.requester(url='https://www.nseindia.com/api/holiday-master', params={'type':ty}, catg=2)
                if data!=None and data!=False:
                    holidays = [{**v2,'catg':k,'type':ty,'description': str(v2['description']).replace("\r", '').strip(),'tradingDate': dateFormat(v2['tradingDate'], '%d-%b-%Y', '%Y-%m-%d')} for k,v in data.items() for v2 in v]
                return holidays
            elif ty =='':
                data =[]
                for i in catg:
                    data += self.get_holidays(ty=i, date=date, fetchNew=True) 
            else: raise ValueError('The values passed in the parameter ty is not valid. It can only be clering & trading.')
        else:
            table = 'holidays'
            query = [(f"type='{ty}'" if ty in catg else '')]
            if date!='':
                nd = dateFormat(date, dt_format,'%Y-%m-%d')
                query.append(f"DATE(tradingDate)=DATE('{nd}')")
            data = self.dbConn.fetch(table, query=query)
            if len(data) == 0:
                data = self.get_holidays(ty=ty, date=date, dt_format=dt_format, fetchNew=True)
                self.dbConn.json_insert(table, data, True)
            self._updateLogs(table, '', 1)
        return data
   
    # analysis and report generating functions
    def analys_FII_DII(self, date:str='', catg:str='oi', client_type:str='', tType:str='',asset_type:str='',call_type:str='',position_type:str='', limit:int=5, dt_format:str='%d-%m-%Y'):
        """
            analys_FII_DII()
            ----------------

            Tis method is responsible for analysing FII and DII data.

            Parameter:
            - date (str,optional): This method takes date and analysis FII & DII data of the specified day.
        """
        
        client_types:list = self.dbConn.fetch_unique('FII_DII', 'client_type') 
        data:dict = {'data':{}}
        for cli in client_types:
            if cli!='TOTAL':
                query = [(f"catg='{catg}'" if catg!='' else '' ),(f"client_type='{cli}'" if cli !='' else '')]
                catg = self.dbConn.fetch('FII_DII', query=f"catg='oi' AND client_type='{cli}'", limit=limit, desc='date')[::-1]
                not_req = ['id', 'date', 'catg', 'client_type', 'total_long_contracts', 'total_short_contracts']
                data_keys = catg[0].keys()
                co = {dt['date']:{k: (catg[i][k] - catg[i-1][k]) for k in data_keys if k not in not_req} for i, dt in enumerate(catg[1:])}  
                ko = [{'date': k,'future_index': (v['future_index_long'] - v['future_index_short']),'future_stock': (v['future_stock_long'] - v['future_stock_short']),'option_index_call': (v['option_index_call_long'] - v['option_index_call_short']),'option_index_put': (v['option_index_put_long'] - v['option_index_put_short']),'option_stock_call': (v['option_stock_call_long'] - v['option_stock_call_long']),'option_stock_put': (v['option_stock_put_long'] - v['option_stock_put_long'])} for k,v in co.items()]
                data[cli] = [{**e, 'avg': (e['future_index'] + e['future_stock'] + e['option_index_call'] + e['option_index_put'] + e['option_stock_call'] + e['option_stock_put']) / 6} for e in ko]
                data['data'][cli] = catg 
        return data

    def analyse_Financial(self, asset:str):
        """
            analyse_Financial()
            -------------------
            Thsi method is to analyse analitical report of quaterly data of assets.
        """
        data = self._get_crop_info(asset, 'financialResult')
        
        return data

    # Other Information Fetching functins #
    def _get_crop_info(self, symbol:str, corptype:str='', market:str='equities')->(list|dict):
        """
            _get_corp_info():
            -----------------
            This method is to fetch corporate inforamtion/ other stock related data from the website.

            Parameter:
            - `symbol` str: Tkaes the name of the asset that is to fetched.
            - `corptype` str: Takes the category for what the data is to be fetched. Accepted values are:
                - `votingresults`: For getting the votes results of directors.
                - `announcement`: for any announcementsmade by the company.
                - `shp`: 
                - `secretorial`:
                - `sast`:
                - `promoterenc`:
                - `insidertrading`: For any insider trading.
                - `investorcomplaints`: For investor complaints against the companies.
                - `financialResult`: Retruns the quaterly financial data results of the company.
                - `dailybuyback`:
                - `corpInfo`:
                - `goveranance`:
                - `compdir`:
                - `corpactions`:
                - `boardmeeting`:
                - `annualreport`: For getting the annul report.
                - `financialresultcompare`: 
            - `market` str: Takes the cateory/class of data that is to be fetched. Default value is `equities` and accepted values are:
                - `equities`:
                - `debt`:
                - `cm`:

        """
        corpTypeList = ['votingresults', 'announcement', 'shp','secretorial','sast','promoterenc','insidertrading','investorcomplaints','financialResult','dailybuyback','corpInfo','goveranance','compdir','corpactions','boardmeeting','annualreport', 'financialresultcompare']
        marketList = ['equities', 'debt', 'cm']
        if market not in marketList: raise ValueError('The market Category given is invaid.')
        if corptype != '':
            if corptype not in corpTypeList: raise ValueError('The Corpeorate Type Category given is invaid.')
            market= 'debt' if corptype=='compdir' else 'cm' if corptype=='annualreport' else 'equities'
            params = {'symbol':symbol, 'corpType':corptype,'market':market}
            data = self.requester('https://www.nseindia.com/api/corp-info', params=params)
            
            if corptype=='financialresultcompare':
                text = data['financialresultcompare']['resCmpData'][0]['re_desc_note_fin']
                financial_data = {}
                ratios = re.findall(r'(\w+)\) (.+?)\s+(\d+\.\d+)\s+(\d+\.\d+)', text)
                financial_data['ratios'] = [{'description': ratio[1], 'quarter': ratio[2], 'nine_months': ratio[3]} for ratio in ratios]
                if net_worth := re.search(r'Net Worth as at (\d+\w+ \w+ \d+) is Rs\. (\d+(?:,\d+)*) Crore', text):
                    financial_data['net_worth'] = {'date': net_worth.group(1), 'value': net_worth.group(2)}
                if capital_reserve := re.search(r'Outstanding Capital Redemption Reserve/Debenture Redemption Reserve as at (\d+\w+ \w+ \d+) is Rs\. (\d+(?:,\d+)*) Crore', text):
                    financial_data['cap_reserve'] = {'date': capital_reserve.group(1), 'value': capital_reserve.group(2)}
                month_dict = {'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06','July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11','December': '12'}
                for key in financial_data:
                    if 'date' in financial_data[key]:
                        day, month, year = re.search(r'(\d+)(\w+) (\w+) (\d+)', financial_data[key]['date']).group(1, 2, 3, 4)
                        financial_data[key]['date'] = f"{year}-{month_dict[month]}-{day}"
                data['financialresultcompare']['resCmpData'][0]['re_desc_note_fin'] = financial_data
        
        else:
            data = {i: self._get_crop_info(symbol, i, market) for i in corpTypeList}
        
        return data

    def getSecurities(self, symbol:str, dt_from:str, dt_to:str, dataType:str='priceVolumeDeliverable', series:str='ALL', format:str='%Y-%m-%d', fetchNew:bool=False):
        """
            getSecurities()
            ---------------

            This method of the class is responsible for getting the securities.

            Parameter:
            - symbol (str): Takes the name of the symbol.
            - dt_from (str): Takes the date from which the data is to be fetched.
            - dt_to (str): Takes the date to which the data is to be fetched.
            - dataType (str): Takes the type or the category of data is to fetched.
            - series (str): Takes the values what needs to be fetched.
            - format (str, optional): This paramerter takes the date format that passed. Default value is `'%Y-%m-%d'`.
            - fetchNew (bool): This parameter takes boolen i.e. True or False. If given True then it will fetch the data from the web based on the given parameters, but if the given False then the search will first go through
        """
        fetc = self.dbConn.fetch('listings', query=f"symbol LIKE '{symbol}' OR identifier LIKE '{symbol}' OR companyName LIKE '{symbol}'")
        symbol = fetc['symbol'] if len(fetc) > 0 else symbol.upper()
        ret = 0
        data = None
        if dataType not in ['priceVolume', 'priceVolumeDeliverable']: raise ValueError('datatype passed is invalid. Only excepts priceVolume,  priceVolumeDeliverable.')

        if fetchNew:
            param = {'from':dt_from,'to':dt_to,'symbol': symbol,'dataType':dataType,'series': series}
            data = self.requester('https://www.nseindia.com/api/historical/securityArchives', params=param)
            if data!=None:
                data = data['data']
                ret = 1
        else:
            data = self.dbConn.fetch('securityArchives', query=f"CH_SYMBOL='{symbol}' AND CH_TIMESTAMP BETWEEN DATE('{dt_from}') to DATE('{dt_to}')")
            if len(data) == 0:
                data = self.getSecurities(symbol=symbol, dt_from=dt_from, dt_to=dt_to, dataType=dataType, format=format, fetchNew=True)
                for d in data:
                    self.dbConn.json_insert('securityArchives', d, True)
            ret=1
        self._updateLogs('securityArchives', symbol, ret)
        return data

    def get_merged_daily_reports(self, catg:str='')->dict:
        """
            get_merged_daily_reports()
            --------------------------
            The method is responsible for getiing the daily reports.

            Prameter:
            - catg (str,optional): This prametre takes what category the data is to be fetched. Default is '' , and with this , all th available categories will be fetched.
                The available categories are:
                - `favDerivatives`:
                - `favDebt` :
                - `favCapital` :
            
            Returns:
            - dict

        """
        catgList = ['', 'favDerivatives', 'favDebt', 'favCapital']
        return self.requester('https://www.nseindia.com/api/merged-daily-reports', params={'key':catg}) if catg!='' and catg in catgList else {i:self.get_merged_daily_reports(i) for i in catgList if i!=''}

    def getCircular(self, catg:str=''):
        """
        
        """
        catgList = ['latest', 'latest-circular']
        if catg=='':
            return {k:self.getCircular(k) for k in catgList}
        elif catg in catgList:
            return self.requester(f'https://www.nseindia.com/api/'+catg)
        else: raise ValueError('')
        
    def get_52W_highLow(self, date:str='', format:str='%d-%m-%Y', fetchNew:bool=False):
        """
            get_52W_highLow()
            -----------------
            Gets 52 Week high and low of the assets.
        """
        data = []
        if fetchNew:
            date = self.current_date.strftime('%d%m%Y') if date =='' else dateFormat(date, format, '%d%m%Y')    
            k = self.requester(f'https://nsearchives.nseindia.com/content/CM_52_wk_High_low_{date}.csv')
            if k is not None and k is not False and k != '':
                data = [[n.strip() for n in i.split(',')] for i in k.strip().replace('"', '').split('\n')[2:]]
                data = setNum(dlist_dict(list_dlist(data[1:], valreplace(valreplace(data[0], 'Adjusted ',''), '52_', ''))))
        else:
            d1 = dateFormat(date, format,'%d-%m-%Y') if date!='' else self.current_date.strftime('%d-%m-%Y')
            data = self.dbConn.fetch('WeeklyHighLow', query=f"Week_High_Date={d1} OR Week_Low_DT={d1}")
            if data==[]:
                data = self.get_52W_highLow(d1, '%d-%m-%Y', True)
                self.dbConn.json_insert('WeeklyHighLow', data)
            self._updateLogs(f'WeeklyHighLow',d1,1)
        return data

    def get_ipo(self):
        pass

    def getGainersLossers(self, catg:str='all', fetchNew:bool=False):
        """
            getGainersLossers()
            -------------------

            The method fetches the data for current days gainers and lossers.

            Parameter:
            - catg (str): This parameter takes the argument what to fetch. It accepts:
                - `gainers`: Passing this value will fetch the gainers.
                - `loossers`: Passing this value will fetch the lossers.
                - `all`: Passing this value will fetch both gainers & lossers.
        """
        catgList = ['gainers', 'lossers']
        data = None
        if fetchNew:
            if catg=='all':
                data = [self.getGainersLossers(i, True) for i in catgList]
            elif catg in catgList:
                url = f'https://www.nseindia.com/api/liveanalysis/{catg}/allSec'
                data = self.requester(url)
                if catg == 'gainers':
                    data = data['gainers']['data']
            else: raise ValueError('The argumuent passed in the parameter catg is not a valid.')
        else:
            pass
        self._updateLogs('getGainersLossers', catg, 1)
        return data
      
    # Option Chain
    def get_optionChain(self, asset:str, optionType:str='', expireDate:str='', strikPrice:int|float=0, entryDate:str='', opttype:str='indices', format:str='%d-%m-%Y', fetchNew:bool=False, newInsert:bool=False):
        """
            get_optionChain()
            -----------------
            This method is for fetching the option chain data from the NSE.

            Paramters:
            - asset str: Name of the asset that's available in derivatives like NIFTY, BANKNIFTY, RELIANCE, etc.
            - optionType (str, optional): The type of option i.e. CE or PE.
            - expireDate (str, optional): The date when the option will expire.
            - strikPrice (int,float, optional): The strike price of the asset. Default is 0 meaning all the available data.
        """
        ret = 0
        if optionType not in ['', 'CE', "PE"]:
            logging.error('Option Chain Data Fetching ERROR - Invalid data({}) provided'.format(optionType))
            raise ValueError('Invalid Data passed in optionType({}) parameter. Only accepted values are CE and PE'.format(optionType))

        if fetchNew and newInsert==False:
            reto = self._get_options(asset, opttype)
            if reto!=None and reto is not False: 
                chain = [{**v, 'catg':k, 'opttype':opttype, 'expiryDate':dateFormat(v['expiryDate'], "%d-%b-%Y", '%Y-%m-%d') } for d in reto['records']['data'] for k,v in d.items() if k=='PE' or k=='CE']
                data = chain
                ret = 1
        elif fetchNew and newInsert:
            query = [f"underlying='{asset}'", (f"catg='{optionType}'" if optionType!='' else ''), ("expiryDate='{}'".format(dateFormat(expireDate, format,'%Y-%m-%d')) if expireDate!='' else ''), (f"strikePrice='{strikPrice}'" if strikPrice > 0 else '')]
            self.dbConn.json_insert('optionChain', self.get_optionChain(asset=asset, opttype=opttype, format=format, fetchNew=True))
            data = self.dbConn.fetch('optionChain', query=query)
            ret = 1
        else:
            query = [f"underlying='{asset}'", (f"catg='{optionType}'" if optionType!='' else ''), ("expiryDate='{}'".format(dateFormat(expireDate, format,'%Y-%m-%d')) if expireDate!='' else ''), (f"strikePrice='{strikPrice}'" if strikPrice > 0 else '')]
            data = self.dbConn.fetch('optionChain', query=query)
            if len(data) == 0:
                self.dbConn.json_insert('optionChain', self.get_optionChain(asset=asset, opttype=opttype, format=format, fetchNew=True))
                data = self.dbConn.fetch('optionChain', query=query)
                ret = 1

        self._updateLogs('optionChain',f'{asset}_{optionType}_{expireDate}_{opttype}_{format}', ret)
        return data

    def getOptionExpiry(self, asset:str, assetType:str='indices',fetchNew=True)->list[str]:
        """
            getOptionExpiry()
            -----------------
            Gets the options expiry of the given asset.
        """
        if fetchNew:
            data = self._get_options(asset, assetType)
            return [dateFormat(i, '%d-%b-%Y', '%Y-%m-%d') for i in data['records']['expiryDates']]
        else:
            pass

    def getOptionPrices(self, asset, assetType:str='indices'):
        return setNum(self._get_options(asset, assetType)['records']['strikePrices'])

    def getOptionContracts(self, asset:str, identifier:str=''):
        pass

    def _get_options(self, asset:str, opttype:str='indices', ignore:bool=False):
        """
            _get_options()
            --------------
            Gets the options data from the website.
            
            Prarameter:
                - asset (str) : This prameter take the name of the asset who's data is to fetched.
                - ignore (bool, optional): This parameter if given true will ignore if the asset given is present or not in the listings table. Default is False, meaning first will validate if the asset exists before prociding. 
            Return:
                - dict: the data is returned as a dict.
        """
        ret = 0
        data = []
        if opttype in ['indices', 'equities']:
            url = 'https://www.nseindia.com/api/option-chain-' + opttype
            data = self.requester(url, params={'symbol':asset}, refer='https://www.nseindia.com/option-chain')
            res = 1 if data != [] or data != None or data != False else 0
        self._updateLogs('Get_Options', asset, res)
        return data
        
    # Deals
    def getDeals(self, catg:str, date:str, format:str='%d-%m-%Y', no_days:int=1, fetchNew:bool=False)->list:
        """
            getDeals()
            ----------

            The method is responsible for getting the list of the deals from the nse website.

            Parameters:
            - catg str: Takes what kinf of deal that is to be fetched. Parameters are:
                - short-selling or short
                - bulk-deals or bulk 
                - block-deals or block
            - dt_to str: Takes the endding date in the %d-%m-%Y format.
            - fethNew str: This prameter dictates wether it will fetch data new altogeter or first go through the database. Default is false.
        
        """
        if fetchNew:
            
            dt_to = date if format=='%d-%m-%Y' else dateFormat(date, format, '%d-%m-%Y')
            di = {'short': 'short-selling', 'bulk':'bulk-deals', 'block': 'block-deals'}
            url = 'https://www.nseindia.com/api/historical/'
            if catg in di.keys():
                url += di[catg]
            elif catg in di.values():
                url += catg
            else: raise ValueError('The cvalue passed in the catg parameter is invalid. please re-check the value passed in the catg parameter in the funtion.This function catg paramerter only takes on of three values, short-shelling, bulk and block.')

            data = self.requester(url, params={'from':get_previous_day(dt_to, '%d-%m-%Y', numberof_days=no_days),'to':dt_to})
            if data is not None or data is not False or data != []:
                data = [{**i,'mTIMESTAMP': dateFormat(i['mTIMESTAMP'], '%d-%b-%Y', '%Y-%m-%d')} for i in space_remover(valreplace(data['data'], '_id', 'keyId', True))]
        else:
            if catg == 'short' or catg == 'short-selling':
                table = 'sortSelling'
            elif catg == 'bulk' or catg == 'bulk-deals':
                table = 'bulk_deals'
            elif catg == 'block' or catg == 'block-deals':
                table = 'block_deals'

            dt_to:str = date if format=='%Y-%m-%d' else dateFormat(date, format, '%Y-%m-%d')
            data = self.dbConn.fetch(table, query=f"DATE(mTIMESTAMP)=DATE('{dt_to}')")
            if len(data) == 0:
                data = self.getDeals(catg=catg, dt_to=dt_to, dt_format='%Y-%m-%d', fetchNew=True)
                if data is not None and data is not False and len(data) > 0:
                    self.dbConn.json_insert(table, data)
        ret = 1 if data is not None and data is not False and len(data) > 0 else 0
        self._updateLogs('deals', f'{catg}_{dt_to}', ret)
        return data

    def getInsider(self, dt_from:str='', dt_to:str='', symbol:str='', format:str='%d-%m-%Y', indexes:str='equities', fetchNew:bool=False):
        """
            getInsider()
            ------------
            This method fetches the insider data from the nse website.

            Parameters:
            - dt_from str: Takes the begining date in the %d-%m-%Y fromat.
            - dt_to str: Takes the endding date in the %d-%m-%Y fromat.
            - indexes str: Takes the parameters of which the data is to be fetched. 
                The parameter it excepts are:
                - equities: The default value.
            
            Rreturns:
            - list: This method returns a list of insider trades.
        """
        idx = ['equities']
        ret = 0
        if indexes not in idx: raise ValueError('The value passed in the indexs param is invalid.')
        if fetchNew:
            dt_to = self.current_date.strftime('%d-%m-%Y') if dt_to=='' else dateFormat(dt_to, format, '%d-%m-%Y')
            dt_from = get_previous_day(dt_to, '%d-%m-%Y') if dt_from=='' else dateFormat(dt_from, format, '%d-%m-%Y')
            params= {'index':indexes,'from_date':dt_from,'to_date':dt_to}
            reto = self.requester('https://www.nseindia.com/api/corporates-pit', params=params)
            if reto is not None and reto is not False and len(reto['data']) > 0:
                data = [{**i,'dateTime': i['date'],'date': dateFormat(i['date'], '%d-%b-%Y %H:%M', '%Y-%m-%d'),'acqfromDt':dateFormat(i['acqfromDt'], '%d-%b-%Y', '%Y-%m-%d'),'acqtoDt': dateFormat(i['acqtoDt'], '%d-%b-%Y', '%Y-%m-%d'),'intimDt': dateFormat(i['intimDt'], '%d-%b-%Y', '%Y-%m-%d'),'tkdAcqm': (i['tkdAcqm'] if i['tkdAcqm']!=None else '-')} for i in setNum(reto['data'])]
                ret = 1    
        else:
            useFormat = '%Y-%m-%d'
            if dt_to!='' and dt_from!='':
                dt_to = dateFormat(dt_to, format, useFormat)
                dt_from = dateFormat(dt_from, format, useFormat)
                query = f"date BETWEEN '{dt_from}' AND '{dt_to}'"
            elif (dt_to!='' and dt_from=='') or (dt_to=='' and dt_from!=''):
                if dt_from!='':
                    dt_to = dt_from
                query = f"DATE(date) <= DATE('{dt_to}')"

            data = self.dbConn.fetch('insider', query=query)
            if len(data)==0:
                data = self.getInsider(dt_to=dt_to, format=useFormat, indexes=indexes, fetchNew=True)
                self.dbConn.json_insert('insider', data)
        self._updateLogs('insider', f'{indexes}_{dt_to}', ret)
        return data

    # Bhavcopy 
    def _get_secBhavdata(self, date:str='', series:str='', format:str='%d-%m-%Y', fetchNew:bool=False, processsed:bool=True)->(str|list[dict[str,float]]):
        """
            get_secBhavdata()
            -----------------

            Gets the BhavCopy from the nse website.
            A bhavcopy is a file showing a days market inforamtion like movment of stock and more.
        """
        
        ret = 0
        data = []
        if fetchNew:
            date = self.current_date.strftime('%d%m%Y') if date == '' else datetime.strptime(date,format).strftime('%d%m%Y')
            k = self.requester(f'https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv')
            if k is not False and k is not None and k!='':
                k = k.replace('DATE1', 'date').replace(' -', '0')
                data = space_remover([{**i, 'date': dateFormat(i['date'], "%d-%b-%Y","%Y-%m-%d")} for i in equalizer_dict(setNum(_handle_csv(k)))]) if processsed else k               
                ret = 1
        else:
            dn = date if format == '%Y-%m-%d' else dateFormat(date, format,'%Y-%m-%d')
            query:list = [f"DATE(date)=DATE('{dn}')", (f" AND SERIES='{series}' OR SERIES LIKE '{series}'" if series!='' else '')]
            data = self.dbConn.fetch(table:='Bhavcopy', query=query)
            if len(data) == 0:
                k = self._get_secBhavdata(date=date, series=series, format=format, fetchNew=True)
                if len(k) > 0: 
                    self.dbConn.json_insert(table, k, True)
                    data = self.dbConn.fetch(table, query=query)
            ret = 1
        self._updateLogs('sec_bhavdata', date, ret)
        return data
    
    def _get_fno_bhavdata(self, date:str='', series:str='', symbol:str='', optType:str='', expiryDate:str='', strikPrice:int=0, format:str='%d-%m-%Y', limit:int=0, fetchNew:bool=False, processsed:bool=True):
        """
            _get_fno_bhavdata():
            -------------------

            Parameters:
            - date (str, optional): The date whose data is to be fetched.
            - series (str, optional): The type of data that is to be fetched. Values accepted are:
                - `OPTIDX`: For option index like `NIFTY`, `BANKNIFTY`, etc.
            - `symbol` (str, optional): The name of the symbol.
            - `optType` (str, optional): The type of 
        """
        if optType not in ['','CE', 'PE', 'ce', 'pe']: raise ValueError(f"Variable {optType} not accepted. The accepted values are CE & PE.")
        chg, cgto = '%d-%b-%Y', '%Y-%m-%d'
        if fetchNew:
            date:str = self.current_date.strftime('%d%m%Y') if date=='' else dateFormat(date, format, '%d%m%Y') #type: ignore
            reto = self.requester(f'https://nsearchives.nseindia.com/content/fo/NSE_FO_bhavcopy_{date}.csv')
            if reto is not None and reto is not False and reto!='':
                print('Process')
                data = setNum([{**d, 'date':dateFormat(date, '%d%m%Y', cgto), 'RptgDt': dateFormat(d['RptgDt'], chg, cgto), 'XpryDt': dateFormat(d['XpryDt'], chg, cgto)} for d in _handle_csv(reto)]) if processsed else reto  #type:ignore
                print('Process done')
            del reto
        else:
            data = self.current_date.strftime(cgto) if date=='' else dateFormat(date, format, cgto)
            expiryDate1,expiryDate2 = dateFormat(expiryDate, format, chg), dateFormat(expiryDate, format, cgto)
            table = 'FnO_Bhavcopy'
            query = [(f"TckrSymb='{symbol}'" if symbol!='' else ''), f"date='{date}'", (f"FinInstrmNm='{series}'" if series!='' else ''), (f"XpryDt='{expiryDate1}' OR XpryDt='{expiryDate2}'" if expiryDate!='' else ''), (f"OptnTp='{optType.upper()}'" if optType!='' else '')]
            print(query)
            data:list=self.dbConn.fetch(table, query=query, useIndex='FnO_Bhavcopy_idx', limit=limit)
            if len(data) == 0:
                reto = self._get_fno_bhavdata(date=date, series=series, symbol=symbol, optType=optType, expiryDate=expiryDate, format=cgto, fetchNew=True)
                self.dbConn.json_insert(table, reto)
                data = self.dbConn.fetch(table, query=query, useIndex='FnO_Bhavcopy_idx')
        return data
        
    def getBhav(self, date:str='', bhavType:str='', dt_format:str='%d-%m-%Y', fetchNew:bool=False, **karg):
        """
            getBhav()
            ---------
            This method is responsible for returning the bhavcopy data from the nse website.
        """
        if bhavType == '': return {k: self.getBhav(date=date, bhavType=k, dt_format=dt_format, fetchNew=fetchNew, **karg) for k in ['sec', 'fno']}
        elif bhavType=='sec': return self._get_secBhavdata(date=date, format=dt_format, fetchNew=fetchNew, **karg)
        elif bhavType == 'fno': return self._get_fno_bhavdata(date=date, format=dt_format, fetchNew=fetchNew, **karg)
        else: raise ValueError('Invalid Value passed in bhavType.')

    # fetching essential data.

    def get_index_stats(self, fetchdate:str='', symbol:str='', dateFormat:str='%d-%m-%Y', fetchNew:bool=False):
        """
            get_index()
            -----------
            This method fetches index details of a particular index or all indices on the NSE website.

            Parameters:
                - `fetchdate` str:  Date till which you want to fetch the data.
                - `symbol` str: Name of the index of which the data is to be fetched.
                - `dateFormat` str: Date format in which the date is passed. Default is '%d-%m-%Y'.
                - `fetchNew bool`: If true, will fetch the data from the web & false, then will fetch the data from the data stored in database. Default is `False`. 
        """
        if fetchNew:
            data = self.requester('https://www.nseindia.com/api/allIndices')
            if data!=None or data!=False:
                data = [dict_filter(d, ['chart365dPath', 'chartTodayPath', 'chart30dPath', 'chartTodayPath'], 0) for d in data['data']]
        else:
            query = []
            datee = datetime.strptime(fetchdate,dateFormat) if fetchdate!='' else self.current_date
            query.append("DATE(timestamp)='{}'".format(datee.strftime('%Y-%m-%d')))

            if symbol!='':
                query.append(f"_index='{symbol}' OR _index LIKE '{symbol}' OR indexSymbol='{symbol}' OR indexSymbol LIKE '{symbol}'")
            query = ' AND '.join(query)
            print(query)
            data = self.dbConn.fetch('indexData', query=query)
            if data == []:
                data = self.get_index(fetchNew=True)
                self.dbConn.json_insert('indexData', data)
        self._updateLogs('indexData', f'{symbol}_{fetchdate}',1)
        return data

    def _get_info(self, asset:str, catg:str='', tradeInfo:bool=False):
        """
            _get_info()
            -----------
            Fetches info in various forms.
            
        """
        catg = self._checkAssetType(asset) if catg=='' else catg
        k = 'symbol'
        if catg == 'stock' or catg == 'eq':
            path = 'quote-equity'
        elif catg == 'index' or catg == 'der':
            path = 'quote-derivative'
        elif catg == 'slb':
            path = 'quote-slb'
            k = 'index'
        return setNum(space_remover(self.requester('https://www.nseindia.com/api/' + path, params={k:asset, 'section':('trade_info' if tradeInfo else None)})))

    def _checkAssetType(self, asset:str):
        """
            _checkAssetType()
            -----------------
            This method checks whether the asset is an index or a stock.
        """
        data = self.dbConn.fetch('listings', 'assetType', f"symbol='{asset}' OR identifier='{asset}' OR name='{asset}'", fetchAll=False)
        if len(data) == 0: return 
        return data['assetType']
            
    #  Helper functions #    
    def requester(self, url:str, method:str='get', params:dict={}, refer:str='', header:dict={}, timeout:int=60, catg:int=2, decode:bool=True, stat_code:int=200)->(str|dict|list|None):
        """
            A method that returns an object of Requester Class for making HTTP requests.
        
            Parameters:
            
        """

        if header == {}:
            header = self.header
        para_dicts = {'url': url, 'method':method,'ref': refer,'params': params,'header': header,'timeout': timeout}
        ret = None
        if catg==1:
            ret = self.request.request(**para_dicts)
        elif catg==2:
            para_dicts = {**para_dicts, 'pre_request':'https://www.nseindia.com'} if self.sessions==None else {**para_dicts, 'sessions':self.sessions}
            ret, self.sessions = self.request.requestSessions(**para_dicts)
        stat = ret.status_code

        if ret!=None and stat==stat_code:
            res = 1
            if decode:
                try: 
                    ret = ret.json()
                except:
                    try:
                        ret = ret.text
                    except:
                        ret = None
        else:
            res = 0 
            ret = None

        self._updateLogs('Requester', url, res)
        return ret
  
    def _updateLogs(self, name, value:str, success:int) -> None:
        """
            updateLogs
            ----------
            This method when fired will log the events that have occured or tasks executed by the class.
        """
        if self.writeLogs:
            self.dbConn.insert('stocklogs', ['name', 'value', 'result'], [name, value, success])

    def create_db_backup(self, type:str='json')->None:
        """
            get_db_backup
            -------------
            This function is used to backup the database file from the current session.

            Parameter:
            - type str: This takese the tyoe of backup that is to be made.
                        Values it accepts are:
                        - json: This value will direct the method to make the backup in json.
                        - excel:  This value will direct the method to make the backup in xslx.
        """     
        if self.dbConn:
            self.dbConn.export_data(type)
            
    def _generate_bar_chart(data, file_path="", title="", x_label="", y_label="", set_xticklabels:list=[], legend_names=None, mode='group'):
        """
            Generate a bar chart using Plotly.

            Parameters:
                data (dict[list]): Dictionary containing data to be plotted. Keys represent the categories,
                                and values are lists of data points.
                file_path (str): File path to save the chart image. If not provided, the chart will not be saved.
                title (str): Title of the chart.
                x_label (str): Label for the x-axis.
                y_label (str): Label for the y-axis.
                legend_names (list[str]): List of legend names for each category. If not provided, default names will be used.
                mode (str): Mode for plotting multiple data sets. Options: 'group', 'stack', 'overlay'.

            Returns:
                None
        """

        traces = []
        for category, values in data.items():
            traces.append(go.Bar(x=(list(range(1, len(values)+1)) if set_xticklabels==[] else set_xticklabels), y=values, name=category))
        if legend_names:
            for i, trace in enumerate(traces):
                trace.name = legend_names[i]
        layout = go.Layout(title=title, xaxis_title=x_label, yaxis_title=y_label, xaxis=dict(type='category'), barmode=mode)
        fig = go.Figure(data=traces, layout=layout)
        fig.write_image(file_path, format='png')

    def _running_stat(self, checktime:bool=True)->bool:
        now = datetime.now()
        weekday,time_now = now.weekday(), now.time()
        start_time, end_time = time(hour=9, minute=0), time(hour=15, minute=30)
        if weekday <= 4: return True
        elif checktime:
            if time_now >= start_time and time_now <= end_time: return True
        else: return False

    def setup_db(self)->None:
        """
            setup_db()
            ----------
            This method is responsible for making any necessary tables in the db if not present.
        """
        from .utils.utils import setup_db
        setup_db('NSE.db')
        
    def local_runner(self)->None:
        """
            local_runner()
            --------------
            This method runs some funcions, that are neccessary for making the programm run properly.
            
            Task it performs:
            - Create neccessary tables in the db if not present.
            - Fetches data from the website like:
                - Holidays: Fetches holidays data from nse website.
                - 
        """
        ret = 0
        self.setup_db()
        val = []
        query = "DATE(time_stamp)='{}'".format(self.current_date.strftime('%Y-%m-%d'))
        if self.dbConn.getCount('stocklogs', query=query) == 0:
            runners={
                'get_equityMasters':{'func': self.get_equityMasters, 'params': []}, 
                'get_indexNames':{'func': self.get_indexNames, 'params': []}, 
                'stocks':{'func':self.get_stocks, 'params':[]},
                'FII_DII':{'func':self.getFII_DII, 'params':[{'date': i, 'format':'%Y-%m-%d'} for i in generate_time_intervals('2024-01-01', datetime_format='%Y-%m-%d', excludeDate=self.dbConn.fetch_unique('FII_DII','date'))], 'runtime': {'before': '',  'after':''}},
                'getGainersLossers':{'func':self.getGainersLossers, 'params':{}, 'runtime': {'before': '',  'after':''}}, 
                'WeeklyHighLow':{'func':self.get_52W_highLow,  'params':{}, 'runtime': {'before': '',  'after':''}},
                'securityArchives':{'func':self.getSecurities,  'params':{}, 'runtime': {'before': '',  'after':''}},
                # 'ipo':{'func':self.get_ipo,'params':{}, 'runtime': {'before': '',  'after':''}},
                'optionChain': {'func': self.get_optionChain, 'params':[{'asset':'NIFTY'}, {'asset':'BANKNIFTY'}, {'asset':'FINNIFTY'}, {'asset': 'MIDCPNIFTY'}], 'runtime': {'before': '','after':''}}
            }
            for k,v in runners.items():
                print(f'{k} Automaticaly running functions....')
                data = self.dbConn.fetch('stocklogs', query=f"name='{k}' AND {query}")
                if data != []:
                    if isinstance(v['params'], list):
                        try:
                            for vl in v['params']:
                                v['func'](**vl)
                                ret = 1
                            val.append(k)
                        except:
                            ret = 0
                    else:
                        try:
                            v['func'](**v['params'])
                            ret = 1       
                            val.append(k)
                        except:
                            ret = 0
                if ret == 1: print(f'func: {str(k)} Runned..')
        self._updateLogs('local_runner','_'.join(val),ret)
     
    def stringer(self, data:dict)->dict:
        for k, v in data.items():
            if isinstance(v, list) and len(data) > 0: data[k]= ','.join(v) 
            elif isinstance(v, bool): data[k]= 'true' if v else 'false' 
            elif v==None: data[k]= ''
        return data


