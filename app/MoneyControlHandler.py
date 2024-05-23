from Requester import Requester
from DbHandler import SqliteHandler 
from DataHandlers import to_unix_timestamp, json_parser, valreplace, dlist_dict, to_human_readable, valreplace, setNum, get_previous_day, dateFormat
from datetime import datetime
from app.utils.utils import setup_db, connect_db, local_runner
from .Trading.chart import candleInfo 

class MoneyControlHandler:
    
    def __init__(self, localRunner:bool=True, logger:bool=True) -> None:
        
        self.sessions, self.header = None, {'Connection': 'keep-alive','Cache-Control': 'max-age=0','DNT': '1','Upgrade-Insecure-Requests': '1','user-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36','Sec-Fetch-User': '?1','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9','Sec-Fetch-Site': 'none','Sec-Fetch-Mode': 'navigate','Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9,hi;q=0.8'}
        self.resolution_dt = {'60': 60,'3': 180,'5': 300,'15': 900,'30': 1800,'60': 3600,'D': 24*3600,'W': 7*24*3600,'M': 30*24*3600,'45': 45*24*3600,'120': 120*24*3600,'240': 240*24*3600}

        self.request = Requester()
        self.logger = logger
        self.dbConn = connect_db('moneycontrol.db', './app/database')
        if localRunner: self.local_runner()

    # query data 

    def get_stocksList(self, asset:str='')->list:
        """
            get_stocks()
            ------------

            gets the stock list
        """
        query = f"'symbol'='{asset}'" if asset!='' else ''
        return self.dbConn.fetch('listings',query=query)

    def get_indexList(self, index_name:str='')->list:
        """
            get_indexList()
            ---------------

            Returns the index list with some info.
        """
        query = f" = '{index_name}'" if index_name!='' else ''
        return self.dbConn.fetch('l', query=query)
  
    def getCharts(self, chartType:str='line', asset_type:str=''):
        pass

    def get_ticker(self, asset:str, dt_from:str, dt_to:str, assetType:str='', resolution:str='1', countback:int=300, currencyCode:str='INR', dt_format:str='%Y-%m-%d', convert_human:bool=True, fetchNew:bool=False, asses:bool=True)->list:
        """
            get_ticker()
            ------------

            Fetches the ticker data in ohlcv format of the gicven asset.

            Parameters:
                - `asset` str: Symbol of the asset that is to be fetched.
                - `dt_from` str: The date from when the data is to be fetched.
                - `dt_to` str: The date till whent the data is to be fetched.
                - `assetType` str: The type of asset data to be fetched. Values possible are
                    - `index`: For if the asseet is an index.
                    - `stock`: for if the asset is a stock.
                - `resolution` str: Thes the time range of each ticker.
                - `countback` int: The number of data points or candles are needs to be fected. Default is `300`.
                - `currrencyCode` str: Takes the currency code in which the data is to be fetched. Defay=ule is `INR`. 
                - `dt_fromat` str: The format in which the date is passed. default is  `%Y-%m-%d %H:%M`.
                - `convert_human` bool: Converts the timestamp values into human readable format. Default is `True`.
                - `fetchNew` bool: Fetches the data newly/directl from the internet. Default is `False`.
                - `asses` bool: Adds index and other basic candlestick info. Default is `True`.
            Returns:
                - list: returns the ticker data in an ohlcv format whith tomestamp. 
        """    
        cag = ['index', 'stock']
        data:list
        if fetchNew:
            dt_to = int(datetime.now()) if dt_from == '' else int(dateFormat(dt_to, dt_format).timestamp())
            dt_from = int(to_unix_timestamp(get_previous_day(dt_to, dt_format), dt_format)) if dt_from == '' else int(to_unix_timestamp(dt_from, dt_format))
            catg =  cag[self.checkAssetType(asset)+1] if assetType=='' else assetType
            params = {'symbol': self.dbConn.fetch('index_list', 'bridgesymbol', query=f"symbol LIKE '{asset}'")[0]['bridgesymbol'],'resolution': resolution, 'from': dt_from,'to': dt_to,'countback': countback, 'currencyCode': currencyCode}
            reto=self.requester(f'https://priceapi.moneycontrol.com/techCharts/indianMarket/{catg}/history', params=params)
            if reto is not False and reto is not None and reto['s'] == 'ok':
                data = {'symbol': [asset for _ in range(len(reto['t']))],'category' : [catg for _ in range(countback)],'resolution' : [resolution for _ in range(countback)],'timestamp':(to_human_readable(reto['t']) if convert_human==True else reto['t']),'open':reto['o'],'high':reto['h'],'low':reto['l'], 'close':reto['c'], 'volume':reto['v']}        
                data = setNum(candleInfo(dlist_dict(data), True) if asses else dlist_dict(data))
        else:
            table = 'tickerData'
            _to = int(datetime.now()) if dt_from == '' else int(dateFormat(dt_to, dt_format).timestamp())
            _from = int(to_unix_timestamp(get_previous_day(dt_to, dt_format), dt_format)) if dt_from == '' else int(to_unix_timestamp(dt_from, dt_format))
            query = [f"symbol='{asset}'", f"category='{assetType}'", f"timestamp >= '{_from}'", f"timestamp <= '{_to}'"]
            data=self.dbConn.fetch(table, query=query)
            if len(data) == 0:
                self.dbConn.json_insert(table, self.get_ticker(asset=asset, dt_from=dt_from, dt_to=dt_to, assetType=assetType, resolution=resolution, countback=countback, currencyCode=currencyCode, dt_format=dt_format, convert_human=False, fetchNew=True, asses=False))
                data=self.dbConn.fetch(table, query=query)
                if convert_human:
                    data = [{**d, 'timestamp':to_human_readable(d['timestamp'])} for d in data]
        return data

    # data Fetching fuctions/db data fetching
    def get_meta_data(self, asset:str, fetchNew:bool=False):
        """
            get_meta_data()
            ---------------
        """
        asetType = self.checkAssetType(asset)
        if fetchNew:            
            if asetType == 1:
                url = ''
            elif asetType == 2:
                url = ''
            else: raise ValueError('Asset is either not listed or contains some mistakes while passing.')
            repr = self.requester(url)
            if repr is not None and repr is not False:
                return repr 
        else:
            if asetType == 1:
                table, query = '', ''
            elif asetType == 2:
                table, query = '', '' 
            else: raise ValueError('Asset is either not listed or contains some mistakes while passing.')
            reqr = self.dbConn.fetch(table, query=query)
            if len(repr) == 0:
                self.get_stocks(asset) if asetType == 2 else self.get_indexes(index_name=asset)
                reqr = self.get_meta_data(asset=asset, fetchNew=True)
                self.dbConn.json_update()
        return reqr
    
    def get_priceFeed(self, asset:str, exchange:str='nse'):
        exch = ['bse', 'nse']
        if exchange.lower() in exch:
            data = self.requester(f'https://priceapi.moneycontrol.com/pricefeed/{exchange}/equitycash/{asset}')            
            if data is not None and data is not False and data['code']==200 and data['message']=='Success':
                return data['data']

    def get_indexes(self, index_id:int=0, index_name:str='', fetchNew:bool=False):
        """
            get_indexes()
            -------------
            Gets the Indexs data int the forum MoneyControl.
        """
        ret = 0
        cath = ''
        if fetchNew:
            pathway =  {'__pathway__': [{'path':'indices','data': ['stkexchg', 'exchange', 'ind_id', 'share_url', 'bridgesymbol', 'region']},{'path':'tabs > item > `list`','data': ['url', 'name']}],'graphurl': 'graph_tab > url'}
            i = 1 if index_id == 0 else index_id
            data = []
            while True:
                resp = self.requester(f'https://appfeeds.moneycontrol.com/jsonapi/market/indices&format=json&t_app=MC&t_version=48&ind_id={i}')
                if resp is not None and resp is not False:
                    d2 = {({**d2, **v} if k not in pathway.keys() else {**d2, k:v}) for k,v in json_parser(resp, pathway).items()}
                    if d2['bridgesymbol'] != '':
                        k = {d2['name'][i].lower()+'Url': d2['url'][i] for i in range(len(d2['url']))}
                        d2 = {**d2, **k}
                        del d2['url'], d2['name']
                        chgs = {'stkexchg':'symbol','page_url':'share_url', 'graphurl':'graph_url'}
                        for kc,vc in chgs.items():
                            d2 = valreplace(d2, kc, vc, True)
                        data.append(setNum(d2))
                        if index_id != 0: 
                            break
                        else:i+=1
                    else: break
                else: break
                
            ret = 1
            cath = 'net'
        else:
            query = [(f"ind_id='{index_id}'" if index_id!=0 else ''), (f"symbol='{index_name}' OR bridgeSymbol='{index_name}'" if index_name!='' else '')]
            data = self.dbConn.fetch('index_list', query=query)
            cath = 'db'
            if len(data) == 0:
                self.dbConn.json_insert('index_list', self.get_indexes(index_id, index_name, True))
                data = self.dbConn.fetch('index_list', query=query)
                cath = 'db_net'
            ret = 1
        self._updateLogs('getIndexes', cath, ret)
        return data

    def getTechnical(self, tecType:str, asset):
        """
            getTechnical()
            --------------
            Gets the techincal info of the provided asset.
        """
        catg = {'stock':f'https://api.moneycontrol.com/mcapi/v1/stock/estimates/price-forecast?scId={asset}&ex=N&deviceType=W', 'index':f'https://api.moneycontrol.com/mcapi/v1/technical-indices/analysis?indexId={asset}&dur=D&deviceType=W'}
        
        if tecType in catg.keys():
            respo = self.requester(catg[tecType] )
        else: raise ValueError('Asset type is invalid. It only accepts ' + ' & '.join(catg.keys()) )

    def get_options(self):
        pass

    def get_index_stocks(self, index_id:int=0, index_name:str='', fetchNew:bool=False)->(list|dict):
        """
            get_index_stocks()
            ------------------

            Gets the stocks related to the indexs.

            Parameters:
                - index_id (int): 
                - index_name (str): 
                - fetchNew (bool): 
            
            Return:
                - (list|dict)
        """
        ret = 0
        cath = ''
        data:list
        if fetchNew:
            if index_id == 0 and index_name=='':
                data = [self.get_index_stocks(index_id=i['ind_id'], index_name=i['symbol']) for i in self.get_indexes()]
            else:
                info = self.get_indexes(index_id=index_id, index_name=index_name)[0]
                reto = self.requester(info['stocksUrl'])
                if reto is not None and reto is not False:
                    data = setNum(valreplace(reto['item'], 'id', 'scId', True))
                ret = 1
                cath = 'net'
        else:
            data = self.dbConn.fetch('stock_index', query=[(f"index_id={index_id}" if index_id!=0 else ''), (f"index_name={index_name}" if index_name!='' else '')])
            cath='db'
            if len(data) == 0:
                if index_id==0:
                    data = [self.get_index_stocks(index_id=i['ind_id'], index_name=i['symbol']) for i in self.dbConn.fetch('index_list', 'ind_id,symbol')]
                elif index_id > 0:
                    data = self.get_index_stocks(index_id=index_id, index_name=index_name, fetchNew=True)
                self.dbConn.json_insert('listings', data)
                self.dbConn.json_insert('stock_index', [{'index_name': index_name, 'index_id':index_id, 'stock_name':i['shortname'], 'stId':i['sc_did']} for i in data])
                cath = 'db_net'
            ret = 1
        self._updateLogs('get_index_stocks', cath, ret)
        return data

    def getStock_price(self, stock_id:list, stockList:list=[]):
        """
            getStock_price()
            ---------------

            Gets  the price details of the given assets.        
        """
        stkIds = self.dbConn.fetch_unique('listing', 'sc_did')
        if stock_id in stkIds:
            stockList = ','.join([i for i in stkIds+[stock_id] if i in stkIds]) if len(stockList) >= 1 else stock_id
            data = self.requester('https://api.moneycontrol.com/mcapi/v1/stock/get-stock-price', params={'scIdList': stockList, 'scId':stock_id})
            if data is not None and data is not False:  
                data = data['data']
                return data
        else:
            raise ValueError('StockId passed is not valid.')

    # Support Functions.

    def checkAssetType(self, asset:str)->int:
        """
            checkAssetType()
            ----------------
            Checks if the asset falls in index or in stock category.
            Return:
            - `int`: Returns a number associated to the asseet type.
                - `0`: Unkown asset/ Unidentifiable asset.
                - `1`: Asset is an index.
                - `2`: Asset is an stock.
        """
        ret = 0
        indx_tb = {i:asset for i in self.dbConn.getColumnNames('')}
        stockTb = {i:asset for i in self.dbConn.getColumnNames('')}
        if self.dbConn.getCount('', query=indx_tb) > 0:
            ret = 1
        elif self.dbConn.getCount('', query=stockTb) > 0:
            ret = 2
        
        return ret
    
    def requester(self, url:str, method:str='get', params:dict={}, refer:str='', header:dict={}, json:dict={}, timeout:int=60, catg:int=2, decode:bool=True, stat_code:int=200):
        """
            A method that returns an object of Requester Class for making HTTP requests.
        
            Parameters:
            - url (str): The URL to which request will be sent.
            - method (str): The metho that will be used to send the request.  Default value is GET.
            - params (dict): The PARAMS that are to be send during request.
        """
        header = self.header if header=={} else header
        ret = None
        if catg==1:
            ret = self.request.request(url, method=method, ref=refer, params=params, header=header, json=json, timeout=timeout)
        elif catg==2:
            if self.sessions==None:
                ret, self.sessions = self.request.requestSessions(url, method=method, ref=refer, params=params, header=header, timeout=timeout, pre_request='https://www.moneycontrol.com/stockmarketindia/')
            else:
                ret, self.sessions = self.request.requestSessions(url, method=method,  ref=refer, header=header, params=params, timeout=timeout, sessions=self.sessions)
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
        self._updateLogs('requester', f'{url} -{stat}', res)
        return ret
  
    def local_runner(self, catg:str='')->None:
        """
            local_runner()
            --------------
            Runner function to be used by other functions.

            Parameter:
                - catg (str): Takes the func key the is to be runned.
        """
        setup_db()
        runner = {
            {'func':'get_indexes', 'params': {}},
            {'func':'get_index_stocks', 'params': {}},
        }
        local_runner(self, runner)

    def check_db(self)->None:
        """
            check_db()
            ----------
            This method is to check the database and make the create tables so that it can run properly.
            Tabes that this method creates are:
            - api_script: Stores the asset lists, Codes & other necessary info. 
            - urls: To store the urls and other values.
            - holdings: Stores the assets in the holding. 
            - orders: Stores the orders made.  
        """

    def _updateLogs(self, name, value:str, success:int) -> bool:
        """
            updateLogs
            ----------
            This method when fired will log the events that have occured or tasks executed by the class.
        """
        if self.logger:
            t = self.dbConn.insert('logbook', ['name', 'value', 'result'], [name, value, success])
            return True if t else False

    def exportDate(self, table_name:str=''):
        """
            exportDate()
            ------------
            
            This method exports the data of the database into a file. 
        """
        self.dbConn.export_data(tableName=table_name)

if __name__ == "__main__":
    
    urls = [
        'https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/symbols?symbol=APOLLOTYRE',
        'https://api.moneycontrol.com/mcapi/v1/stock/get-stock-price?scIdList=AT%2CMRF%2CBI03%2CJKI%2CC07&scId=AT',
        'https://api.moneycontrol.com/mcapi/v1/stock/financial-historical/overview?scId=AT&ex=N',
        'https://www.moneycontrol.com/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did=AT14',
        'https://api.moneycontrol.com/mcapi/v1/stock/price-volume?scId=AT',
        'https://api.moneycontrol.com/mcapi/extdata/v2/mc-insights?scId=AT&type=c&deviceType=W',
        'https://api.moneycontrol.com/mcapi/v1/stock/estimates/check-forecast?deviceType=W&scId=AT&ex=N',
        'https://api.moneycontrol.com/mcapi/technicals/v2/details?scId=AT&dur=D&deviceType=W',
        'https://www.moneycontrol.com/mc/widget/stockdetails/getChartInfo?classic=true&scId=AT&type=N',
        'https://priceapi.moneycontrol.com/pricefeed/nse/equitycash/AT',
        'https://priceapi.moneycontrol.com/pricefeed/bse/equitycash/AT',
        'https://api.moneycontrol.com/mcapi/v1/fno/futures/getExpDts?id=AT',
        'https://api.moneycontrol.com/mcapi/v1/fno/options/getExpDts?id=AT',
        'https://api.moneycontrol.com/mcapi/v1/stock/estimates/price-forecast?scId=AT&ex=N&deviceType=W',
        'https://api.moneycontrol.com/mcapi/v1/stock/estimates/earning-forecast?scId=AT&ex=N&deviceType=W&frequency=12&financialType=C',
        'https://api.moneycontrol.com/mcapi/v1/stock/estimates/valuation?deviceType=W&scId=AT&ex=N&financialType=C',
        'https://api.moneycontrol.com/mcapi/v1/stock/estimates/hits-misses?deviceType=W&scId=AT&ex=N&type=eps&financialType=C',
        'https://api.moneycontrol.com/mcapi/v1/stock/estimates/analyst-rating?deviceType=W&scId=AT&ex=N',
        'https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol=APOLLOTYRE&resolution=1D&from=1670112000&to=1709856000&countback=329&currencyCode=INR'
            ]
