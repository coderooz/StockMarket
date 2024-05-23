from custom_functions.Requester import Requester
from custom_functions.DataHandlers import get_unique, valreplace
from custom_functions.FileHandler import read, write
from datetime import datetime, time

from modules.StockMarket.NSE.NseQuote import NSEQUOTE 

class NSEHANDLER:

    def __init__(self, userInfo:dict={}, storagePath:str='.', file_name:str=None, pre_load:bool=False):
        self.reto = Requester(ref='https://www.nseindia.com', agent_file='./storage/others/user-agent.txt',set_agent=True, set_header=True, set_ref=True)
        self.sessions,self.header = None,{'Connection': 'keep-alive','Cache-Control': 'max-age=0','DNT': '1','Upgrade-Insecure-Requests': '1','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36','Sec-Fetch-User': '?1','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9','Sec-Fetch-Site': 'none','Sec-Fetch-Mode': 'navigate','Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',}

        self.pathway = ['allIndices','block-deal','chart-databyindex','circulars','commodity-futures','currency-derivatives','equity-master','equity-stockIndices','etf','getNifty50VsindiaVix','historical-chart/snapshot/getNifty50IndiaVixFetchData','historical/cm/equity/series','historical/cm/slb','historical/fo/derivatives','historical/fo/derivatives/meta','home-board-meetings?index=equities','home-corporate-actions','home-corporate-announcements','index-names','irf-derivatives','latest-circular','live-analysis-52Week-hlsummary','live-analysis-emerge','live-analysis-most-active-etf','live-analysis-most-active-securities','live-analysis-most-active-sme','live-analysis-most-active-underlying','live-analysis-price-band-hitter','live-analysis-variations','live-analysis-volume-gainers','liveanalysis/gainers/allSec','liveanalysis/lossers/allSec','market-turnover','marketStatus','merged-daily-reports','option-chain-equities','option-chain-indices','othermarketStatus','othermarketStatus?market=CMOT','quote-derivative','quote-equity','quote-slb','quotes-commodity-derivatives-master','search/autocomplete','snapshot-capital-market-ews','snapshot-capital-market-largedeal','snapshot-capital-market-platform','snapshot-capital-market-tbg','snapshot-derivatives-equity','sovereign-gold-bonds']
        self.dyrp = ['favCapital', 'favDerivatives', 'favDebt']
        self.subscribed, self.cms = {}, self.get_marketStatus()
        self.storagePath,self.file_name = storagePath, file_name
        self.quoteCatg = None
        if storagePath!=None and file_name !=None:
            filePath = storagePath +'/'+ file_name
            data = read(filePath, decode_json=True)
            self.quoteCatg, self.index_list, self.stock_list = data['quoteCatg'] , data['index'], data['stocks']
        else:
            k = self.data_update()
            self.quoteCatg, self.index_list ,self.stock_list = k['quoteCatg'], k['index'], k['stocks']

    def running_stat(self):
        now = datetime.now()
        weekday, current_time = now.weekday(), now.time()
        start_time, end_time = time(hour=9, minute=0), time(hour=15, minute=30)
        if weekday >= 5: return False
        elif start_time <= current_time <= end_time:
            return True
        else: return False

    def requester(self, path, url=None, refer=None, params=None, catg=2):
        """This method is for making request to fetch/scrape necessary data."""
        ret = None
        if path in self.pathway:
            url = self.reto.set_url_params(f'https://www.nseindia.com/api/{path}', params) if params is not None else f'https://www.nseindia.com/api/{path}'

            print(url)
            if catg==1:
                ret = self.reto.request(url, ref=refer, param=params, header=self.header, timeout=60)
            elif catg==2:
                ret, self.sessions = self.reto.requestSessions(url, ref=refer, header=self.header, timeout=60, pre_request=True,sessions=self.sessions)
            return ret

    def subscribe(self, asset):
        ty = type(asset)
        if ty==list: return {i: self.subscribe(i) for i in asset}
        elif ty == dict:
            n = self.subscribe(asset['name'])
            if 'indicator' in list(asset.keys()):
                self.subscribed[asset['name']]['indicator'] += asset['indicator']
            return n
        elif ty == str:
            k = asset.upper()
            if k not in list(self.subscribed.keys()):
                try:
                    n = NSEQUOTE(symbol=k, handle=self)
                    self.subscribed[k] = {'indicator':[], 'link':n}
                except:
                    pass
                else:
                    n = None
                return n
            else: return self.subscribed[k]
        else: return None

    def unsubscribe(self, asset=None):
        if asset==None:
            self.subscribed={}
        else:
            ty = type(asset)
            if ty==str and asset in self.subscribed.keys(): del self.subscribed[asset]
            elif ty==list: [self.unsubscribe(i) for i in asset]
            else: raise TypeError(f'Only valid type is  str & list containing string, but given type is {ty}.')

    def save_data(self, file_path:str=None, overWrite:bool=True):
        if file_path== None:
            file_path = self.storagePath + '/' + self.file_name
        data = {'updated':datetime.now().timestamp(), 'quoteCatg':self.quoteCatg,'index':self.index_list,'stocks':self.stock_list, 'info':{}, 'user':{'subscribed':list(self.subscribed.keys())}}
        if overWrite: write(file_path, data)
        else: write(file_path, data)
        return file_path

    def data_update(self, catg:str=None):
        """
            This method is to update the existiong data the can get old . the parmeters that are need arease follows
            :param catg string: this parameter is None by default which can work and return all the paramenter but this is for the to get the updated data version. this parameter takes quoteCatg, assets, and also index and returns in the list format.
            :return (dict|list): If the catg parameter is left to default then the return type will be in the dict format but the catg value if provided wil return in lst format.
        """
        if catg == None: return {'quoteCatg':self.get_masterQuote(), 'index': self.get_indexlist(), 'assets':self.get_stocklist()}
        elif catg == 'quoteCatg': return self.get_masterQuote()
        elif catg == 'index': return self.get_indexlist()
        elif catg == 'assets': return self.get_stocklist()
        else: raise ValueError(f'Parramenter passed {catg} is not valid of use.')

    def search(self, symb:str):
        return self.requester('search/autocomplete', params={'q':symb}).json()

    def purify(self, symbol):
        return symbol.replace('&','%26').replace(' ', '%20')

    def get_marketStatus(self):
        ret = self.requester('marketStatus').json()
        return {r['market']: r['marketStatus'].lower() for r in ret['marketState']}

    def get_dailyreport(self, catg=None):
        if catg==None: return {k:self.get_dailyreport(k) for k in self.dyrp}
        elif catg in self.dyrp:
            return self.requester('merged-daily-reports', params={'key':catg}).json()
        else: raise ValueError(f'{catg} in param catg is invalid.The valid params are'+ ','.join(self.dyrp))

    def get_ciricular(self, latest=False):
        if latest==True: return self.requester('latest-circular').json()
        else: return self.requester('circulars').json()

    def get_marketTurnOver(self):
        return self.requester('market-turnover').json()

    def get_quoteInfo(self, symbol:str, catg:str='eq'):
        symbol= symbol.upper()
        st_url = {'eq':'quote-equity','der':'quote-derivative','slb':'quote-slb'}
        k = 'index' if catg=='slb' else 'symbol'
        if catg in list(st_url.keys()):
            return self.requester(st_url[catg], params={k:symbol.upper()}).json()
        else: raise ValueError('Valid parameters are '+', '.join(st_url))

    def get_indexlive(self, idxval=False):
        """ Gets all the indexes present in NSE."""
        l = self.reto.request('https://iislliveblob.niftyindices.com/jsonfiles/LiveIndicesWatch.json').json()['data']
        k = {}
        for lo in l:
            idx = lo.pop('indexName')
            k[idx] = lo
        return valreplace(k, '-', '')

    def get_indexlist(self):
        req = self.requester('index-names')
        if req!=None and req.status_code==200:
            tex = list(self.get_indexlive().keys())
            for tx in req.json()['stn']:
                tex = tex + tx
            return sorted(list(set(tex)))
        return req

    def get_masterQuote(self, mix:bool=False):
        ret = self.requester('equity-master').json()
        if mix: return ret
        else:
            all=[]
            for _,v in ret.items():
               all+= [i.upper() for i in v]
            return sorted(all)

    def get_indices(self, catg=None, dataFetch=None, meta:bool=True):
        """This method gets the list of futures & options.
        :return list
        """
        if catg==None: return {k: self.get_indices(k) for k in self.quoteCatg}
        elif catg.upper() in self.quoteCatg:
            params = {'index':catg.upper()}
            if meta == False:
                params['nometa']='true'
            ret = self.requester('equity-stockIndices', params=params)
            if ret != None and ret.status_code==200:
                ret = ret.json()
                if dataFetch==None: return ret
                elif dataFetch == 'data' or dataFetch == 1: return ret['data']
                elif dataFetch == 'advance' or dataFetch == 0: return ret['advance']
                else: raise ValueError(f'{dataFetch} that is passed in parameter dataFetch is invalid. Either use [data or 1] to get the data or use [advance or 0] to get which one\'s are positive or negative.')
            else: return ret
        else: raise ValueError (f'{catg} value passed is invalid. The valid values that can be passed are ' + ', '.join(self.quoteCatg))

    def get_quoteAssets(self, catg='SECURITIES IN F&O', fetchdata='symb'):
        fno = self.get_indices(catg, 'data')
        if fetchdata=='symb': return sorted([x['symbol'] for x in fno])
        else: return fno

    def get_stocklist(self):
        stocks = []
        for t in self.quoteCatg:
            try:
                stocks += self.get_quoteAssets(catg=t)
            except:
                pass
        return get_unique(stocks)