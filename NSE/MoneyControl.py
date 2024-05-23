from custom_functions.Requester import Requester
from custom_functions.DbHandler import SqliteHandler 
from custom_functions.DataHandlers import to_unix_timestamp, json_parser, valreplace, dlist_dict, to_human_readable
from datetime import datetime, timezone
import pandas as pd

class MoneyControl:
    
    def __init__(self, symbol, countryCode:str, dbname:str='moneycontrol.db', pathway:str='.') -> None:
        self.resolution_dt = {
            '1': 60,'3': 180,'5': 300,'15': 900,'30': 1800,
            '60': 3600,'D': 24*3600,'W': 7*24*3600,'M': 30*24*3600,
            '45': 45*24*3600,'120': 120*24*3600,'240': 240*24*3600,
        }
        
        self.conCode = countryCode
        self.req = Requester()
        self.sessions = None
        self.delta_time = None
        self.symbol = symbol

        self.dbConn = SqliteHandler(dbname, pathway)
        self.urls = {}
        if self.dbConn.getCount('urls', query=f"code='{countryCode}'") > 0:
            for i in self.dbConn.fetch('urls', 'name,url', f"code='{countryCode}'", detailed=False):
                self.urls[i[0]] = i[1]

    def requester(self, url, params:dict={}, timeout:int=30, decode:bool=True):
        if self.sessions == None:
            data, self.sessions = self.req.requestSessions(url, params=params, timeout=timeout, pre_request=self.urls['main'])
        else:
            data, self.sessions = self.req.requestSessions(url, params=params,timeout=timeout, sessions=self.sessions)
        if decode:
            try: 
                return data.json()
            except:
                return data.text
        return data
            
    def fetch_meta_data(self):
        """
            This method is responsible for getting the meta data.
        """

        url = self.urls['meta_data']
        params = {'symbol':self.symbol}
        data = self.requester(url, params=params, timeout=10)       
        return data 

    def getTicker(self, resolution:int=1, dt_from:str='', dt_to:str='', countBack:int=300, format='%d-%m-%Y', organize:bool=True, timestampAlt:bool=True):
        """
            getTicker():
            --------------
            This method is responsible to get the intraday data of the given asset.

        """
        dt_to = datetime.now() if dt_to=='' else int(to_unix_timestamp(dt_to, format))
        dt_from = None if dt_from=='' else int(to_unix_timestamp(dt_from, format))

        data = self.requester(self.urls['historical'], {'symbol':self.symbol,'resolution': resolution,'from':dt_from,'to':dt_to,'countback':countBack})
        if data['s']=='ok':
            del data['s']
            if timestampAlt:
                data['t'] = [to_human_readable(i, format) for i in data['t']]
            k = {'t':'timestamp', 'o':'open', 'h':'high','l':'low','c':'close', 'v':'volume'}
            for l, i in k.items():
                data = valreplace(data, l, i, True) 
            if organize: return dlist_dict(data)
            return data
        else:
            print("error", data['s'])

    def get_indexes(self):
        """
            Gets the indexs info.
        """
        table = 'indexes'
        if self.dbConn.getTb(table)==False:
            for i in range(1, 100):
                pass
            
        return self.dbConn.fetch(table)
    
    def insertUrls(self, name:str='', country:str='',code:str='',url:str='',parameter:str='', unique:bool=True):
        if unique:
            if self.dbConn.getCount('urls', query=f"url={url} OR  name='{name}'") > 0:
                raise Exception(f"A URL or a Name with '{name}' already exists.")
        self.dbConn.insert('urls',['name', 'country', 'code', 'url', 'parameter'], [name, country, code, url, parameter])
    
    def get_indices(self, index):
        if isinstance(index, int):
            query = f""

    def verifySymbol(self, symbol):
        data = self.dbConn.fetch('listings',query=f"symbol='{symbol}' OR bridgesymbol='{symbol}'", fetchAll=False)
        if data!=[]:
            return data['bridgesymbol']
        else:
            data = self.requester('https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/symbols', {'symbol':symbol}, decode=False)
            if data.status_code == 200:
                data = data.json()
                self.dbConn.insert('listings', 'symbol')
            else: return False

class MControlApi:
    def __init__(self, symbol, resolution=1, date_from=None, date_to=datetime.now())->None:
        self.symbol = symbol
        self.resolution = resolution
        self.date_to = int(date_to.timestamp())
        self.resolution_dt = {
            '1h': 60, '3h': 180,'5': 300,'15': 900,'30': 1800,
            '60': 3600,'D': (res_d := 24*3600),'W': 7*res_d,'M': 30*res_d,
            '45': 45*res_d,'120': 120*res_d,'240': 240*res_d,
        }
        self.delta_time = self.resolution_dt[str(self.resolution)]
        if date_from == None:
            self.date_from = self.date_to - self.delta_time * 400
        else: 
            self.date_from = int(date_from.timestamp())
        self.req, self.sessions = Requester(), None
        self.symbol_meta = None
        self.dateframe = []

    def requester(self, url, params:dict={}, decode:bool=True):
        if self.sessions == None:
            data, self.sessions = self.req.requestSessions(url, params=params,pre_request='https://www.moneycontrol.com/stockmarketindia/')
        else:
            data, self.sessions = self.req.requestSessions(url, params=params,sessions=self.sessions)
        if decode:
            try: 
                return data.json()
            except:
                return data.text
        return data
            
    def fetch_meta_data(self):
        if self.symbol_meta == None:
            self.symbol_meta = self.requester('https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/symbols', {'symbol':self.symbol})
        return self.symbol_meta
    
    def fetch_intraday_data(self, countback=None):
        new_data = 0
        try:
            if countback == None:
                countback = int((self.date_to - self.date_from)/self.delta_time)
                if countback > 330:
                    countback = 330
            
            param = {'symbol': self.symbol,'resolution': self.resolution,
                'from': self.date_from,'to': self.date_to,'countback': countback}
            
            data = self.requester('https://priceapi.moneycontrol.com/techChart/indianMarket/stock/history',param)    
            
            data = data.json()
            if data['s'] == 'no_data': return -1
            df = pd.DataFrame.from_dict(data)
            df['dt'] = pd.to_datetime(df['t']+19800, unit='s')
            nl = len(self.dateframe)
            if nl == 0:
                self.dateframe = df.copy()
                new_data = len(self.dateframe)
            else:
                df = pd.concat([self.datafrom, df[df['t'].isin(self.dateframe)==False]]).reset_index(drop=True)
                self.dataframe = df.copy()
                new_data = len(self.dataframe) - nl

            self.date_from = self.date_to
            self.date_to += self.delta_time 

        except Exception as e:
            new_data = -1
            print('Error Fetching Data:', e)

        return new_data
    
if __name__  == "__main__":
    from pprint import pprint
    from time import sleep
    obj=MControlApi('SNBI')
    nd=0
    while nd > -1:
        nd = obj.fetch_intraday_data()
        if nd > 0:
            pprint(obj.dataframe)
            last = obj.dataframe.iloc[-1].iloc[0]['dt']
            if (last.hour + last.minute/60) > 15.5:
                print('Market is closed')
                break
        sleep(obj.delta_time)