from DataHandlers import list_dlist, add_index, timestamp, list_dict
from Requester import Requester
from DbHandler import SqliteHandler
from FileHandler import write, read
from difflib import SequenceMatcher


import ccxt


"""##Crypto Exchange Handle"""

class CryptoHandler:
    def __init__ (self, dbName:str='crypto_handle.db', pathway='.'):
        self.request = Requester()
        self.dbConn = SqliteHandler(dbName, pathway)
        self.exchange_pt = {}

    def get_exchange(self, exchange:str=None):
        if exchange!= None: return (exchange in ccxt.exchanges)
        else: return ccxt.exchanges

    def subscribe_exchange(self, exchange, api_key=None, private_key=None):
        if self.get_exchange(exchange):
            crypto = CryptoExchange(self)
            crypto.connect_exchange(exchange, api_key, private_key)
            if crypto.exchange != None:
                self.exchange_pt[exchange] = {'link':crypto,'API_KEY':api_key,'PRIVATE_KEY': private_key ,'desc': crypto.desc, 'assets':[]}
                return crypto

    def is_subscribed(self, exchange):
        '''
        Checks if the Exchange is Subscribed or not.
        :param str exchange: The exchange parameter takes the name of the exchange that is to be checked.
        :return [bool]: Returns True if the exchange is subscribed or False if not subscribed.
        '''
        return (exchange in self.exchange_pt.keys())

    def add_asset(self, exchange:str, asset):
        '''
        Adds symbol/asset to a perticular exchange of that is subscribed.
        :param str exchange: Takes the name of the exchange in which assets are to be add/subscribed.
        :param (str/list) asset: Takes either a single asset (as a string) or multiple assets (as a list) that are to be subscribed into the desired exchange.
        '''
        if self.is_subscribed(exchange):
            asty = type(asset)
            if asty==str:
                asset_link = self.exchange_pt[exchange]['link'].add_symb(asset)
                self.exchange_pt[exchange]['assets'].append(asset)
                return asset_link
            elif asty==list:
                return {aset:self.add_asset(exchange, aset) for aset in asset}


    def store_exchanges(self, filename:str):
        '''The method is for storing the exchanges that are subscribed in a file in json format.'''
        data = {}
        for k, v in self.exchange_pt.items():
            v['limit'] = v['desc']['rateLimit']
            data[k] = v
        return write(filename, data, '\n', emptyPervious=True)

    def load_exchanges(self, data:dict):
        ret = {}
        for k,v in data.items():
            t1 = self.subscribe_exchange(k, v['API_KEY'], v['PRIVATE_KEY'])
            t2 = self.add_asset(k, v['assets'])
            ret[k] = {'handle':t1, 'assets':t2}
        return ret

    def load_exchange_file(self, filename:str):
        ''' The method is to load json data from the given file.'''
        self.load_exchanges(read(filename, '\n'))

    def get_exchangehandle(self, exchange=None):
        if exchange==None: return self.exchange_pt
        elif self.is_subscribed(exchange): return self.exchange_pt[exchange]

    def remove_exchange(self, exchange):
        """
        Removes the exchange if pre-subscribed.
        :param str exchange: Takes the name of the exchange that is to be unsubscribed.
        :return: None
        """
        if self.is_subscribed(exchange): self.exchange_pt.remove(exchange)
        else: raise ValueError(f'{exchange} exchange is not connected/subscribed.')


class CryptoHandle:
    def __init__(self, asset, exchange_pt, set_timeframe:str=None):
        self.asset = asset
        self.exchange = exchange_pt.exchange
        self.exch_timestamp = exchange_pt.desc['timeframes'] if ('timeframes' in exchange_pt.desc.keys()) else ['1m', '2m', '5m', '1d', '5d', '1mo']

    def get_trades(self):
        return self.exchange.fetch_trades(self.asset)

    def get_ticker(self):
        '''Gets the "Ticker Data" of the selected asset.'''
        return self.exchange.fetch_ticker(self.asset)

    def get_quotes(self, timeframe:str='1m', since=None, limit=None, params:dict={'price':'mark'}, date_convert:str=None, arrange:bool=True, add_ids:bool=False):
        '''
        Returns the historical data/candlestick data of the selected asset.
        :param str timeframe: The timeframe parameter takes the timeframe in which the data is to be fetched.
        :param dict param: Takes the extra info on how to fetch.
        :param str date_convert: Takes the format in which the Unix time is to be converted to.
        :param bool arrange: If given tru, will return the arranged version of the data with keys in each blocks.
        :returns list: Returns the fetched data in a list format.
        '''
        if timeframe in self.exch_timestamp:
            quotes = self.exchange.fetch_ohlcv(self.asset, timeframe, since=since, limit=limit, params=params)
            if arrange == True:
                keys = ['timestamp', 'open','high','low','close','volume']
                quotes = list_dlist(quotes, keys)
                if date_convert!=None:
                    quotes['timestamp'] = [timestamp(uni, date_convert) for uni in quotes['timestamp']]
                quotes = list_dict(quotes)
                if add_ids==True:
                    quotes = add_index(quotes)

            return quotes

    def get_orderbook(self, limit=10):
        orderbook = self.exchange.fetch_order_book(self.asset, limit=limit)
        return orderbook['bids'], orderbook['asks']

    def get_chart(self, name:str=None, quotes:list=None, timeframe='1m', since=None, limit=None):
        """
        The method `get_chart()` gives a candlestick chart.
        """
        name = name if name!=None else f'{self.asset} Chart.'
        quotes = self.get_quotes(timeframe=timeframe, since=since, limit=limit, arrange=True, add_ids=True) if quotes==None else quotes
        # go_plot(quotes, title=name)

"""##CryptoExchange"""

class CryptoExchange:
    '''
        The class "CryptoExchange" is to handle task related to a specific exchange(like: binance).
    '''

    def __init__(self, handle=None):
        self.symbols, self.exchange, self.handle, self.desc = None, None, handle, None
        self.assets_link = {}

    def description(self):
        self.desc = self.exchange.describe()
        self.desc['timeframe'] = list(self.desc['timeframes'].keys()) if 'timeframes' in self.desc.keys() else ['1m','5m','15m']
        self.desc['api_url'] = self.desc['urls']['api']
       # del self.desc['has'], self.desc['urls'], self.desc['countries'], self.desc['version'], self.desc['userAgent'], self.desc['pro']
        return self.desc

    def symbol_info(self, symbol):
        if self.check_symb(symbol): return self.exchange.load_markets()[symbol]
        else: raise ValueError(f'The given symbol[{symbol}] doesn\'t exists.')

    def fetch_ticks(self, symbol=None):
        if symbol==None: return self.exchange.fetch_tickers()
        elif symbol!=None and type(symbol)==str and self.check_symb(symbol): return self.get_ticker(symbol)
        elif type(symbol)==list: return self.get_ticker([symb for symb in symbol if self.check_symb(symb)])
        else: raise ValueError('The value passed is invalid. Please check and try again.')

    def check_symb(self, symbol):
        return (symbol in self.symbols)

    def connect_exchange(self, exchange, api_key:str=None, private_key:str=None):
        if exchange in ccxt.exchanges:
            if api_key!=None and private_key!=None:
                 self.exchange = getattr(ccxt, exchange)({'apiKey': api_key,'secret': private_key})
            else:
                 self.exchange = getattr(ccxt, exchange)()
            self.get_market_list()
            self.description()
        else: raise ValueError(f'The exchange provided({exchange}) is not available. Please check the exchange list and then retry.')

    def get_market_list(self):
        """Gets the list of the symbols/assets available in the selected/given exchange."""
        self.symbols = self.exchange.load_markets().keys()
        return self.symbols

    def add_symb(self, symbol):
        """
        Adds the symbol/asset in a list which basically means, it's at work.
        :param (str|list) symbol: This param takes either an asset or a list of assets which are to be subscribed.
        """
        if type(symbol)==str and self.check_symb(symbol) and self.is_subscribed(symbol)==0:
            symb = CryptoHandle(symbol, self)
            self.assets_link[symbol] = {'link':symb}
            return symb
        elif type(symbol)==list:
            return {symb:self.add_symb(symb) for symb in symbol}

    def get_assethandle(self, symbol=None):
        if symbol==None: return self.assets_link
        elif self.is_subscribed(symbol): return self.assets_link[symbol]['link']
        elif self.check_symb(symbol)==False: raise ValueError(f'The symbol/asset provided [{symbol}] is not available on the exchange.')
        else: raise Exception(f'{symbol} is not yet subscribed.')

    def is_subscribed(self, symbol):
        '''Checks if the given symbol/asset is subscribed or not.'''
        return (symbol in self.assets_link.keys())

    def get_ticker(self, symbol):
        return self.exchange.fetch_ticker(symbol)

    def fetch_asset_price(self, limit=None, lesser=None, greater=None):
        ticks, rets = self.fetch_ticks(), {}
        limit= len(ticks.keys()) if limit==None else limit
        for tick in ticks.keys():
            if ticks[tick]['last']!=None:
                rets[tick] = ticks[tick]['last']
        if lesser!=None: rets = {k: v for k, v in rets.items() if v <= lesser}
        if greater!=None: rets = {k: v for k, v in rets.items() if v >= greater}
        return dict(sorted(rets.items(), key=lambda x: x[1])[:limit])

    def start_trades(self, symbol:str):
        pass

    def search_symb(self, symbol:str, threshold=0.8):
        similar_strings=[]
        for s in self.symbols:
            similarity = SequenceMatcher(None, symbol, s).ratio()
            if similarity >= threshold:
                similar_strings.append(s)
        return similar_strings

    def check_status(self):
        return self.exchange.fetch_status()

    def create_limit_order(self, symbol, side, amount, price):
        return self.exchange.create_order(symbol, 'limit', side, amount, price)

    def cancel_order(self, order_id):
        return self.exchange.cancel_order(order_id)
