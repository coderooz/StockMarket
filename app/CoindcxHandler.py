import hmac, hashlib, time, json
from DataHandlers import setNum, space_remover, valreplace
from Requester import Requester
from SqliteHandler import SqliteHandler

class CoinDCXHandler:
    
    """
        CoinDCXHandler
        ==============
        - Creator: Codzees (Ranit Saha)
        - Version: 0.0.1
        - E-mail Id: viewersweb02@gmail.com
        - Documentaion Page Link: https://docs.coindcx.com
        - Description: A class programm that allows the user to interact with the CoinDCX Api progrmatically with relative ease. This also adds the benifits of database with it for mainly storing and processing data at later times.
    """

    def __init__(self, api_key:str, secret_key:str, dbName:str='coindcx.db', pathway='./database'):
        
        self.request = Requester()
        self.dbConn = SqliteHandler(dbName, pathway)
        self.API_KEY, self.SECRET_KEY = api_key, secret_key
        self.sessions = None
        self.logData = {'tradeHistory': {},'orderBook': {}, 'tickerdata': {}}

    ## user data
    def getUserInfo(self):
        """
            getUserInfo()
            -------------
            Retrieves user info.
        """
        
        response = self._fetcher('https://api.coindcx.com/exchange/v1/users/info', 'post')
        return response
        
    def getBalance(self):
        """
            getUserInfo()
            -------------
            Retrieves user account balance.
        """
        response = self._fetcher('https://api.coindcx.com/exchange/v1/users/balances', 'post')
        return response         

    # basic info 
    def getAssetList(self, fetchNew:bool=False):
        """
            getAssetList()
            --------------
        """
        if fetchNew:
            data = self.requester('https://api.coindcx.com/exchange/v1/markets')
        else:
            data = self.dbConn.fetch_unique('assets', 'name')
            if len(data) == 0:
                data = self.getAssetList(True)
                self.dbConn.json_insert('assets', [{'name': i} for i in data])
        return data

    def getDetails(self, asset:str='', fetchNew:bool=False)->list:
        """
            getDetails()
            ------------
            Gets the details of the asset. 

            Parameters:
            - `asset` str: Takes the name or symbol or currency name as the search value. If left blank it will return all available assets.
            
        """
        data:list
        if fetchNew:
            data = setNum(space_remover(self.requester('https://api.coindcx.com/exchange/v1/markets_details')))
            data = valreplace(data, 'coindcx_name', 'name', True)
            data = [{**i,'bo_sl_safety_percent': (0 if i['bo_sl_safety_percent']==None else i['bo_sl_safety_percent']), 'max_leverage': (0 if i['max_leverage']==None else i['max_leverage']), 'max_leverage_short': (0 if i['max_leverage_short']==None else i['max_leverage_short']), 'order_types': ','.join(i['order_types'])} for i in data]
            if asset != '':
                data = [i for i in data if i['name']==asset or i['symbol']==asset or i['target_currency_name']==asset]
        else:
            query = f"name='{asset}' OR symbol='{asset}' OR target_currency_name='{asset}'" if asset != '' else ''
            data = self.dbConn.fetch('assets', query=query)
            if len(data)==0:
                data = self.getDetails(asset=asset, fetchNew=True)
                self.dbConn.json_insert('assets', data)
        return data

    def getAssetsTicker(self, fetchNew:bool=False)->list:
        """
            getAssets()
            -----------

            Return:
            - `market` : the market for which the ticker data is generated.
            - `change_24_hour` : the change in rates in the past 24 hours.
            - `high` : value in the past 24 hours.
            - `low` : value in the past 24 hours.
            - `volume` : volume of the market in the past 24 hours.
            - `last_price` : of the market when the ticker was generated.
            - `bid` : highest bid offer in the order book.
            - `ask` : highest ask offer in the order book.
            - `timestamp` : Time at which the ticker was generated.

        """
        return space_remover(setNum(self.requester('https://api.coindcx.com/exchange/ticker')))

    def getTradeHistory(self, symbol_pair:str, limit:int=200, fetchNew:bool=False):
        """
            getTradeHistory()
            -----------------

            Parameters:
            - `symbol_pair` str: The symbol pair that is to be fetched. Example `B-BTC_USDT` for `BTCUSDT`.
            - `limit` int: Takes the number of data to be fetched. Default is 200; Max: 500 (if fetchNew is True and will seet the limit 500 if more.)

            Return:
            - `price` str: Indicates the trade price.
            - `quantity` str: Indicates the quantity.
            - `symmbol` str: Denotes the name of the market.
            - `timestamp` str: Indicates the time at which the trade took place.
            - `m` bool: Indicates if the buyer is a market maker or not. Values for this parameter are:
                - true: The trader is a market maker.
                - False: The trader is not a market maker.
        """

        if fetchNew:
            limit = 500 if limit > 500 else limit
            data = self.requester('https://public.coindcx.com/market_data/trade_history',params={'pair': symbol_pair, 'limit':limit})
            cghs = {'p': 'price', 'q': 'quantity', 's':'symbol', 'T':'timestamp'}
            for k,v in cghs.items():
                data =  valreplace(data, k,v, True)
        else:
            pass
        return data

    def getOrderBook(self, symbolPair:str, timeStamp:int=0, fetchNew:bool=False, raw:bool=False):
        """
            getOrderBook()
            --------------
            Returns the orderbook of the given asset.

        """
        data = []
        if fetchNew:
            data = setNum(self.requester(f'https://public.coindcx.com/market_data/orderbook?pair={symbolPair}'))
        else:
            if timeStamp == 0:
                d = self.getOrderBook(symbolPair=symbolPair, fetchNew=True)
                for i in ['asks', 'bids']:
                    data += [{'timeStamp' :d['timestamp'], 'type':i, 'price':k} for k,v in d[i].items()] if i in d.keys() else []
            else:
                data = self.dbConn.fetch('orderBook', query=f"timestamp={timeStamp}")
                if len(data) == 0:
                    data = self.getOrderBook(symbolPair=symbolPair)     
                    self.dbConn.json_insert('orderBook', data)
        return data

    def getCandleStick(self, symbolPair:str, interval:str='1m', startTime:int=0,  endTime:int=0, limit:int=500, fetchNew:bool=False):
        """
            getCandleStick()
            ----------------

            Parameter:
            - `pair` str: The symbol pair (like B-BTC_USDT).
            - `interval` Yes	any of the valid intervals given below Values  can be one of the following:
                - `1m` : For 1 Minute Interval
                - `5m` : For 5 Minute Interval 
                - `15m` : For 15 Minute Interval 
                - `30m` : For 30 Minute Interval 
                - `1h` : For 1 Hour Interval 
                - `2h` : For 2 Hour Interval 
                - `4h` : For 4 Hour Interval 
                - `6h` : For 6 Hour Interval 
                - `8h` : For 8 Hour Interval 
                - `1d` : For 1 Day Interval 
                - `3d` : For 3 Day Interval 
                - `1w` : For 1 Week Interval 
                - `1M` : For 1 Month Interval 
            - `startTime` int: Takes the date from when the data is to be fetched in millisecond format. eg: 1562855417000
            - `endTime` int: Takes the date till when the data is to be fetched in millisecond format. eg: 1562855417000
            - `limit`int: Takes the number of candles to be fetched. Default: 500; Max: 1000           
        """
        
        intervalList = ['1m','5m','15m','30m','1h','2h','4h','6h','8h','1d','3d','1w','1M']
        if interval not in intervalList : raise ValueError('The interval provided is not valid. The accepted onces are :' + ','.join(intervalList))
        if fetchNew:
            params = {'pair':symbolPair, 'interval':interval, 'limit': (1000 if limit > 1000 else limit)}
            if startTime!=0:
                params['startTime'] = startTime
            if endTime!=0:
                params['endTime'] = endTime
            data = self.requester('https://public.coindcx.com/market_data/candles', params=params) 
            if data is not None and data is not False:
                data = [{**i, 'symbol':symbolPair, 'interval': interval,'trend':(-1 if ['close'] < i['open'] else 1 if ['close'] > i['open'] else 0)} for i in valreplace(data, 'time', 'timestamp', True)]
                self.logData['tickerdata'][symbolPair] = data[-1]['timestamp']
        else:
            query = [f"symbol='{symbolPair}'"]
            if interval!='': query.append(f"interval='{interval}'")
            if startTime != 0 and endTime != 0: query.append(f"TIME(timestamp) BETWEEN TIME({startTime}) AND TIME({endTime})")
            elif startTime !=0: query.append(f"TIME(timestamp) = TIME({startTime})")
            elif endTime != 0: query.append(f"TIME(timestamp) = TIME({endTime})")
            data = self.dbConn.fetch('candleData',query=query)
            if len(data) == 0:
                data = self.getCandleStick(symbolPair, interval, startTime, endTime, limit, fetchNew=True)
                self.dbConn.json_insert('candleData', data)
        return data

    # user_orders
    def fetchOrders(self):
        """
            fetchOrders()
            -------------
            Use this endpoint to fetch orders and its details
        """
        response = self._fetcher("https://api.coindcx.com/exchange/v1/funding/fetch_orders", 'post')
        return response

    def settle(self, settle_Id:str):
        """
            settle()
            --------
        """
        response = self._fetcher("https://api.coindcx.com/exchange/v1/funding/settle", 'post', {"id": settle_Id,"timestamp": int(round(time.time() * 1000))})
        return response
            
    def lend(self, curency_short_note:str, duration:int=20, amount:float=0.5):
        """
            lend()
            ------
        """
        response = self._fetcher("https://api.coindcx.com/exchange/v1/funding/lend", 'post', {"currency_short_name": curency_short_note,"duration": duration,"amount": amount,"timestamp": int(round(time.time() * 1000))})
        return response

    #place orders
    def newOrder(self, side:str, market:str, price:float, quantity:int=1, order_type:str='limit_order', encode:str='B', leverage:float=0.0, client_order_id:str=''):
        """
            newOrder
            --------
            Create a new order.
            
            Parameters:
            - `side` str: The type of task that is to be ordered. Either its for buying(`buy`) or selling(`sell`).
            - `market` str:	The trade pair (eg: BTCUSDT).
            - `total_quantity` int:Quantity to trade.
            - `quantity` float: Price per unit (not required for market order).
            - `order_type` str:The type of order.
            - `client_order_id`: To add id to the order.
        """

        response = self._fetcher('https://api.coindcx.com/exchange/v1/margin/create', 'post', {"side": side,"order_type": order_type,"market": market,"price_per_unit": price,"total_quantity": quantity, "timestamp": int(round(time.time() * 1000)), "client_order_id": client_order_id})
        return response

    def multiple_orders(self, orders:list):
        """
            multiple_orders()
            -----------------
            Places a multiple/a list of orders in one API call.
            Each Dict in the list should have same keys as `newOrder()`.
        """
        timestamp = int(round(time.time() * 1000))
        data = {"orders": [{**o, "timestamp": timestamp} for o in orders]}
        response = self._fetcher('https://api.coindcx.com/exchange/v1/orders/create_multiple', 'post', data)
        return response

    def orderStatus(self, order_id:str, client_order_id:str=''):
        """
            orderStarus()
            -------------
            Gets the status of the given order id. 
        """

        response = self._fetcher('https://api.coindcx.com/exchange/v1/orders/status', 'post', {"id":order_id, "client_order_id":(client_order_id if client_order_id!='' else None), "timestamp":int(round(time.time() * 1000))})
        return response

    def mulipleOrderStatus(self, order_id:list[str], client_order_id:list[str]=[])->list[dict[str, str | int | float]]:
        """
            mulipleOrderStatus()
            --------------------
            Gets the status of the list of given order id(s).
        """
        response = self._fetcher('https://api.coindcx.com/exchange/v1/orders/status_multiple', 'post', {"id":order_id, "client_order_id":(client_order_id if client_order_id!='' else None), "timestamp":int(round(time.time() * 1000))})
        return response

    def activeOrders(self, market:str='', side:str='')->list|dict:
        """
            This endpoint to fetch trades associated with your account.
        """
        side_list = ['buy', 'sell']
        if side in side_list:
            response = self._fetcher('https://api.coindcx.com/exchange/v1/orders/active_orders', 'post', {'side':side, "market":market,"timestamp":int(round(time.time() * 1000))})
            return response
        else:
            return {i: self.activeOrders(market, i) for i in side_list}

    def orderHistory(self, symbol:str='', limit=''):
        pass

    def edit_target(self, order_id:str, targetPrice:float)->dict[str, str|int]:
        """
            edit_target()
            -------------
            Updates the target price of the order.

            Parameter:
            - `order_id` str: The if of the order that is to be updated.
            - `targetPrice` float: THe new target price/level  
        """
        data =  {'timestamp':int(round(time.time() * 1000)), "id":order_id}
        response = self._fetcher('https://api.coindcx.com/exchange/v1/margin/edit_target', 'post',  data)
        return response

    def edit_target_price(self, order_id:str, target_price:float, itpo_id:str)->dict[str, str|int]:
        """
            edit_price()
            ------------
            Edits the price of internal target order.

            Parameters:
            - `order_id` str:
            - `target_price` float: The new price to buy/sell or close the order position at.
            - `itop_id` str: ID of internal order to edit
        """
        data =  {'timestamp':int(round(time.time() * 1000)),"target_price":target_price, "itpo_id": itpo_id, "id":order_id}
        response = self._fetcher('https://api.coindcx.com/exchange/v1/margin/edit_price_of_target_order', 'post',  data)
        return response

    def edit_stopLoss(self, order_id:str, sl_price:float, traling:bool=False)->dict[str, str|int]:
        """
            edit_stopLoss()
            ------------
            Edits the stop-loss price of the order.

            Parameters:
            - `order_id` str: The system/exchange id of the order.
            - `sl_price` float: The new price of the stop-loss.
            - `trailing` bool: This is to identify for normal stop-loss or a trailing stop-loss.
        """
        t = 'edit_trailing_sl' if traling else 'edit_sl' 
        response = self._fetcher(f'https://api.coindcx.com/exchange/v1/margin/{t}', 'post', {'timestamp':int(round(time.time() * 1000)),"sl_price":sl_price, "id":order_id})
        return response
    
    def add_margin(self, order_id:str, amount:float)->dict[str, str|int]:
        """
            add_margin()
            ------------
            Use this endpoint to add a particular amount to your margin order, decreasing the effective leverage.

            Parameters:
            - `order_id` str: The system/exchange id of the order.
            - `amaount` float:
        """
        response = self._fetcher('https://api.coindcx.com/exchange/v1/margin/add_margin', 'post',  {'timestamp':int(round(time.time() * 1000)),"amount":amount, "id":order_id})
        return response
    
    def remove_margin(self, order_id:str, amount:float):
        """
            remove_margin()
            ---------------
            Use this endpoint to remove a particular amount from your Margin order, increasing the effective leverage.
            Parameters:
            - `order_id` str: The system/exchange id of the order.
            - `amaount` float:
        """
        
        response = self._fetcher("https://api.coindcx.com/exchange/v1/margin/remove_margin", 'post', {'id': order_id, 'amount':amount, 'timestamp':int(round(time.time() * 1000))})
        return response

    def fetch_orders(self, market:str, status:str, size:int=10, details:bool=True,)->list[dict[str, str|float|int|None|bool|list]]:
        """
            fetch_orders()
            --------------
            Use this endpoint to fetch orders and optionally its details which include all buy/sell related orders

            Parameter:
            - `market` str: The trading pair.
            - `status` int: The status of the order.
            - `size` int: Number of records per page, default: 10
            - `details` bool: Whether you want detailed information or not, default: `True`
        """
        if status not in (statusLst:=['init','open','close','rejected','cancelled','partial_entry','partial_close','triggered']):
            raise ValueError(f'The data allowed are : {','.join(statusLst)}')
        
        response = self._fetcher('https://api.coindcx.com/exchange/v1/margin/fetch_orders', 'post', {"details": details,"market": market,"status":status, "size":size, "timestamp": int(round(time.time() * 1000))})
        return response

    def queryOrder(self, order_id:str, details:bool=False)->list[dict[str,str|float|int|list|None]]:
        """
            queryOrder()
            ------------
            Use this endpoint to query specific order and optionally its details.

            Parameters:
            - `order_id` str: Id of the order.
            - `details` bool: Whether you want detailed information or not, default: false
        """
        
        response = self._fetcher('https://api.coindcx.com/exchange/v1/margin/order', 'post', {'timestamp':int(round(time.time() * 1000)), "id":order_id, 'details':details})
        return response

    def cancelOrder(self, order_id:str)->dict[str, str|int]:
        """
            cancelOrder()
            -------------
            Takes the order id and cancels the order.

            Parameter:
            - order_id str: The if of the order that is to be cancled.  
        """
        data =  {'timestamp':int(round(time.time() * 1000)), "id":order_id}
        response = self._fetcher('https://api.coindcx.com/exchange/v1/margin/cancel', 'post',  data)
        return response

    def exit_order(self, order_id:str)->dict[str, str|int]:
        """
            exit_order()
            ------------
            This methods exits an order/closes a position.
        """
        data =  {'timestamp':int(round(time.time() * 1000)), "id":order_id}
        response = self._fetcher('https://api.coindcx.com/exchange/v1/margin/exit', 'post',  data)
        return response
        
    # other funcions

    def shortsAvailable(self):
        """
            shortsAvailable()
            -----------------
            This metod will returns the list of assets and their availability for shorting.
        """
        data = self.requester('https://api.coindcx.com/api/v1/funding/data/company_wallets')
        return data

    ## support functions

    def _fetcher(self, url:str, method:str, data:dict={}, header:dict={}) -> list|dict:
        """
            _fetcher()
            ----------
            This method is responsible for adding necessary info with the special requests that are made for the api.
            
            Parameter:
            - `url` str: The API url of the task.
            - `method`: THe method that is to be used for requesting the url provided.
            - `data` dict: Takes the data that is to be added in the api call. If left empty, then it will by default add the current time in unix format.
            - `header` dict: Thake the header that is to be used while making the API call. If left empy then it will by default add  `{'Content-Type':'application/json','X-AUTH-APIKEY': API_KEY,'X-AUTH-SIGNATURE': SIGNATURE}` to the header requeseted.
        """
        data = data if data=={} else {"timestamp": int(round(time.time() * 1000))}
        data = json.dumps(data, separators=(',', ':'))
        signature = hmac.new(bytes(self.SECRET_KEY, encoding='utf-8'), data.encode(), hashlib.sha256).hexdigest()
        header = {'Content-Type':'application/json','X-AUTH-APIKEY': self.API_KEY,'X-AUTH-SIGNATURE': signature} if header=={} else header
        return self.requester(url, method, data=data, header=header)
        
    def requester(self, url:str, method:str='get', params:dict={}, refer:str='', header:dict={}, data:dict={}, json:dict={}, timeout:int=60, catg:int=1, decode:bool=True, stat_code:int=200):
        """
            A method that returns an object of Requester Class for making HTTP requests.
        
            Parameters:
            - url (str): The URL to which request will be sent.
            - method (str): The metho that will be used to send the request.  Default value is GET.
            - headers (dict): Headers for the request. If no argument provided then default headers are used.
            - params (dict): The PARAMS that are to be send during request.
        """

        # header = self.header if header=={} else {**self.header, **header}
        ret = None
        if catg==1:
            ret = self.request.request(url, method=method, ref=refer, params=params, data=data, header=header, json=json, timeout=timeout)
        elif catg==2:
            if self.sessions==None:
                ret, self.sessions = self.request.requestSessions(url, method=method, ref=refer, params=params, data=data, header=header, timeout=timeout, pre_request='')
            else:
                ret, self.sessions = self.request.requestSessions(url, method=method,  ref=refer, header=header, params=params, data=data, timeout=timeout, sessions=self.sessions)
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

        self._updateLogs('requester', f'{url}-{stat}', res)
        return ret
  
    def local_runner(self, catg:str='')->None:
        """
            local_runner()
            --------------
            Runner function to be used by other functions.

            Parameter:
                - catg (str): Takes the func key the is to be runned.
        """
        self.check_db()
        runner = {
            '':{'func':None, 'params': {}},
        }
        if catg=='':
            for k,v in runner.items():
                v['func'](**v['params']) if v['params']!={} else v['func']()
                print(f"Done with '{k}'")
        elif catg!='' and catg in runner.keys():
            v['func'](**v['params']) if v['params']!={} else v['func']()
        else:
            raise ValueError('The value passed is invalid.')

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

        db_tables = self.dbConn.getTb()
        tables = [
            {'table_name': 'logbook', 'column_names':['name TEXT', 'value TEXT', 'result INTEGER'], 'indexes': ''},
            {'table_name': 'assets', 'column_names':['id INTEGER PRIMARY KEY AUTOINCREMENT','name TEXT', 'symbol TEXT', 'base_currency_short_name TEXT', 'target_currency_short_name TEXT', 'target_currency_name TEXT', 'base_currency_name TEXT', 'min_quantity REAL', 'max_quantity REAL', 'max_quantity_market REAL', 'min_price REAL', 'max_price REAL', 'min_notional REAL', 'base_currency_precision INTEGER', 'target_currency_precision INTEGER', 'step REAL', 'order_types TEXT', 'ecode TEXT', 'bo_sl_safety_percent REAL', 'max_leverage INTEGER', 'max_leverage_short INTEGER', 'pair TEXT', 'status TEXT'], 'indexes': 'name, symbol'},
            {'table_name': 'asset_live_status', 'column_names':['id INTEGER PRIMARY KEY AUTOINCREMENT', 'market REAL', 'change_24_hour REAL', 'high REAL', 'low REAL', 'volume REAL', 'last_price REAL', 'bid REAL', 'ask REAL', 'timestamp INTEGER'], 'indexes': 'market'},
            # {'table_name': 'orderBook', 'column_names':['id INTEGER PRIMARY KEY AUTOINCREMENT',], 'indexes': 'name, symbol'},
            # {'table_name': 'tradeHistory', 'column_names':['id INTEGER PRIMARY KEY AUTOINCREMENT',], 'indexes': 'name, symbol'},
            {'table_name': 'candleData', 'column_names':['id INTEGER PRIMARY KEY AUTOINCREMENT', 'symbol TEXT','interval TEXT','timestamp INTEGER', 'open REAL', 'high REAL', 'low REAL', 'close REAL','volume REAL', 'avgPrice REAL', 'direction REAL', 'body_height REAL', 'upperWick REAL', 'lowerWick REAL', 'totalHeight REAL'], 'indexes': 'symbol,interval,timestamp'},
            ]
        for i in tables:
            if i['table_name']!='' and i['table_name'] not in db_tables:
                self.dbConn.createTb(i['table_name'], i['column_names'], indexCol=i['indexes'])
                print(i['table_name'], ' Created..')

    def _updateLogs(self, name, value:str, success:int) -> bool:
        """
            updateLogs
            ----------
            This method when fired will log the events that have occured or tasks executed by the class.
        """
        t = self.dbConn.insert('logbook', ['name', 'value', 'result'], [name, value, success])
        return True if t else False

    def exportDate(self, table_name:str=''):
        """
            exportDate()
            ------------
            
            This method exports the data of the database into a file. 
        """
        self.dbConn.export_data(tableName=table_name)

        