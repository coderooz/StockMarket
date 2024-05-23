from DataHandlers import valreplace, setNum, flatten_dict, list_dlist, dlist_dict, dict_filter, equalizer_dict, space_remover, generate_time_intervals, get_previous_day, dateFormat
from Requester import Requester
from datetime import datetime, timedelta, time
from app.utils import utils

class BSEHandler:

    def __init__(self,  runLocal:bool=True) -> None:
        self.dbConn = utils.connect_db('BSE.db', './app/database')
        self.request = Requester()

    def requester(self, url:str, method:str='get', params:dict={}, refer:str='', header:dict={}, json:dict={}, timeout:int=60, catg:int=1, decode:bool=True, stat_code:int=200)->(str|dict|Any):
        """
            A method that returns an object of Requester Class for making HTTP requests.
        
            Parameters:
                - `url` (str): The URL to which request will be sent.
                - `method` (str, optional): The method that will be used to send the request. Default value is GET.
                - `headers` (dict, optional): Headers for the request. If no argument provided then default headers are used.
                - `params` (dict, optional): The PARAMS that are to be send during request.
                - `refer` (str, optional): The page refered url.
                - `json` (dict, optional): The payload/data in dict format that is to be send during the requeset.
                - `timeout` (int, optional): The amount of seconds, the code will wait for the response to be recived. Default is 60.
                - `catg` (int, optiona): Dictates whether the request will be with a sessions or not. Default is 1.The values signify are:
                                        - `1` : Will request without a sessions. 
                                        - `2` : Will request with a sessions. 
                - `decode` (bool, optional): Dictates whether the response should be decoded as text or json. Default is `True`.
                - `stat_code` (int, optional): The response code that will determine whether the response is successfull or not. Default is `200`.
        """

        # header = self.header if header=={} else {**self.header, **header}
        ret = None
        if catg==1:
            ret = self.request.request(url, method=method,  ref=refer, params=params, header=header, json=json, timeout=timeout)
        elif catg==2:
            if self.sessions==None:
                ret, self.sessions = self.request.requestSessions(url, method=method, ref=refer, params=params, header=header, timeout=timeout, pre_request='https://www.nseindia.com')
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
            '':{'func':'', 'params': {'':True}},
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
            {'table_name': 'logbook', 'column_names':['id INTEGER PRIMARY KEY','name TEXT', 'value TEXT', 'result INTEGER', 'time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP'], 'indexes': 'name,result,time_stamp'},
            {'table_name': '', 'column_names':[], 'indexes': ''},
            {'table_name': '', 'column_names':[], 'indexes': ''},
            ]
        runTabs = []
        for i in tables:
            if i['table_name']!='' and i['table_name'] not in db_tables:
                self.dbConn.createTb(i['table_name'], i['column_names'], indexCol=i['indexes'])
                runTabs.append(i['table_name'])
                print(i['table_name'], ' Created..')
        self._updateLogs('',','.join(runTabs), 1)

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

