import matplotlib as plt
from datetime import datetime, date
from custom_functions.DataHandlers import DataHandler


class NSEQUOTE:
    def __init__(self, symbol, handle=None, init_trade:bool=False):
        self.symbol, self.handle = symbol.upper(), handle
        self.info = self.handle.get_quoteInfo(symbol, 'eq')
        if 'error' in self.info.keys():
            self.info = self.handle.get_quoteInfo(symbol, 'der')
            self.idf = self.info['info']['symbol']
            self.quoteType='derivative'
        else:
            self.idf = self.info['info']['identifier']

        self.lastClose=None

    def get_chart(self,chartType='line', preopen:bool=True, file_path=None, plotShow:bool=True, dwnld:bool=False, dwnld_format:str='png', dpi:int=200):
        """
            Plots the asset's full day ticker/stock-prices in a 2d line chart.
        """
        plt.close()
        if preopen:
            po = self.get_ticker(True, False)
        intrd = self.get_ticker()

        if chartType=='line':
            if preopen:
                plt.plot(po['timestamp'], po['tick'],label='Pre-Open')
            plt.plot(intrd['timestamp'], intrd['tick'],label='Intraday')

        plt.legend()
        plt.title(f'{self.symbol} Chart')

        if dwnld==True:
            path = file_path if file_path!=None else self.handle.storagePath
            c = date.today()
            c, ft = c.strftime("%Y-%m-%d"),  dwnld_format.lower()
            plt.savefig(f'{path}/{self.symbol}_{c}_Chart.{ft}', format=ft, dpi=dpi)

        if plotShow==True:
            plt.show()
        else:
            plt.close()

    def get_ticker(self, preopen:bool=False, intra:bool=True, organize:bool=True, ch_format:bool=False, timestamp:str='%d-%m-%Y %H:%M:%S %p %A'):
        """This method is used to get the ticker data i.e. data of each second of the given symbol.
           :param string symbol: Takes the name of the asset which is to be fetched.
           :param string timestamp: An optional parameter within is to modify the timestam data into the desired format.
           :return dict: Return the data in a dict format.
        """
        param={'index':self.idf, 'indices':None}
        ticker = []
        ref = 'https://www.nseindia.com/get-quotes/equity?symbol={self.symbol}'
        if preopen:
            param['preopen']= 'true'
            ticker += self.handle.requester('chart-databyindex', refer=ref, params=param).json()['grapthData']
        if intra:
            param['preopen'] = None
            ticker += self.handle.requester('chart-databyindex', refer=ref, params=param).json()['grapthData']

        if organize==False: return ticker
        else:
            data_ref = DataHandler.list_dlist(ticker, ['timestamp','tick'])
            if ch_format==True:
                data_ref['timestamp'] = DataHandler.timestamp(data_ref['timestamp'], timestamp)
            return data_ref

    def get_ohlcv(self, timeframe:int=60, preopen:bool=True, indices:bool=True, date_format=None):
        """_summary_

        Args:
            timeframe (int, optional): _description_. Defaults to 60.
            preopen (bool, optional): _description_. Defaults to True.
            indices (bool, optional): _description_. Defaults to True.
            date_format (_type_, optional): _description_. Defaults to None.
        """
        tick = self.get_ticker(preopen=preopen, indices=indices)
        tick_f=tick['timestamp'][0]
        end_tf = timeframe * (10*(len(str(tick_f)) - len(str(datetime.now())) ))
        tick_end = tick_f + end_tf
