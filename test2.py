# from app.NseHandler import NSEHandler
from app.MoneyControlHandler import MoneyControlHandler
from FileHandler import read, write
# from DataHandlers import generate_time_intervals, json_parser, to_human_readable

mch = MoneyControlHandler(False)


file = './data/Nifty 50.json'
# write(file, 
data = read(file, '\n', decode_json=True)[0]





# for k,v in  mch.dbConn.get_info().items():
    # if v['rows'] > 0: print(k, v['rows'])




exit()
# nse = NSEHandler(False, False)
# fom:str ='%Y-%m-%d'

# data = read('./data/NIFTY_opt.json', decode_json=True)
# print(data.keys())
# underlyingValue:int

# data = nse.get_press_release(fetchNew=True)
# write('./data/get_press_release.json', data, '\n')
# # print(data)

# nse.get_optionChain('NIFTY')
# nse.get_optionChain('BANKNIFTY')
# nse.get_optionChain('FINNIFTY')




# expiry = nse.getOptionExpiry('NIFTY')
# expd:str= expiry[0] 
# data:list= [i for i in nse.get_optionChain('NIFTY', fetchNew=True) if i['expiryDate'] == expd]
# underlyingValue = data[0]['underlyingValue']

# prices = nse.getOptionPrices('NIFTY')
# pos = prices.index(round(underlyingValue, -2))
# fo = prices[pos-10:pos+10]
# data = [i for i in data if i['strikePrice'] in fo]
# ce_data = [i for i in data if i['catg'] == 'CE']
# pe_data = [i for i in data if i['catg'] == 'PE']
# print(ce_data[0], pe_data[0])

