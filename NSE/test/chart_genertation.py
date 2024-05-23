from fpdf import FPDF
from Projects.StockMarket.NseHandler import NSEHandler
# from custom_functions.DbHandler import SqliteHandler
from custom_functions.DataHandlers import dict_lister
from custom_functions.FileHandler import write
import plotly.graph_objects as go
import plotly.express as px

dbpath = './storage/db'
dbName = 'NSE.db'
nse = NSEHandler(dbName,dbpath, False)
# nse.getFII_DII('16-04-2024')
# nse.getFII_DII('18-04-2024')
# nse.getFII_DII('19-04-2024')

def make_charts(data, table_lists, catg, client_type, filePath:str='.'):
    imgList = []
    listedData = dict_lister(data) 
    for i in table_lists:
        file_title = f'{client_type} {' '.join(i.capitalize() for i in i[0].split('_')[:-1])} {catg} Data' 
        t = go.Bar(x=listedData['date'], y=listedData[i[0]], name='Long/Buys')
        t2 = go.Bar(x=listedData['date'], y=listedData[i[1]], name='Short/Sells')
        layout = go.Layout(title=file_title, xaxis=dict(type='category'),barmode='group')
        fig = go.Figure(data=[t, t2], layout=layout)
        fig.write_image(fileName:=f'{filePath}/{file_title}.png', format='png')
        imgList.append({fileName:i})
    return imgList

setList = [['future_index_long', 'future_index_short'], ['future_stock_long', 'future_stock_short'], ['option_index_call_long', 'option_index_call_short'], ['option_index_put_long', 'option_index_put_short'], ['option_stock_call_long', 'option_stock_call_short'], ['option_stock_put_long','option_stock_put_short']]
#  data chart
img_list:dict = {'data':{}, 'analysis': {}, 'net':''}
filePath = './storage/NSE/report'
for catg in nse.dbConn.fetch_unique('FII_DII','catg'):
    if catg not in img_list['data'].keys(): img_list['data'][catg] = {}
    for client_type in nse.dbConn.fetch_unique('FII_DII', 'client_type'):
        query = [f"catg='{catg}'", f"client_type='{client_type}'"]
        getData = nse.dbConn.fetch('FII_DII', query=query, limit=5, desc='date')[::-1]
        img_list['data'][catg][client_type] = make_charts(getData,setList, catg, client_type)

#  analysis
analysis_data = {}
for catg in nse.dbConn.fetch_unique('FII_DII','catg'):
    analysis_data[catg] = {}
    for client_type in nse.dbConn.fetch_unique('FII_DII', 'client_type'):
        query = [f"catg='{catg}'", f"client_type='{client_type}'"]
        getData = nse.dbConn.fetch('FII_DII', query=query, limit=5, desc='date')[::-1]
        analysis_data[catg][client_type] = []
        for i in range(1, len(getData)):
            analysis_data[catg][client_type].append({'date': getData[i]['date'],'future_index_long': (getData[i-1]['future_index_long'] - getData[i]['future_index_long']),'future_index_short': (getData[i-1]['future_index_short'] - getData[i]['future_index_short']),'future_stock_long': (getData[i-1]['future_stock_long'] - getData[i]['future_stock_long']),'future_stock_short': (getData[i-1]['future_stock_short'] - getData[i]['future_stock_short']),'option_index_call_long': (getData[i-1]['option_index_call_long'] - getData[i]['option_index_call_long']),'option_index_call_short': (getData[i-1]['option_index_call_short'] - getData[i]['option_index_call_short']),'option_index_put_long': (getData[i-1]['option_index_put_long'] - getData[i]['option_index_put_long']),'option_index_put_short': (getData[i-1]['option_index_put_short'] - getData[i]['option_index_put_short']),'option_stock_call_long': (getData[i-1]['option_stock_call_long'] - getData[i]['option_stock_call_long']),'option_stock_put_long': (getData[i-1]['option_stock_put_long'] - getData[i]['option_stock_put_long']),'option_stock_call_short': (getData[i-1]['option_stock_call_short'] - getData[i]['option_stock_call_short']),'option_stock_put_short': (getData[i-1]['option_stock_put_short'] - getData[i]['option_stock_put_short'])})   #type: ignore
write(f'{filePath}/report_data.json', analysis_data, emptyPervious=True)
img_list['analysis'] = analysis_data

imgList2 = []
listedData = dict_lister(analysis_data['oi']['FII']) 
for i in setList:
    file_title = f'FII {' '.join(i.capitalize() for i in i[0].split('_')[:-1])} OI Report Data' 
    t = go.Bar(x=listedData['date'], y=listedData[i[0]], name='Long/Buys')
    t2 = go.Bar(x=listedData['date'], y=listedData[i[1]], name='Short/Sells')
    layout = go.Layout(title=file_title, xaxis=dict(type='category'),barmode='group')
    fig = go.Figure(data=[t, t2], layout=layout)
    fig.write_image(fileName:=f'{filePath}/{file_title}.png', format='png')
    imgList2.append({fileName:i})
img_list['analysis_img'] = imgList2

# net
net_outCome = {n['date']:{i[0].replace('_long', ''): (n[i[0]] - n[i[1]]) for i in setList} for n in analysis_data['oi']['FII']}
kx = list(net_outCome.keys())
vals = dict_lister(list(net_outCome.values()))
file_title = 'Net Data'
layout = go.Layout(title=file_title, xaxis=dict(type='category'),barmode='group')
fig = go.Figure(data=[go.Bar(x=kx, y=v, name=k) for k,v in vals.items()]  , layout=layout)
fig.write_image(netName:=f'{filePath}/{file_title}.png', format='png')

img_list["net"] = net_outCome
write('./report.json', img_list, emptyPervious=True)


