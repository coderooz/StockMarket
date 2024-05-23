import streamlit as st
import time
import plotly.graph_objects as go
from app.NseHandler import NSEHandler
from DataHandlers import dict_lister, dateFormat
# from app.utils.utils import setup_db
# import time # to simulate a real time data, time loop 

nse = NSEHandler(False, False)
fom:str ='%Y-%m-%d'

st.set_page_config(page_title ='Stock Dashboard',layout = 'wide')

st.title('Stock Dashboard')
st.markdown("Prototype v0.0.1")

# @st.cache_data
def getOpt(catg:str, expd:str, optType:str, limit):

    underlyingValue:int
    data:list

    data = [i for i in nse.get_optionChain(catg, fetchNew=True) if i['expiryDate'] == expd]
    underlyingValue = data[0]['underlyingValue']

    prices = nse.getOptionPrices(catg)
    pos = prices.index(round(underlyingValue, -2))
    fo = prices[pos-limit:pos+limit]

    data = [i for i in data if i['strikePrice'] in fo]
    ce_data = dict_lister([i for i in data if i['catg'] == 'CE'], ['strikePrice','openInterest', 'lastPrice', 'catg']) 
    pe_data = dict_lister([i for i in data if i['catg'] == 'PE'], ['strikePrice','openInterest', 'lastPrice', 'catg'])

    t = go.Bar(x=ce_data['strikePrice'], y=ce_data[optType], name='CE Data', marker={'color': 'green'})
    t2 = go.Bar(x=pe_data['strikePrice'], y=pe_data[optType], name='PE Data', marker={'color': 'red'})
    
    fig = go.Figure(data=[t, t2], layout=go.Layout(title=f'{str(underlyingValue)} {optType.capitalize()} Chart', xaxis=dict(type='category'),barmode='group'))
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    # return fig

def getTicker():
    pass    

fig_col1, fig_col2, fig_col3, fig_col4 = st.columns(4)
with fig_col1:
    catg = st.selectbox('Select Category', ['NIFTY','BANKNIFTY','FINNIFTY'])
with fig_col2:
    # client_type = st.selectbox('Select Client Type', ['openInterest', 'lastPrice'])
    pass
with fig_col3:
    expiry = nse.getOptionExpiry(catg)
    expd = st.selectbox('Expiry Date', expiry)
with fig_col4:
    range = st.number_input('Select Range', value=10, min_value=5, max_value=50)

placeholder = st.empty()
n = 1
range_o:bool = False
while True:
    with placeholder.container():
        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            getOpt(catg, expd, 'openInterest', range)            
            
        with fig_col2:
            getOpt(catg, expd, 'lastPrice', range)

        time.sleep(2)
        st.markdown(n)
        if n > 200:
            break
        else:
            n+=1

# print(nse.dbConn.getColumnNames('optionChain'))
