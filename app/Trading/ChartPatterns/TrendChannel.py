## Trend Channel

import numpy as np
import pandas as pd

def trend_channel(price, candleid, backcandles=40, brange=10, wind=6, sidf=10000, showChart:bool=False, othervalues:bool=False):
    """
        trend_channel()
        ---------------

        This method finds the trend channels i.e. the upper trend line as well as the low trend line that fits the candles patters well according to the specified parameters.
    """

    optbackcandles, sldiff= backcandles, sidf
    df=pd.DataFrame(price)
    for r1 in range(backcandles-brange, backcandles+brange):
        maxim = np.array([])
        minim = np.array([])
        xxmin = np.array([])
        xxmax = np.array([])
        for i in range(candleid-r1, candleid+1, wind):
            minim = np.append(minim, df.low.iloc[i:i+wind].min())
            xxmin = np.append(xxmin, df.low.iloc[i:i+wind].idxmin())
        for i in range(candleid-r1, candleid+1, wind):
            maxim = np.append(maxim, df.high.loc[i:i+wind].max())
            xxmax = np.append(xxmax, df.high.iloc[i:i+wind].idxmax())
        slmin, intercmin = np.polyfit(xxmin, minim, 1)
        slmax, intercmax = np.polyfit(xxmax, maxim, 1)

        if(abs(slmin-slmax)<sldiff):
            sldiff = abs(slmin-slmax)
            optbackcandles=r1
            slminopt = slmin
            slmaxopt = slmax
            intercminopt = intercmin
            intercmaxopt = intercmax
            maximopt = maxim.copy()
            minimopt = minim.copy()
            xxminopt = xxmin.copy()
            xxmaxopt = xxmax.copy()

    dfpl = df[candleid-wind-optbackcandles-backcandles:candleid+optbackcandles]
    adjintercmax = (df.high.iloc[xxmaxopt] - slmaxopt*xxmaxopt).max()
    adjintercmin = (df.low.iloc[xxminopt] - slminopt*xxminopt).min()
    channel = {'min':{'x':xxminopt, 'y':slminopt*xxminopt + adjintercmin},'max':{'x':xxmaxopt, 'y':slmaxopt*xxmaxopt + adjintercmax}}
    # if showChart:
    #     fig = go.Figure(data=[go.Candlestick(x=dfpl.index,open=dfpl['open'],high=dfpl['high'],low=dfpl['low'],close=dfpl['close'])])
    #     fig.add_trace(go.Scatter(x=channel['min']['x'], y=channel['min']['y'], mode='lines', name='min slope'))
    #     fig.add_trace(go.Scatter(x=channel['max']['x'], y=channel['max']['y'], mode='lines', name='max slope'))
    #     fig.show()
    if othervalues:
        other = {'optbackcandles':optbackcandles, 'slminopt':slminopt, 'slmaxopt':slmaxopt, 'interceptmin':intercminopt, 'interceptmax':intercmaxopt, 'adj_interc_max':adjintercmax, 'adj_interc_min':adjintercmin}
        return [channel, other]
    return channel

