from DataHandlers import add_index

def candleInfo(prices, index=False, indexName='index'):
    pri_typ = type(prices)
    if isinstance(prices, dict):
        high = prices['high'] if ('high' in prices.keys() and prices['high'] != None) else 0
        open = prices['open'] if ('open' in prices.keys() and prices['open'] != None) else 0
        close = prices['close'] if ('close' in prices.keys() and prices['close'] != None) else 0
        low = prices['low'] if ('low' in prices.keys() and prices['low'] != None) else 0
        prices['trend'] = 1 if (open < close) else -1

        uw = (high - open) if (open > close) else (high-close)
        lw = (open-low) if (open > close) else (close-low)
        bh = (open-close) if (open > close) else (close-open)

        prices['upperWik'] = uw
        prices['lowerWik'] = lw
        prices['bodyheight'] = bh
        prices['cd_height'] = uw + lw + bh
        return prices
    elif isinstance(prices, list):
        prices = add_index(prices, indexName) if index else prices
        return [candleInfo(d) for d in prices]
    else: raise TypeError('Prices parameter only accepts only a dict or a list(dict)')
