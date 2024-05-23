##ATR

def ATR(prices, n=14, data_on:str='close'):
    '''Average True Range (ATR)'''
    prices = pri_lister(prices, ['high', 'low', data_on])
    # Initialize variables
    atr_values = []
    tr_values = []
    # Loop through the prices
    for i in range(1, len(prices[data_on])):
        cp, h, l = prices[data_on][i-1], prices['high'][i], prices['low'][i]
        tr_values.append(max((h-l),(h-cp),(l-cp)))

        # Calculate the ATR
        if i < n:
            atr = 0 #sum(tr_values[i-n:i]) / i
        else:
            atr = sum(tr_values[-n:]) / n
        atr_values.append(atr)

    # Return the ATR values
    return atr_values
