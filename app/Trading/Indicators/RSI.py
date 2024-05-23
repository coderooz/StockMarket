


def RSI(prices, n=14, data_on='close', add_all:bool=False):
    gains, losses, rsi_values= [], [], []
    prices = pri_lister(prices, [data_on])
    for i in range(1,len(prices)):
        change = prices[i] - prices[i-1]
        if change > 0:
            gains.append(change)
        elif change < 0:
            losses.append(-change)
        if i >= n:
            avg_gain = sum(gains[-n:]) / n
            avg_loss = sum(losses[-n:]) / n
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            rsi_values.append(rsi)
        else:
            rsi_values.append(0)
    if add_all==True:
       return rsi_values
    return rsi_values[-1]
