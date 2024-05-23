

def macd(prices, n_fast=12, n_slow=26, n_signal=9, data_on='close', setall=True):
    fast_ma = exponential_moving_average(prices, n_fast, data_on=data_on)
    slow_ma = exponential_moving_average(prices, n_slow, data_on=data_on)
    min_len = min(len(fast_ma), len(slow_ma))
    fast_ma = fast_ma[:min_len]
    slow_ma = slow_ma[:min_len]
    macd = get_differences(fast_ma, slow_ma)
    signal = []
    for i in range(len(macd)):
        if i < n_signal:
            signal.append(exponential_moving_average({'macd':macd[:i+1]}, i+1, data_on='macd'))
        else:
            signal.append(exponential_moving_average({'macd':macd[i-n_signal+1:i+1]}, n_signal, data_on='macd'))
    signal = [s[0] for s in signal]
    zeroline = sum(macd)/len(macd)+sum(signal)/len(signal)/2
    sig_diff = get_differences(macd, signal)
    if setall:
        return macd, signal, zeroline, sig_diff
    return macd[-1], signal[-1], zeroline, sig_diff[-1]
