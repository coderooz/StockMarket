##Triangular Moving Average (TRIMA)

def TRIMA(data, length:int=14, data_on='close'):
    n,le,smam = round(length + 1),len(data), []
    dat = pri_lister(data, ['index', data_on])
    for i in range(length, le):
        cls = dat[data_on]
        smam.append(sum(cls, n)/n)
        ma = moving_average(cls[i-length:i], length, data_on)
