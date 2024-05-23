


def ma_check(data, data_on):
    dty = type(data)
    if dty == list:
        dty1 = type(data[0])
        if dty1 == dict:
            drr = pri_lister(data, ['index',data_on])
            ind, cls = drr['index'], drr[data_on]
        elif dty1 == float:
            cls, add_id = data, False
    elif dty == dict and data[data_on] == list:
        ind, cls = data['index'], data[data_on]
    else:
        raise ValueError ('Data provided is invalid.')

###Simple Moving Average

def sma(data, n:int=10, data_on:str='close', add_id:bool=False):
    data_len = len(data)
    if data_len < n:raise ValueError(f'The amount of data that the MA would be taken  can not be higher than the amount of data given. \nHere data length is {data_len}')
    dtr = pri_lister(data, ['index', data_on])
    calv = dtr[data_on]

    if dty == list :
        dty1 = type(data[0])
        if dty1 == dict:
            drr = pri_lister(data, ['index',data_on])
            ind, cls = drr['index'], drr[data_on]
        elif dty1 == float:
            cls, add_id = data, False
    elif dty == dict and data[data_on] == list:
        ind, cls = data['index'], data[data_on]
    else:
        raise ValueError('Data provided is invalid.')

    y = [sum(calv[i-n:i])/n for i in range(n, data_len)]
    if add_id:
        return {'x':dtr['index'][n:data_len],'y': y}
    return y

###Exponential Moving Average

def ema(data, n=10, data_on='close', add_id:bool=False):
    data_len = len(data)
    if data_len < n:raise ValueError(f'The amount of data that the MA would be taken  can not be higher than the amount of data given. \nHere data length is {data_len}')

    k = 2 / (n + 1)
    dty = type(data)

    if dty == list :
        dty1 = type(data[0])
        if dty1 == dict:
            drr = pri_lister(data, ['index',data_on])
            ind, cls = drr['index'], drr[data_on]
        elif dty1 == float:
            cls, add_id = data, False
    elif dty == dict and data[data_on] == list:
        ind, cls = data['index'], data[data_on]
    else:
        raise ValueError ('Data provided is invalid.')

    ema = [cls[0]]
    for i in range(1, data_len):
        ema.append((cls[i] * k) + (ema[i - 1] * (1 - k)))
    if add_id:
        return {'x':ind,'y':ema}
    return ema

### Double Exponential Moving Average

def dema(data, n:int=14, data_on:str='close',add_id:bool=False):
    data_len = len(data)
    if data_len < n: raise ValueError(f'The amount of data that the MA would be taken  can not be higher than the amount of data given. \nHere data length is {data_len}')

    ema1 = ema(data, n=n, data_on=data_on)
    ema2 = ema(ema1, n=n)

    dema_v = [2 * ema1[0] - ema2[0]]
    for i in range(1, len(ema1)):
        dema_v.append(2 * ema1[i] - ema2[i])
    if add_id:
        return {'x': pri_lister(data, ['index']), 'y':dema_v}
    return dema

###Triple Exponential Moving Average

def tema(data, n:int=14, data_on:str='close',add_id:bool=False):
    data_len = len(data)
    if data_len < n: raise ValueError(f'The amount of data that the MA would be taken  can not be higher than the amount of data given. \nHere data length is {data_len}')

    ema1 = ema(data, n=n, data_on=data_on)
    ema2 = ema(ema1, n=n)
    ema3 = ema(ema2, n=n)

    tema_v = [3 * ema1[0] - 3 * ema2[0] + ema3[0]]
    for i in range(1, len(data)):
        tema_v.append(3 * ema1[i] - 3 * ema2[i] + ema3[i])

    if add_id:
        return {'x': pri_lister(data, ['index']), 'y':tema_v}
    return tema

### Wma

def wma(data, n:int=14, data_on:str='close',add_id:bool=False):
    data_len, dty = len(data), type(data)

###Dwma

def dwma(data, n:int=14, data_on:str='close',add_id:bool=False):
    data_len, dty = len(data), type(data)

###Adaptive Moving Average (AMA)

def ama(prices, period, sigma):
    le = len(prices)


    weights = []
    for i in range(-int((period - 1) / 2), int((period - 1) / 2) + 1):
        weight = (1 / (sigma * (2 * np.pi) ** 0.5)) * np.exp(-0.5 * (i / sigma) ** 2)
        weights.append(weight)
    weights = weights / sum(weights)
    alma = []
    for i in range(n - period + 1):
        weighted_sum = 0
        for j in range(period):
            weighted_sum += prices[i + j] * weights[j]
        alma.append(weighted_sum)
    return alma

###Ma

def ma(data, n:int=14, data_on:str='close',add_id:bool=False, maType:str='sma'):
    _matype = {'sma':sma,'ema':ema,'dema':dema,'tema':tema,'ama':ama, 'dwma':dwma,'wma': wma}
    if maType not in _matype: return ValueError (f'maType given {maType} is invalid. Posible values are '+ '\n •'.join(_matype))
    else: return _matype[maType](data, n=n, data_on=data_on,add_id=add_id)
