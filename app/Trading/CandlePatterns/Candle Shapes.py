#Candle Shapes
from custom_functions.DataHandlers import dict_lister

## Hammer & Hangging Man
def cd_hammer(data, win=10, trck=None, up_diff=2, lw_diff=0.5, trend_h=0.5, opt='open'):
    '''
        The detcet hammer is a function that detects the hammer patterns and then show the trend in which the next candles are likily to go.
    '''
    le = len(data)
    if win < le:
        u = hammer(data=data, win=win, trck=trck, up_diff=up_diff, lw_diff=lw_diff, trend_h=trend_h, opt=opt)
        d = hanggingman(data=data, win=win, trck=trck, up_diff=up_diff, lw_diff=lw_diff, trend_h=trend_h, opt=opt)
        return {'up':u,'dwn':d}
    else:
        raise ValueError(f'Given look back(win) is {win}. Needs to be greater than length of provided data.')

## Hanging Man
def hanggingman(data, win=10, trck=None,up_diff=2, lw_diff=0.5, trend_h=0.5, opt='open'):
    le = len(data)
    if trend_h < 0: return
    if win < le:
        trck = trck if (trck != None) else win
        x, y =[], []
        d = dict_lister(data,['index', opt, 'upperWik','bodyheight','lowerWik']) # arranging data into list
        for i in range(win, le):
            bdh, uwk, lwk, ind, op = d['bodyheight'][i], d['upperWik'][i], d['lowerWik'][i], d['index'][i], d[opt][i]
            tr = get_trend(data[i-trck:i-1])
            th, tr = tr['mark'], tr['trend']
            if (uwk/bdh >= up_diff) and (lwk/uwk <= lw_diff) and tr == 1 and th >= trend_h:
                x.append(ind)
                y.append(op)
        return {'x':x,'y':y}

## Hammer
def hammer(data, win:int=10, trck:int=None, up_diff:float=2, lw_diff:float=0.5, trend_h:float=0.5, opt:str='open'):
    le = len(data)
    if trend_h < 0: return
    if win < le:
        trck = trck if (trck != None) else win
        x, y =[],[]
        d = pri_lister(data,['index', opt, 'upperWik','bodyheight','lowerWik']) # arranging data into list
        for i in range(win, le):
            bdh, uwk, lwk, ind, op = d['bodyheight'][i], d['upperWik'][i], d['lowerWik'][i], d['index'][i], d[opt][i]
            tr = get_trend(data[i-trck:i-1])
            th, tr = tr['mark'], tr['trend']
            if (lwk/bdh >= up_diff) and (uwk/lwk <= lw_diff) and tr == -1 and th <= -trend_h:
                x.append(ind)
                y.append(op)

        return {'x':x,'y':y}

##Morning Star
def morningstar(data):
    le = len(data)
    if le > 3:
        trd = get_trend(data)['trend']
        data = pri_lister(data, ['index','bodyheight'])
        idx, bdh, trd = data['index'], data['bodyheight'], data['trend']
        for i in range(3, le, 6):
            o, ol = (i-3), (i+3)

##Evening Star
def eveningstar(data):
    le = len(data)
    data = pri_lister(data, ['index','bodyheight', 'trend'])
    for i in range(3, le, 6):
        pass

## Bullish Engulfing
def bull_engulf(data, cd_height=0.003):
    le = len(data)
    if le >= 2:
        eng,data = {'x':[],'y':[]},pri_lister(data, ['index','open','bodyheight', 'trend'])
        idx, bdh, tre, op = data['index'], data['bodyheight'], data['trend'], data['open']
        for i in range(1, le):
            if bdh[i] >= cd_height and bdh[i-1] >= cd_height and bdh[i] > bdh[i-1] and tre[i] == 1 and tre[i-1] == -1:
                eng['x'].append(idx[i])
                eng['y'].append(op[i])
        return eng

##Bearish Engulfing
def bear_engulf(data, cd_height=0.003):
    le = len(data)
    if le >= 2:
        eng,data = {'x':[],'y':[]},pri_lister(data, ['index','open','bodyheight', 'trend'])
        idx, bdh, tre, op = data['index'], data['bodyheight'], data['trend'], data['open']
        for i in range(1, le):
            if bdh[i] >= cd_height and bdh[i-1] >= cd_height and bdh[i] < bdh[i-1] and tre[i] == -1 and tre[i-1] == 1:
                eng['x'].append(idx[i])
                eng['y'].append(op[i])
        return eng