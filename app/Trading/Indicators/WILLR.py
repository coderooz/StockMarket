from custom_functions.DataHandlers import DataHandler


def willr(data, n:int=14, add_index:bool=False):
    le = len(data)
    if n > le: return
    data = pri_lister(data, ['high', 'low', 'close', 'index'])
    hi, lw, cls = data['high'], data['low'], data['close']
    willrv=[]
    for i in range(n, le):
        hh, ll = max(hi[i-n: i]), min(lw[i-n: i])
        r = -100 * ((hh - cls[i])/(hh - ll))
        willrv.append(r)
    if add_index:
        ind = data['index'][n:]
        return {'x':ind, 'y':willrv}
    return willrv
