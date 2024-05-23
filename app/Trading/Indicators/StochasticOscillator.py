
def StochasticOscillator(data, n=14, data_on:str='close', add_index:bool=False):
    le = len(data)
    k, d, data = [], [], pri_lister(data, ['index', data_on])
    cls = data[data_on]
    for i in range(n, le):
        cl = cls[i-n:i]
        l, h = min(cl), max(cl)
        value = 100 * (cls[i] - l) / (h - l)
        k.append(value)
        if i >= n+2:
            d.append(statistics.mean(k[-3:]))
        else:
            d.append(0)
    if add_index:
        return {'x':data['index'][n:], 'y1':k, 'y2':d}
    return k, d

