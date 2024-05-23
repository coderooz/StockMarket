

def mom(data, n:int=14, data_on:str='close', add_index:bool=False):
    le = len(data)
    if le < n: return
    data = pri_lister(data, [data_on, 'index'])
    cs = data[data_on]
    momv = []
    for i in range(n, le):
        m = cs[i] - cs[i-n]
        momv.append(m)
    if add_index:
        return {'x': data['index'][n:], 'y':momv}
    return momv
