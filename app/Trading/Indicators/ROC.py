
def roc(data, n:int=14, data_on:str='close', add_id:bool=False):
    le = len(data)
    if le < n: raise ValueError (f'The n[{n}] parameter should be less than data parama->{le}')
    data = pri_lister(data, [data_on, 'index'])
    cs = data[data_on]
    roc = []
    for i in range(n, le):
        previous_price = cs[i - n]
        roc_value = (cs[i] - previous_price) / previous_price * 100
        roc.append(roc_value)
    if add_index:
        return {'x': data['index'][n:], 'y':roc}
    return roc
