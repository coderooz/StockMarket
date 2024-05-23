##Super Trend

def supertrend(data, n=14, f=3, data_on:str='close', add_index:bool=False):
    le = len(data)
    atr = sum(true_range(data=data, data_on=data_on, add_index=False)) / n
    data = pri_lister(data, ['index',data_on])
    cls = data[data_on]

    supertrend_values,base_price=[],cls[0]
    for i in range(1, le):
        if cls[i] > base_price:
            base_price = cls[i]
        else:
            base_price = max(cls[i], base_price)

        if i == 1:
            supertrend = base_price
        else:
            if base_price > supertrend_values[-1]:
                supertrend = max(base_price, supertrend_values[-1] - atr * f)
            else:
                supertrend = min(base_price, supertrend_values[-1] + atr * f)
        supertrend_values.append(supertrend)

    if add_index:
        return {'x': data['index'][1:], 'y':supertrend_values}
    return supertrend_values

