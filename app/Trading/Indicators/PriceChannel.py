

def price_channel(data, n:int=14, r_offset=0, add_index:bool=True):
    le = len(data)
    data = pri_lister(data, ['high', 'low', 'index'])
    dah, dal = data['high'], data['low']
    p_ch, ah, p_cl, al = 0, 0, 0, 0
    n_h, n_l = [], []
    for i in range(n, le):
        ct = i - n
        if n == i:
            p_ch,p_cl = max(dah[ct:i]), min(dal[ct:i])
        else:
            h,l = max(dah[ct:i]), min(dal[ct:i])
            if ah > n or p_ch < h:
                p_ch, ah = h, 0
            else:
                 ah += 1
            if al > n or p_cl > l:
                p_cl, al = l , 0
            else:
                 al += 1
        n_h.append(p_ch + r_offset)
        n_l.append(p_cl - r_offset)
    if add_index:
        ind = data['index'][n:]
        return {'high':{'x':ind, 'y':n_h}, 'low':{'x':ind,'y':n_l}}
    return {'high':n_h, 'low':n_l}
