from custom_functions.DataHandlers import dict_lister

def fvg(data, pipheight:float=0.003, gap_lg:int=2, re_data:list=None):
    le = len(data)
    pr = []
    data = dict_lister(data, ['index','high','low'])
    higs, los = data['high'], data['low']
    for i in range(2, le):
        x, y, t, tg = [], [], 0, None
        cd1_h, cd1_l = higs[i-2], los[i-2]
        cd3_h, cd3_l = higs[i], los[i]
        if cd1_l > cd3_h:
            t, tg, diff = 1, cd3_h, (cd1_l - cd3_h) > pipheight
            y= [cd3_h, cd1_l]
        elif cd1_h < cd3_l:
            t,tg, diff = -1, cd3_l, (cd3_l - cd1_h) > pipheight
            y= [cd1_h, cd3_l]
        if t != 0:
            for ii in range(i+1, le):
                if t == 1 and cd3_h < higs[ii] and diff:
                    if (ii - i) > gap_lg:
                        x = [data['index'][i], data['index'][ii-1]]
                    break
                elif t == -1 and los[ii] < cd3_l and diff:
                    if (ii - i) > gap_lg:
                        x = [data['index'][i], data['index'][ii-1]]
                    break
            if x != []:
                pr.append({'trend':t,'x':x, 'y':y})
    return pr
