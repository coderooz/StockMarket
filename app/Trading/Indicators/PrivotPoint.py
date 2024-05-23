
def piv_point(data, n:int=10, add_index:bool=False):
    le = len(data)
    if le > n:
        data = pri_lister(data, ['high','low'])
        xh, xl, yh, yl, alx, aly = [],[],[],[],[],[]
        for i in range(0, le, n):
            lim = i+n
            hy,ly = max(data['high'][i:lim]),min(data['low'][i:lim])
            xhid,xlid = data['high'].index(hy) + 1, data['low'].index(ly) + 1
            yh.append(hy)
            yl.append(ly)
            xh.append(xhid)
            xl.append(xlid)
            alx += [xh[-1], xl[-1]]
            aly += [yh[-1], yl[-1]]
    if add_index:
        return {'main':{'x':alx,'y':aly},'high':{'x':xh,'y':yh},'low':{'x':xl,'y':yl}}
    return {'main':aly,'high':yh,'low':yl}
