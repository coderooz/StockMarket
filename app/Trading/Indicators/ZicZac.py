


def zic_zac(data):
    le = len(data)
    data = pri_lister(data, ['high', 'low'])
    l, fl= 0,[]
    for i in range(le):
        h,l = data['high'][i], data['low'][i]
        if l < lh and h < lh:
            pass

