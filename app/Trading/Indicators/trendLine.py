

def trend_line(data, cdid=200, lookbk:int=25, subblk=5, slope=1, extend=0, catg=None):
    '''
        Trend Line Is a function that looks at the data and thne plots the best line for the trend.
    '''
    le = len(data)
    if le > lookbk and cdid > lookbk and lookbk>subblk:
        a1, trend = [], get_trend(data)['trend']
        if trend == 1:
            catg = 'low'
        elif trend == -1:
             catg = 'high'
        else:
            raise ValueError(f'trend={trend} is invalid. Valid values=[1,-1]')

        for ri in range(lookbk-subblk, lookbk+subblk):
            idp, pn = [],[]
            for i in range((cdid-ri), (cdid+1), subblk):

               midt = pri_lister(data[i:i+subblk], ['index', catg])
               mi,mx = midt[catg],0
               if trend == 1:
                   mx = min(mi)
               elif trend == -1:
                   mx = max(mi)
               pn.append(mx)
               idp.append(midt['index'][mi.index(mx)])

            slp, incp = get_slope_intercept(idp, pn)
            a = get_line(idp, slp, incp)

        price = pri_lister(data, [catg])[catg]
        adj = [price[id] - slp*id for id in idp]
        adj = min(adj) if (trend==1) else max(adj)
        line = [slp*id + adj for id in idp]

        if extend > 0:
            idp_last = idp[-1]
            slo = list(range(idp_last, idp_last+extend))
            extra = [slp*i + adj for i in slo]
            line = line + extra
            idp = idp + slo
        return idp, line
    else:
        raise ValueError('')
