##Parabolic Sar

def sar(data, af_start:float=0.02, af_increment:float=0.02, af_max:float=0.2, add_id:bool=False):

    dtr = pri_lister(data, ['index','high', 'low'])
    idx , highs, lows = dtr['index'], dtr['high'], dtr['low']
    sar, ep = [lows[0]], [highs[0]]
    trend, af = 1, af_start

    for i in range(1, len(lows)):
        if trend == 1:
            sar_new = sar[-1] + af * (ep[-1] - sar[-1])
            if sar_new > lows[i]:
                trend = -1
                sar.append(ep[-1])
                ep.append(highs[i])
                af = af_start
            else:
                sar.append(sar_new)
                ep.append(max(ep[-1], highs[i]))
                af = min(af + af_increment, af_max)

        else:
            sar_new = sar[-1] + af * (ep[-1] - sar[-1])
            if sar_new < highs[i]:
                trend = 1
                sar.append(ep[-1])
                ep.append(lows[i])
                af = af_start
            else:
                sar.append(sar_new)
                ep.append(min(ep[-1], lows[i]))
                af = min(af + af_increment, af_max)
    if add_id:
        return {'x':idx, 'y':sar}
    return sar