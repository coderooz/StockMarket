##Support & Registance
from custom_functions.DataHandlers import dict_lister

def support_regis(data, n:int=14):
    le = len(data)
    if le > n:
        data = dict_lister(data, ['index','open','high','low','close'])
        rig_sup = []
        idx, hig, los, opn, cls = data['index'], data['high'], data['low'], data['open'], data['close']
        r,s, ind_r, ind_s = None, None, None, None
        for i in range(0, le, n):
            t = None
            if i < 1:
                r = max(hig[i:i+n])
                s = min(los[i:i+n])
                ind_r, ind_s = idx[i:i+n][hig[i:i+n].index(r)], idx[i:i+n][los[i:i+n].index(s)]
            else:
                if r < max(opn[i:i+n]):
                    r = max(hig[i:i+n])
                    ind_r = idx[i:i+n][hig[i:i+n].index(r)]


                if s < min(cls[i:i+n]):
                    s = min(los[i:i+n])
                    ind_s = idx[i:i+n][los[i:i+n].index(s)]

                    for i in range(i+n, le):
                        pass

                if r < max(cls[i:i+n]):
                    r = max(hig[i:i+n])
                    ind_r = idx[i:i+n][hig[i:i+n].index(s)]

                    for i in range(i+n, le):
                        pass

