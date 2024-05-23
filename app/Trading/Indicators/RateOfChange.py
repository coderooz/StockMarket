


def change_rate_inic(data, ma=10, range_times=3, data_on:str='close'):
    d = calculate_difference_percentage(data[ma-1:], moving_average(open, ma))
    avj = sum(d)/len(d)
    lower_limit = avj - (avj * 3)
    upper_limit = avj + (avj * 3)
    t = []
    n = avj
    for i in range(len(d)):
        if (d[i] < upper_limit):
            t.append(d[i])
        elif (d[i] > lower_limit):
            t.append(d[i])
        else:
            t.append(None)
    return t
