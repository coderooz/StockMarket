## True Range
def true_range(data, data_on='close', add_index:bool=False):
    # Initialize variables
    tr_values = []
    le = len(data)
    data = pri_lister(data, ['index', data_on])
    cls = data[data_on]
    # Loop through the prices
    for i in range(1, le):
        # Calculate the true range
        high_low_range = abs(cls[i] - cls[i-1])
        high_close_range = abs(cls[i] - cls[i-1])
        close_low_range = abs(cls[i-1] - cls[i])
        tr = max(high_low_range, high_close_range, close_low_range)
        tr_values.append(tr)

    if add_index:
        return {'x':data['index'][1:],'y':tr_values}
    return tr_values

