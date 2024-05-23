from modules.Trading.Indicators.ROC import roc 

def rocr(data, data_on:str='close', add_id:bool=False):
    le = len(data)
    if le < n: raise ValueError (f'The n[{n}] parameter should be less than data parama->{le}')
    return roc(data, 1, data_on, add_id)
