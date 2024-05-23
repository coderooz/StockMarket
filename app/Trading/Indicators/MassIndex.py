##Mass Index

def massi(data, fast=25, slow=9, offset=None, **kwargs):
    """Indicator: Mass Index (MASSI)"""
    data = pri_lister(data, ['high', 'low'])
    high, low = data['high'], data['low']
    if slow < fast:
        fast, slow = slow, fast
    _length = max(fast, slow)
    offset = get_offset(offset)
    if "length" in kwargs: kwargs.pop("length")

    if high is None or low is None: return

    # Calculate Result
    high_low_range = non_zero_range(high, low)
    hl_ema1 = ema(close=high_low_range, length=fast, **kwargs)
    hl_ema2 = ema(close=hl_ema1, length=fast, **kwargs)

    hl_ratio = hl_ema1 / hl_ema2
    massi = hl_ratio.rolling(slow, min_periods=slow).sum()

    # Offset
    if offset != 0:
        massi = massi.shift(offset)

    #Handle fills
    if "fillna" in kwargs:
        massi.fillna(kwargs["fillna"], inplace=True)
    if "fill_method" in kwargs:
        massi.fillna(method=kwargs["fill_method"], inplace=True)

    # Name and Categorize it
    massi.name = f"MASSI_{fast}_{slow}"
    massi.category = "volatility"

    return massi

