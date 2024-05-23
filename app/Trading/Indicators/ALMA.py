##Arnaud Legoux Moving Average

def alma(close, length=None, sigma=None, distribution_offset=None, offset=None, **kwargs):
    """Indicator: Arnaud Legoux Moving Average (ALMA)"""
    # Validate Arguments
    length = int(length) if length and length > 0 else 10
    sigma = float(sigma) if sigma and sigma > 0 else 6.0
    distribution_offset = float(distribution_offset) if distribution_offset and distribution_offset > 0 else 0.85
    close = verify_series(close, length)
    offset = get_offset(offset)

    if close is None: return

    # Pre-Calculations
    m = distribution_offset * (length - 1)
    s = length / sigma
    wtd = list(range(length))
    for i in range(0, length):
        wtd[i] = npExp(-1 * ((i - m) * (i - m)) / (2 * s * s))

    # Calculate Result
    result = [npNaN for _ in range(0, length - 1)] + [0]
    for i in range(length, close.size):
        window_sum = 0
        cum_sum = 0
        for j in range(0, length):
            # wtd = math.exp(-1 * ((j - m) * (j - m)) / (2 * s * s))        # moved to pre-calc for efficiency
            window_sum = window_sum + wtd[j] * close.iloc[i - j]
            cum_sum = cum_sum + wtd[j]

        almean = window_sum / cum_sum
        result.append(npNaN) if i == length else result.append(almean)

    alma = Series(result, index=close.index)
    # Offset
    if offset != 0:
        alma = alma.shift(offset)

    # Handle fills
    if "fillna" in kwargs:
        alma.fillna(kwargs["fillna"], inplace=True)
    if "fill_method" in kwargs:
        alma.fillna(method=kwargs["fill_method"], inplace=True)

    # Name & Category
    alma.name = f"ALMA_{length}_{sigma}_{distribution_offset}"
    alma.category = "overlap"

    return alma
