


def trendline_with_breaks(prices, n=14, break_threshold=0.1, data_on:str='close'):
    prices = pri_lister(prices, [data_on])

    # Calculate the moving average
    ma = moving_average(prices, n)

    # Initialize the trend values
    trend = []
    prices = prices[-len(ma):]
    # Loop through the prices
    for i in range(len(ma)):
        # If the price is within the break threshold of the moving average, the trend is neutral
        if abs(prices[i] - ma[i]) < break_threshold:
            trend.append(0)
        # If the price is above the moving average, the trend is up
        elif prices[i] > ma[i]:
            trend.append(1)
        # If the price is below the moving average, the trend is down
        elif prices[i] < ma[i]:
            trend.append(-1)

    # Return the trend values
    return trend
