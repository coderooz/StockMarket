import math
from scipy.stats import norm

def convert_to_years(time, unit):
    """
    Convert time to years based on the given unit.
    
    Parameters:
    time : float
        Time value
    unit : str
        Unit of time ('years', 'days', 'weeks', 'hours', 'minutes')
    
    Returns:
    float
        Time in years
    """
    if unit == 'years':
        return time
    elif unit == 'days':
        return time / 365.0
    elif unit == 'weeks':
        return time / 52.0
    elif unit == 'hours':
        return time / (365.0 * 24)
    elif unit == 'minutes':
        return time / (365.0 * 24 * 60)
    else:
        raise ValueError("Invalid time unit. Use 'years', 'days', 'weeks', 'hours', or 'minutes'.")

def black_scholes(S, K, time, r, sigma, option_type='call', time_unit='years'):
    """
        Calculate the Black-Scholes option price for a European call or put option.
        
        Parameters:
        S : float
            Current stock price
        K : float
            Option strike price
        time : float
            Time to expiration
        r : float
            Risk-free interest rate
        sigma : float
            Volatility of the underlying stock
        option_type : str
            'call' for call option, 'put' for put option (default is 'call')
        time_unit : str
            Unit of time ('years', 'days', 'weeks', 'hours', 'minutes')
        
        Returns:
        float
            Black-Scholes option price
    """
    T = convert_to_years(time, time_unit)
    
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == 'call':
        option_price = (S * norm.cdf(d1)) - (K * math.exp(-r * T) * norm.cdf(d2))
        delta = norm.cdf(d1)
        theta = (- (S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T)) - r * K * math.exp(-r * T) * norm.cdf(d2)) / 365
    elif option_type == 'put':
        option_price = (K * math.exp(-r * T) * norm.cdf(-d2)) - (S * norm.cdf(-d1))
        delta = -norm.cdf(-d1)
        theta = (- (S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T)) + r * K * math.exp(-r * T) * norm.cdf(-d2)) / 365
    else:
        raise ValueError("Invalid option type. Use 'call' or 'put'.")
    
    gamma = (norm.pdf(d1) / (S * sigma * math.sqrt(T)))
    vega = (S * norm.pdf(d1) * math.sqrt(T)) / 100
    rho = (K * T * math.exp(-r * T) * norm.cdf(d2) / 100) if option_type == 'call' else (-K * T * math.exp(-r * T) * norm.cdf(-d2) / 100)
    return {'option_price': option_price,'delta': delta,'gamma': gamma,'theta': theta,'vega': vega,'rho': rho}

# Provided parameters
S = 22476.6      # Current stock price
time = 7           # Time to expiration
r = 0.10           # Risk-free interest rate
sigma = 0.1962     # Volatility
time_unit = 'days' # Time unit

# for i in range()
prices = {'call':{}, 'put': {}}
for K in range(22300, 22850, 50):
    prices['call'].update({str(K): black_scholes(S, K, time, r, sigma, 'call', time_unit)})
    prices['put'].update({str(K): black_scholes(S, K, time, r, sigma, 'put', time_unit)})





# print(f"Call option price: {call_price:.2f}")
# print(f"Put option price: {put_price:.2f}")

