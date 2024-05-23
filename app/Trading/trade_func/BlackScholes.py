#./modules/Trading/trade_func/BlackScholes.py

import numpy as np
from scipy.stats import norm



def BlackScholes(r, S, K, T, sigma, type:str='call'):
    """
        Calculating Black Scholes option price for call/put. 

        Args:
            r (_type_): _description_
            S (_type_): _description_
            K (_type_): _description_
            T (_type_): _description_
            sigma (_type_): _description_
            type (str, optional): _description_. Defaults to 'call'.
    """

    d1 = (np.log(S/K) + (r + sigma**2/2)*T)/(sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    type = type.lower()
    try: 
        if type == 'call' or type == 'c':
            price = S*norm.cdf(d1, 0, 1) - K*np.exp(-r*T)*norm.cdf(d2,0,1)
        elif  type == 'put' or type == 'p':
            price =  K*np.exp(-r*T)*norm.cdf(-d2,0,1) - S*norm.cdf( -d1, 0, 1)
        return price
    except Exception as e:
        print('Plz check the paramerter.')

if __name__ == '__main__':

    # define variables
    interestRate = 0.01 #r
    underLine = 30 #S
    strike_price = 40 #K
    Time = 240/365 #T
    volitility = 0.30 #sigma

    price = BlackScholes(interestRate,underLine,strike_price,Time,volitility, 'P')
    print("Option Price is:", round(price, 2))