import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class FinancialAnalytics:
    def __init__(self, data):
        self.data = data

    def log_returns(self):
        # Calculate daily log returns
        self.data['Log Returns'] = np.log(self.data['Close'] / self.data['Close'].shift(1))
        self.data.dropna(inplace=True)
        return self.data

    def mean_return(self):
        return self.data['Log Returns'].mean()
    
    def volatility(self):
        return self.data['Log Returns'].std()
    
    
    def monte_carlo_simulation(self, num_simulations=1000, num_days=252):
        last_price = self.data['Close'].iloc[-1]
        mean_return = self.mean_return()
        vol = self.volatility()
        
        simulation_df = pd.DataFrame()
        
        for x in range(num_simulations):
            price_series = [last_price]
            for y in range(num_days):
                price = price_series[-1] * np.exp(np.random.normal(mean_return, vol))
                price_series.append(price)
            simulation_df[x] = price_series
            
        return simulation_df
    
    def z_score(self):
        pass
    
    def plot_returns(self):
        plt.figure(figsize=(10, 6))
        plt.plot(self.data['Log Returns'], label='Daily Log Returns')
        plt.title('Financial Analytics - Daily Log Returns')
        plt.xlabel('Date')
        plt.ylabel('Returns')
        plt.legend()
        plt.show()
        
    def plot_monte_carlo(self, simulation_df):
        plt.figure(figsize=(10, 6))
        plt.plot(simulation_df)
        plt.title('Monte Carlo Simulations of Future Prices')
        plt.xlabel('Days')
        plt.ylabel('Price')
        plt.show()