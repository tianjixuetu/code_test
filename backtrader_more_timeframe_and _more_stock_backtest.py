from __future__ import (absolute_import, division, print_function, 
                        unicode_literals)
import datetime  # For datetime objects
import os  # To manage paths
import sys  # To find out the script name (in argv[0])
import pandas as pd
import numpy as np
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
class MyStrategy(bt.Strategy):
    params = (
        ('period', 5), 
    )
    def log(self, txt, dt = None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, %.2f' % order.executed.price)
            elif order.issell():
                self.log('SELL EXECUTED, %.2f' % order.executed.price)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None
    def __init__(self):
        #获取交易的股票的股本（默认一段时间不改变）
        stock_data = pd.read_csv('/home/yjj/融资融券与流通市值.csv', dtype = {'代码':str})
        stock_data['流通股_float'] = [float(i.split('亿')[0]) for i in stock_data['流通股']]
        self.liu_name_num = dict(zip(stock_data['代码'], stock_data['流通股_float']))
        #自定义庄股名单
        stock_zhuanggu = pd.read_excel('/home/yjj/剔除庄股.xlsx', encoding = 'utf-8')
        self.stock_zhuanggu_list = [ i[2:] for i in stock_zhuanggu['代码']]
        #交易的股票名称
        self.trading_pair = [i[2:-4] for i in os.listdir('/opt/data/stock_taobao/1M_R/') if i[2:-4] not in self.stock_zhuanggu_list]
        
        self.pre_holding_pair = []
        self.target_holding_pair = []  
    def get_whether_true(self, i, short_data_close, long_data_close, long_data_high, long_data_low):
        result = np.random.choice([0, 1])
        if result == 0:
            return False
        else:
            return True
    def next(self):
        #datetime
        dt = self.datas[0].datetime.datetime(0)
        print(dt)
        dt_time = str(dt)[-8:]
        if dt_time > '09:30:00' and dt_time < '14:55:00':
            print('平仓时间')
            if len(self.pre_holding_pair) > 0:
                need_remove = []
                for name in self.pre_holding_pair:
                    now_close_price = self.datas[name[0]].close[0]
                    pre_close_price = name[1]
                    up_percent = (now_close_price-pre_close_price)/pre_close_price
                    if up_percent > 0.02 or up_percent < -0.02:
                        self.close(self.datas[name[0]])
                        need_remove.append(name)
                for name in need_remove:
                    self.pre_holding_pair.remove(name)        
        # 当时间为14:55分的时候开始进行判断
        if dt_time == '14:55:00':
            target_trading_pair = []
            for i in range(len(self.trading_pair)):
                self.log('close price:{}'.format(self.datas[i].close[0]))
                short_data_close = pd.Series(self.datas[i].close.get(size = 235))
                long_size = int((len(self)-235)/240)
                long_size = 10 if long_size >= 10 else long_size
                long_data_close = pd.Series(self.datas[len(self.trading_pair)+i].close.get(size = long_size))
                long_data_high = pd.Series(self.datas[len(self.trading_pair)+i].high.get(size = long_size))
                long_data_low = pd.Series(self.datas[len(self.trading_pair)+i].low.get(size = long_size))
                con = self.get_whether_true(i, short_data_close, long_data_close, long_data_high, long_data_low)
                con = False
                if con == True:
                    target_trading_pair.append(i)
            self.target_holding_pair = target_trading_pair
            if len(self.target_holding_pair) > 0:
                size = self.broker.getvalue()/len(self.target_holding_pair)
                for i in self.pre_holding_pair:
                    if i[0] not in self.target_holding_pair:
                        self.close(self.datas[i[0]])
                for i in self.target_holding_pair:
                    if i in [j[0] for j in self.pre_holding_pair]:
                        pass
                    else:
                        self.buy(self.datas[i], size = size)
                self.pre_holding_pair = [[i, self.datas[i].close[0]] for i in target_trading_pair]          
if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()
    cerebro.addstrategy(
            MyStrategy
        )
   #自定义庄股名单
    stock_zhuanggu = pd.read_excel('/home/yjj/剔除庄股.xlsx', encoding = 'utf-8')
    stock_zhuanggu_list = [ i[2:] for i in stock_zhuanggu['代码']]
     # 1 minute data
    short_data_path = '/opt/data/stock_taobao/1M_R/'
    short_file_list = [i for i in os.listdir(short_data_path)  if i[2:-4] not in stock_zhuanggu_list]
    for file in short_file_list:
        df_1M = pd.read_csv(short_data_path+file, encoding = 'gbk', index_col = 0)
        df_1M = df_1M.sort_values(by = 'datetime')
        df_1M.index = pd.to_datetime(df_1M['datetime'])
        # Create a Data Feed
        data = bt.feeds.PandasData(dataname = df_1M)
        cerebro.adddata(data, name = file[2:-4])
    
    
    #tframes = dict(minutes = bt.TimeFrame.Minutes, daily = bt.TimeFrame.Days)
    # Handy dictionary for the argument timeframe conversion
    # Resample the data
    long_data_path = '/opt/data/stock_taobao/day/'
    long_file_list = [ i.lower() for i in short_file_list]
    #print(long_file_list)
    for file in long_file_list:
        df_day = pd.read_csv(long_data_path+file, encoding = 'gbk')
        df_day = df_day.sort_values(by = '交易日期')
        df_day = df_day[['交易日期', '开盘价', '最高价', '最低价', '收盘价', '成交量']]
        df_day.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        df_day.index = pd.to_datetime(df_day['datetime'])
        df_day = df_day[df_day['datetime'] > '2015-01-01']
        data1 = bt.feeds.PandasData(dataname = df_day)
        # Load the Data
        cerebro.adddata(data1, name = file[2:-4])
    # Set our desired cash start
    cerebro.broker.setcash(1000000.0)
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name = 'pyfolio')
    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # Run over everything
    results = cerebro.run()
    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    strat = results[0]
    pyfoliozer = strat.analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
