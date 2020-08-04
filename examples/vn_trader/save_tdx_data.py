'''
使用说明:

future_download状态，下载期货数据时设置为True，下载股票数据时设置为False
file_path:通达信数据保存路径大家自行替换
通达信期货数据对齐datetime到文华财经后数据与文化财经完全一致，包括指数合约
单个文件较大时多进程只有两个进程在运行，原因不明
通达信股票只能下载最近100天数据，期货数据下载没有时间限制
期货数据存储路径:D:\tdx\vipdoc\ds,上交所股票数据路径:D:\tdx\vipdoc\sh,深交所股票数据路径:D:\tdx\vipdoc\sz
建议下载通达信期货通可以同时下载股票和期货数据，enjoy it！
附上vnpy\trader\engine.py里面的合约数据保存读取代码

'''


from typing import TextIO
from datetime import datetime,timedelta
from datetime import time as dtime
from time import time
import numpy as np
import pandas as pd
import csv
import os
import multiprocessing

from vnpy.trader.database import database_manager
from vnpy.trader.constant import (Exchange, Interval)
from vnpy.trader.object import (BarData, TickData)
from vnpy.trader.utility import (load_json, save_json,extract_vt_symbol)
from peewee import chunked
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine


from vnpy.trader.database.initialize import init_sql
from vnpy.trader.database.database import Driver


#--------------------------------------------------------------------------------------------
def save_tdx_data(file_path,vt_symbol:str,future_download:bool,interval: Interval = Interval.MINUTE):
    """
    保存通达信导出的lc1分钟数据,期货数据对齐datetime到文华财经
    """
    bars = []
    start_time = None
    count = 0
    time_consuming_start = time()
    symbol,exchange= extract_vt_symbol(vt_symbol)
    #读取二进制文件
    dt = np.dtype([
        ('date', 'u2'),
        ('time', 'u2'),
        ('open_price', 'f4'),
        ('high_price', 'f4'),
        ('low_price', 'f4'),
        ('close_price', 'f4'),
        ('amount', 'f4'),
        ('volume', 'u4'),
        ('reserve','u4')])
    data = np.fromfile(file_path, dtype=dt)
    df = pd.DataFrame(data, columns=data.dtype.names)
    df.eval('''
    year=floor(date/2048)+2004
    month=floor((date%2048)/100)
    day=floor(date%2048%100)
    hour = floor(time/60)
    minute = time%60
    ''',inplace=True)
    df.index=pd.to_datetime(df.loc[:,['year','month','day','hour','minute']])
    df.drop(['date','time','year','month','day','hour','minute',"amount","reserve"],1,inplace=True)
    for index in range(len(df)):
        bar = BarData(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            open_price=df["open_price"][index],
            high_price=df["high_price"][index],
            low_price=df["low_price"][index],
            close_price=df["close_price"][index],
            volume=df["volume"][index],
            datetime = df.index[index],
            gateway_name = 'DB'
            )
        if future_download:
            if bar.datetime.time() >= dtime(21,0) or bar.datetime.time() <= dtime(2,30): #夜盘时间21：00 - 2：30
                bar.datetime -= timedelta(days=1)
            bar.datetime-= timedelta(minutes=1)
        if not start_time:
            start_time = bar.datetime

        tmpDBbar =formate_DBbar(bar)   #从BarData到DBBarData 
        bars.append(tmpDBbar)
        count += 1
        end_time = bar.datetime  
    settings={
    "database": "database.db",
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "authentication_source": "admin"}
    sqlite_manager = init_sql(driver=Driver.SQLITE, settings=settings)

    for bar_data in chunked(bars, 10000):               #分批保存数据
        #database_manager.save_bar_data(bar_data)      #保存数据到数据库 
        sqlite_manager.save_bar_data(bar_data)
    time_consuming_end =time()
    msg = f'载入通达信标的：{vt_symbol} 分钟数据，开始时间：{start_time}，结束时间：{end_time}，数据量：{count},耗时：{round(time_consuming_end-time_consuming_start,3)}秒'
    return msg

def formate_DBbar(bar: BarData):
    """
    从BarData生成DbBarData
    """
    try:     
        #datetime timestamp   
        dt = bar.datetime.astimezone('UTC')
    except:     
        #pandas timestamp
        dt = bar.datetime.tz_localize('UTC')

    bar.datetime = dt

    return bar
#--------------------------------------------------------------------------------------------
if __name__ == '__main__':
    file_path = "D:\\Software\\TDX\\new_tdxqh\\vipdoc\\ds\\minline"
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    contracts = main_engine.load_contracts()
    if contracts:
        print(len(contracts)) 

    if not contracts:
        print("load_contracts = 0,return")
        exit(0)
    vt_symbol = ""
    file_names =[]              #  文件名列表
    vt_symbols = [] # vt_symbol列表
    future_download = True #    期货数据下载状态
    pool = multiprocessing.Pool(multiprocessing.cpu_count(), maxtasksperchild=1)
    for dirpath, dirnames, filenames in os.walk(file_path):
        for file_name in filenames:         #当前目录所有文件名
            #过滤压缩文件
            if file_name.split(".")[1] in ["rar","7z"]:
                continue
            if file_name.endswith("lc1"):
                if file_name not in file_names:
                    file_names.append(f"{file_path}\\{file_name}")
                if future_download:
                    symbol = file_name.split(".")[0].split("#")[1]
                    for contract in list(contracts.values()):
                        #指数合约vt_symbol合成,给大家提供个例子
                        if symbol.endswith("L9"):
                            symbol = symbol.split("L9")[0] + "99"
                            shfe_index = ["rb99","hc99"]
                            dce_index = ["i99","j99"]
                            czce_index = ["AP99","ZC99"]
                            if symbol.lower() in shfe_index:
                                vt_symbol = symbol.lower() + "." + "SHFE"
                            elif symbol.lower() in dce_index:
                                vt_symbol = symbol.lower() + "." + "DCE"
                            elif symbol in czce_index:
                                vt_symbol = symbol + "." + "CZCE"
                        else:
                            #合约symbol与文件名相同(满足合约大写或小写)使用合约vt_symbol
                            if contract.symbol in [symbol, symbol.lower()]:
                                vt_symbol = contract.vt_symbol
                else:
                    symbol = file_name.split(".")[0]
                    if symbol.startswith("sh"):
                        exchange_str = "SSE"
                    elif symbol.startswith("sz"):
                        exchange_str = "SZSE"

                    vt_symbol = symbol[-6:] + "." + exchange_str
                if vt_symbol not in vt_symbols:
                    vt_symbols.append(vt_symbol)
    


    for setting in list(zip(file_names,vt_symbols)):
        if setting[1].strip()=='':
            continue

        setting += (future_download,)
        result = pool.apply_async(save_tdx_data, setting)
        print(result.get())
    pool.close()
    pool.join()
    #保存股票列表
    if not future_download:
        stock_vt_symbols = load_json("stock_vt_symbols.json")
        for vt_symbol in vt_symbols:
            if vt_symbol not in stock_vt_symbols:
                stock_vt_symbols.append(vt_symbol)
        save_json("stock_vt_symbols.json",stock_vt_symbols)