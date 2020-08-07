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
import string

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
    print("%(processName)s %(message)s save_tdx_data函数")

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

    # 需要标准datetime格式，非datetime64[ns],timeStamp，此处将datetime64[ns]生成int型timestamp
    df['datetime2']=pd.to_datetime(df.loc[:,['year','month','day','hour','minute']])
    #tz_localize('Asia/Shanghai') 下面处理时区的问题 '1970-01-01T00:00:00-08:00' 与 UTC差8个小时
    df['datetime3'] =((df['datetime2'] - np.datetime64('1970-01-01T00:00:00-08:00')) / np.timedelta64(1, 's'))
    df['datetime'] = df['datetime3'].astype(int)
    #df['datetime'] = datetime.fromtimestamp(df['datetime4'] )  #这一步将int型timestamp转换为datetime，放到最后的BarData赋值时


    #删除多余字段
    df.drop(['date','time','year','month','day','hour','minute',"amount","reserve",'datetime2','datetime3'],1,inplace=True)

    #补全信息
    df['symbol'] = symbol
    df['exchange'] = exchange
    df['interval'] = interval
    #将整理好的df存入数据库
    return move_df_to_db(df,future_download)


def formate_DBbar(bar: BarData):
    """
    从BarData生成DbBarData
    """
    print("formate_DBbar 函数")
    try:
        #datetime timestamp
        dt = bar.datetime.astimezone('Asia/Shanghai')
    except:
        #pandas timestamp
        dt = bar.datetime.tz_localize('Asia/Shanghai')
    bar.datetime = dt
    return bar
#--------------------------------------------------------------------------------------------

# 封装函数
def move_df_to_db(imported_data:pd.DataFrame,future_download:bool):
    print("move_df_to_db 函数")

    bars = []
    count = 0
    time_consuming_start = time()
    tmpsymbol = None
    start_time = None

    for row in imported_data.itertuples():
        bar = BarData(

            datetime=datetime.fromtimestamp(row.datetime),   #为标准datetime格式，非datetime64[ns],timeStamp
            symbol=row.symbol,
            exchange=row.exchange,
            interval=row.interval,

            open_price=row.open_price,
            high_price=row.high_price,
            low_price=row.low_price,
            close_price=row.close_price,

            # open_interest=row.open_interest,
            volume=row.volume,
            gateway_name="DB",

        )
        if not tmpsymbol :
            tmpsymbol = bar.symbol

        if future_download:
            # 夜盘时间21：00 - 2：30 日期减1天
            if bar.datetime.time() >= dtime(21,0) or bar.datetime.time() <= dtime(2,30):
                bar.datetime -= timedelta(days=1)
            # 其他时间分钟减1 ？？？
            bar.datetime-= timedelta(minutes=1)

        if not start_time:
            start_time = bar.datetime

        bars.append(bar)
        # do some statistics
        count += 1
    end_time = bar.datetime

    # insert into database
    for bar_data in chunked(bars, 10000):  # 分批保存数据
        database_manager.save_bar_data(bar_data)

    time_consuming_end =time()
    print(f'载入通达信标的：{tmpsymbol} 分钟数据，开始时间：{start_time}，结束时间：{end_time}，数据量：{count},耗时：{round(time_consuming_end-time_consuming_start,3)}秒')

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

    file_names =[]              #  文件名列表
    vt_symbols = []             #  vt_symbol列表
    future_download = True      #  期货数据下载状态
    for dirpath, dirnames, filenames in os.walk(file_path):
        for file_name in filenames:         #当前目录所有文件名
            b_found = False
            vt_symbol = ""
            #过滤压缩文件
            if file_name.split(".")[1] in ["rar","7z"]:
                continue
            if file_name.endswith("lc1"):
                if future_download:
                    symbol = file_name.split(".")[0].split("#")[1]
                    for contract in list(contracts.values()):       #查看合约是否在constracts列表中
                        #指数合约vt_symbol合成,给大家提供个例子, 这里合成了99合约后需要跟交易所_index对比才能找出交易所后缀
                        if b_found == True:
                            break
                        if symbol.endswith("L9"):       #如TSL9
                            tmp_symbol = symbol.split("L9")[0]  #把TSL9合成TS
                            tmp_contract_symbol:str = contract.symbol.split(".")[0].rstrip(string.digits)
                            if tmp_symbol in [tmp_contract_symbol.upper(), tmp_contract_symbol.lower()]:
                                vt_symbol = tmp_contract_symbol+"99."+contract.exchange.value  #把TSL9合成TS99.CFFEX
                                b_found = True
                                break
                        elif symbol.endswith("L8"):  # 如TSL8
                            break
                        else:
                            #合约symbol与文件名相同(满足合约大写或小写)使用合约vt_symbol，这是contracts的，如'sc2109.INE'
                            if contract.symbol in [symbol, symbol.lower()]:
                                vt_symbol = contract.vt_symbol
                                b_found = True
                                break
                else:  #股票数据的处理
                    symbol = file_name.split(".")[0]
                    if symbol.startswith("sh"):
                        exchange_str = "SSE"
                        b_found = True
                        break
                    elif symbol.startswith("sz"):
                        exchange_str = "SZSE"
                    vt_symbol = symbol[-6:] + "." + exchange_str #后6位.SSE/SZSE
                    b_found = True
                #收集的vt_symbol放入vt_symbls:
                if(b_found == True) and (vt_symbol not in vt_symbols ) :
                    # 将文件夹内文件整理到file_names中
                    #if file_name not in file_names:
                    file_names.append(f"{file_path}\\{file_name}")
                    vt_symbols.append(vt_symbol)
                    pass


    pool = multiprocessing.Pool(multiprocessing.cpu_count(), maxtasksperchild=1)
    print("multiprocessing.cpu_count(): %d" % multiprocessing.cpu_count())
    for setting in list(zip(file_names,vt_symbols)):
        if setting[1].strip()=='':
            continue

        setting += (future_download,)

        #pool.apply_async(save_tdx_data, setting)
        pool.apply(save_tdx_data, setting)
    pool.close()
    pool.join()
    #保存股票列表
    if not future_download:
        stock_vt_symbols = load_json("stock_vt_symbols.json")
        for vt_symbol in vt_symbols:
            if vt_symbol not in stock_vt_symbols:
                stock_vt_symbols.append(vt_symbol)
        save_json("stock_vt_symbols.json",stock_vt_symbols)