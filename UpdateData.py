import pandas as pd
import pymysql
from sqlalchemy import create_engine
import tushare as ts
import openpyxl
import datetime
import os
import xlrd

engine = create_engine("mysql+pymysql://root:950910@127.0.0.1:3306/tick_data?charset=UTF8")
df = ts.get_today_all()
df.to_sql(name='code', con=engine, if_exists='replace', index=False, index_label=False)
print("代码更新完成")

now_date = datetime.datetime.now().strftime("%Y-%m-%d")

db = pymysql.connect("127.0.0.1", "root", "950910", "tick_data")
code_cur = db.cursor()
data_cur = db.cursor()
mar_cur = db.cursor()
write_cur = db.cursor()
code_cur.execute("select code from code group by code order by code")
code_results = code_cur.fetchall()
for row in code_results:
    #清除临时表中数据
    try:
        data_sql = "select max(date) from c"+row[0]  # 确定历史数据的最后日期
        data_cur.execute(data_sql)
        data_results = data_cur.fetchone()
        old_date = datetime.datetime.strptime(data_results[0], '%Y-%m-%d')
        s_date = old_date + datetime.timedelta(days=1)  # 确定历史数据的最后日期+1天
        s_date = s_date.strftime('%Y-%m-%d')
        data = ts.get_hist_data(row[0], start=s_date, end=now_date, ktype="D")
        if not data is None:
            data_sql = "delete from data_trans"
            try:
                data_cur.execute(data_sql)
                db.commit()
            except:
                db.rollback()
    # 复制最新8天的历史数据到临时表中
            data_sql = "insert into data_trans (date,open,high,close,low,ma10,KDJ_K,KDJ_D) (select date,open,high," \
                   "close,low,ma10,KDJ_K,KDJ_D from c"+row[0]+"  order by date DESC limit 0,8)"
            data_cur.execute(data_sql)
            db.commit()
            data.to_excel(row[0] + ".xlsx")
            df = pd.read_excel(row[0] + ".xlsx")
            df.to_sql(name="data_trans", con=engine, if_exists="append", index=False, index_label=False)
            df.to_sql(name="c" + row[0], con = engine, if_exists = "append", index = False, index_label = False)
            os.remove(row[0] + ".xlsx")
            data_sql = "select date,open,high,close,low,ma10,KDJ_K,KDJ_D,KDJ_J from data_trans order by date "
            data_cur.execute(data_sql)
            data_results = data_cur.fetchone()
            KDJ_date = data_results[0]
            KDJ_max = data_results[2]
            KDJ_close = data_results[3]
            KDJ_min = data_results[4]
            for i in range(data_cur.rowcount ):
                if i > 7:
                    mar_sql = "select high,low from data_trans order by date limit "+str(i-8)+",9"
                    mar_cur.execute(mar_sql)
                    mar_results = mar_cur.fetchall()
                    for row1 in mar_results:
                        if KDJ_min > row1[1]:
                            KDJ_min = row1[1]
                        if KDJ_max < row1[0]:
                            KDJ_max = row1[0]
                    KDJ_K1 = 2 / 3 * KDJ_K + 1 / 3 * (data_results[3] - KDJ_min) / (KDJ_max - KDJ_min) * 100  # K值
                    KDJ_D1 = 2 / 3 * KDJ_D + 1 / 3 * KDJ_K1  # D值
                    KDJ_J1 = 3 * KDJ_K1 - 2 * KDJ_D1
                    KDJ_G = None
                    ma_note = None
                    if (KDJ_K1 >= KDJ_D1) and (KDJ_K < KDJ_D):
                        KDJ_G = "金叉"
                    if  (KDJ_K1 <= KDJ_D1) and (KDJ_K > KDJ_D):
                        KDJ_G = "死叉"
                    if (data_results[5] >= v_ma10) and (data_results[3] > data_results[5]) and (per_close < v_ma10):
                        ma_note = "上穿"
                    if  (data_results[5] <= v_ma10) and (data_results[3] < data_results[5]) and (per_close > v_ma10):
                        ma_note = "下穿"
                    if (KDJ_G != None) and (ma_note!=None):
                        write_sql = "update c" + row[0] + " set KDJ_K= " + str(KDJ_K1) + ", KDJ_D=" + str(KDJ_D1) \
                                    + ",KDJ_J= " + str(KDJ_J1) + ",KDJ_Gold= '%s',ma_gold='%s' where date='%s' " \
                                    % (KDJ_G, ma_note, KDJ_date)
                    if (KDJ_G!=None) and (ma_note==None):
                        write_sql = "update c" + row[0] + " set KDJ_K= " + str(KDJ_K1) + ", KDJ_D=" + str(KDJ_D1) \
                                    + ",KDJ_J= " + str(KDJ_J1) + ",KDJ_Gold= '%s' where date='%s' " \
                                    % (KDJ_G, KDJ_date)
                    if (KDJ_G==None) and (ma_note!=None):
                        write_sql = "update c" + row[0] + " set KDJ_K= " + str(KDJ_K1) + ", KDJ_D=" + str(KDJ_D1) \
                                    + ",KDJ_J= " + str(KDJ_J1) + ",ma_gold='%s' where date='%s' " \
                                    % (ma_note, KDJ_date)
                    if (KDJ_G==None) and (ma_note==None):
                        write_sql = "update c" + row[0] + " set KDJ_K= " + str(KDJ_K1) + ", KDJ_D=" + str(KDJ_D1) \
                                    + ",KDJ_J= " + str(KDJ_J1) + " where date='%s' " % ( KDJ_date)
                    try:
                        write_cur.execute(write_sql)
                        db.commit()
                    except:
                        db.rollback()
                    KDJ_K = KDJ_K1
                    KDJ_D = KDJ_D1
                    per_close = data_results[3]  # 前日收盘价
                    v_ma10 = data_results[5]  # 前日10日均线值
                if i == 7:
                    per_close = data_results[3]
                    v_ma10 = data_results[5]
                    KDJ_K = data_results[6]
                    KDJ_D = data_results[7]
                data_results = data_cur.fetchone()
                if data_results is None:
                    break;
                KDJ_date = data_results[0]
                KDJ_max = data_results[2]
                KDJ_min = data_results[4]
                KDJ_close = data_results[3]
            print(row[0] + "完成计算！")
    except:
        print(row[0] + "无法完成KDJ计算！")
code_cur.close()
data_cur.close()
write_cur.close()
db.close()
print("全部完成")