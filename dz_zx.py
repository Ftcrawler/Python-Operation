import pymysql.cursors
import time,datetime,csv
from dateutil.relativedelta import relativedelta

#定义select_d_zx查询函数
def select_d_zx(start_date,end_date,dz_date,n):
    data = list()
    # 连接数据库
    conn = pymysql.connect(
        host='256.256.256.256',#公库主机地址
        port=3306,#端口号
        user='cqf2001',#账号
        passwd='123456',#密码
        db='datebase_test',#数据库名
        charset='utf8'
    )
    # 获取游标
    cursor = conn.cursor()
    """
    传入的是日期型，不需要字符转日期处理，忽略该段
    #将字符参数转化为日期形式
    start_date = time.strptime(start_date, '%Y-%m-%d')
    end_date = time.strptime(end_date, '%Y-%m-%d')
    #取日期数组的前三位（年-月-日）
    start_date = datetime.datetime(*start_date[0:3])
    end_date = datetime.datetime(*end_date[0:3])
    """
    begin_date = start_date-datetime.timedelta(days=7)
    end_date = end_date - datetime.timedelta(seconds=1)
    data = list()
    while n>0:

        cursor.execute("""
查询代码不提供，将日期改为'%s'即可，一一对应于下方的参数
        """%(dz_date,start_date,end_date,begin_date,end_date,start_date,end_date))
        for row in cursor.fetchall():
            data.append(row)
            """
            有时候测试需要，测试完后注释掉
            with open('dz1.csv', 'a', encoding='GB18030') as csv_file:
                writer = csv.writer(csv_file, lineterminator='\n')
                writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6]])
            """
        n=n-1
        #将日期减1个月
        start_date = start_date+relativedelta(days=-1)
        end_date = end_date + relativedelta(days=-1)
        begin_date = start_date - datetime.timedelta(days=7)
    return data
    cursor.close()
    conn.close()

def insert_d_zx(data,wdate):
    # 连接数据库
    conn = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='123456',
        db='datebase_test1',
        charset='utf8'
    )
    # 获取游标
    cursor = conn.cursor()
    cursor.execute("""delete from d_zx where wdate >='%s'"""%(wdate))
    for each in data:
        cursor.execute("""INSERT INTO d_zx(wdate,createtime,city,zx_fk_c,zx_order_fk_c,zx_order_c,zx_order_pay_fk_c,zx_order_tk_fk_c) VALUES('%s','%s','%s',%s,%s,%s,%s,%s)
        """%(each[0],time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),each[1],each[2],each[3],each[4],each[5],each[6]))
    cursor.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    insert_d_zx(select_d_zx(datetime.date.today()+datetime.timedelta(days=-1),datetime.date.today(),datetime.date.today()+datetime.timedelta(days=-1),1),datetime.date.today()+datetime.timedelta(days=-1))

