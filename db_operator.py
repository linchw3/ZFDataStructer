import psycopg2
import csv
import sys
import codecs
import pymysql
import traceback

def getCon(database=None, user=None, password=None, host=None, port=5432):
    """
    :param database: 数据库名称
    :param user: 该数据库的使用者
    :param password: 密码
    :param host: 数据库地址
    :param port: 数据库链接的端口号
    :return: 返回数据库链接
    """
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    print("Connection successful.")
    return conn


def getConMySQL(database=None, user=None, password=None, host=None, port=3306):
    """
    :param database: 数据库名称
    :param user: 该数据库的使用者
    :param password: 密码
    :param host: 数据库地址
    :param port: 数据库链接的端口号
    :return: 返回数据库链接
    """
    connMy = pymysql.connect(db=database, user=user, passwd=password, host=host, port=port, charset="utf8")
    print("MySQL Connection successful.")
    return connMy


def get_org_list(loc, My_cur):
    # My_Connection = getConMySQL(database='demo', user='root', password='root', host='opsrv.mapout.lan')
    # My_cur = My_Connection.cursor()
    select_sql = "SELECT org FROM demo.org_standard_map_new WHERE loc = '{0}';"
    finish_select_sql = select_sql.format(loc)
    My_cur.execute(finish_select_sql)
    results = My_cur.fetchall()
    result_list = []
    for item in results:
        #print('..........................',item[0])
        result_list.append(item[0])
    # My_cur.close()
    # My_Connection.close()
    return result_list


def set_org_to_map(loc, org, My_Connection, My_cur):
    # My_Connection = getConMySQL(database='demo', user='root', password='root', host='opsrv.mapout.lan')
    # My_cur = My_Connection.cursor()
    insert_sql = "INSERT demo.org_standard_map_new(loc, org) VALUES('{0}', '{1}');"
    finish_select_sql = insert_sql.format(loc, org)
    My_cur.execute(finish_select_sql)
    My_Connection.commit()
    # My_cur.close()
    # My_Connection.close()


#以下两个函数为了获取部门代码
def get_org_code(org, cur):
    cur.execute("SELECT organization_code FROM demo.organization_code_new WHERE organization_name = '{0}';".format(org))
    org_list = cur.fetchall()
    if len(org_list) > 0:
        return org_list[0][0]
    else:
        return None


def set_org_code(org, con, cur):
    insert_sql = "INSERT demo.organization_code_new(organization_name) VALUES('{0}');"
    finish_select_sql = insert_sql.format(org)
    cur.execute(finish_select_sql)
    con.commit()


def get_site_list(org_code,  cur):
    cur.execute("SELECT * FROM demo.site_code_new WHERE org_code = '{0}';".format(org_code))
    site_list = cur.fetchall()
    if len(site_list) > 0:
        return site_list
    else:
        return None


def set_site_code(org_code, site_code, site_name, con, cur):
    insert_sql = "INSERT demo.site_code_new VALUES('{0}', '{1}', '{2}');"
    finish_select_sql = insert_sql.format(site_code, site_name, org_code)
    cur.execute(finish_select_sql)
    con.commit()


def insert_into_resume(data_list, con, cur):
    insert_sql = "INSERT demo.resume_new(id_index, name, id_index_new, id_index_n, start_time, " \
                 "end_time, resume, work_place_id, work_place, institution_id, institution, " \
                 "position_id, position, message_source, source_id) VALUES('{0}', '{1}', '{2}'," \
                 " '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}');"
    for item in data_list:
        try:
            finish_select_sql = insert_sql.format(item[0], item[1], item[2], item[3], item[4],
                                                  item[5], item[6], item[7], item[8], item[9],
                                                  item[10], item[11], item[12], '百度百科', item[13])
            print(finish_select_sql)
            cur.execute(finish_select_sql)
        except:
            f = open("relative_data/log/log.txt", 'a')
            traceback.print_exc(file=f)
            f.flush()
            f.close()
            try:
                out1 = open('relative_data/log/data_can_not_write_back.csv', 'w', newline='')
                csv_writer1 = csv.writer(out1, dialect='excel')
                csv_writer1.writerow(item)
                out1.close()
            except:
                pass
    con.commit()


def get_con_cur():
    My_Connection = getConMySQL(database='demo', user='root', password='root', host='opsrv.mapout.lan')
    My_cur = My_Connection.cursor()
    return My_Connection, My_cur


def db_close(My_Connection, My_cur):
    My_Connection.close()
    My_cur.close()


