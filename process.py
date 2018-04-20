from Structer import Structer
from Mapping import Mapping_data
import csv
from process_introduce import process_introduce
import re
import db_operator
import time
import traceback

#this one use to stucter all the data in person table to resume table
def process_all():
    relativ_data = Mapping_data()
    relativ_data.init_site_data()
    relativ_data.init_loc_data()

    stucter = Structer()
    con, cur = db_operator.get_con_cur()
    #print('initing.....')

    out = open('relative_data/data_from_person_structed_ai_after_standard_20180418.csv', 'w', newline='')
    csv_writer = csv.writer(out, dialect='excel')

    out1 = open('relative_data/dirty_data_from_person_structed_ai.csv', 'w', newline='')
    csv_writer1 = csv.writer(out1, dialect='excel')

    # My_Connection = getConMySQL(database='demo', user='root', password='root', host='opsrv.mapout.lan')
    # My_cur = My_Connection.cursor()
    cur.execute("SELECT id_index, officer_name, time_and_job, source_id FROM demo.person;")
    all_num = 0
    num = 0
    work_list = []
    for line in cur.fetchall():
        if num == 10:
            all_num += num
            print('we have struct  ' + str(all_num) + 'persons')
            print('new time is : ', time.asctime(time.localtime(time.time())))
            print('print the work list')
            print(work_list)
            db_operator.insert_into_resume(work_list, con, cur)
            num = 0
            work_list.clear()
            #pass
        else:
            id_index, officer_name, time_and_job, source_id = line
            structural_data = process_introduce(time_and_job)
            person_work_list = []
            if not structural_data:
                continue
            num += 1
            for item in structural_data:
                time_and_jobs_list = process_one_item(item, relativ_data, stucter, csv_writer, csv_writer1, con, cur)
                person_work_list.extend(time_and_jobs_list)
            resume_list = make_mapping(id_index, officer_name, person_work_list, source_id, relativ_data, con, cur)
            work_list.extend(resume_list)
    print('print the work list')
    print(work_list)
    db_operator.insert_into_resume(work_list, con, cur)

    db_operator.db_close(con,cur)
        #school_dict[name] = code
        #print(name + code)

#该函数用于做好映射，得到一系列id
def make_mapping(id_index, officer_name, time_and_job_list, source_id, relative_data, con, cur):

    resume_list = []

    num = 0
    for item in time_and_job_list:
        try:
            num += 1
            data_item, time, loc, org, work = item
            # get introduce id
            id_index_new = id_index + ('00000000' + str(num))[-3:]
            id_index_n = id_index_new
            # get start time and end time
            time_list = time.split('-')
            start_time = time_list[0]
            if len(time_list) > 1:
                end_time = time_list[1]
            else:
                end_time = '不详'
            # get loc id
            if loc == -1 or loc == '-1' or (loc not in relative_data.loc_data):
                loc_id = '00199999900000000000'
            else:
                loc_id = '001' + relative_data.loc_to_id[loc] + '00000000000'

            # get org id
            org_code = db_operator.get_org_code(org, cur)
            if not org_code:
                db_operator.set_org_code(org, con, cur)
            org_code = db_operator.get_org_code(org, cur)
            # print(loc_id, org_code)
            standart_org_code = loc_id[:9] + ('000000000000000000' + str(org_code))[-8:] + '000'

            # get work code
            work_list = db_operator.get_site_list(standart_org_code, cur)
            site_code = ''
            if work_list:
                for workitem in work_list:
                    if workitem[1][0] == work:
                        site_code = workitem[0][0]
                        break
                if site_code == '':
                    site_code = str(len(work_list) + 1)
                    db_operator.set_site_code(standart_org_code, site_code, work, con, cur)
            else:
                db_operator.set_site_code(standart_org_code, 1, work, con, cur)
            standart_site_code = standart_org_code + ('0000' + str(site_code))[-3:]
            resume_list.append([id_index, officer_name, id_index_new, id_index_n, start_time, end_time,
                                data_item, loc_id, loc, standart_org_code, org, standart_site_code, work, source_id])
        except:
            f = open("relative_data/log/log.txt", 'a')
            traceback.print_exc(file=f)
            f.flush()
            f.close()

            try:
                out1 = open('relative_data/log/data_can_not_mapping.csv', 'w', newline='')
                csv_writer1 = csv.writer(out1, dialect='excel')
                csv_writer1.writerow(item)
                out1.close()
            except:
                pass

    return resume_list



def process():

    relativ_data = Mapping_data()
    relativ_data.init_site_data()
    relativ_data.init_loc_data()

    stucter = Structer()
    con, cur = db_operator.get_con_cur()

    print('start..')
    sv_reader = csv.reader(open('relative_data/data_from_db.csv', encoding='gbk'))

    out = open('relative_data/data_from_db_structed_ai_after_standard_20180411.csv', 'w', newline='')
    csv_writer = csv.writer(out, dialect='excel')

    out1 = open('relative_data/dirty_data_from_db_structed_ai.csv', 'w', newline='')
    csv_writer1 = csv.writer(out1, dialect='excel')

    # 人工职位库表
    #work_list = data_structe.get_list('../data/work.txt')
    # 代码生成职位库
    work_list = relativ_data.loc_data

    for row in sv_reader:
        # print(row)
        if len(row) == 0:
            continue
        if row[0] == '':
            continue

        print(row)
        structural_data = process_introduce(row[0])

        if structural_data is not None:
            for item in structural_data:
                result = process_one_item(item, relativ_data, stucter, csv_writer, csv_writer1, con, cur)
                #struct(item, work_list, csv_writer, csv_writer1)


        # lines = info.split('\n')
        # for item in lines:
        #    if item == '':
        #        continue
        #    data_structe.get_info(row[0], item, work_list, csv_writer)
    db_operator.db_close(con, cur)
    out.close()
    out1.close()
    stucter.close()


def process_one_item(item, data_class, structer, csv_w, dir_csv_w, con, cur):
    print(item)
    result_list = []
    time = structer.get_time(item[0])
    data = item[1]
    loc_map = data_class.loc_mapping

    # 此处预处理，将句子分成多个履历原句
    # data = data_structe.delete(data, '\(.*\)?|\(.*\）?|\（.*\）?')
    # data = data_structe.delete(data, '加入|曾前后|当选|被聘为|在|担任|调任|曾任|作为|历任|先后任|任聘|\+|\-|\?')
    data = re.sub('兼', ',', data)
    data_list = re.split('、|，|,', data)

    # 使用old_ 系列来记录旧的记录，方便下延
    # old_time = -1
    old_loc = -1
    old_work = -1
    old_dep = -1
    for data_item in data_list:
        try:
            loc = -1
            org = -1

            # 淘汰没用的信息
            if data_item == '' or data_item == ' ':
                continue
            print(data_item)
            # 直接使用模型抽取地名和组织结构名
            loc_list, org_list = structer.get_long_org(data_item)
            # 使用字典抽取职位
            work = structer.get_site(data_item)
            # 一旦模型抽取不出地名，使用预先生成的字典抽取
            # 最后将地名并标准化
            if len(loc_list) == 0:
                loc = structer.get_loc(data_item)
            else:
                loc = loc_list[0]
                if loc in loc_map.keys():
                    loc = loc_map[loc]
            '''
            if len(site_list) == 0:
                dir_csv_w.writerow([data_item, time, loc, site_list, work])
                continue
            elif not work:
                dir_csv_w.writerow([data_item, time, loc, site_list, work])
                continue
            else:
            '''
            # 如果这一句里面没有地名，那么可能是是 上海市书记，副省长这种，使用上一地名即可
            if loc == -1 and old_loc != -1:
                loc = old_loc

            # 判断部门有没有抽取到，没有的话，可能是：
            # 1. 上海市书记，副省长
            # 2. 上海市省长
            # 3. 真的没有
            if len(org_list) > 0:
                org = org_list[0]
                # 解决模型把教师也放进部门的问题
                if len(org) > 2 and org[-2] == '教' and org[-1] == '师':
                    org = org[:-2]
                # 解决抽取模型把总工程师误认为是部门的问题
                if org == '总工程师':
                    org = -1
            else:
                if old_dep != -1 and work != -1:
                    org = old_dep
                # 解决模型不能识别人民政府的问题
                if loc != -1 and work != -1 and work in data_class.special_people:
                    org = '人民政府'
                '''
                if old_dep != -1 and work != -1:
                    org = old_dep
                else :
                    org = None
                '''

            org = structer.standard_org(data_item, loc, org, con, cur)
            if org != -1:
                old_dep = org
            if loc != -1:
                old_loc = loc
            old_work = work

            if time != -1 and org != -1 and work != -1:
                result_list.append([data_item, time, loc, org, work])
                print([data_item, time, loc, org, work])
                csv_w.writerow([data_item, time, loc, org, work])
            else:
                dir_csv_w.writerow([data_item, time, loc, org, work])
        except:
            f = open("relative_data/log/log.txt", 'a')
            traceback.print_exc(file=f)
            f.flush()
            f.close()
            try:
                out1 = open('relative_data/log/data_can_not_struct.csv', 'w', newline='')
                csv_writer1 = csv.writer(out1, dialect='excel')
                csv_writer1.writerow(data_item)
                out1.close()
            except:
                pass

    return result_list









if __name__ == "__main__":
    start_time = time.time()
    #process()
    process_all()
    print('start time is : ', time.asctime(time.localtime(start_time)))
    print('end time is : ', time.asctime(time.localtime(time.time())))