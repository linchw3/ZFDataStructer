import csv
import re

class Mapping_data:
    site_data = []
    loc_data = []

    #机构为二维数组，0 代表国家机构， 1 代表 企业， 2代表学校
    org_data = [[], [], []]

    site_mapping = {}
    loc_mapping = {}
    org_mapping = {}

    site_to_id = {}
    loc_to_id = {}
    org_to_id = {}

    #这里添加一种数据为了人工识别人民政府
    special_people = ['省长','副省长','省长助理','市长','副市长','市长助理','区长','副区长','区长助理','镇长','副镇长',
                      '镇长助理','乡长','副乡长','县长','副县长','州长','副州长','秘书长','副秘书长']

    minority = ['蒙古族','回族','藏族','维吾尔族','苗族','彝族', '壮族', '布依族','朝鲜族','满族','侗族','瑶族','白族','土家族','哈尼族','哈萨克族','傣族',
                '黎族','僳僳族','佤族','畲族','高山族','拉祜族','水族','东乡族','纳西族','景颇族','柯尔克孜族','土族','达斡尔族',
                '仫佬族','羌族','布朗族','撒拉族','毛南族','仡佬族','锡伯族','阿昌族','普米族','塔吉克族','怒族',
                '乌孜别克族','俄罗斯族','鄂温克族','德昂族','保安族','裕固族','京族','塔塔尔族','独龙族','鄂伦春族','赫哲族','门巴族','珞巴族','基诺族']
    def __init__(self):
        pass

    def init_site_data(self):
        for line in open('relative_data/more_work.txt', encoding='UTF-8'):
            line = line.split('\n')[0]
            self.site_data.append(line)

    def init_loc_data(self):

        temp_list = []

        self.loc_data.append('天津河北')
        self.loc_data.append('天津市河北区')

        self.loc_mapping['天津河北'] = '天津市河北区'
        self.loc_mapping['天津市河北区'] = '天津市河北区'

        #with open('D:\projects\OfficerDataProcess\dict_data\地名.csv', 'r', encoding='utf_8') as my_csv_file:
        with open('relative_data/地名.csv', 'r', encoding='gbk') as my_csv_file:
            lines = csv.reader(my_csv_file)
            for item in lines:
                item = item[0].split('\t')
                if item[0] == '天津市河北区':
                    continue

                if item[0] == '\ufeff"七台河市"':
                    item[0].encode('utf-8').decode('utf-8-sig')

                # city_long_dict[]
                self.loc_data.append(item[0])
                self.loc_mapping[item[0]] = item[0]
                self.loc_to_id[item[0]] = item[1]

                original_name = item[0]

                #去掉自治区和民族的干扰
                simple_name = re.sub('自治', '', item[0])
                for it in self.minority:
                    simple_name = re.sub(it, '', simple_name)

                name1 = simple_name.split('市')

                if len(name1) == 1 and name1 != '贵州省':
                    name1 = name1[0].split('州')
                # print(name1)
                if len(name1) > 1 and name1[-1] != '':
                    new_name = name1[-1][:-1]

                    if len(new_name) > 1:
                        self.loc_data.append(name1[0] + new_name)
                        self.loc_mapping[name1[0] + new_name] = original_name
                        #self.loc_data.append(new_name)
                        temp_list.append(new_name)
                        # if new_name == '思南':
                        #     print(temp_list[-3:])
                        self.loc_mapping[new_name] = original_name
                    else:
                        self.loc_data.append(name1[0] + name1[-1])
                        self.loc_mapping[name1[0] + name1[-1]] = original_name

                        # self.loc_data.append(new_name)
                        temp_list.append(name1[-1])
                        self.loc_mapping[name1[-1]] = original_name

                else:
                    if name1[0][-1] == '省' or name1[0][-1] == '市' or name1[0][-1] == '区':
                        new_name = name1[0][:-1]
                    else:
                        new_name = name1[0]
                        #少数民族



                    if len(new_name) > 1:
                        #self.loc_data.append(new_name)
                        temp_list.append(new_name)
                        self.loc_mapping[new_name] = original_name
        #print(temp_list.index('思南'))
        self.loc_data.extend(temp_list)
        #print(self.loc_data.index('思南'))

    def init_org(self):
        #初始化政府机构
        for line in open('relative_data/政府机构大全.txt', encoding='UTF-8'):
            line = line.split('\n')[0]
            self.org_data[0].append(line)

        for line in open('relative_data/企业名录.txt', encoding='UTF-8'):
            line = line.split('\n')[0]
            self.org_data[1].append(line)

        for line in open('relative_data/院校大全.txt', encoding='UTF-8'):
            line = line.split('\n')[0]
            self.org_data[2].append(line)


if __name__ == "__main__":
    data = Mapping_data()
    data.init_loc_data()

    print(data.loc_data)
    print(data.loc_data.index('思南'))
    f = open('test.txt', 'w', encoding='utf_8')
    i = 0
    for item in data.loc_data:
        #print(item)
        #print(i)
        print(i, item)
        i += 1
        f.write(item + '\n')
    f.close()
    f = open('map_test.txt', 'w', encoding='utf_8')
    for item in data.loc_mapping.items():
        print(item)
        f.write(item[0] + '  ' + item[1] + '\n')
    f.close()
    #print(data.loc_mapping)
