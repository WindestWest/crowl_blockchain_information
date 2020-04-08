#coding=utf-8
import requests
import lxml
from lxml import etree
import json
import time,datetime,sys
from retrying import retry
import arrow
import os,sys
from collections import OrderedDict
import csv

@retry(wait_fixed=2,stop_max_attempt_number=3)
def get_block_html(url, date_time):
    url = url + str(date_time)
    headers = {
            "cookie":"uid=412210; _globalGA=GA1.2.1185037246.1571712614; _ga=GA1.2.359479691.1571712614;    intercom-id-yttqkdli=a6516f7a-e0dc-44f2-ba55-6d11f88b686d",
            "user-agent":"your bot 0.1"
            }

    try:
        reponse = requests.get(url,headers=headers)
    # print(date_time)
        return reponse
    except requests.exceptions.ReadTimeout as time_out:
        print(date_time + ' error time_out')
        return time_out



def get_day_block_inf(html):
    page = etree.HTML(html)
    block_inf = {}
    heights = page.xpath('//a[@ga-type="block"]/text()')

    times = page.xpath('//span[@class="text-muted -btc-time-localization"]/text()')

    # for num in range(len(times)):
    #     block_inf[int(heights[num].replace(",",""))] = times[num].strip()
    # return block_inf

    poolnames = page.xpath('//div[@class="cell-poolname"]/a/text()')

    outputs = page.xpath('//td[@class="text-right"]//text()')

    chanchus = page.xpath("/html/body/div[@class='main-body']/div[@class='container']/div[@class='row blockList']/div[@class='blockList-inner']/div[@class='blockList-list']/table[@class='table']/tr/td[8]/text()")

    return (heights, times, poolnames, outputs, chanchus)


class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    ''' 二级字典赋值'''
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value



def new_blockchain(block_information):

    heights = block_information[0]
    times = block_information[1]
    poolnames = block_information[2]
    outputs = block_information[3]
    chanchus = block_information[4]

    # block = Blocks()

    blocks = AutoVivification()

    infs = []
    for height_num in block_information[3]:
        inf = height_num.strip().replace("\\n",'')  
        if inf != '':
            infs.append(inf)


    # block = {}
    # for num in range(len(times)):
    for num in range(len(heights)):
        height = int(heights[num].strip().replace(',',''))
        outputs_num = num * 9 
        chanchu_num = num * 2
        blocks[height]['height'] = int(heights[num].strip().replace(',',''))
        blocks[height]['time'] = times[num].strip()
        blocks[height]['poolname'] = poolnames[num].strip()
        blocks[height]['trans_num'] = infs[num*7 + 1]
        blocks[height]['stripped_Size'] = infs[num * 7 + 2]
        blocks[height]['size_B'] = infs[num*7 + 3]
        blocks[height]['weight'] = infs[num*7 + 4]
        blocks[height]['Average_cost'] = infs[num*7 + 5]
        blocks[height]['profit'] = chanchus[chanchu_num]

    one_blocks = blocks

    return one_blocks


def read_file(block_json):
    with open( block_json,'r') as blocks:
        blocks = blocks.read()
    # dict_blocks = json.loads(blocks)
    return json.loads(blocks)

# def save(new_blocks, json_name):
#     json_blocks = json.dumps(new_blocks)
#     # print (json_name)
#     with open( 'block_csv/'+json_name +'.json','w') as blocks:
#         blocks.write(json_blocks)

def save_to_csv(dict_blcoks, json_name):
    csvfile = open('block_csv/'+json_name + '_blockinf.csv', 'w', newline='')

    writer = csv.writer(csvfile)

    writer.writerow(['height','time','poolname','trans_num','stripped_Size', "size_B", "weight", "Average_cost", "profit"])

    list1=[]

    for i in sorted (dict_blcoks): 
        s=tuple(list((dict_blcoks[i].values())))
        # list1.append(key)
        list1.append(s)

    writer.writerows(list1)

    csvfile.close()





def save_error_log(error):
    with open( 'error.log','a+') as blocks:
        blocks.write(error)
        blocks.write("\n")



def new_year(date_time):
    # 01-01 happy new year 
    today = time.strptime(date_time,"%Y-%m-%d")
    if (today.tm_mon == 12 and today.tm_mday == 31):
        print("Tomorrow is %s, HAPPY NEW YEAR" % date_time)
        # sys.stdout.flush()
        return(today.tm_year, True)
    else:
        return(today.tm_year, False)



def isLeapYear(years):
    assert isinstance(years, int), "请输入整数年，如 2018"
 
    if ((years % 4 == 0 and years % 100 != 0) or (years % 400 == 0)):  # 判断是否是闰年
        # print(years, "是闰年")
        days_sum = 366
        return days_sum
    else:
        # print(years, '不是闰年')
        days_sum = 365
        return days_sum


def getAllDayPerYear(year):

    startDate = str(year) + '-1-1'
    a = 0
    all_day_lists = []
    if year !=  arrow.now().year:
        days_sum = isLeapYear(year)
        while  a < days_sum:
            day = arrow.get(startDate).shift(days=a).format("YYYY-MM-DD")

            a += 1
            all_day_lists.append(day)

        return all_day_lists

    else:
        for num in range(366):
            if arrow.get(startDate).shift(days=num).format("YYYY-MM-DD") == arrow.now().format("YYYY-MM-DD"):
                break
            else:
                all_day_lists.append(arrow.get(startDate).shift(days=num).format("YYYY-MM-DD"))

        return all_day_lists
# if __name__ == '__main__':
    # print(getAllDayPerYear(2019))



# def if_height_continue(height_list):
#     if height_list:
#         diff = max(height_list) - min(height_list)
#         hei = len(height_list) - 1
#         if diff  == hei:
#             return True
#         else:
#             return False


def found_miss_block_list(blocks_heights):
    all_heights = list(range(min(blocks_heights),max(blocks_heights)))

    return list(set(all_heights) - set(blocks_heights))



def block_main(url, per_date):


    # datestart=datetime.datetime.strptime(start,'%Y-%m-%d')
    # dateend=datetime.datetime.strptime(end,'%Y-%m-%d')
    startDay = OrderedDict()
    
    # while datestart<dateend:
        # datestart+=datetime.timedelta(days=1)
        # date_time = datestart.strftime('%Y-%m-%d')

        # print(date)
    html = get_block_html(url, per_date).text
    inf_tuple = get_day_block_inf(html)

    one_day_blocks = new_blockchain(inf_tuple)

    height_list = [key for key in one_day_blocks.keys()]
    
    # if not if_height_continue(height_list):
    #     error_day = per_date
    #     print(error_day + ' Complete data not obtained')
    #     # save_error_log(error)
    #     return (False,error_day)
    # return (True,one_day_blocks)

    return(one_day_blocks, height_list)
    

def get_block_informatin(html):
    page = etree.HTML(html)
    block_inf = {}
    # 
    height_inf = page.xpath("//div[@class='panel panel-bm blockAbstract']/div[@class='panel-body text-center']/div[@class='blockAbstract-inner']/div[@class='blockAbstract-section']//text()")

    return height_inf


def get_block_dict(height_infs, height):
    infs = []
    for height_inf in height_infs:
        inf = height_inf.strip().replace("\\n",'')  
        if inf != '':
            infs.append(inf)
    
    blocks = AutoVivification()

    blocks[height]["height"] = height
    blocks[height]['time'] = infs[26]
    blocks[height]['poolname'] = infs[24]
    blocks[height]['trans_num'] = infs[13]
    blocks[height]['stripped_Size'] = infs[8]
    blocks[height]['size_B'] = infs[5]
    blocks[height]['weight'] = infs[11]
    blocks[height]['Average_cost'] = None
    blocks[height]['profit'] = None

    # print(get_block_html('2009-01-03').status_code)

    return blocks