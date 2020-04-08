import threading
from threading import Lock
from collections import OrderedDict
import os,sys
import multiprocessing
import datetime,time
from get_block_time1 import block_main
from get_block_time1 import getAllDayPerYear
from get_block_time1 import save_to_csv
from get_block_time1 import new_year
from get_block_time1 import read_file
# from get_block_time1 import deal_with_error_day
from get_block_time1 import save_error_log
from get_block_time1 import found_miss_block_list
from get_block_time1 import get_block_html
from get_block_time1 import get_block_dict
import queue



'''
程序目的：
 通过对btc.com的多次访问，获取到比特币的历史区块：高度、时间、手续费的等信息

1、首先使用 requests抓取网站数据，并使用lxml进行解析，获取到想要的数据。
2、将抓取到的数据整合进csv文件。
3、采用多线程的方式进程爬虫。


遇到的难点：
1、编程逻辑混乱，调成了很多次思路，写程序前需要列好流程图
2、按照天爬虫时，可能会有高度遗漏， 需要设立检查机制。
3、感谢btc.com对本次程序的帮助。


用到知识点：
1、通过年份 获取到当年的所有天数的列表
2、如何检测高度是否有遗漏
3、用到的新模块 retry、 arrow
4、 多线程 + 队列


'''

if __name__ == '__main__':
    url_day = 'https://btc.com/block?date='

    #输入需要抓取的年份列表
    year_start = 2014
    year_end = 2020

    url_height = 'https://btc.com/block/'

    
    queueLock = Lock()
    # error_days = []
    # exitFlag = 0
    workQueue = queue.Queue()
    startDayLock =Lock()
    completed_listsLock = Lock()
    # threads = []


    global startDay
    # global completed_list

    threadList = ['thread-1','thread-2','thread-3','thread-4','thread-5']
    
    

    class MyThread(threading.Thread):
        """docstring for MyThread"""
        def __init__(self, threadName, q):
            super(MyThread, self).__init__()
            self.threadName = threadName
            self.q = q
        
        def run(self):
            print('%s starting, pid is  %s' %(self.threadName, os.getpid()))
            process_data(self.threadName, self.q)
            print('%s ended' % self.threadName)

    for year in range(year_start, year_end+1):

        error_days = []
        exitFlag = 0
        threads = []
        completed_lists = []


        all_days_list = getAllDayPerYear(year)

        for day in all_days_list:
            workQueue.put(day)

        startDay = OrderedDict()
        completed_lists = []
        # completed_lists = []
        def process_data(threadName, q):

            while not exitFlag:
                queueLock.acquire()
                if not workQueue.empty():
                    date = q.get()
                    queueLock.release()

                    one_day_blocks_height = block_main(url_day, date)


                    startDayLock.acquire()
                    startDay.update(one_day_blocks_height[0])
                    startDayLock.release()


                    print(' %s :  %s first time  is  ok ' %(threadName, date))

                    completed_listsLock.acquire()
                    completed_lists.extend(one_day_blocks_height[1])
                    completed_listsLock.release()
                    
                else:
                    queueLock.release()
                time.sleep(1)



        for tName in threadList:
            thread = MyThread(tName, workQueue)
            thread.start()
            threads.append(thread)


        # 等待队列清空
        while not workQueue.empty():
            pass

        # 通知线程是时候退出
        exitFlag = 1

        # 等待所有线程完成
        for t in threads:
            t.join()


        #处理异常数据
        # if os.path.isfile('error.log'):
        #     errorDayList = read_file('error.log')
        #     error_block_inf = deal_with_error_day(errorDayList)

        print("开始查重")
        lost_block_list = found_miss_block_list(completed_lists)

        for lost_block in lost_block_list:
            html = get_block_html(url_height, lost_block)
            lost_block_dict = get_block_dict(html,lost_block)

            print('%s Inspection completed' % lost_block)

            startDay.update(lost_block_dict)
    # sys.stdout.flush()
            time.sleep(1)

        this_year = new_year(max(all_days_list))

    # save(blocks)

        if this_year[1] :
                # print(this_year,'asdsasaddad')
            save_to_csv(startDay, str(this_year[0]))
                # startDay = OrderedDict()
        if not this_year[1]:
            save_to_csv(startDay, str(this_year[0]))

        print('************************')
        print('%s Finished data obtained' %str(this_year[0]))
        print('************************')