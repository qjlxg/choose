# !/usr/bin/python
# -*- coding: utf-8 -*-
import time
import datetime
import glob
import urllib.request
import json
import sys
import re
import threading
import queue
import csv
import os

# 使用方法
def usage():
    print('fund-rank.py usage:')
    print('\tpython fund.py start-date end-date fund-code=none\n')
    print('\tdate format ****-**-**')
    print('\t\tstart-date must before end-date')
    print('\t\tfund-code default none')
    print('\t\tif not input, get top 20 funds from all more than 6400 funds')
    print('\t\telse get that fund\'s rate of rise\n')
    print('\teg:\tpython fund-rank.py 2017-03-01 2017-03-25')
    print('\teg:\tpython fund-rank.py 2017-03-01 2017-03-25 377240')

# 获取某一基金在某一日的累计净值数据
def get_jingzhi(strfundcode, strdate):
    try:
        url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + \
              strfundcode + '&page=1&per=20&sdate=' + strdate + '&edate=' + strdate
        response = urllib.request.urlopen(url)
        content = response.read().decode('utf-8')
        p = re.compile(r'<td>(.*?)</td><td>(.*?)</td><td>.*?</td><td>.*?</td><td>.*?</td><td>.*?</td>')
        jingzhi_data = p.findall(content)
        if jingzhi_data:
            return jingzhi_data[0]
        else:
            return None, None
    except urllib.error.HTTPError as e:
        print(e)
        return None, None

def read_funds_from_csv(filename):
    """
    Reads fund codes from a CSV file.
    The CSV is expected to have a column named '代码'.
    """
    funds_list = []
    # Make the path absolute to ensure it's found
    file_path = os.path.join(os.getcwd(), filename)
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            code_index = -1
            try:
                code_index = header.index('代码')
            except ValueError:
                print("Error: The CSV file does not contain a '代码' column.")
                return funds_list

            for row in reader:
                if row[code_index]:
                    funds_list.append(row[code_index])
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
    return funds_list

# 工作线程函数，从队列中获取基金代码并处理
def worker(task_queue, strsdate, stredate, result_queue):
    while True:
        try:
            fund_code = task_queue.get(timeout=1) # 1s timeout
            print('Processing fund:', fund_code)
            
            # 获取开始日期和结束日期的净值
            start_jingzhi, start_date = get_jingzhi(fund_code, strsdate)
            end_jingzhi, end_date = get_jingzhi(fund_code, stredate)

            if start_jingzhi and end_jingzhi:
                start_jingzhi = float(start_jingzhi)
                end_jingzhi = float(end_jingzhi)
                jingzhi_delta = end_jingzhi - start_jingzhi
                rose_rate = (end_jingzhi - start_jingzhi) / start_jingzhi
                
                # 获取基金名称
                try:
                    url = f'http://fund.eastmoney.com/{fund_code}.html'
                    response = urllib.request.urlopen(url)
                    html_content = response.read().decode('utf-8')
                    # 查找基金名称
                    fund_name_match = re.search(r'<title>(.*?)</title>', html_content)
                    fund_name = fund_name_match.group(1).replace('基金净值_估值_行情走势_最新净值-天弘基金网', '').strip() if fund_name_match else '未知'
                except Exception as e:
                    fund_name = '未知'
                    print(f"Error fetching name for {fund_code}: {e}")
                    
                result_queue.put([fund_code, fund_name, jingzhi_delta, rose_rate])
            else:
                print(f"Could not get data for fund {fund_code} from {strsdate} to {stredate}")
                
        except queue.Empty:
            break
        finally:
            task_queue.task_done()

# 主程序
def main():
    if len(sys.argv) < 3:
        usage()
        sys.exit()

    strsdate = sys.argv[1]
    stredate = sys.argv[2]
    strfundcode = 'none'

    # 基金代码，通过传入参数决定
    if len(sys.argv) >= 4:
        strfundcode = sys.argv[3]
    
    start_time = time.time()
    all_funds_list = []

    # --- 从CSV文件读取基金代码 ---
    print("Reading fund codes from 'recommended_cn_funds.csv'...")
    all_funds_list = read_funds_from_csv('recommended_cn_funds.csv')
    print(f"Found {len(all_funds_list)} fund codes to process.")
    # --- 修改部分结束 ---
    
    if strfundcode != 'none':
        # 如果指定了基金代码，只处理这个基金
        all_funds_list = [strfundcode]
    
    # 使用多线程处理
    task_queue = queue.Queue()
    result_queue = queue.Queue()

    # 将所有基金放入任务队列
    for fund in all_funds_list:
        task_queue.put(fund)

    # 创建并启动线程
    threads = []
    for i in range(10): # 使用10个线程，可以根据网络情况调整
        t = threading.Thread(target=worker, args=(task_queue, strsdate, stredate, result_queue))
        t.daemon = True # 设置为守护线程，主线程退出时自动结束
        t.start()
        threads.append(t)

    # 等待所有任务完成
    task_queue.join()

    # 收集所有结果
    all_funds_list = []
    while not result_queue.empty():
        all_funds_list.append(result_queue.get())
    # --- 并行处理部分结束 ---

    fileobject = open('result_' + strsdate + '_' + stredate + '_C类.txt', 'w')
    
    # 修复：按增长率（索引3）排序
    all_funds_list.sort(key=lambda fund: fund[3], reverse=True)
    strhead = '排序\t' + '编码\t\t' + '名称\t\t' + '净增长' + '\t' + '增长率' + '\n'
    print(strhead)
    fileobject.write(strhead)
    
    for index in range(len(all_funds_list)):
        strcontent = str(index+1) + '\t' + all_funds_list[index][0] + '\t' + \
        all_funds_list[index][1] + '\t' + str(all_funds_list[index][2]) + '\t' + \
        str(all_funds_list[index][3]) + '\n'
        print(strcontent)
        fileobject.write(strcontent)

    fileobject.close()
    end_time = time.time()
    
    print('总共耗时：' + str(end_time - start_time) + '秒')
    print('数据已经写入到文件：result_' + strsdate + '_' + stredate + '_C类.txt 中')

if __name__ == '__main__':
    main()
