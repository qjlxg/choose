# !/usr/bin/python
# -*- coding: utf-8 -*-
import time
import datetime
import urllib.request
import sys
import re
import threading
import queue
import csv
import io
import os # 确保文件路径处理

# 使用方法
def usage():
    print('fund-rank.py usage:')
    print('\tpython fund.py start-date end-date fund-code=none\n')
    print('\tdate format ****-**-**')
    print('\t\tstart-date must before end-date')
    print('\tfund-code default none')
    print('\t\tif not input, get top 20 funds from recommended_cn_funds.csv')
    print('\t\telse get that fund\'s rate of rise\n')
    print('\teg:\tpython fund-rank.py 2017-03-01 2017-03-25')
    print('\teg:\tpython fund-rank.py 2017-03-01 2017-03-25 377240')

# 获取某一基金在某一日的累计净值数据
def get_jingzhi(strfundcode, strdate):
    try:
        url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + \
              strfundcode + '&page=1&per=20&sdate=' + strdate + '&edate=' + strdate
        response = urllib.request.urlopen(url, timeout=10) # 增加超时设置
    except urllib.error.HTTPError as e:
        print(f"[{strfundcode}] HTTPError: {e}")
        return '-1'
    except Exception as e:
        # 捕获 Connection reset by peer 等连接错误
        print(f"[{strfundcode}] Error accessing URL: {e}")
        return '-1'

    json_fund_value = response.read().decode('utf-8')
    # 注意：这里的正则提取逻辑和原始脚本一样，只提取日期对应的净值
    p = re.compile(r'<td>(.*?)</td><td>(.*?)</td><td>.*?</td><td>.*?</td><td>.*?</td><td>.*?</td>')
    jingzhi_data = p.findall(json_fund_value)
    
    if jingzhi_data:
        # 净值通常在第二个 td (entry[1])，但为了保持原始脚本逻辑，需要更精确的解析
        # 原始脚本的逻辑是复杂的，它似乎试图在 "单位净值" 和 "累计净值" 之间选择一个。
        # 我们保留原有的复杂逻辑，但简化为提取第一个日期的数据
        try:
            # 原始脚本的复杂正则匹配
            tr_re = re.compile(r'<tr>(.*?)</tr>')
            item_re = re.compile(r'''<td>(\d{4}-\d{2}-\d{2})</td><td.*?>(.*?)</td><td.*?>(.*?)</td><td.*?>(.*?)</td><td.*?>(.*?)</td><td.*?>(.*?)</td><td.*?></td>''', re.X)
            
            for line in tr_re.findall(json_fund_value):
                match = item_re.match(line)
                if match:
                    entry = match.groups()
                    jingzhi1 = entry[1] # 单位净值
                    jingzhi2 = entry[2] # 累计净值
                    
                    if jingzhi2.strip() == '':
                        jingzhi = '-1'
                    elif jingzhi2.find('%') > -1:
                        jingzhi = '-1'
                    # 这里的逻辑是选择累计净值和单位净值中较大的一个（如果它们都是数字）
                    elif jingzhi1.replace('.', '', 1).isdigit() and jingzhi2.replace('.', '', 1).isdigit():
                        if float(jingzhi1) > float(jingzhi2):
                            jingzhi = entry[1]
                        else:
                            jingzhi = entry[2]
                    else:
                         jingzhi = entry[2] # 默认为累计净值
                    
                    return jingzhi # 只需要返回第一个匹配到的数据
        except Exception as e:
            print(f"[{strfundcode}] Error parsing jingzhi: {e}")
            return '-1'

    return '-1'

# --- 线程工作函数 ---
def worker(q, strsdate, stredate, result_queue):
    while True:
        try:
            fund = q.get(timeout=1) # 1s timeout
        except queue.Empty:
            break
            
        strfundcode = fund[0]
        print(f'process fund:\t{strfundcode}\t{fund[2]}')

        # 获取净值
        jingzhimin = get_jingzhi(strfundcode, strsdate)
        jingzhimax = get_jingzhi(strfundcode, stredate)

        # ----------------------------------------------------
        # 修正：防止因为连接错误导致脚本失败，同时保留原始脚本的容错逻辑
        # ----------------------------------------------------
        jingzhidif = 0
        jingzhirise = 0.0

        if jingzhimin == '-1' or jingzhimax == '-1' or jingzhimin.strip() == '' or jingzhimax.strip() == '':
            jingzhimin = '0'
            jingzhimax = '0'
        elif jingzhimin.find('%') > -1 or jingzhimax.find('%') > -1:
            pass # 保持 0 
        else:
            try:
                # 原始逻辑：保留两位小数的百分比
                jingzhidif = float('%.4f' %(float(jingzhimax) - float(jingzhimin)))
                if float(jingzhimin) != 0:
                     jingzhirise = float('%.2f' %(jingzhidif * 100 / float(jingzhimin)))
            except ValueError:
                # 如果净值无法转换为浮点数，保持 0
                pass

        # 确保列表有足够的空间来附加数据
        # 原始列表 fund 长度为 6: [代码, '', 名称, 类型, '', '']
        # 附加数据后长度为 10: [..., 净值min, 净值max, 净增长, 增长率]
        fund[5] = jingzhimin
        fund.append(jingzhimax)
        fund.append(jingzhidif)
        fund.append(jingzhirise)
        
        result_queue.put(fund)
        q.task_done()
        
        # ----------------------------------------------------
        # 关键修正：增加延时，防止触发服务器反爬机制 (Connection reset by peer)
        # ----------------------------------------------------
        time.sleep(1) # 每个基金处理间隔 1 秒

# --- 从本地 CSV 文件加载基金列表 ---
def load_fund_list_from_csv():
    encodings = ['utf-8', 'gbk', 'gb18030']  # 尝试的编码列表
    filename = 'recommended_cn_funds.csv'
    file_path = os.path.join(os.getcwd(), filename)
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as csv_file:
                reader = csv.DictReader(csv_file)
                fund_list = []
                for row in reader:
                    # ----------------------------------------------------
                    # 修正：构造一个包含足够占位符的列表，使其与 worker 函数中最终附加的数据对齐
                    # 原始列表结构：[代码, '', 名称, 类型, '', ''] （长度为 6）
                    # ----------------------------------------------------
                    fund = [
                        row['代码'], 
                        '', # 占位符 1
                        row['名称'], 
                        '混合型', # 类型占位符
                        '', # 占位符 2
                        ''  # 占位符 3 (用于 fund[5] 赋值)
                    ]
                    fund_list.append(fund)
                print(f"Successfully loaded CSV with {encoding} encoding")
                return fund_list
        except UnicodeDecodeError:
            continue
        except KeyError:
            print("Error: CSV file is missing required columns ('代码' or '名称').")
            return []
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
            return []
        except Exception as e:
            print(f"Error loading local CSV: {e}")
            return []
            
    print("Failed to load recommended_cn_funds.csv. Please ensure the file exists and is correctly formatted.")
    return []

# --- 主函数 ---
def main(argv):
    # ----------------------------------------------------
    # 修正：移除 gettopnum 的硬编码限制，让其处理所有基金
    # ----------------------------------------------------
    
    if len(sys.argv) != 3 and len(sys.argv) != 4:
        usage()
        sys.exit(1)
    
    strsdate = sys.argv[1]
    stredate = sys.argv[2]
    
    # ... [日期校验逻辑保持不变] ...
    strtoday = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    tdatetime = datetime.datetime.strptime(strtoday, '%Y-%m-%d')
    
    sdatetime = datetime.datetime.strptime(strsdate, '%Y-%m-%d')
    if sdatetime.isoweekday() in [6, 7]:
        sdatetime += datetime.timedelta(days=- (sdatetime.isoweekday() - 5))
    strsdate = datetime.datetime.strftime(sdatetime, '%Y-%m-%d')

    edatetime = datetime.datetime.strptime(stredate, '%Y-%m-%d')
    if edatetime.isoweekday() in [6, 7]:
        edatetime += datetime.timedelta(days=- (edatetime.isoweekday() - 5))
    stredate = datetime.datetime.strftime(edatetime, '%Y-%m-%d')

    if edatetime <= sdatetime or tdatetime <= sdatetime or tdatetime <= edatetime:
        print('date input error!\n')
        usage()
        sys.exit(1)

    # --- 处理单个基金的逻辑 (保持不变) ---
    if len(sys.argv) == 4:
        strfundcode = sys.argv[3]
        jingzhimin = get_jingzhi(strfundcode, strsdate)
        jingzhimax = get_jingzhi(strfundcode, stredate)
        
        if jingzhimin == '-1' or jingzhimax == '-1' or jingzhimin.strip() == '' or jingzhimax.strip() == '':
            print('maybe date input error or network error!\n')
            usage()
            sys.exit(1)
            
        jingzhidif = float(jingzhimax) - float(jingzhimin)
        jingzhirise = float('%.2f' %(jingzhidif * 100 / float(jingzhimin)))
    
        print('fund:' + strfundcode + '\n')
        print(strsdate + '\t' + stredate + '\t净增长' + '\t' + '增长率')
        print(jingzhimin + '\t\t' + jingzhimax + '\t\t' + str(jingzhidif) + '\t' + str(jingzhirise) + '%')
        sys.exit(0)
    # --------------------------------------

    # 从本地 CSV 文件加载基金列表
    all_funds_list = load_fund_list_from_csv()
    
    if not all_funds_list:
        print('无法加载基金列表，程序退出。')
        sys.exit(1)
    
    print('筛选中，只处理场外C类基金...')
    c_funds_list = []
    for fund in all_funds_list:
        # 这里的筛选逻辑保留原始脚本的复杂判断
        if fund[0].endswith('C') or 'C' in fund[2] or ('C' in fund[3] and '场外' in fund[3]):
            c_funds_list.append(fund)
    
    all_funds_list = c_funds_list
    print('筛选后，场外C类基金数量：' + str(len(all_funds_list)))
      
    print('start:')
    print(datetime.datetime.now())
    print('funds sum:' + str(len(all_funds_list)))
    
    # --- 并行处理部分开始 ---
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
    
    # ----------------------------------------------------
    # 关键修正：排序逻辑使用正确的索引 (增长率是第 8 个元素，索引为 8)
    # 列表结构: [代码(0), '', 名称(2), 类型(3), '', 净值min(5), 净值max(6), 净增长(7), 增长率(8)]
    # ----------------------------------------------------
    all_funds_list.sort(key=lambda fund: fund[8], reverse=True)
    strhead = '排序\t' + '编码\t\t' + '名称\t\t' + '类型\t\t' + \
    strsdate + '\t' + stredate + '\t' + '净增长' + '\t' + '增长率' + '\n'
    print(strhead)
    fileobject.write(strhead)
    
    for index in range(len(all_funds_list)):
        # 修正：打印逻辑确保索引正确对应
        strcontent = str(index+1) + '\t' + all_funds_list[index][0] + '\t' + all_funds_list[index][2] + \
        '\t\t' + all_funds_list[index][3] + '\t\t' + all_funds_list[index][5] + '\t\t' + \
        all_funds_list[index][6] + '\t\t' + str(all_funds_list[index][7]) + '\t' + str(all_funds_list[index][8]) + '%\n'
        print(strcontent)
        fileobject.write(strcontent)
        
        # 移除 gettopnum 的硬编码限制
            
    fileobject.close()
    
    print('end:')
    print(datetime.datetime.now())
    
    sys.exit(0)
    
if __name__ == "__main__":
    main(sys.argv)
