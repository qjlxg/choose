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

# ----------------------------------------------------
# 关键新增：定义需要计算的周期配置
# ----------------------------------------------------
# 注：第一个周期 ('Custom') 将使用命令行输入的 strsdate 和 stredate
PERIOD_CONFIGS = [
    {'days': 0, 'label': 'Custom'}, # 周期 1: 命令行输入的日期范围
    {'days': 180, 'label': '6months'}, # 周期 2: 6个月 (约180天)
    {'days': 365, 'label': '1year'}    # 周期 3: 1年 (约365天)
]
# 结果数组结构说明（在 worker 中扩展）：
# [..., P1_min(6), P1_max(7), P1_diff(8), P1_rise(9), P2_min(10), P2_max(11), P2_diff(12), P2_rise(13), P3_min(14), P3_max(15), P3_diff(16), P3_rise(17)]
# ----------------------------------------------------


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
        response = urllib.request.urlopen(url, timeout=15) # 增加超时设置到 15s
    except urllib.error.HTTPError as e:
        print(f"[{strfundcode}] HTTPError: {e}")
        return '-1'
    except Exception as e:
        # 捕获 Connection reset by peer 等连接错误
        print(f"[{strfundcode}] Error accessing URL: {e}")
        return '-1'

    json_fund_value = response.read().decode('utf-8')
    
    # ----------------------------------------------------
    # 完整保留你提供的复杂净值解析逻辑
    # ----------------------------------------------------
    p = re.compile(r'<td>(.*?)</td><td>(.*?)</td><td>.*?</td><td>.*?</td><td>.*?</td><td>.*?</td>')
    jingzhi_data = p.findall(json_fund_value)
    
    if jingzhi_data:
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
                    
                    # 健壮性检查
                    is_jingzhi1_valid = jingzhi1.replace('.', '', 1).isdigit()
                    is_jingzhi2_valid = jingzhi2.replace('.', '', 1).isdigit()
                    
                    if jingzhi2.strip() == '':
                        jingzhi = '-1'
                    elif jingzhi2.find('%') > -1:
                        jingzhi = '-1'
                    # 这里的逻辑是选择累计净值和单位净值中较大的一个
                    elif is_jingzhi1_valid and is_jingzhi2_valid:
                        if float(jingzhi1) > float(jingzhi2):
                            jingzhi = entry[1]
                        else:
                            jingzhi = entry[2]
                    elif is_jingzhi2_valid:
                        jingzhi = entry[2] # 默认为累计净值
                    else:
                        jingzhi = '-1'
                    
                    return jingzhi # 只需要返回第一个匹配到的数据
        except Exception as e:
            print(f"[{strfundcode}] Error parsing jingzhi: {e}")
            return '-1'

    return '-1'

# --- 线程工作函数 ---
# 关键修改：worker 现在接受 date_configs 参数
def worker(q, date_configs, result_queue):
    while True:
        try:
            fund = q.get(timeout=1) # 1s timeout
        except queue.Empty:
            break
            
        strfundcode = fund[0]
        # 确保基金列表长度为 6 (Code, '', Name, Type, '', '')
        fund = fund[:6]
        
        print(f'processing fund:\t{strfundcode}\t{fund[2]}')

        # ----------------------------------------------------
        # 循环处理所有周期 - 关键修改
        # ----------------------------------------------------
        all_period_results = []
        
        for config in date_configs:
            strsdate = config['start_date']
            stredate = config['end_date']
            
            # 获取净值，并增加线程延迟
            jingzhimin = get_jingzhi(strfundcode, strsdate)
            # 关键修改 2a: 将 0.5s 增加到 1.0s，降低反爬触发率
            time.sleep(1.0) 
            jingzhimax = get_jingzhi(strfundcode, stredate)

            # 关键修改 2b: 每完成一个周期（2次请求），多休息一下
            time.sleep(1.0) 

            jingzhidif = 0.0
            jingzhirise = 0.0

            if jingzhimin == '-1' or jingzhimax == '-1' or jingzhimin.strip() == '' or jingzhimax.strip() == '':
                # 如果数据获取失败，记录为 0
                str_jingzhimin = '0'
                str_jingzhimax = '0'
            elif jingzhimin.find('%') > -1 or jingzhimax.find('%') > -1:
                # 如果数据格式错误 (包含%)，记录为 0
                str_jingzhimin = jingzhimin
                str_jingzhimax = jingzhimax
            else:
                try:
                    str_jingzhimin = jingzhimin
                    str_jingzhimax = jingzhimax
                    jingzhidif = float('%.4f' %(float(jingzhimax) - float(jingzhimin)))
                    if float(jingzhimin) != 0:
                         jingzhirise = float('%.2f' %(jingzhidif * 100 / float(jingzhimin)))
                except ValueError:
                    str_jingzhimin = jingzhimin
                    str_jingzhimax = jingzhimax
                except ZeroDivisionError:
                    str_jingzhimin = jingzhimin
                    str_jingzhimax = jingzhimax

            # 将本周期结果附加到结果列表中
            all_period_results.extend([str_jingzhimin, str_jingzhimax, jingzhidif, jingzhirise])

        # 将所有周期的数据一起附加到 fund 列表
        fund.extend(all_period_results)
        result_queue.put(fund)
        q.task_done()
        
        # ----------------------------------------------------
        # 关键修改 3: 增加线程处理完后的延时，降低整体并发压力
        # ----------------------------------------------------
        time.sleep(4.0) 

# --- 从本地 CSV 文件加载基金列表 ---
def load_fund_list_from_csv():
    encodings = ['utf-8', 'gbk', 'gb18030'] 
    filename = 'recommended_cn_funds.csv'
    file_path = os.path.join(os.getcwd(), filename)
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as csv_file:
                reader = csv.DictReader(csv_file)
                fund_list = []
                for row in reader:
                    # 构造一个包含足够占位符的列表 (长度 6)
                    fund = [
                        row['代码'], 
                        '', # 占位符 1
                        row['名称'], 
                        '混合型', # 类型占位符
                        '', # 占位符 2
                        ''  # 占位符 3 
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
    gettopnum = 50 # 默认输出 top 50
    
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

    # ----------------------------------------------------
    # 关键修改：生成所有周期的日期配置
    # ----------------------------------------------------
    date_configs = []
    
    # 1. 周期 1: Custom (使用命令行输入的日期)
    date_configs.append({'start_date': strsdate, 'end_date': stredate, 'label': PERIOD_CONFIGS[0]['label']})
    
    # 2. 周期 2 & 3: 6个月和1年 (基于命令行输入的结束日期 stredate)
    base_end_date = datetime.datetime.strptime(stredate, '%Y-%m-%d')
    
    for config in PERIOD_CONFIGS[1:]:
        start_dt = base_end_date - datetime.timedelta(days=config['days'])
        
        # 确保起始日期不是周末
        if start_dt.isoweekday() in [6, 7]:
            start_dt += datetime.timedelta(days=- (start_dt.isoweekday() - 5))
            
        start_date = datetime.datetime.strftime(start_dt, '%Y-%m-%d')
        date_configs.append({'start_date': start_date, 'end_date': stredate, 'label': config['label']})
    
    # 从本地 CSV 文件加载基金列表
    all_funds_list_raw = load_fund_list_from_csv()
    
    if not all_funds_list_raw:
        print('无法加载基金列表，程序退出。')
        sys.exit(1)
    
    # 复制列表，避免线程间冲突
    all_funds_list = list(all_funds_list_raw)
    
    print('筛选中，只处理场外C类基金...')
    c_funds_list = []
    for fund in all_funds_list:
        # fund[0]=代码, fund[2]=名称, fund[3]=类型
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

    for fund in all_funds_list:
        task_queue.put(fund)

    threads = []
    # 关键修改：将线程数从 5 降低到 3，以缓解网络压力
    for i in range(3): 
        # 关键修改：将 date_configs 传入 worker
        t = threading.Thread(target=worker, args=(task_queue, date_configs, result_queue))
        t.daemon = True 
        t.start()
        threads.append(t)

    task_queue.join()

    # 收集所有结果
    all_funds_list = []
    while not result_queue.empty():
        all_funds_list.append(result_queue.get())
    # --- 并行处理部分结束 ---

    # ----------------------------------------------------
    # 关键修改：循环输出所有周期的报告
    # ----------------------------------------------------
    # 定义输出配置: (排序索引, 数据起始索引)
    OUTPUT_CONFIGS = [
        {'label': 'Custom', 'sort_index': 9, 'data_start_index': 6},     # P1: rise=9, data starts at 6
        {'label': '6months', 'sort_index': 13, 'data_start_index': 10},  # P2: rise=13, data starts at 10
        {'label': '1year', 'sort_index': 17, 'data_start_index': 14},    # P3: rise=17, data starts at 14
    ]
    
    for period_idx, config in enumerate(OUTPUT_CONFIGS):
        sort_key_index = config['sort_index']
        data_start_index = config['data_start_index']
        
        # 1. 排序
        # 确保使用复制的列表进行排序，以免影响后续排序的初始顺序
        sorted_funds_list = list(all_funds_list) 
        sorted_funds_list.sort(key=lambda fund: fund[sort_key_index], reverse=True)
        
        # 2. 生成文件名
        period_label = config['label']
        if period_label == 'Custom':
             filename = f'result_{strsdate}_{stredate}_C类.txt'
        else:
             filename = f'result_{period_label}_{stredate}_C类.txt'
        
        fileobject = open(filename, 'w')
        
        # 3. 构造表头
        period_strsdate = date_configs[period_idx]['start_date']
        strhead = '排序\t' + '编码\t\t' + '名称\t\t' + '类型\t\t' + \
        period_strsdate + '\t' + stredate + '\t' + '净增长' + '\t' + '增长率' + '\n'
        print(f"\n--- {period_label} 排名报告 ---")
        print(strhead)
        fileobject.write(strhead)
        
        # 4. 打印和写入结果
        i_min = data_start_index
        i_max = data_start_index + 1
        i_diff = data_start_index + 2
        i_rise = data_start_index + 3
        
        for index in range(len(sorted_funds_list)):
            fund_data = sorted_funds_list[index]
            
            # 使用正确的索引来读取数据
            strcontent = str(index+1) + '\t' + fund_data[0] + '\t' + fund_data[2] + \
            '\t\t' + fund_data[3] + '\t\t' + fund_data[i_min] + '\t\t' + \
            fund_data[i_max] + '\t\t' + str(fund_data[i_diff]) + '\t' + str(fund_data[i_rise]) + '%\n'
            
            print(strcontent, end='') # 使用 end='' 避免重复换行
            fileobject.write(strcontent)
            
            if index >= gettopnum and gettopnum > 0: # 仅输出 gettopnum 行
                break
            
        fileobject.close()
        print(f"数据已经写入到文件：{filename} 中") 

    print('\nend:')
    print(datetime.datetime.now())
    
    sys.exit(0)
    
if __name__ == "__main__":
    main(sys.argv)
