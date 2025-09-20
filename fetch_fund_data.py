# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import re
import os

class FundSpider(scrapy.Spider):
    name = 'fund_earning'

    start_urls = [
        'http://fund.eastmoney.com/data/fundranking.html#tall;cgt;r;zsd;p;st;r;tt;1;;2;20'
    ]

    # 基金代码和年份的列表，可以根据需要自定义
    # 示例: [(基金代码, 年份)]
    funds_to_scrape = [
        ('017836', 2025),
        ('020398', 2023),
        ('020398', 2024),
        ('020398', 2025),
        ('017836', 2024),
        ('017836', 2023)
    ]

    def parse(self, response):
        """
        这个方法将不再用于直接爬取基金排名，而是生成持仓数据的请求。
        """
        for fund_code, year in self.funds_to_scrape:
            # 访问基金持仓详情页面，这里通过修改 URL 参数来获取完整的持仓数据
            # 移除 topline 参数，或者将它的值设置得足够大，例如 1000
            url = f'http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&year={year}&topline=1000'
            yield scrapy.Request(url, callback=self.parse_fund_data, meta={'fund_code': fund_code, 'year': year})

    def parse_fund_data(self, response):
        """
        解析基金持仓数据。
        """
        fund_code = response.meta['fund_code']
        year = response.meta['year']

        try:
            # 从响应文本中提取 var apidata = { ... } 部分
            data_str = re.search(r'var apidata=\{ content:\"(.*?)\",', response.text, re.DOTALL).group(1)
            
            # 使用 pandas 读取 HTML 表格。这里需要 html5lib 依赖。
            # 解决 'Missing optional dependency 'html5lib'' 错误
            tables = pd.read_html(data_str, encoding='utf-8')
            
            if not tables:
                self.logger.info(f'❌ 未找到数据表 - 基金 {fund_code}, 年份 {year}')
                return

            # 解析并保存每个季度的持仓数据
            for i, df in enumerate(tables):
                # 季度信息通常在表格上方
                quarter_info = re.findall(r'(\d{4}年\d季度)', data_str)[i] if len(re.findall(r'(\d{4}年\d季度)', data_str)) > i else f'第{i+1}季度'
                
                # 确保 DataFrame 包含我们需要的列，并进行清理
                if '股票代码' in df.columns and '占净值比例' in df.columns:
                    # 获取基金名称和基金代码
                    fund_name_match = re.search(r'([\u4e00-\u9fa5]+)\s+第', data_str)
                    fund_name = fund_name_match.group(1) if fund_name_match else fund_code

                    # 为数据添加年份和季度信息
                    df['基金代码'] = fund_code
                    df['基金名称'] = fund_name
                    df['年份'] = year
                    df['季度'] = quarter_info
                    
                    # 清理列名中的空格
                    df.columns = df.columns.str.replace(r'\s+','', regex=True)
                    
                    # 定义保存路径和文件名
                    # 例如: fund_data/持仓_017836_2025_4季度.csv
                    filename = os.path.join('fund_data', f'持仓_{fund_code}_{year}_{quarter_info}.csv')

                    # 检查文件夹是否存在，不存在则创建
                    if not os.path.exists('fund_data'):
                        os.makedirs('fund_data')

                    # 将数据保存为 CSV 文件
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                    self.logger.info(f'✅ 成功获取基金 {fund_code} 在 {quarter_info} 的持仓数据，记录数：{len(df)}')
                    self.logger.info(f'💾 数据已保存: {filename}')
                else:
                    self.logger.warning(f'⚠️ 表格结构不匹配，无法解析持仓数据 - 基金 {fund_code}, 年份 {year}, 表 {i+1}')
            
        except Exception as e:
            self.logger.error(f'❌ 未知错误 - 基金 {fund_code}, 年份 {year}: {e}')
            self.logger.info('💡 提示: 如果你看到 "Missing optional dependency \'html5lib\'" 错误，请运行以下命令安装：')
            self.logger.info('pip install html5lib')
            self.logger.info('pip install lxml')
