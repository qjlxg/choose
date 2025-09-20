import scrapy
import pandas as pd
from datetime import datetime
import os
import re

class FundEarningSpider(scrapy.Spider):
    name = 'fund_earning_spider'
    allowed_domains = ['fundf10.eastmoney.com']

    def start_requests(self):
        """
        覆盖默认的 start_requests 方法，直接从本地 CSV 文件读取基金代码
        """
        # 确定当天的文件路径
        today_date = datetime.now().strftime('%Y%m%d')
        input_csv_path = f'data/买入信号基金_{today_date}.csv'
        
        # 检查文件是否存在
        if not os.path.exists(input_csv_path):
            self.logger.error(f"输入文件 {input_csv_path} 不存在，无法开始爬取。")
            return

        try:
            df = pd.read_csv(input_csv_path)
            fund_codes = df['fund_code'].unique()
            self.logger.info(f"成功读取到 {len(fund_codes)} 个基金代码。")
        except Exception as e:
            self.logger.error(f"读取 CSV 文件时出错: {e}")
            return
            
        # 遍历基金代码，为每个代码生成持仓数据请求
        for code in fund_codes:
            # 东方财富的基金代码有时前面会补0，这里确保是6位字符串
            fund_code = str(code).zfill(6)
            url = f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=10&year=2024"
            
            # 使用 meta 参数传递基金代码，以便在解析时使用
            yield scrapy.Request(url, callback=self.parse_holdings, meta={'fund_code': fund_code})

    def parse_holdings(self, response):
        """
        解析基金持仓数据
        """
        fund_code = response.meta['fund_code']
        self.logger.info(f"正在解析基金 {fund_code} 的持仓数据。")
        
        # 使用正则表达式从 JavaScript 变量中提取 HTML 内容
        html_content = re.search(r'var apidata="(.*)";', response.text)
        if html_content:
            html_content = html_content.group(1).replace('\'', '"')
            
            # 使用 pandas 读取 HTML 表格
            try:
                tables = pd.read_html(html_content, encoding='utf-8')
                if tables:
                    holdings_table = tables[0]
                    self.logger.info(f"成功解析基金 {fund_code} 的持仓表格。")
                    
                    # 保存数据
                    yield {
                        'fund_code': fund_code,
                        'holdings': holdings_table.to_dict('records')
                    }
                else:
                    self.logger.warning(f"基金 {fund_code} 的页面上没有找到表格数据。")
            except Exception as e:
                self.logger.error(f"解析基金 {fund_code} 的表格时出错: {e}")
        else:
            self.logger.warning(f"基金 {fund_code} 的页面上没有找到 'apidata' 变量。")
