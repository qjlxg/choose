import scrapy
import pandas as pd
import json
import re

# 导入必要的Firebase Firestore模块
from firebase_admin import credentials, firestore, initialize_app

# 请确保你已经通过 `pip install firebase-admin pandas lxml html5lib` 安装了所有依赖库。

# Firestore数据库初始化
# 这里的__firebase_config和__app_id是来自Canvas环境的全局变量
# 如果在本地测试，请替换为你的Firebase配置
firebase_config = json.loads(__firebase_config)
cred = credentials.Certificate(firebase_config)
app = initialize_app(cred)
db = firestore.client()

class FundSpider(scrapy.Spider):
    name = 'fund_spider'

    # 基金代码、年份和季度列表，用于生成爬取任务
    # 你可以根据需要修改这些列表
    fund_list = ['017836', '020398', '000001']
    years_to_scrape = [2023, 2024, 2025]
    quarters_to_scrape = [1, 2, 3, 4]

    def start_requests(self):
        """
        生成所有基金、年份和季度的爬取请求。
        """
        for fund_code in self.fund_list:
            for year in self.years_to_scrape:
                for quarter in self.quarters_to_scrape:
                    # 构造包含年份和季度的URL，以获取完整的持仓数据
                    url = f'http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=10&year={year}&quarter={quarter}'
                    
                    # 附带元数据，以便在回调函数中识别
                    yield scrapy.Request(url, self.parse, meta={'fund_code': fund_code, 'year': year, 'quarter': quarter})

    def parse(self, response):
        """
        解析网页响应，提取基金持仓数据。
        """
        fund_code = response.meta['fund_code']
        year = response.meta['year']
        quarter = response.meta['quarter']

        try:
            # 网页内容是一个JavaScript变量，使用正则表达式提取
            content_match = re.search(r'var apidata = { content:"(.*)"', response.text, re.S)
            if not content_match:
                self.log(f'ℹ️ 警告：未在响应中找到基金 {fund_code} 在 {year} 年第 {quarter} 季度的数据内容。')
                return

            html_content = content_match.group(1)
            
            # 使用pandas的read_html函数解析HTML表格
            # lxml和html5lib是可选的解析器，如果出现错误，请确保已安装
            dfs = pd.read_html(html_content, parser='lxml')
            
            if dfs:
                df = dfs[0]

                # 数据清洗与重构
                df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
                df = df.iloc[:, 1:] # 移除"序号"列
                df.insert(0, '基金代码', fund_code)
                df.insert(1, '年份', year)
                df.insert(2, '季度', quarter)

                # 将数据转换为JSON格式
                data_records = df.to_dict('records')
                
                # 将数据保存到Firestore
                collection_path = f'artifacts/{__app_id}/public/data/fund_holdings'
                for record in data_records:
                    doc_ref = db.collection(collection_path).add(record)
                    self.log(f'💾 数据已保存到 Firestore: {doc_ref.id}')

                self.log(f'✅ 成功获取基金 {fund_code} 在 {year} 年第 {quarter} 季度的持仓数据，记录数：{len(df)}')
            else:
                self.log(f'ℹ️ 警告：未找到基金 {fund_code} 在 {year} 年第 {quarter} 季度的数据表格')

        except Exception as e:
            self.log(f'❌ 错误 - 基金 {fund_code}, 年份 {year}, 季度 {quarter}: {e}')
