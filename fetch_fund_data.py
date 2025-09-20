import scrapy
import re
import json

class FundHoldingsSpider(scrapy.Spider):
    name = "fund_holdings_spider"
    
    # 基金代码，你可以根据需要修改
    fund_code = "002580"
    
    # 爬取数据的年份范围
    start_year = 2020  # 开始年份
    end_year = 2025    # 结束年份

    def start_requests(self):
        """
        生成初始请求。
        这个方法会根据设定的年份范围，为每个年份的四个季度生成请求。
        """
        # 定义季度的URL参数
        quarters = {
            1: "1",  # 一季度
            2: "2",  # 二季度
            3: "3",  # 三季度
            4: "4"   # 四季度
        }
        
        # 遍历年份和季度，生成请求URL
        for year in range(self.start_year, self.end_year + 1):
            for quarter in quarters.keys():
                # 东方财富网站通常使用 year 和 quarter 参数来获取持仓数据
                # 移除 topline 参数以获取所有持仓数据
                url = f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={self.fund_code}&year={year}&quarter={quarter}"
                yield scrapy.Request(url=url, callback=self.parse, meta={'year': year, 'quarter': quarter})

    def parse(self, response):
        """
        解析响应并提取数据。
        这个方法会处理来自网站的 JSONP 格式响应，解析出股票持仓信息。
        """
        year = response.meta['year']
        quarter = response.meta['quarter']
        
        # 使用正则表达式从 JSONP 响应中提取 JSON 字符串
        # 响应格式通常为 var apidata={ content:"...", ... }
        try:
            json_str = re.search(r'content:"(.*)"', response.text).group(1)
            # 对特殊字符进行转义，确保 JSON 解析成功
            json_str = json_str.replace('\\"', '"').replace('\\/', '/')
            
            # 由于可能包含中文，需要处理编码问题
            data = json.loads(json_str)

            # 提取表格数据
            table_data_html = data.get('content', '')
            
            # 使用 Scrapy 的选择器解析 HTML 表格
            selector = scrapy.Selector(text=table_data_html)
            rows = selector.css('tbody tr')
            
            if not rows:
                self.logger.info(f"年份 {year} 第 {quarter} 季度没有持仓数据，URL: {response.url}")
                return

            # 解析表格头
            headers = [th.css('::text').get().strip() for th in selector.css('thead th')]
            
            for row in rows:
                item = {}
                # 提取每一行的数据
                values = [td.css('::text').get().strip() for td in row.css('td')]
                
                # 将数据与表头对应起来
                for i, header in enumerate(headers):
                    if i < len(values):
                        item[header] = values[i]
                
                # 添加年份和季度信息
                item['年份'] = str(year)
                item['季度'] = str(quarter)
                
                yield item

        except (re.search, json.JSONDecodeError, IndexError) as e:
            self.logger.error(f"解析响应时出错，URL: {response.url}, 错误: {e}")
