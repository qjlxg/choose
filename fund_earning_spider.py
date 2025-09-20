# -*- coding: utf-8 -*-
import scrapy
import re
import json
import logging
from lxml import etree
from fake_useragent import UserAgent

# 配置日志
logger = logging.getLogger(__name__)

# Scrapy 爬虫类
# Scrapy 会自动处理请求和响应，并使用内置的调度器和下载器，
# 与您当前使用 requests 和 selenium 的脚本运行方式完全不同。
class FundEarningSpider(scrapy.Spider):
    """
    爬取基金排名、基本信息和前十大持仓股数据
    """
    name = 'fund_earning'
    allowed_domains = ['eastmoney.com']

    # 基金类型：gp=股票型，hh=混合型。可以按需添加其他类型。
    start_urls = [
        "http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft={}&rs=&gs=0&sc=zzf&st=desc&qdii=&"
        "tabSubtype=,,,,,&pi=1&pn=10000&dx=1&v=0.42187391938911856".format(i) for i in ["gp", "hh"]
    ]
    
    # 爬取开关
    need_fund_earning_perday = False  # 每日净值，数据量大，慎用
    need_fund_basic_info = False  # 基金基本信息
    need_fund_position = True     # 前十大持仓股

    # Scrapy 将使用此方法发送初始请求
    def start_requests(self):
        ua = UserAgent()
        # 模仿浏览器请求头
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'fund.eastmoney.com',
            'Referer': 'http://fund.eastmoney.com/data/fundranking.html',
            'User-Agent': ua.random,
        }
        for url in self.start_urls:
            yield scrapy.Request(url, headers=headers)

    # 1. 基金当日收益排名情况（基金排名页）
    def parse(self, response):
        fund_type = re.findall(r'ft=(.*?)&rs', response.url)[0]
        # 提取JSON数据
        match = re.search(r"var rankData = (.*?);$", response.text, re.DOTALL)
        if not match:
            logger.error("无法从响应中找到基金排名数据")
            return
            
        data_str = match.group(1).replace("rankData", "").strip()
        data_str = re.findall(r"\[(.*?)]", data_str)[0]
        fund_list = data_str.split('","')
        
        for fund_data in fund_list:
            f = fund_data.replace('"', '').split(',')
            if len(f) < 21:
                logger.warning(f"跳过不完整的数据行: {fund_data}")
                continue
            
            item = {
                "fund_type": fund_type,
                "code": f[0],
                "name": f[1],
                "today_earning_rate": f[3],
                "net_value": f[4],
                "accumulative_value": f[5],
                "rate_day": f[6],
                "rate_recent_week": f[7],
                "rate_recent_month": f[8],
                "rate_recent_3month": f[9],
                "rate_recent_6month": f[10],
                "rate_recent_year": f[11],
                "rate_recent_2year": f[12],
                "rate_recent_3year": f[13],
                "rate_from_this_year": f[14],
                "rate_from_begin": f[15],
                "rate_buy": f[20]
            }
            item["url"] = "http://fund.eastmoney.com/" + item["code"] + ".html"
            
            # 打印或保存数据
            logger.info(f"爬取到基金排名数据: {item['code']} - {item['name']}")
            # yield item # 如果要将数据保存到文件中，需要配置 item pipeline

            # 2.1 基金成立以来每日净值
            if self.need_fund_earning_perday:
                yield scrapy.Request(
                    "http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery&fundCode={}&pageIndex=1&pageSize=20000".format(item["code"]),
                    headers={"Referer": "http://fundf10.eastmoney.com"},
                    callback=self.parse_fund_earning_perday,
                    meta={"item": item},
                )
            
            # 2.2 基金基本信息
            if self.need_fund_basic_info:
                yield scrapy.Request(
                    "http://fundf10.eastmoney.com/jbgk_{}.html".format(item["code"]),
                    callback=self.parse_fund_basic_info,
                    meta={"item": item},
                )
            
            # 2.3 基金10大持仓股(指定按年)
            if self.need_fund_position:
                # 爬取指定年份的持仓数据，可以按需修改年份
                for year in ["2024", "2023"]:
                    yield scrapy.Request(
                        "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={}&topline=10&year={}".format(item["code"], year),
                        callback=self.parse_fund_position,
                        meta={"item": item},
                    )

    # 2.1 基金成立以来每日净值
    def parse_fund_earning_perday(self, response):
        item = response.meta.get("item")
        data = re.findall(r'jQuery\d+\((.*)\)', response.text)[0]
        data = json.loads(data)
        for i in data.get("Data", {}).get("LSJZList", []):
            new_item = {
                "fund_type": item["fund_type"],
                "code": item["code"],
                "name": item["name"],
                "date": i.get("FSRQ"),
                "net_value": i.get("DWJZ"),
                "accumulative_value": i.get("LJJZ"),
                "rate_day": i.get("JZZZL"),
                "buy_status": i.get("SGZT"),
                "sell_status": i.get("SHZT"),
                "profit": i.get("FHSP")
            }
            logger.info(f"爬取到每日净值数据: {new_item['code']} - {new_item['date']}")
            # yield new_item

    # 2.2 基金基本信息
    def parse_fund_basic_info(self, response):
        item = response.meta.get("item")
        html_doc = etree.HTML(response.text)
        
        info = {
            "full_name": html_doc.xpath("//th[text()='基金全称']/../td[1]/text()"),
            "code": item["code"],
            "fund_url": response.url,
            "type": html_doc.xpath("//th[text()='基金类型']/../td[2]/text()"),
            "publish_date": html_doc.xpath("//th[text()='发行日期']/../td[1]/text()"),
            "setup_date_and_scale": html_doc.xpath("//th[text()='成立日期/规模']/../td[2]/text()"),
            "asset_scale": html_doc.xpath("//th[text()='资产规模']/../td[1]/text()"),
            "amount_scale": html_doc.xpath("//th[text()='份额规模']/../td[2]/a/text()"),
            "company": html_doc.xpath("//th[text()='基金管理人']/../td[1]/a/text()"),
            "bank": html_doc.xpath("//th[text()='基金托管人']/../td[2]/a/text()"),
            "manager": html_doc.xpath("//th[text()='基金经理人']/../td[1]//a/text()"),
            "management_feerate": html_doc.xpath("//th[text()='管理费率']/../td[1]/text()"),
            "trustee_feerate": html_doc.xpath("//th[text()='托管费率']/../td[2]/text()"),
            "standard_compared": html_doc.xpath("//th[text()='业绩比较基准']/../td[1]/text()"),
            "followed_target": html_doc.xpath("//th[text()='跟踪标的']/../td[2]/text()")
        }
        
        for key in info:
            if isinstance(info[key], list) and info[key]:
                info[key] = info[key][0].strip()
            elif isinstance(info[key], list):
                info[key] = None
        
        logger.info(f"爬取到基本信息: {info['code']} - {info['full_name']}")
        # yield info

    # 2.3 基金10大持仓股(指定按年)
    def parse_fund_position(self, response):
        item = response.meta.get("item")
        html_doc = etree.HTML(response.body.decode())
        
        # 查找所有包含持仓数据的div
        div_list = html_doc.xpath("//div[@class='boxitem w790']")
        for div in div_list:
            quarter = div.xpath(".//label[@class='left']/text()")
            quarter = quarter[0].strip() if quarter else "未知"
            
            tr_list = div.xpath(".//table[@class='w782 comm tzxq']/tbody/tr")
            for tr in tr_list:
                stock_data = tr.xpath("./td/text() | ./td/a/text()")
                if len(stock_data) < 6:
                    continue
                
                position_item = {
                    "fund_code": item["code"],
                    "fund_name": item["name"],
                    "quarter": quarter,
                    "stock_code": stock_data[1],
                    "stock_name": stock_data[2],
                    "stock_proportion": stock_data[3],
                    "stock_amount": stock_data[4],
                    "stock_value": stock_data[5]
                }
                logger.info(f"爬取到持仓数据: {position_item['fund_code']} - {position_item['stock_name']} ({position_item['quarter']})")
                # yield position_item
