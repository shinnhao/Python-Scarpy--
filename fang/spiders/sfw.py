# -*- coding: utf-8 -*-
import scrapy
import re
from fang.items import NewHouseItem,ESFHouseItem

class SfwSpider(scrapy.Spider):
    name = 'sfw'
    allowed_domains = ['fang.com']
    start_urls = ['https://www.fang.com/SoufunFamily.htm']

    def parse(self, response):
        trs_list = response.xpath("//div[contains(@class,'outCont') and contains(@id,'c02')]//tr")
        province = None
        for tr in trs_list:
            tds = tr.xpath(".//td[not(@class)]")
            province_td = tds[0]
            province_text = province_td.xpath(".//text()").get()
            province_text = re.sub(r"\s","",province_text)
            #不爬取其它国家
            if province_text == "其它":
                continue
            if province_text:
                province = province_text
            city_td = tds[1]
            city_links = city_td.xpath(".//a")
            for city_link in city_links:
                city = city_link.xpath(".//text()").get()
                city_url = city_link.xpath(".//@href").get()
                #构建新房url链接
                #构建二手房url链接
                url_module = city_url.split("//")
                scheme = url_module[0]
                domain = url_module[1]
                if 'bj.' not in domain:
                    newhouse_url = scheme + '//' + "newhouse." + domain + "house/s/"
                    esf_url = scheme + "//" + "esf." + domain

                else:
                    newhouse_url = "https://newhouse.fang.com/house/s/"
                esf_url = scheme + "//" + "esf." + domain

                #请求下一页链接
                if province == "广东":
                    print("newhouse_url:",newhouse_url)
                    yield scrapy.Request(
                        url=newhouse_url,
                        callback=self.parse_newhourse,
                        meta={"info":(province,city)}
                    )

                    if 'bj.' not in domain:
                        print("esf_url:",esf_url)
                        yield scrapy.Request(
                            url=esf_url,
                            callback=self.parse_esf,
                            meta={"info": (province, city)}
                        )

                    else:
                        continue





    def parse_newhourse(self,response):
        province,city=response.meta.get("info")
        print("newurl response:", response.url)
        list_div = response.xpath("//div[contains(@class,'nl_con')]/ul/li")
        for li in list_div:
            name = li.xpath(".//div[@class='nlcd_name']/a/text()").get()
            if name is not  None:
                name=name.strip()
            else:
                continue
            house_type_list = li.xpath(".//div[contains(@class,'house_type')]/a/text()").getall()
            house_type_list = list(map(lambda x:re.sub(r"\s","",x),house_type_list))
            rooms_list = list(filter(lambda x:x.endswith("居"),house_type_list))
            rooms=""
            for room in rooms_list:
                rooms=rooms+room
            #  "".join()  以空字符作为链接
            area = "".join(li.xpath(".//div[contains(@class,'house_type')]/text()").getall())
            area =re.sub(r"\s|-|/|－","",area)
            address = li.xpath(".//div[@class='address']/a/@title").get()
            district_text = "".join(li.xpath(".//div[@class='address']/a//text()").getall())
            if '[' in district_text:
                district = re.search(r".*\[(.+)\].*",district_text).group(1)
            else:
                district=city
            sale = li.xpath(".//div[contains(@class,'fangyuan')]/span/text()").get()
            price = "".join(li.xpath(".//div[@class='nhouse_price']//text()").getall())
            price = re.sub(r"\s|广告","",price)
            origin_url = "http:"+li.xpath(".//div[@class='nlcd_name']/a/@href").get()

            item = NewHouseItem(name=name,
                                rooms=rooms,
                                area=area,
                                address=address,
                                district=district,
                                sale=sale,
                                price=price,
                                origin_url=origin_url,
                                province=province,
                                city=city)
            yield item
        #下一页
        next_url = response.xpath("//a[text()='下一页']/@href").get()
        #https://newhouse.fang.com/house/s/b92/
        #https://newhouse.fang.com/house/s/b91/
        next_url =response.urljoin(next_url)
        if next_url:
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_newhourse,
                meta={"info": (province, city)}
            )



    def parse_esf(self,response):
        province, city = response.meta.get("info")
        print("esf response:",response.url)
        dl_list = response.xpath("//div[contains(@class,'shop_list')]//dl[@class='clearfix']")
        for dl in dl_list:
            if dl is not None:
                item = ESFHouseItem(province=province,city=city)
                item['name'] =dl.xpath(".//p[@class='add_shop']/a/@title").get()
                infos = dl.xpath(".//p[@class='tel_shop']/text()").getall()
                infos = list(map(lambda x:re.sub(r"\s","",x),infos))
                item['rooms'] = None
                item['floor'] = None
                item['toward'] = None
                item['area'] = None
                item['year'] = None
                item['origin_url'] = None
                if infos:
                    for info in infos:
                        if '厅' in info:
                            item['rooms']= info
                        elif '层' in info:
                            item['floor'] = info
                        elif '向' in info:
                            item['toward'] = info
                        elif '㎡' in info:
                            item['area'] = info
                        elif '年' in info:
                            item['year'] = info

                item["address"] = dl.xpath(".//p[@class='add_shop']/span/text()").get()
                item["price"] = "".join(dl.xpath(".//dd[@class='price_right']/span[1]//text()").getall())
                item["unit"] = "".join(dl.xpath(".//dd[@class='price_right']/span[2]/text()").getall())
                detail = dl.xpath(".//h4[@class='clearfix']/a/@href").get()
                if detail is not None:
                    item['origin_url'] =response.urljoin(detail)
                    #print(item['origin_url'] )
                    #item['origin_url'] = dl.xpath("./dt/a/@href").get()
                yield item
        #下一页
        next_url = response.xpath("//a[text()='下一页']/@href").get()
        next_url = response.urljoin(next_url)
        if next_url:
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_newhourse,
                meta={"info": (province, city)}
            )






