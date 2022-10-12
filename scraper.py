import scrapy
from scrapy.crawler import CrawlerProcess
from datetime import date

class RightToMoveSpider(scrapy.Spider):
    name="RightToMove"

    first_part_url='https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E475'
    index_url='&index='
    second_part_url='&propertyTypes=&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords='

    start_urls=[first_part_url+second_part_url]

    

    def parse(self,response): #I get the urls for the list of properties
        first_part_url='https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E475'
        index_url='&index='
        second_part_url='&propertyTypes=&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords='

        total_num_prop_xpath='//span[@class="searchHeader-resultCount"]/text()'
        total_num_prop=int(response.xpath(total_num_prop_xpath).get())

        if total_num_prop%24==0:#24 is the number of properties displayes in a single webpage
            num_webpages=int(total_num_prop//24)
        else:
            num_webpages=int(total_num_prop//24+1)

        for i in range(num_webpages):
            url=first_part_url+index_url+str(i*24)+second_part_url
            yield scrapy.Request(url=url, callback=self.parse_webpage)

    def parse_webpage(self,response): #Parse webpage to get properties urls
        partial_prop_urls=response.xpath('//a[@class="propertyCard-priceLink propertyCard-rentalPrice"]/@href').getall()
        prop_urls=[]
        for partial_prop_url in partial_prop_urls:
            prop_urls.append('https://www.rightmove.co.uk'+partial_prop_url)
            
        for url in prop_urls:
            yield scrapy.Request(url=url, callback=self.parse_property,cb_kwargs=dict(url=url))
        
    def parse_property(self,response,**kwargs): #parse each property url to get info
        price_xpath='//div[@class="_1gfnqJ3Vtd1z40MlC0MzXu"]/span/text()'
        title_xpath='//h1[@class="_2uQQ3SV0eMHL1P6t5ZDo2q"]/text()'
        letting_details_xpath='//dl[@class="_2E1qBJkWUYMJYHfYJzUb_r"]/div[@class="_2RnXSVJcWbWv4IpBC1Sng6"]/*/text()'
        partial_agent_url_xpath='//a[@class="_2rTPddC0YvrcYaJHg9wfTP"]/@href'
        property_details_xpath='//div[@class="_4hBezflLdgDMdFtURKTWh"]//text()'
        other_features_xpath='//ul[@class="_1uI3IvdF5sIuBtRIvKrreQ"]//text()'
        description_xpath='//div[@class="STw8udCxUaBUMfOOZu0iL _3nPVwR0HZYQah5tkVJHFh5"]//text()'
    
        property_info={
        'url':kwargs['url'],
        'price':response.xpath(price_xpath).getall(),
        'title':response.xpath(title_xpath).getall(),
        'letting_details':response.xpath(letting_details_xpath).getall(),
        'partial_agent_url':response.xpath(partial_agent_url_xpath).getall(),
        'property_details':response.xpath(property_details_xpath).getall(),
        'other_features':response.xpath(other_features_xpath).getall(),
        'description':response.xpath(description_xpath).getall(),
        'date':date.today()
        }

        yield(property_info)
        

        
process = CrawlerProcess(settings={
    'FEED_URI': 'right_to_move-{}.csv'.format(date.today()),
    'FEED_FORMAT':'csv'
})


process.crawl(RightToMoveSpider)
process.start() # the script will block here until the crawling is finished