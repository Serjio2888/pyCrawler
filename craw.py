import asyncio
import aiohttp
#from aioelasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from time import sleep
import requests

from datetime import datetime
from aioelasticsearch import Elasticsearch


class Parsing:
    def __init__(self):
        self.urls = set()
        self.main = 'https://docs.python.org/'
        

    async def crawling(self, session, es, link='https://docs.python.org/'):
        try:
            async with session.get(link) as r: 
                
                #r = requests.get(link)
                html = await r.read()
                soup = BeautifulSoup(html, 'html.parser')
                
                doc = {
                    'link': link,
                    'text': html,
                    'timestamp': datetime.now(),
                    }                    
                await es.index(index="test-index", doc_type='tweet', body=doc)

                findall = soup.find_all('link') + soup.find_all('a') 
                for link in findall:
                    link = link.get('href')
                    
                    if link not in self.urls:
                        
                        self.urls.add(link)
                        if not link.startswith(self.main):
                            if link.startswith('http'): #значит, это сайт другого домена
                                continue
                            if link.startswith('../'):
                                link = self.main+link[3:]
                            else:
                                link = self.main+link

                        await self.crawling(session, es, link)
        except:
            await es.close()
    
    async def myfun(self, quantity):

        tasks = list()
        async with aiohttp.ClientSession() as session:
            for _ in range(quantity):
                es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
                task = asyncio.create_task(self.crawling(session, es))
                tasks.append(task)
            await asyncio.gather(*tasks)

    #async def closing(self):
    #    await self.es.close()
            

p = Parsing()

asyncio.run(p.myfun(10))