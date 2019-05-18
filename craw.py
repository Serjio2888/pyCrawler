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
        
    async def crawling(self, q, session, es):
        while True:
            try:
                link = await q.get()
                print(link)
                async with session.get(link) as r:                    
                    html = await r.read()
                    soup = BeautifulSoup(html, 'html.parser')
                    doc = {
                        'link': link,
                        'text': soup.get_text(),
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
                                    await q.put(link)
                                else:
                                    link = self.main+link
                                    await q.put(link)
            except:
                await es.close()
    
    async def myfun(self, quantity):
        tasks = list()
        q = asyncio.Queue()
        q.put_nowait('https://docs.python.org/')
        async with aiohttp.ClientSession() as session:
            es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
            for _ in range(quantity):
                task = asyncio.create_task(self.crawling(q, session, es))
                tasks.append(task)
            await asyncio.gather(*tasks)

            

p = Parsing()

asyncio.run(p.myfun(10))