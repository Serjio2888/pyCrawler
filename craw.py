import asyncio
import aiohttp
#from aioelasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from time import sleep
import requests



class Parsing:
    def __init__(self):
        self.urls = set()
        self.main = 'https://docs.python.org/'

    async def crawling(self, session, link='https://docs.python.org/'):
        async with session.get(link) as r: 
            
            #r = requests.get(link)
            html = await r.read()
            soup = BeautifulSoup(html, 'html.parser')

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
                    await self.crawling(session, link)
    
    async def myfun(self, quantity):

        tasks = list()
        async with aiohttp.ClientSession() as session:
            for _ in range(quantity):
                task = asyncio.create_task(self.crawling(session))

                tasks.append(task)


            await asyncio.gather(*tasks)
            

p = Parsing()

asyncio.run(p.myfun(10))