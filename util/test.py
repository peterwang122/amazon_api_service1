import asyncio

from util.searchterm_asin2 import searchterm_asin

def test():
    asyncio.run(searchterm_asin('amazon_ads','LAPASA','JP',60,3))



if __name__ == '__main__':
    test()