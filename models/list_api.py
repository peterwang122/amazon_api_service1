from method.sp_api import auto_api_sp

def list_api(data):
    if data['type'] == 'SP':
        code, info, e = sp_api(data)
    # elif data['type'] == 'SD':
    #     code = sd_api(data)
    return code, info, e

def sp_api(data):
    api = auto_api_sp(data['brand'],data['market'],data['db'],data['user'])
    if data['require'] == 'list':
        if data['position'] == 'TargetingClause':
            code,info,e = api.list_adGroup_TargetingClause(data['text'])
        elif data['position'] == 'product':
            code,info,e = api.get_product_api(data['text'])
        elif data['position'] == 'Targetingrecommendations':
            code,info,e = api.list_adGroup_Targetingrecommendations(data['text'])
        elif data['position'] == 'refinements':
            code,info,e = api.list_category_refinements(data['text'])
        elif data['position'] == 'CampaignNegativeKeywords':
            code,info,e = api.list_CampaignNegativeKeywords(data['text'])
        elif data['position'] == 'CrawlerAsin':
            code,info,e = api.list_CrawlerAsin(data['ID'],data['text'])
        elif data['position'] == 'SearchtermCrawlerAsin':
            code,info,e = api.searchterm_CrawlerAsin(data['ID'],data['text'])
    return code, info, e