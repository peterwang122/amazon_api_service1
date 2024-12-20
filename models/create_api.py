from method.sp_api import auto_api_sp

def create_api(data):
    if data['type'] == 'SP':
        code, id, e = sp_api(data)
    # elif data['type'] == 'SD':
    #     code = sd_api(data)
    return code, id, e

def sp_api(data):
    api = auto_api_sp(data['brand'],data['market'],data['db'],data['user'])
    if data['require'] == 'create':
        if data['position'] == 'campaign':
            code,id,e = api.create_campaign(data['ID'], data['text'], data['matchType'])
        elif data['position'] == 'adgroup':
            code,id,e = api.create_adgroup(data['ID'], data['text'], data['campaignId'])
        elif data['position'] == 'sku':
            code,id,e = api.create_sku(data['ID'], data['campaignId'], data['adGroupId'])
    elif data['require'] == 'create_batch':
        if data['position'] == 'sku':
            code,id,e = api.create_sku_batch(data['ID'], data['campaignId'], data['adGroupId'])
    return code, id, e
