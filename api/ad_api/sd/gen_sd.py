import asyncio
import json
from datetime import datetime
from api.ad_api.sd.tools_sd import ToolsSD
from db.tools_db_new_sp import DbNewSpTools

class GenSD(ToolsSD):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    async def create_camapign(self, name, startDate, costType, portfolioId, endDate, tactic, state, budgetType, budget,
                        user='test'):
        campaigninfo = [
            {
                "name": name,
                "budgetType": budgetType,
                "budget": budget,
                "startDate": startDate,
                "endDate": endDate,
                "costType": costType,
                "state": state,
                "portfolioId": portfolioId,
                "tactic": tactic
            }
        ]

        # 执行创建
        res = await self.create_campaigns_api(campaigninfo)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            campaign_id = res[0]['campaignId']
            error_message = None
        else:
            status = "failed"
            campaign_id = None
            error_message = res['details'] if isinstance(res, dict) else res
        # 根据创建结果更新log
        dbNewTools = DbNewSpTools(self.db, self.brand, self.market)
        await dbNewTools.init()
        await dbNewTools.create_sp_campaigin(self.market, portfolioId, endDate, name, campaign_id, tactic, state,
                                       startDate, budgetType, budget, status, datetime.now(), "SD", costType,
                                       user)

        return campaign_id, error_message

    async def update_camapign_v0(self, campaignId, campaignName, state, budgetType, budget_new, budget_old=None,
                           user='test'):
        campaign_info = [
            {
                "name": campaignName,
                "budgetType": budgetType,
                "budget": budget_new,
                "state": state,
                "campaignId": campaignId
            }
        ]

        # 调用api
        res = await self.update_campaigns(campaign_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            campaign_id = res[0]['campaignId']
            error_message = None
        else:
            status = "failed"
            campaign_id = None
            error_message = res['details'] if isinstance(res, dict) else res
        # 根据创建结果更新log
        # 更新log
        #     def update_sp_campaign(self,market,campaign_name,campaign_id,budget_old,budget_new,standards_acos,acos,beizhu,status,update_time):
        newdbtool = DbNewSpTools(self.db, self.brand, self.market)
        await newdbtool.init()
        await newdbtool.update_sp_campaign(self.market, campaignName, campaignId, 'budget', budget_old, budget_new,
                                         None, None, "SD", status, datetime.now(), user)
        return error_message

    async def update_camapign_name(self, campaignId, campaignName, campaignName_new, user='test'):
        campaign_info = [
            {
                "name": campaignName_new,
                "campaignId": campaignId
            }
        ]
        # 调用api
        res = await self.update_campaigns(campaign_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            campaign_id = res[0]['campaignId']
            error_message = None
        else:
            status = "failed"
            campaign_id = None
            error_message = res['details'] if isinstance(res, dict) else res
        # 更新log
        #     def update_sp_campaign(self,market,campaign_name,campaign_id,budget_old,budget_new,standards_acos,acos,beizhu,status,update_time):
        newdbtool = DbNewSpTools(self.db, self.brand, self.market)
        await newdbtool.init()
        await newdbtool.update_sp_campaign(self.market, campaignName, campaignId, 'campaignName', campaignName,
                                         campaignName_new, None, None, "SD", status, datetime.now(), user)
        return error_message

    async def update_camapign_status(self, campaignId, campaignName,state,state_new, user='test'):
        campaign_info = [
            {
                "state": state_new,
                "campaignId": campaignId
            }
        ]
        # 调用api
        res = await self.update_campaigns(campaign_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            campaign_id = res[0]['campaignId']
            error_message = None
        else:
            status = "failed"
            campaign_id = None
            error_message = res['details'] if isinstance(res, dict) else res
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        await newdbtool.init()
        await newdbtool.update_sp_campaign(self.market, campaignName, campaignId, 'state', state,
                                         state_new, None, None, "SD", status, datetime.now(), user)

        return error_message

    async def create_adgroup(self,campaignId,name,bidOptimization,creativeType,state,defaultBid,user='test'):
        adgroup_info = [
          {
            "name": name,
            "campaignId": campaignId,
            "defaultBid": defaultBid,
            "bidOptimization": bidOptimization,
            "state": state,
            "creativeType": creativeType
          }
        ]
        res = await self.create_adGroup_api(adgroup_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            adGroupId = res[0]['adGroupId']
            error_message = None
        else:
            status = "failed"
            adGroupId = None
            error_message = res['details'] if isinstance(res, dict) else res
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        await dbNewTools.init()
        await dbNewTools.create_sp_adgroups(self.market,campaignId,name,adGroupId,state,defaultBid,error_message,status,datetime.now(),creativeType,"SD",user)
        return adGroupId,error_message

    async def create_adGroup_Targeting1(self,adGroupId,expression_type,state,bid,user='test'):

        adGroup_info = [
          {
            "expression":[
           {
               "type": "similarProduct",
           }
          ],
            "bid": bid,
            "adGroupId": adGroupId,
            "expressionType": expression_type,
            "state": state
          }
        ]
        # api更新
        res = await self.create_adGroup_Targeting(adGroup_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            targetId = res[0]['targetId']
            error_message = None
        else:
            status = "failed"
            targetId = None
            error_message = res['details'] if isinstance(res, dict) else res
        # 结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        await newdbtool.init()
        await newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,"similarProduct","SD",status,datetime.now(),targetId,user)
        return targetId,error_message

    async def create_adGroup_Targeting2(self,adGroupId,categoryid,brand_id,expression_type,state,bid, user='test'):
        adGroup_info = [
          {
            "expression":[
                {
                    "type": "asinCategorySameAs",
                    "value": str(categoryid)
                },
                {'type': 'asinBrandSameAs',
                 'value': brand_id
                 }
            ],
            "bid": bid,
            "adGroupId": adGroupId,
            "expressionType": expression_type,
            "state": state
          }
        ]
        # api更新
        res = await self.create_adGroup_Targeting(adGroup_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            targetId = res[0]['targetId']
            error_message = None
        else:
            status = "failed"
            targetId = None
            error_message = res['details'] if isinstance(res, dict) else res
        # 结果写入日志
        expression = f"Category={categoryid},brand={brand_id}"
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        await newdbtool.init()
        await newdbtool.add_sd_adGroup_Targeting(self.market, adGroupId, bid, expression_type, state, expression, "SD",
                                               status, datetime.now(),targetId, user)
        return targetId,error_message

    async def create_adGroup_Targeting3(self,adGroupId,asin,expression_type,state,bid, user='test'):
        adGroup_info = [
          {
            "expression":[
                {
                    "type": "asinSameAs",
                    "value": str(asin)
                }
          ],
            "bid": bid,
            "adGroupId": adGroupId,
            "expressionType": expression_type,
            "state": state
          }
        ]
        # api更新
        res = await self.create_adGroup_Targeting(adGroup_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            targetId = res[0]['targetId']
            error_message = None
        else:
            status = "failed"
            targetId = None
            error_message = res['details'] if isinstance(res, dict) else res
        # 结果写入日志
        expression = f"asin={asin}"
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        await newdbtool.init()
        await newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,expression,"SD",status,datetime.now(),targetId, user)
        return targetId,error_message

    async def create_adGroup_Targeting4(self,adGroupId,expression,expression_type,state,bid, user='test'):
        adGroup_info = [
          {
            "expression": expression,
            "bid": bid,
            "adGroupId": adGroupId,
            "expressionType": expression_type,
            "state": state
          }
        ]
        # api更新
        res = await self.create_adGroup_Targeting(adGroup_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            targetId = res[0]['targetId']
            error_message = None
        else:
            status = "failed"
            targetId = None
            error_message = res['details'] if isinstance(res, dict) else res
        # 结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        await newdbtool.init()
        await newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,json.dumps(expression),"SD",status,datetime.now(),targetId, user)
        return targetId,error_message

    async def update_adGroup_Targeting(self, target_id, bid, state, user='test'):
        adGroup_info = [
            {
                "state": state,
                "bid": bid,
                "targetId": target_id
            }
        ]
        # api更新
        res = await self.update_adGroup_Targeting1(adGroup_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            targetId = res[0]['targetId']
            error_message = None
        else:
            status = "failed"
            targetId = None
            error_message = res['details'] if isinstance(res, dict) else res
        # 结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand, self.market)
        await newdbtool.init()
        await newdbtool.update_sd_adGroup_Targeting(self.market, None, None, bid, state, target_id, "SD",
                                                  status, datetime.now(), user)
        return error_message

    async def create_productsku(self,campaignId,adGroupId,sku,state,user='test'):
        product_info = [
          {
            "state": state,
            "adGroupId": adGroupId,
            "campaignId": campaignId,
            "sku": sku
          }
        ]
        # 执行新增品 返回adId
        res = await self.create_product_api(product_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            adId = res[0]['adId']
            error_message = None
        else:
            status = "failed"
            adId = None
            error_message = res['details'] if isinstance(res, dict) else res

        # 如果执行成功或者失败 记录到log表记录
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        await dbNewTools.init()
        await dbNewTools.create_sp_product(self.market,campaignId,None,sku,adGroupId,adId[1],status,datetime.now(),"SD",user)
        return adId,error_message

    async def update_product(self,adId,state, user='test'):
        product_info = [
          {
            "state": state,
            "adId": adId
          }
        ]
        # 执行修改品
        res = await self.update_product_api(product_info)
        if isinstance(res, list) and res[0]['code'] == "SUCCESS":
            status = "success"
            adid = res[0]['adId']
            error_message = None
        else:
            status = "failed"
            adid = None
            error_message = res['details'] if isinstance(res, dict) else res

        # 如果执行成功或者失败 记录到log表记录
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        await dbNewTools.init()
        await dbNewTools.update_sp_product(self.market, adId, state, status, datetime.now(), user)
        return error_message