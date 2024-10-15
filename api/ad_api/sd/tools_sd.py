import asyncio
import aiohttp
import os
import pymysql
from ad_api.api import sponsored_display
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal
from api.base_api import BaseApi
from util.proxies import ProxyManager


class ToolsSD(BaseApi):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    async def list_campaigns_api(self, campaignId):
        return await self.make_request(sponsored_display.Campaigns, "get_campaign", campaignId=str(campaignId))

    async def list_all_campaigns_api(self):
        return await self.make_request(sponsored_display.Campaigns, "list_campaigns")

    async def create_campaigns_api(self, campaign_info):
        return await self.make_request(sponsored_display.Campaigns, "create_campaigns", body=json.dumps(campaign_info))

    async def update_campaigns(self, campaign_info):
        return await self.make_request(sponsored_display.Campaigns, "edit_campaigns", body=json.dumps(campaign_info))

    async def create_adGroup_api(self, adGroup_info):
        return await self.make_request(sponsored_display.AdGroups, "create_ad_groups", body=json.dumps(adGroup_info))

    async def get_adGroup_api(self, adGroupID):
        return await self.make_request(sponsored_display.AdGroups, "get_ad_group", adGroupId=str(adGroupID))

    async def get_adGroup_bycampaignid_api(self, campaignid):
        return await self.make_request(sponsored_display.AdGroups, "list_ad_groups", campaignIdFilter=str(campaignid))

    async def create_adGroup_negative_targeting1(self, adGroup_negativekw_info):
        return await self.make_request(sponsored_display.NegativeTargets, "create_negative_targets", body=json.dumps(adGroup_negativekw_info))

    async def list_adGroup_Targeting(self, adGroupID):
        return await self.make_request(sponsored_display.Targets, "list_products_targets", adGroupIdFilter=adGroupID)

    async def list_adGroup_Targeting_by_targetId(self, targetId):
        return await self.make_request(sponsored_display.Targets, "get_products_target", targetId=targetId)

    async def list_adGroup_Targeting_by_campaignId(self, campaignId):
        return await self.make_request(sponsored_display.Targets, "list_products_targets", campaignIdFilter=campaignId)

    async def create_adGroup_Targeting(self, adGroup_info):
        return await self.make_request(sponsored_display.Targets, "create_products_targets", body=json.dumps(adGroup_info))

    async def update_adGroup_Targeting1(self, adGroup_info):
        return await self.make_request(sponsored_display.Targets, "edit_products_targets", body=json.dumps(adGroup_info))

    async def list_adGroup_Targetingrecommendations(self, products):
        return await self.make_request(sponsored_display.TargetsRecommendations, "list_targets_recommendations", body=json.dumps({
          "tactic": "T00020",
          "products": products,
          "typeFilter": [
            "CATEGORY"
          ]
        }))

    async def create_product_api(self, product_info):
        return await self.make_request(sponsored_display.ProductAds, "create_product_ads", body=json.dumps(product_info))

    async def update_product_api(self, product_info):
        return await self.make_request(sponsored_display.ProductAds, "edit_product_ads", body=json.dumps(product_info))

    async def get_product_api(self, adGroupID):
        return await self.make_request(sponsored_display.ProductAds, "list_product_ads", adGroupIdFilter=adGroupID)

    async def get_creatives_api(self, adGroupID):
        return await self.make_request(sponsored_display.Creatives, "list_creatives", adGroupIdFilter=adGroupID)

    async def create_creatives_api(self, creatives_info):
        return await self.make_request(sponsored_display.Creatives, "create_creatives", body=json.dumps(creatives_info))



if __name__ == "__main__":
    campaign_tools = ToolsSD('amazon_outdoormaster_jp', 'OutdoorMaster', 'JP')
    result = asyncio.run(campaign_tools.list_campaigns_api(430759271073064))  # 正确的调用方式
    print(result)