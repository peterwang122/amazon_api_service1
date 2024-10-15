import asyncio
import aiohttp
import os
import pymysql
from ad_api.api import sponsored_products
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal
from api.base_api import BaseApi
from util.proxies import ProxyManager


class ToolsSP(BaseApi):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    async def list_campaigns_api(self, campaignId):
        return await self.make_request(sponsored_products.CampaignsV3, "list_campaigns", body=json.dumps({
              "campaignIdFilter": {
                  "include": [
                     str(campaignId)
                  ]
              }}))

    async def create_campaigns_api(self, campaign_info):
        return await self.make_request(sponsored_products.CampaignsV3, "create_campaigns", body=json.dumps(campaign_info))

    async def update_campaigns(self, campaign_info):
        return await self.make_request(sponsored_products.CampaignsV3, "edit_campaigns", body=json.dumps(campaign_info))

    async def create_adGroup_api(self, adGroup_info):
        return await self.make_request(sponsored_products.AdGroupsV3, "create_ad_groups", body=json.dumps(adGroup_info))

    async def update_adGroup_api(self, adGroup_info):
        return await self.make_request(sponsored_products.AdGroupsV3, "edit_ad_groups", body=json.dumps(adGroup_info))

    async def get_adGroup_api(self, adGroupID):
        return await self.make_request(sponsored_products.AdGroupsV3, "list_ad_groups", body=json.dumps({
              "adGroupIdFilter": {
                "include": [
                  str(adGroupID)
                ]
              }}))

    async def get_adGroup_negativekw(self, adGroupID):
        return await self.make_request(sponsored_products.NegativeKeywordsV3, "list_negative_keywords", body=json.dumps({
              "adGroupIdFilter": {
                "include": [
                  str(adGroupID)
                ]
              }}))

    async def add_adGroup_negativekw(self, adGroup_negativekw_info):
        return await self.make_request(sponsored_products.NegativeKeywordsV3, "create_negative_keyword", body=json.dumps(adGroup_negativekw_info))

    async def update_adGroup_negativekw(self, adGroup_negativekw_info):
        return await self.make_request(sponsored_products.NegativeKeywordsV3, "edit_negative_keyword", body=json.dumps(adGroup_negativekw_info))

    async def delete_adGroup_negativekw(self, adGroup_negativekw_info):
        return await self.make_request(sponsored_products.NegativeKeywordsV3, "delete_negative_keywords", body=json.dumps(adGroup_negativekw_info))

    async def list_adGroup_negative_product(self, adGroupID):
        return await self.make_request(sponsored_products.NegativeTargetsV3, "list_negative_product_targets", body=json.dumps({
              "adGroupIdFilter": {
                "include": [
                  str(adGroupID)
                ]
              }}))

    async def list_adGroup_TargetingClause(self, adGroupID):
        return await self.make_request(sponsored_products.TargetsV3, "list_product_targets", body=json.dumps({
              "adGroupIdFilter": {
                "include": [
                  str(adGroupID)
                ]
              }}))

    async def list_adGroup_TargetingClause_by_campaignId(self, campaignId):
        return await self.make_request(sponsored_products.TargetsV3, "list_product_targets", body=json.dumps({
              "campaignIdFilter": {
                "include": [
                  str(campaignId)
                ]
              }}))

    async def list_adGroup_TargetingClause_by_targetId(self, targetId):
        return await self.make_request(sponsored_products.TargetsV3, "list_product_targets", body=json.dumps({
              "targetIdFilter": {
                "include": [
                  str(targetId)
                ]
              }}))

    async def create_adGroup_TargetingC(self, adGroup_info):
        return await self.make_request(sponsored_products.TargetsV3, "create_product_targets", body=json.dumps(adGroup_info))

    async def update_adGroup_TargetingC(self, adGroup_info):
        return await self.make_request(sponsored_products.TargetsV3, "edit_product_targets", body=json.dumps(adGroup_info))

    async def list_adGroup_Targetingrecommendations(self, asins):
        return await self.make_request(sponsored_products.TargetsV3, "list_products_targets_categories_recommendations", body=json.dumps({
          "asins": asins,
          "includeAncestor": False
        }))

    async def list_category_refinements(self, categoryId):
        return await self.make_request(sponsored_products.TargetsV3, "list_products_targets_category_refinements", categoryId=categoryId)

    async def list_category_bid_recommendations(self, categoryId,new_campaign_id,new_adgroup_id):
        return await self.make_request(sponsored_products.BidRecommendationsV3, "`get_bid_recommendations`", body=json.dumps({
              "targetingExpressions": [
                {
                  "type": "PAT_CATEGORY",
                  "value": str(categoryId)
                }
              ],
              "campaignId": new_campaign_id,
              "recommendationType": "BIDS_FOR_EXISTING_AD_GROUP",
              "adGroupId": new_adgroup_id
            }), version=4)

    async def list_product_bid_recommendations(self, asin, new_campaign_id, new_adgroup_id):
        return await self.make_request(sponsored_products.BidRecommendationsV3, "get_bid_recommendations", body=json.dumps({
              "targetingExpressions": [
                {
                  "type": "PAT_ASIN",
                  "value": asin
                }
              ],
              "campaignId": new_campaign_id,
              "recommendationType": "BIDS_FOR_EXISTING_AD_GROUP",
              "adGroupId": new_adgroup_id
            }), version=4)

    async def list_automatic_targeting_bid_recommendations(self, new_campaign_id, new_adgroup_id):
        return await self.make_request(sponsored_products.BidRecommendationsV3, "get_bid_recommendations", body=json.dumps({
                "targetingExpressions": [
                    {
                    "type": "CLOSE_MATCH"
                    },
                    {
                    "type": "LOOSE_MATCH"
                    },
                    {
                    "type": "SUBSTITUTES"
                    },
                    {
                    "type": "COMPLEMENTS"
                    }
                ],
                "campaignId": new_campaign_id,
                "recommendationType": "BIDS_FOR_EXISTING_AD_GROUP",
                "adGroupId": new_adgroup_id
            }), version=4)

    async def create_adGroup_Negative_TargetingClauses(self, adGroup_info):
        return await self.make_request(sponsored_products.NegativeTargetsV3, "create_negative_product_targets", body=json.dumps(adGroup_info))

    async def update_adGroup_Negative_TargetingClauses(self, adGroup_info):
        return await self.make_request(sponsored_products.NegativeTargetsV3, "edit_negative_product_targets", body=json.dumps(adGroup_info))

    async def delete_adGroup_Negative_TargetingClauses(self, adGroup_info):
        return await self.make_request(sponsored_products.NegativeTargetsV3, "delete_negative_product_targets", body=json.dumps(adGroup_info))

    async def list_category(self):
        return await self.make_request(sponsored_products.TargetsV3, "list_targets_categories")

    async def create_product_api(self, product_info):
        return await self.make_request(sponsored_products.ProductAdsV3, "create_product_ads", body=json.dumps(product_info))

    async def update_product_api(self, product_info):
        return await self.make_request(sponsored_products.ProductAdsV3, "edit_product_ads", body=json.dumps(product_info))

    async def get_product_api(self, adGroupID):
        return await self.make_request(sponsored_products.ProductAdsV3, "list_product_ads", body=json.dumps({
            "adGroupIdFilter": {
                "include": [
                    str(adGroupID)
                ]
            }}))

    async def create_spkeyword_api(self, keyword_info):
        return await self.make_request(sponsored_products.KeywordsV3, "create_keyword", body=json.dumps(keyword_info))

    async def update_spkeyword_api(self, keyword_info):
        return await self.make_request(sponsored_products.KeywordsV3, "edit_keyword", body=json.dumps(keyword_info))

    async def delete_spkeyword_api(self, keyword_info):
        return await self.make_request(sponsored_products.KeywordsV3, "delete_keywords", body=json.dumps(keyword_info))

    async def get_spkeyword_api(self, adGroupID):
        return await self.make_request(sponsored_products.KeywordsV3, "list_keywords", body=json.dumps({
            "adGroupIdFilter": {
                "include": [
                    str(adGroupID)
                ]
            }}))

    async def get_spkeyword_api_by_campaignid(self, campaignId):
        return await self.make_request(sponsored_products.KeywordsV3, "list_keywords", body=json.dumps({
            "campaignIdFilter": {
                "include": [
                    str(campaignId)
                ]
            }}))

    async def get_spkeyword_api_by_keywordId(self, keywordId):
        return await self.make_request(sponsored_products.KeywordsV3, "list_keywords", body=json.dumps({
            "keywordIdFilter": {
                "include": [
                    str(keywordId)
                ]
            }}))

    async def get_spkeyword_recommendations_api(self, campaignId, adGroupID):
        return await self.make_request(sponsored_products.RankedKeywordsRecommendations, "list_ranked_keywords_recommendations", body=json.dumps({
              "campaignId": str(campaignId),
              "recommendationType": "KEYWORDS_FOR_ADGROUP",
              "adGroupId": adGroupID
            }))




if __name__ == "__main__":
    campaign_tools = ToolsSP('amazon_outdoormaster_jp', 'OutdoorMaster', 'JP')
    result = asyncio.run(campaign_tools.list_category())  # 正确的调用方式
    print(result)