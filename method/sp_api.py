import asyncio
import multiprocessing
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
import pandas as pd
import json
import yaml
from api.ad_api.sp.gen_sp import GenSP
from api.ad_api.sp.tools_sp import ToolsSP
from configuration.path import get_config_path
from util.expanded_asin import expanded_asin
from util.searchterm_asin2 import searchterm_asin


class auto_api_sp:
    def __init__(self, brand, market, db, user):
        self.brand = brand
        self.market = market
        self.db = db
        self.user = user
        self.exchange_rate = self.load_config('exchange_rate.json').get('exchange_rate', {}).get("DE", {}).get(self.market)

    def load_config(self,config_file):
        config_path = os.path.join(get_config_path(), config_file)
        with open(config_path) as f:
            return json.load(f) if config_file.endswith('.json') else yaml.safe_load(f)

    def update_sp_ad_budget(self, campaignId, bid):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            api2 = ToolsSP(self.db, self.brand, self.market)
            campaign_info = asyncio.run(api2.list_campaigns_api(campaignId))
            if campaign_info["campaigns"] is not None:
                for item in campaign_info["campaigns"]:
                    campaignId = item['campaignId']
                    name = item['name']
                    state = item['state']
                    bid1 = item['budget']['budget']
                    e = asyncio.run(api1.update_camapign_v0(str(campaignId), name, float(bid1), float(bid), state, self.user))
                    if e:
                        return 400,e
                    else:
                        return 200,e
            else:
                return 404, "Campaign not found"  # Campaign not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def update_sp_ad_placement(self, campaignId, bid, placementClassification):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            api2 = ToolsSP(self.db, self.brand, self.market)
            campaign_info = asyncio.run(api2.list_campaigns_api(campaignId))
            if campaign_info["campaigns"] is not None:
                for item in campaign_info["campaigns"]:
                    placement_bidding = item['dynamicBidding']['placementBidding']
                    possible_placements = ['PLACEMENT_REST_OF_SEARCH', 'PLACEMENT_PRODUCT_PAGE', 'PLACEMENT_TOP']
                    placement_percentages = {placement: 0 for placement in possible_placements}
                    for item1 in placement_bidding:
                        placement = item1['placement']
                        percentage = item1['percentage']
                        if placement in possible_placements:
                            placement_percentages[placement] = percentage
                    campaignId = item['campaignId']
                    for placement, percentage in placement_percentages.items():
                        if placement == placementClassification:
                            print(f'Placement: {placement}, Percentage: {percentage}')
                            bid1 = percentage
                            if bid1 is not None:
                                e = asyncio.run(api1.update_campaign_placement(str(campaignId), bid1, float(bid), placement, self.user))
                                if e:
                                    return 400, e
                                else:
                                    return 200, e
            else:
                return 404,"Campaign not found"  # Campaign not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def update_sp_ad_keyword(self, keywordId, bid):
        try:
            api = GenSP(self.db, self.brand, self.market)
            api1 = ToolsSP(self.db, self.brand, self.market)
            spkeyword_info = asyncio.run(api1.get_spkeyword_api_by_keywordId(keywordId))
            if spkeyword_info["keywords"] is not None:
                for spkeyword in spkeyword_info["keywords"]:
                    bid1 = spkeyword.get('bid')
                    state = spkeyword['state']
                    e = asyncio.run(api.update_keyword_toadGroup(str(keywordId), bid1, float(bid), state, self.user))
                    if e:
                        return 400, e
                    else:
                        return 200, e
            else:
                return 404,"Keyword not found"  # Keyword not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def update_sp_ad_keyword_batch(self, keywordId, bid):
        try:
            api = GenSP(self.db, self.brand, self.market)
            api1 = ToolsSP(self.db, self.brand, self.market)
            spkeyword_info = asyncio.run(api1.get_spkeyword_api_by_keywordId_batch(keywordId))
            keyword_bid_mapping = {k: v for k, v in zip(keywordId, bid)}
            if spkeyword_info["keywords"] is not None:
                merged_info = []
                for info in spkeyword_info["keywords"]:
                    keyword_id = info['keywordId']
                    if keyword_id in keyword_bid_mapping:
                        merged_info.append({
                            "keywordId": keyword_id,
                            "state": info["state"],
                            "bid": info.get('bid', None),
                            "bid_new": float(keyword_bid_mapping[keyword_id])  # 从 mapping 中获取 bid_old
                        })
                asyncio.run(api.update_keyword_toadGroup_batch(merged_info, self.user))
                return 200,None
            else:
                return 404,"Keyword not found"  # Keyword not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def update_sp_ad_automatic_targeting(self, keywordId, bid):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            api2 = ToolsSP(self.db, self.brand, self.market)
            automatic_targeting_info = asyncio.run(api2.list_adGroup_TargetingClause_by_targetId(keywordId))
            if automatic_targeting_info["targetingClauses"] is not None:
                for item in automatic_targeting_info["targetingClauses"]:
                    targetId = item['targetId']
                    state = item['state']
                    bid1 = item.get('bid')
                    e = asyncio.run(api1.update_adGroup_TargetingClause(str(targetId), float(bid), state, self.user))
                    if e:
                        return 400, e
                    else:
                        return 200, e
            else:
                return 404,"Targeting not found"  # Targeting not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def update_sp_ad_automatic_targeting_batch(self, keywordId, bid):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            api2 = ToolsSP(self.db, self.brand, self.market)
            automatic_targeting_info = asyncio.run(api2.list_adGroup_TargetingClause_by_targetId_batch(keywordId))
            keyword_bid_mapping = {k: v for k, v in zip(keywordId, bid)}
            if automatic_targeting_info["targetingClauses"] is not None:
                merged_info = []
                for info in automatic_targeting_info["targetingClauses"]:
                    keyword_id = info['targetId']
                    if keyword_id in keyword_bid_mapping:
                        merged_info.append({
                            "keywordId": keyword_id,
                            "state": info["state"],
                            "bid": info.get('bid', None),
                            "bid_new": float(keyword_bid_mapping[keyword_id]) # 从 mapping 中获取 bid_old
                        })
                asyncio.run(api1.update_adGroup_TargetingClause_batch(merged_info, self.user))
                return 200,None
            else:
                return 404,"Keyword not found"  # Keyword not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def update_sp_ad_product_targets(self, keywordId, bid):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            api2 = ToolsSP(self.db, self.brand, self.market)
            automatic_targeting_info = asyncio.run(api2.list_adGroup_TargetingClause_by_targetId(keywordId))
            if automatic_targeting_info["targetingClauses"] is not None:
                for automatic_targeting in automatic_targeting_info["targetingClauses"]:
                    targetId = automatic_targeting['targetId']
                    state = automatic_targeting['state']
                    bid1 = automatic_targeting.get('bid')
                    e = asyncio.run(api1.update_adGroup_TargetingClause(str(targetId), float(bid), state, self.user))
                    if e:
                        return 400, e
                    else:
                        return 200, e
            else:
                return 404,"Targeting not found"  # Targeting not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def auto_campaign_status(self, campaignId, status):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            api2 = ToolsSP(self.db, self.brand, self.market)
            campaign_info = asyncio.run(api1.list_campaigns_api(campaignId))
            if campaign_info["campaigns"] is not None:
                for item in campaign_info["campaigns"]:
                    campaignId = item['campaignId']
                    name = item['name']
                    state = item['state']
                    e = asyncio.run(api1.update_camapign_status(str(campaignId), name, state, status, self.user))
                    if e:
                        return 400, e
                    else:
                        return 200, e
            else:
                return 404,"Campaign not found"  # Campaign not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def auto_sku_status(self, adId, status):
        try:
            api = GenSP(self.db, self.brand, self.market)
            e = asyncio.run(api.update_product(str(adId), status, self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def auto_sku_status_task(self, adId, status,campaignId,campaignName,click,cpc,acos):
        try:
            api = GenSP(self.db, self.brand, self.market)
            e = asyncio.run(api.update_product(str(adId), status, self.user,campaignId,campaignName,click,cpc,acos))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def auto_keyword_status(self, keywordId, status):
        try:
            api = GenSP(self.db, self.brand, self.market)
            e = asyncio.run(api.update_keyword_toadGroup(str(keywordId), None, bid_new=None, state=status, user=self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error
    def auto_keyword_status_batch(self, keywordId, status):
        try:
            api = GenSP(self.db, self.brand, self.market)
            merged_info = []
            for keywordid, statu in zip(keywordId, status):
                merged_info.append({
                            "keywordId": keywordid,
                            "state": statu,
                            "bid": None,
                            "bid_new": None  # 从 mapping 中获取 bid_old
                        })
            asyncio.run(api.update_keyword_toadGroup_batch(merged_info, self.user))
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def auto_targeting_status_batch(self, keywordId, status):
        try:
            api = GenSP(self.db, self.brand, self.market)
            merged_info = []
            for keywordid, statu in zip(keywordId, status):
                merged_info.append({
                            "keywordId": keywordid,
                            "state": statu,
                            "bid": None,
                            "bid_new": None  # 从 mapping 中获取 bid_old
                        })
            asyncio.run(api.update_adGroup_TargetingClause_batch(merged_info, self.user))
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def auto_sku_status_task_batch(self, adIds, status, campaignIds, campaignNames, clicks, cpcs, acoss):
        try:
            api = GenSP(self.db, self.brand, self.market)
            merged_info = []
            for adId, statu, campaignId, campaignName, click, cpc, acos in zip(adIds, status, campaignIds,
                                                                               campaignNames, clicks, cpcs, acoss):
                merged_info.append({
                        "adId": adId,
                        "campaignId": campaignId,
                        "statu": statu,
                        "campaignName": campaignName,
                        "click": click,
                        "cpc": cpc,
                        "acos": acos
                    })
            res = asyncio.run(api.update_product_batch(merged_info, self.user))
            return 200, res
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    # def negative_keyword_status(self, keywordId, status):
    #     try:
    #         api = GenSP(self.db, self.brand, self.market)
    #         api.update_adGroup_negative_keyword(str(keywordId), status, user=self.user)
    #         return 200
    #     except Exception as e:
    #         print(e)
    #         return 500  # Internal Server Error

    def auto_targeting_status(self, keywordId, status):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            e = asyncio.run(api1.update_adGroup_TargetingClause(str(keywordId), bid=None, state=status, user=self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    # def negative_target_status(self, keywordId, status):
    #     try:
    #         api1 = GenSP(self.db, self.brand, self.market)
    #         api1.update_adGroup_Negative_Targeting(str(keywordId), status, user=self.user)
    #         return 200
    #     except Exception as e:
    #         print(e)
    #         return 500,e  # Internal Server Error

    def delete_negative_target(self, keywordId):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            asyncio.run(api1.delete_adGroup_Negative_Targeting(keywordId, user=self.user))
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def delete_negative_keyword(self, keywordId):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            asyncio.run(api1.delete_adGroup_negative_keyword(keywordId, user=self.user))
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def delete_keyword(self, keywordId):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            asyncio.run(api1.delete_keyword_toadGroup_batch(keywordId, user=self.user))
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def delete_product_target(self, keywordId):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            asyncio.run(api1.delete_adGroup_Targeting(keywordId, user=self.user))
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def delete_sku(self, keywordId):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            asyncio.run(api1.delete_sku_batch(keywordId, user=self.user))
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def delete_sku(self, keywordId):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            asyncio.run(api1.delete_sku_batch(keywordId, user=self.user))
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error


    def create_product_target(self, keywordId, bid, campaignId, adGroupId):
        try:
            apitool1 = ToolsSP(self.db, self.brand, self.market)
            api2 = GenSP(self.db, self.brand, self.market)
            brand_info = asyncio.run(apitool1.list_category_refinements(keywordId))
            # 检查是否存在名为"LAPASA"的品牌
            target_brand_name = self.brand
            target_brand_id = None

            for brand in brand_info['brands']:
                if brand['name'] == target_brand_name:
                    target_brand_id = brand['id']
                    targetId,e = asyncio.run(api2.create_adGroup_Targeting2(campaignId, adGroupId,
                                                              float(bid),
                                                              keywordId, target_brand_id, self.user))
                    if e:
                        return 400, e
                    else:
                        return 200, e
            return 200,"category no brand"
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_product_target_asin(self, asin, bid, campaignId, adGroupId):
        try:
            api2 = GenSP(self.db, self.brand, self.market)
            targetId,e = asyncio.run(api2.create_adGroup_Targeting1(campaignId, adGroupId, asin, float(bid),
                                           state='ENABLED', type='ASIN_SAME_AS', user=self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_product_target_asin_expended(self, asin, bid, campaignId, adGroupId):
        try:
            api2 = GenSP(self.db, self.brand, self.market)
            targetId,e = asyncio.run(api2.create_adGroup_Targeting1(campaignId, adGroupId, asin, float(bid),
                                           state='ENABLED', type='ASIN_EXPANDED_FROM', user=self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_keyword(self, keywordtext, bid, campaignId, adGroupId,matchType):
        try:
            api2 = GenSP(self.db, self.brand, self.market)
            targetId,e = asyncio.run(api2.add_keyword_toadGroup_v0(campaignId, adGroupId, keywordtext, matchType,
                                           'ENABLED', float(bid), self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_negative_target(self, searchTerm, campaignId, adGroupId,matchType):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            if len(searchTerm) == 10 and searchTerm.startswith('B0'):
                targetId,e = asyncio.run(api1.create_adGroup_Negative_Targeting_by_asin(str(campaignId), str(adGroupId), searchTerm.upper(), user=self.user))
            else:
                targetId,e = asyncio.run(api1.add_adGroup_negative_keyword_v0(str(campaignId), str(adGroupId), searchTerm,
                                                         matchType=matchType, state="ENABLED", user=self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_keyword_batch(self, keyWord, Bid, campaignId, adGroupId,matchType):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            merged_keyword_info = []
            for keyword, bid, campaignid, adGroupid, matchtype in zip(keyWord, Bid, campaignId,
                                                                       adGroupId, matchType):
                merged_keyword_info.append({
                    "keywordText": keyword,
                    "bid": bid,
                    "campaignId": campaignid,
                    "adGroupId": adGroupid,
                    "matchType": matchtype  # 从 mapping 中获取 bid_old
                })

            res = asyncio.run(api1.add_keyword_toadGroup_batch(merged_keyword_info, user=self.user))
            return 200,res
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_product_target_batch(self, keyWord, Bid, campaignId, adGroupId,matchType):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            merged_keyword_info = []
            for keyword, bid, campaignid, adGroupid, matchtype in zip(keyWord, Bid, campaignId,
                                                                       adGroupId, matchType):
                merged_keyword_info.append({
                    "type": matchtype,
                    "asin": keyword,
                    "bid": bid,
                    "campaignId": campaignid,
                    "adGroupId": adGroupid,
                })

            res = asyncio.run(api1.create_adGroup_Targeting_by_asin_batch(merged_keyword_info, user=self.user))
            return 200,res
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_negative_target_batch(self, searchTerm, campaignId, adGroupId, matchType, campaignNames, clicks, cpcs, acoss):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            merged_asin_info = []
            merged_keyword_info = []
            for searchterm, campaignid, adGroupid, matchtype, campaignName, click, cpc, acos in zip(searchTerm, campaignId,
                                                                       adGroupId, matchType,campaignNames,clicks,cpcs,acoss):
                if len(searchterm) == 10 and searchterm.startswith('B0'):
                    merged_asin_info.append({
                        "asin": searchterm,
                        "campaignId": campaignid,
                        "adGroupId": adGroupid,
                        "campaignName": campaignName,
                        "click": click,
                        "cpc": cpc,
                        "acos": acos
                    })
                else:
                    merged_keyword_info.append({
                        "keywordText": searchterm,
                        "campaignId": campaignid,
                        "adGroupId": adGroupid,
                        "matchType": matchtype,  # 从 mapping 中获取 bid_old
                        "campaignName": campaignName,
                        "click": click,
                        "cpc": cpc,
                        "acos": acos
                    })
            print(merged_asin_info)
            print("-------------")
            print(merged_keyword_info)
            res0 = []
            res1 = []
            if len(merged_asin_info) > 0:
                res0 = asyncio.run(api1.create_adGroup_Negative_Targeting_by_asin_batch(merged_asin_info, user=self.user))
            if len(merged_keyword_info) > 0:
                res1 = asyncio.run(api1.add_adGroup_negative_keyword_batch(merged_keyword_info, user=self.user))
            res = res0 + res1
            return 200,res
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def auto_campaign_name(self, campaignId, new_name):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            campaign_info = asyncio.run(api1.list_campaigns_api(campaignId))
            if campaign_info["campaigns"] is not None:
                for item in campaign_info["campaigns"]:
                    campaignId = item['campaignId']
                    name = item['name']
                    e = asyncio.run(api1.update_camapign_name(str(campaignId), name, new_name, self.user))
                    if e:
                        return 400, e
                    else:
                        return 200, e
            else:
                return 404,"Campaign not found"  # Campaign not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_campaign(self, name, bid,matchType):
        try:
            api = GenSP(self.db, self.brand, self.market)
            today = datetime.today()
            startDate = today.strftime('%Y-%m-%d')
            campaign_id, e = asyncio.run(api.create_camapign(name, startDate,{"placementBidding":[],"strategy":"LEGACY_FOR_SALES"},
                                                         None,None, matchType,
                                           'ENABLED','DAILY', float(bid), self.user))
            if e:
                return 400, None, e
            else:
                return 200, campaign_id, e
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def create_adgroup(self, name, bid, campaignId):
        try:
            api = GenSP(self.db, self.brand, self.market)
            adGroupId, e = asyncio.run(api.create_adgroup(campaignId, name, bid,
                                           'ENABLED', self.user))
            if e:
                return 400, None, e
            else:
                return 200, adGroupId, e
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def create_sku(self, sku, campaignId,adGroupId):
        try:
            api = GenSP(self.db, self.brand, self.market)
            adId, e = asyncio.run(api.create_productsku(campaignId,adGroupId, sku, None,
                                           'ENABLED', self.user))
            if e:
                return 400, None, e
            else:
                return 200, adId, e
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def create_sku_batch(self, skus, campaignIds,adGroupIds):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            merged_keyword_info = []
            for sku, campaignId, adGroupId in zip(skus, campaignIds, adGroupIds):
                merged_keyword_info.append({
                    "campaignId": campaignId,
                    "adGroupId": adGroupId,
                    "sku": sku  # 从 mapping 中获取 bid_old
                })
            res = asyncio.run(api1.create_productsku_batch(merged_keyword_info, user=self.user))
            return 200,res,None
        except Exception as e:
            print(e)
            return 500,None,e  # Internal Server Error

    def list_adGroup_TargetingClause(self, adGroupId):
        try:
            api = GenSP(self.db, self.brand, self.market)
            info = asyncio.run(api.list_adGroup_TargetingClause(adGroupId))
            if "errors" in info and info["errors"]:
                return 400, None, info["errors"][0]["errorType"]
            else:
                return 200, info["targetingClauses"], None
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def get_product_api(self, adGroupId):
        try:
            api = GenSP(self.db, self.brand, self.market)
            info = asyncio.run(api.get_product_api(adGroupId))
            if "errors" in info and info["errors"]:
                return 400, None, info["errors"][0]["errorType"]
            else:
                return 200, info["productAds"], None
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def list_adGroup_Targetingrecommendations(self, asins):
        try:
            api = GenSP(self.db, self.brand, self.market)
            info = asyncio.run(api.list_adGroup_Targetingrecommendations(asins))
            if "code" in info and info["code"]:
                return 400, None, info["details"]
            else:
                return 200, info, None
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def list_category_refinements(self, categoryId):
        try:
            api = GenSP(self.db, self.brand, self.market)
            info = asyncio.run(api.list_category_refinements(categoryId))
            if "code" in info and info["code"]:
                return 400, None, info["details"]
            else:
                return 200, info, None
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def list_CampaignNegativeKeywords(self, categoryId):
        try:
            api = GenSP(self.db, self.brand, self.market)
            info = asyncio.run(api.list_Campaign_Negative_Keywords(categoryId))
            if "code" in info and info["code"]:
                return 400, None, info["details"]
            else:
                return 200, info, None
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def list_CrawlerAsin(self, day, order):
        try:
            info = []
            info1 =expanded_asin(self.db, self.brand, self.market)
            info.append(info1)
            # info2 = asyncio.run(searchterm_asin(self.db, self.brand, self.market, day, order))
            # info.append(info2)
            print(info)
            return 200, info, None
        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error

    def searchterm_CrawlerAsin(self, day, order):
        try:
            print(111)
            current_process = multiprocessing.current_process()
            print(current_process.name)
            if isinstance(threading.current_thread(), threading.Thread):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)  # 设置当前线程的事件循环

                # 执行异步任务
            info2 = loop.run_until_complete(searchterm_asin(self.db, self.brand, self.market, day, order))

            return 200, None, None

            # 在返回之前异步执行任务
            # 等待任务完成（如果需要等待结果的话）
            # info2 =  task
            # info.append(info2)

        except Exception as e:
            print(e)
            return 500, None, e  # Internal Server Error