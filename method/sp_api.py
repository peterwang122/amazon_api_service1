from datetime import datetime
import os
import pandas as pd
import json
import yaml
from api.ad_api.sp.gen_sp import GenSP
from api.ad_api.sp.tools_sp import ToolsSP
from configuration.path import get_config_path

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
            campaign_info = api2.list_campaigns_api(campaignId)
            if campaign_info["campaigns"] is not None:
                for item in campaign_info["campaigns"]:
                    campaignId = item['campaignId']
                    name = item['name']
                    state = item['state']
                    bid1 = item['budget']['budget']
                    e = api1.update_camapign_v0(str(campaignId), name, float(bid1), float(bid), state, self.user)
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
            campaign_info = api2.list_campaigns_api(campaignId)
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
                                e = api1.update_campaign_placement(str(campaignId), bid1, float(bid), placement, self.user)
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
            spkeyword_info = api1.get_spkeyword_api_by_keywordId(keywordId)
            if spkeyword_info["keywords"] is not None:
                for spkeyword in spkeyword_info["keywords"]:
                    bid1 = spkeyword.get('bid')
                    state = spkeyword['state']
                    e = api.update_keyword_toadGroup(str(keywordId), bid1, float(bid), state, self.user)
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
            spkeyword_info = api1.get_spkeyword_api_by_keywordId(keywordId)
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
                api.update_keyword_toadGroup_batch(merged_info, self.user)
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
            automatic_targeting_info = api2.list_adGroup_TargetingClause_by_targetId(keywordId)
            if automatic_targeting_info["targetingClauses"] is not None:
                for item in automatic_targeting_info["targetingClauses"]:
                    targetId = item['targetId']
                    state = item['state']
                    bid1 = item.get('bid')
                    e = api1.update_adGroup_TargetingClause(str(targetId), float(bid), state, self.user)
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
            automatic_targeting_info = api2.list_adGroup_TargetingClause_by_targetId(keywordId)
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
                api1.update_adGroup_TargetingClause_batch(merged_info, self.user)
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
            automatic_targeting_info = api2.list_adGroup_TargetingClause_by_targetId(keywordId)
            if automatic_targeting_info["targetingClauses"] is not None:
                for automatic_targeting in automatic_targeting_info["targetingClauses"]:
                    targetId = automatic_targeting['targetId']
                    state = automatic_targeting['state']
                    bid1 = automatic_targeting.get('bid')
                    e = api1.update_adGroup_TargetingClause(str(targetId), float(bid), state, self.user)
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
            campaign_info = api1.list_campaigns_api(campaignId)
            if campaign_info["campaigns"] is not None:
                for item in campaign_info["campaigns"]:
                    campaignId = item['campaignId']
                    name = item['name']
                    state = item['state']
                    e = api1.update_camapign_status(str(campaignId), name, state, status, self.user)
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
            e = api.update_product(str(adId), status, self.user)
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
            e = api.update_keyword_toadGroup(str(keywordId), None, bid_new=None, state=status, user=self.user)
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
            api.update_keyword_toadGroup_batch(merged_info, self.user)
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
            api.update_adGroup_TargetingClause_batch(merged_info, self.user)
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

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
            e = api1.update_adGroup_TargetingClause(str(keywordId), bid=None, state=status, user=self.user)
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
            api1.delete_adGroup_Negative_Targeting(keywordId, user=self.user)
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def delete_negative_keyword(self, keywordId):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            api1.delete_adGroup_negative_keyword(keywordId, user=self.user)
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_product_target(self, keywordId, bid, campaignId, adGroupId):
        try:
            apitool1 = ToolsSP(self.db, self.brand, self.market)
            api2 = GenSP(self.db, self.brand, self.market)
            brand_info = apitool1.list_category_refinements(keywordId)
            # 检查是否存在名为"LAPASA"的品牌
            target_brand_name = self.brand
            target_brand_id = None

            for brand in brand_info['brands']:
                if brand['name'] == target_brand_name:
                    target_brand_id = brand['id']
                    targetId,e = api2.create_adGroup_Targeting2(campaignId, adGroupId,
                                                              float(bid),
                                                              keywordId, target_brand_id, self.user)
                    if e:
                        return 400, e
                    else:
                        return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_product_target_asin(self, asin, bid, campaignId, adGroupId):
        try:
            api2 = GenSP(self.db, self.brand, self.market)
            targetId,e = api2.create_adGroup_Targeting1(campaignId, adGroupId, asin, float(bid),
                                           state='ENABLED', type='ASIN_SAME_AS', user=self.user)
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
            targetId,e = api2.create_adGroup_Targeting1(campaignId, adGroupId, asin, float(bid),
                                           state='ENABLED', type='ASIN_EXPANDED_FROM', user=self.user)
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
            targetId,e = api2.add_keyword_toadGroup_v0(campaignId, adGroupId, keywordtext, matchType,
                                           'ENABLED', float(bid), self.user)
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
                targetId,e = api1.create_adGroup_Negative_Targeting_by_asin(str(campaignId), str(adGroupId), searchTerm.upper(), user=self.user)
            else:
                targetId,e = api1.add_adGroup_negative_keyword_v0(str(campaignId), str(adGroupId), searchTerm,
                                                         matchType=matchType, state="ENABLED", user=self.user)
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def create_negative_target_batch(self, searchTerm, campaignId, adGroupId,matchType):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            merged_asin_info = []
            merged_keyword_info = []
            for searchterm, campaignid, adGroupid, matchtype in zip(searchTerm, campaignId,
                                                                       adGroupId, matchType):
                if len(searchterm) == 10 and searchterm.startswith('B0'):
                    merged_asin_info.append({
                        "asin": searchterm,
                        "campaignId": campaignid,
                        "adGroupId": adGroupid
                    })
                else:
                    merged_keyword_info.append({
                        "keywordText": searchterm,
                        "campaignId": campaignid,
                        "adGroupId": adGroupid,
                        "matchType": matchtype  # 从 mapping 中获取 bid_old
                    })
            print(merged_asin_info)
            print("-------------")
            print(merged_keyword_info)
            if len(merged_asin_info) > 0:
                api1.create_adGroup_Negative_Targeting_by_asin_batch(merged_asin_info, user=self.user)
            if len(merged_keyword_info) > 0:
                api1.add_adGroup_negative_keyword_batch(merged_keyword_info, user=self.user)
            return 200,None
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error

    def auto_campaign_name(self, campaignId, new_name):
        try:
            api1 = GenSP(self.db, self.brand, self.market)
            campaign_info = api1.list_campaigns_api(campaignId)
            if campaign_info["campaigns"] is not None:
                for item in campaign_info["campaigns"]:
                    campaignId = item['campaignId']
                    name = item['name']
                    e = api1.update_camapign_name(str(campaignId), name, new_name, self.user)
                    if e:
                        return 400, e
                    else:
                        return 200, e
            else:
                return 404,"Campaign not found"  # Campaign not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error
