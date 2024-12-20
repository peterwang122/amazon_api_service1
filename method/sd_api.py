import asyncio
from datetime import datetime
import os
import pandas as pd
import json
import ast
import yaml
from api.ad_api.sd.gen_sd import GenSD
from api.ad_api.sd.tools_sd import ToolsSD
from api.ad_api.sp.tools_sp import ToolsSP
from configuration.path import get_config_path


class auto_api_sd:
    def __init__(self, brand, market, db, user):
        self.brand = brand
        self.market = market
        self.db = db
        self.user = user
        self.exchange_rate = self.load_config('exchange_rate.json').get('exchange_rate', {}).get("DE", {}).get(self.market)

    def load_config(self, config_file):
        config_path = os.path.join(get_config_path(), config_file)
        with open(config_path) as f:
            return json.load(f) if config_file.endswith('.json') else yaml.safe_load(f)

    def update_sd_ad_budget(self, campaign_id, bid):
        try:
            api1 = ToolsSD(self.db, self.brand, self.market)
            api2 = GenSD(self.db, self.brand, self.market)
            campaign_info = asyncio.run(api1.list_campaigns_api(campaign_id))
            if campaign_info:
                campaignId = campaign_info['campaignId']
                name = campaign_info['name']
                state = campaign_info['state']
                bid1 = campaign_info['budget']
                e = asyncio.run(api2.update_camapign_v0(str(campaignId), name, state, "daily", float(bid), float(bid1), self.user))
                if e:
                    return 400, e
                else:
                    return 200, e
            else:
                return 404, "Campaign not found"  # Campaign not found
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    def update_sd_ad_product_targets(self, keywordId, bid):
        try:
            api1 = GenSD(self.db, self.brand, self.market)
            api2 = ToolsSD(self.db, self.brand, self.market)
            automatic_targeting_info = asyncio.run(api2.list_adGroup_Targeting_by_targetId(keywordId))
            if automatic_targeting_info:
                targetId = automatic_targeting_info['targetId']
                state = automatic_targeting_info['state']
                e = asyncio.run(api1.update_adGroup_Targeting(str(targetId), float(bid), state, self.user))
                if e:
                    return 400, e
                else:
                    return 200, e
            else:
                return 404, "Targeting not found"  # Targeting not found
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    def auto_campaign_status(self, campaignId, status):
        try:
            api1 = GenSD(self.db, self.brand, self.market)
            api2 = ToolsSD(self.db, self.brand, self.market)
            campaign_info = asyncio.run(api2.list_campaigns_api(campaignId))
            if campaign_info:
                campaignId = campaign_info['campaignId']
                name = campaign_info['name']
                state = campaign_info['state']
                e = asyncio.run(api1.update_camapign_status(str(campaignId), name, state, status.lower(), self.user))
                if e:
                    return 400, e
                else:
                    return 200, e
            else:
                return 404, "Campaign not found"  # Campaign not found
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    def auto_sku_status(self, adId, status):
        try:
            api = GenSD(self.db, self.brand, self.market)
            e = asyncio.run(api.update_product(str(adId), status.lower(), self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    def auto_targeting_status(self, keywordId, status):
        try:
            api1 = GenSD(self.db, self.brand, self.market)
            e = asyncio.run(api1.update_adGroup_Targeting(str(keywordId), bid=None, state=status.lower(), user=self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    def create_product_target(self, keywordId, bid, campaignId, adGroupId):
        try:
            apitool1 = ToolsSP(self.db, self.brand, self.market)
            api3 = GenSD(self.db, self.brand, self.market)
            brand_info = asyncio.run(apitool1.list_category_refinements(keywordId))
            # 检查是否存在名为"LAPASA"的品牌
            target_brand_name = self.brand
            target_brand_id = None
            for brand in brand_info['brands']:
                if brand['name'] == target_brand_name:
                    target_brand_id = brand['id']
                    targetId,e = asyncio.run(api3.create_adGroup_Targeting2(adGroupId, keywordId, target_brand_id,
                                                                  expression_type='manual', state='enabled',
                                                                  bid=float(bid), user=self.user))
                    if e:
                        return 400, e
                    else:
                        return 200, e
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    def create_product_target_new(self, keywordId, bid, campaignId, adGroupId):
        try:
            api3 = GenSD(self.db, self.brand, self.market)
            variable = ast.literal_eval(keywordId)
            targetId,e = asyncio.run(api3.create_adGroup_Targeting4(adGroupId, variable,
                                                                  expression_type='manual', state='enabled',
                                                                  bid=float(bid), user=self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    def create_product_target_asin(self, asin, bid, adGroupId):
        try:
            api2 = GenSD(self.db, self.brand, self.market)
            targetId,e = asyncio.run(api2.create_adGroup_Targeting3(adGroupId, asin, 'manual', 'enabled',float(bid), self.user))
            if e:
                return 400, e
            else:
                return 200, e
        except Exception as e:
            print(e)
            return 500, e  # Internal Server Error

    def auto_campaign_name(self, campaignId, new_name):
        try:
            api1 = GenSD(self.db, self.brand, self.market)
            api2 = ToolsSD(self.db, self.brand, self.market)
            campaign_info = asyncio.run(api2.list_campaigns_api(campaignId))
            if campaign_info:
                campaignId = campaign_info['campaignId']
                name = campaign_info['name']
                e = asyncio.run(api1.update_camapign_name(str(campaignId), name, new_name, self.user))
                if e:
                    return 400, e
                else:
                    return 200, e
            else:
                return 404, "Campaign not found"  # Campaign not found
        except Exception as e:
            print(e)
            return 500,e  # Internal Server Error
