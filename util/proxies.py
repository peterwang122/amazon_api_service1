class ProxyManager:
    # def __init__(self):
    #     self.toggle = True  # 初始为有代理
    #
    # def get_proxies(self, region):
    #     proxies = {
    #         "http": "http://192.168.2.165:7890",
    #         "https": "https://daili.deepbi.com"  # https://daili2.deepbi.com
    #     }
    #     if region == "JP":
    #         if self.toggle:
    #             self.toggle = False
    #             print("有代理")
    #             return proxies
    #         else:
    #             self.toggle = True
    #             print("无代理")
    #             return {}
    #     else:
    #         return {}
    def __init__(self):
        self.proxy_states = [
            {},  # 无代理
            {  # 代理1
                "http": "http://192.168.2.165:7890",
                "https": "https://daili.deepbi.com"
            },
            {  # 代理2
                "http": "http://192.168.2.165:7890",
                "https": "https://daili2.deepbi.com"
            }
        ]
        self.toggle = 0  # 初始时设置为无代理

    def get_proxies(self, region):
        proxies = self.proxy_states[self.toggle]
        if self.toggle == 0:
            print("无代理")
        elif self.toggle == 1:
            print("有代理1")
        else:
            print("有代理2")

        # 切换状态：无代理 -> 有代理1 -> 有代理2 -> 无代理
        self.toggle = (self.toggle + 1) % len(self.proxy_states)

        return proxies


# 示例用法
if __name__ == "__main__":
    proxy_manager = ProxyManager()
    print(proxy_manager.get_proxies("JP"))  # 返回 {}
    print(proxy_manager.get_proxies("JP"))  # 返回 proxies
    print(proxy_manager.get_proxies("JP"))
    print(proxy_manager.get_proxies("JP"))
    print(proxy_manager.get_proxies("JP"))
