import random

class RandomUserAgentMiddleware:
    def process_request(self,request,spider):
        ua = random.choice(spider.settings.get("USER_AGENTS_LIST"))
        request.headers["User-Agent"] = ua

class CheckUserAgent:
    def precess_response(self,request,response,spider):
        print(dir(response.request))
        print(request.headers["User-Agent"])
        return response
