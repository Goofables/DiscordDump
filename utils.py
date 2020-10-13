from json import loads
from time import sleep, time

import requests


class APIUtil:
    BASE_URL = "https://discord.com/api/v8"

    limit_left = 0
    limit_reset = 0

    headers = {
        # 'User-Agent': ,
        "X-Ratelimit-Precision": "millisecond",
        # "Authorization": "Bot "
        # "Authorization": token,
    }

    def __init__(self, token, bot=True):
        self.headers["Authorization"] = f"{'Bot ' if bot else ''}{token}"

    def get(self, url: str):
        out = {}
        for i in range(5):
            if i == 4:
                print(f"Could not process query {url}")
                exit()
            if self.limit_left is not None and self.limit_left == 0 and self.limit_reset > time():
                if self.limit_reset - time() > 10:
                    print(f"Sleeping too long? {self.limit_reset - time()}")
                sleep(self.limit_reset - time())
            response = requests.get(self.BASE_URL + url, headers=self.headers)
            out = loads(response.text)
            try:
                self.limit_left = int(response.headers.get('x-ratelimit-remaining'))
                self.limit_reset = float(response.headers.get('x-ratelimit-reset'))
            except TypeError:
                pass

            if 'retry_after' in out and float(out['retry_after']) > 0:
                print(out)
                print(f"Sleeping {out['retry_after']}")
                sleep(float(out['retry_after']))
                continue
            out = loads(response.text)
            break
        return out


def parse_time(string: str) -> str: return string.replace("T", " ").replace("+00:00", "")
