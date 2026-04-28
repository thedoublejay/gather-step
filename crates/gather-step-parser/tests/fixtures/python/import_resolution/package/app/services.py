from shared.models import Account

from .base import BaseRunner


class SessionClient:
    pass


class Processor:
    def __init__(
        self,
        client: SessionClient,
        *,
        account: Account = Account(),
        **kwargs,
    ):
        self.client = client
        self.account = account
        self.options = kwargs

    async def session(self):
        return self


class Runner(BaseRunner):
    def run(self, item):
        return item
