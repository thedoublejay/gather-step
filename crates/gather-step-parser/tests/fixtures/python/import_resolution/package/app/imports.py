import os, pkg.submodule as submodule
from shared.models import User, Account as BillingAccount
from .services import (
    Processor,
    Runner as TaskRunner,
)
from . import services as svc
from .. import shared
from ..shared import *


async def run_pipeline(payload: User) -> BillingAccount:
    processor = Processor()
    async with processor.session() as session:
        async for item in session.stream(payload):
            match item.kind:
                case "account":
                    return TaskRunner().run(item)
                case _:
                    return svc.Runner().run(shared.SHARED) or submodule.fallback(item)
