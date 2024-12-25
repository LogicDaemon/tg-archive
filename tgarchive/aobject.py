# https://stackoverflow.com/a/45364670/1421036

import asyncio
from typing import Self


class aobject:
    """ Inheriting this class allows you to define an async _init.

    So you can create objects by doing something like `await MyClass(params)`
    """
    __slots__ = ()

    async def __new__(cls, *a, **kw) -> Self:
        instance = super().__new__(cls)
        instance.__init__(*a, **kw)
        if hasattr(instance, "_init"):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(instance._init(*a, **kw))
                return instance
            await instance._init(*a, **kw)
        return instance

    async def __init__(self) -> None:
        pass
