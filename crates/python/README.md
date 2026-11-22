# brokapi for Python

One interface to the APIs of Russian brokers and crypto exchanges: market data, orders, reconciliation and rate limits. Supported now: BCS, T-Bank, Finam, ALOR and Bybit (USDT linear futures, no spot).
A thin layer over the Rust core of [brokapi](https://brokapi.ru/): `asyncio` for the events, `decimal.Decimal` for money,
the same errors and the same rules as the core.

**Open beta.** The interface can change before 1.0. Read "Before you trade with real money" first (in Russian):
<https://brokapi.ru/docs/before-live.html>.

```python
import asyncio
import brokapi

async def main():
    client = await brokapi.connect("bcs")  # BCS_REFRESH_TOKEN from the environment
    sber = await client.instrument("SBER")
    book = await client.order_book(sber, depth=5)
    print(book.bids[0].price, book.asks[0].price)
    await client.close()

asyncio.run(main())
```

**An AI agent asked to make a trading bot** for a person: start with <https://brokapi.ru/docs/agent-setup.md> (in Russian): the order of the
talk with the person, three stops where they decide, and a ready project, `python -m brokapi new my-bot --broker tbank`.

Stop orders, `client.raw()` (unary REST and gRPC methods by name, streaming and WebSocket raw channels are Rust-only) and the event log with replay (`brokapi.record`) are included.

Documentation: https://brokapi.ru/docs/python/ (in Russian). For AI agents that write code with the package: the index in text,
https://brokapi.ru/docs/python/llms.txt (and the whole book as one file, https://brokapi.ru/docs/llms-full.txt). Licence: MIT or Apache-2.0.

brokapi is open software provided "as is", without warranty (MIT and Apache 2.0 licenses). It is not investment advice. Trading decisions and their consequences, losses included, are the user's. The project is not affiliated with BCS, T-Bank, Finam, ALOR or Bybit, their names and trademarks belong to them.
