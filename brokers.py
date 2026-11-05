"""
Broker connectors. Both expose the same async interface so the engine
doesn't care which one is used.

  mt5      - talks directly to the MetaTrader 5 desktop terminal (Windows only,
             or Windows inside a VPS). Free.
  metaapi  - talks to your MT5 account through metaapi.cloud. Works from any
             OS (Mac, Linux, Windows, cloud server). Third-party service; check
             their pricing.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("broker")


class BrokerError(Exception):
    pass


class BaseBroker:
    async def connect(self): ...
    async def account_info(self) -> Dict: ...              # balance, equity, currency, is_demo, login, server
    async def symbol_info(self, symbol: str) -> Dict: ...  # digits, vol_min, vol_max, vol_step, stops_level
    async def price(self, symbol: str) -> Tuple[float, float]: ...       # bid, ask
    async def loss_per_lot(self, symbol, direction, entry, sl) -> float: ...
    async def place(self, symbol, direction, kind, volume, price, sl, tp, comment) -> str: ...
    async def positions(self, symbol: str) -> List[Dict]: ...
    async def orders(self, symbol: str) -> List[Dict]: ...
    async def modify_position(self, ticket, symbol, sl, tp): ...
    async def modify_order(self, ticket, symbol, price, sl, tp): ...
    async def close_position(self, ticket, symbol, volume=None): ...
    async def cancel_order(self, ticket): ...


# =========================================================== MetaTrader 5
class MT5Broker(BaseBroker):
    def __init__(self, cfg: Dict, magic: int):
        try:
            import MetaTrader5 as mt5  # noqa
        except ImportError as e:
            raise BrokerError(
                "The MetaTrader5 Python package is not installed or this is not Windows.\n"
                "On Windows run:  pip install MetaTrader5\n"
                "On Mac/Linux use backend: metaapi instead."
            ) from e
        self.mt5 = mt5
        self.cfg = cfg or {}
        self.magic = magic

    async def _run(self, fn, *a, **kw):
        return await asyncio.to_thread(fn, *a, **kw)

    # ---- connection / info
    async def connect(self):
        def _c():
            kw = {}
            if self.cfg.get("path"):
                kw["path"] = self.cfg["path"]
            if self.cfg.get("login"):
                kw.update(login=int(self.cfg["login"]), password=str(self.cfg.get("password", "")),
                          server=str(self.cfg.get("server", "")))
            if not self.mt5.initialize(**kw):
                raise BrokerError(f"MT5 initialize failed: {self.mt5.last_error()}. "
                                  "Is the MT5 terminal installed, open and logged in? "
                                  "Is 'Algo Trading' enabled?")
        await self._run(_c)

    async def account_info(self):
        def _a():
            a = self.mt5.account_info()
            if a is None:
                raise BrokerError(f"account_info failed: {self.mt5.last_error()}")
            return dict(balance=a.balance, equity=a.equity, currency=a.currency,
                        is_demo=(a.trade_mode == self.mt5.ACCOUNT_TRADE_MODE_DEMO),
                        login=a.login, server=a.server)
        return await self._run(_a)

    async def symbol_info(self, symbol):
        def _s():
            self.mt5.symbol_select(symbol, True)
            i = self.mt5.symbol_info(symbol)
            if i is None:
                raise BrokerError(f"Symbol '{symbol}' not found. Check the exact gold symbol "
                                  "name in MT5 Market Watch (e.g. XAUUSD, XAUUSDm, GOLD).")
            return dict(digits=i.digits, point=i.point, vol_min=i.volume_min, vol_max=i.volume_max,
                        vol_step=i.volume_step, stops_level=i.trade_stops_level * i.point,
                        filling_mask=i.filling_mode)
        return await self._run(_s)

    async def price(self, symbol):
        def _p():
            t = self.mt5.symbol_info_tick(symbol)
            if t is None:
                raise BrokerError(f"No price for {symbol}: {self.mt5.last_error()}")
            return t.bid, t.ask
        return await self._run(_p)

    async def loss_per_lot(self, symbol, direction, entry, sl):
        def _l():
            ot = self.mt5.ORDER_TYPE_BUY if direction == "BUY" else self.mt5.ORDER_TYPE_SELL
            p = self.mt5.order_calc_profit(ot, symbol, 1.0, entry, sl)
            if p is None:
                raise BrokerError(f"order_calc_profit failed: {self.mt5.last_error()}")
            return abs(p)
        return await self._run(_l)

    # ---- trading
    def _fillings(self, symbol):
        m = self.mt5
        mask = m.symbol_info(symbol).filling_mode
        order = []
        if mask & 1:
            order.append(m.ORDER_FILLING_FOK)
        if mask & 2:
            order.append(m.ORDER_FILLING_IOC)
        order.append(m.ORDER_FILLING_RETURN)
        for f in (m.ORDER_FILLING_FOK, m.ORDER_FILLING_IOC, m.ORDER_FILLING_RETURN):
            if f not in order:
                order.append(f)
        return order

    def _send(self, req, symbol):
        m = self.mt5
        last = None
        for fill in self._fillings(symbol):
            req["type_filling"] = fill
            res = m.order_send(req)
            if res is None:
                last = f"order_send returned None: {m.last_error()}"
                continue
            if res.retcode in (m.TRADE_RETCODE_DONE, m.TRADE_RETCODE_PLACED, m.TRADE_RETCODE_DONE_PARTIAL):
                return res
            last = f"retcode {res.retcode}: {res.comment}"
            if res.retcode != m.TRADE_RETCODE_INVALID_FILL:
                break
        raise BrokerError(last)

    async def place(self, symbol, direction, kind, volume, price, sl, tp, comment):
        def _p():
            m = self.mt5
            buy = direction == "BUY"
            if kind == "market":
                t = m.symbol_info_tick(symbol)
                req = dict(action=m.TRADE_ACTION_DEAL, type=m.ORDER_TYPE_BUY if buy else m.ORDER_TYPE_SELL,
                           price=t.ask if buy else t.bid)
            else:
                types = {("BUY", "limit"): m.ORDER_TYPE_BUY_LIMIT, ("SELL", "limit"): m.ORDER_TYPE_SELL_LIMIT,
                         ("BUY", "stop"): m.ORDER_TYPE_BUY_STOP, ("SELL", "stop"): m.ORDER_TYPE_SELL_STOP}
                req = dict(action=m.TRADE_ACTION_PENDING, type=types[(direction, kind)], price=price,
                           type_time=m.ORDER_TIME_GTC)
            req.update(symbol=symbol, volume=float(volume), sl=float(sl), tp=float(tp),
                       deviation=int(self.cfg.get("deviation_points", 30)), magic=self.magic,
                       comment=comment[:31])
            res = self._send(req, symbol)
            return str(res.order)
        return await self._run(_p)

    async def positions(self, symbol):
        def _p():
            ps = self.mt5.positions_get(symbol=symbol) or []
            return [dict(ticket=str(p.ticket), identifier=str(p.identifier), symbol=p.symbol,
                         direction="BUY" if p.type == 0 else "SELL", volume=p.volume,
                         open_price=p.price_open, sl=p.sl, tp=p.tp, magic=p.magic) for p in ps]
        return await self._run(_p)

    async def orders(self, symbol):
        def _o():
            os_ = self.mt5.orders_get(symbol=symbol) or []
            return [dict(ticket=str(o.ticket), symbol=o.symbol, price=o.price_open, sl=o.sl, tp=o.tp,
                         volume=o.volume_current, magic=o.magic) for o in os_]
        return await self._run(_o)

    async def modify_position(self, ticket, symbol, sl, tp):
        def _m():
            m = self.mt5
            res = m.order_send(dict(action=m.TRADE_ACTION_SLTP, position=int(ticket), symbol=symbol,
                                    sl=float(sl), tp=float(tp), magic=self.magic))
            if res is None or res.retcode != m.TRADE_RETCODE_DONE:
                raise BrokerError(f"modify position failed: {getattr(res, 'retcode', None)} "
                                  f"{getattr(res, 'comment', m.last_error())}")
        await self._run(_m)

    async def modify_order(self, ticket, symbol, price, sl, tp):
        def _m():
            m = self.mt5
            res = m.order_send(dict(action=m.TRADE_ACTION_MODIFY, order=int(ticket), symbol=symbol,
                                    price=float(price), sl=float(sl), tp=float(tp),
                                    type_time=m.ORDER_TIME_GTC))
            if res is None or res.retcode != m.TRADE_RETCODE_DONE:
                raise BrokerError(f"modify order failed: {getattr(res, 'retcode', None)} "
                                  f"{getattr(res, 'comment', m.last_error())}")
        await self._run(_m)

    async def close_position(self, ticket, symbol, volume=None):
        def _c():
            m = self.mt5
            p = (m.positions_get(ticket=int(ticket)) or [None])[0]
            if p is None:
                return
            t = m.symbol_info_tick(symbol)
            buy = p.type == 0
            req = dict(action=m.TRADE_ACTION_DEAL, position=p.ticket, symbol=symbol,
                       volume=float(volume or p.volume),
                       type=m.ORDER_TYPE_SELL if buy else m.ORDER_TYPE_BUY,
                       price=t.bid if buy else t.ask,
                       deviation=int(self.cfg.get("deviation_points", 30)), magic=self.magic,
                       comment="tg-bot close")
            self._send(req, symbol)
        await self._run(_c)

    async def cancel_order(self, ticket):
        def _c():
            m = self.mt5
            res = m.order_send(dict(action=m.TRADE_ACTION_REMOVE, order=int(ticket)))
            if res is None or res.retcode != m.TRADE_RETCODE_DONE:
                raise BrokerError(f"cancel failed: {getattr(res, 'retcode', None)} "
                                  f"{getattr(res, 'comment', m.last_error())}")
        await self._run(_c)


# =========================================================== MetaApi cloud
class MetaApiBroker(BaseBroker):
    def __init__(self, cfg: Dict, magic: int):
        try:
            from metaapi_cloud_sdk import MetaApi  # noqa
        except ImportError as e:
            raise BrokerError("metaapi-cloud-sdk not installed. Run: pip install metaapi-cloud-sdk") from e
        self.MetaApi = MetaApi
        self.cfg = cfg or {}
        self.magic = magic
        self.conn = None
        self._spec: Dict[str, Dict] = {}

    async def connect(self):
        token, acc_id = self.cfg.get("token"), self.cfg.get("account_id")
        if not token or not acc_id:
            raise BrokerError("metaapi.token and metaapi.account_id must be set in config.yaml")
        api = self.MetaApi(token)
        account = await api.metatrader_account_api.get_account(acc_id)
        if account.state not in ("DEPLOYING", "DEPLOYED"):
            await account.deploy()
        await account.wait_connected()
        self.conn = account.get_rpc_connection()
        await self.conn.connect()
        await self.conn.wait_synchronized()

    async def account_info(self):
        a = await self.conn.get_account_information()
        return dict(balance=a["balance"], equity=a["equity"], currency=a.get("currency", "USD"),
                    is_demo=a.get("type", "") in ("ACCOUNT_TRADE_MODE_DEMO", "ACCOUNT_TRADE_MODE_CONTEST"),
                    login=a.get("login"), server=a.get("server"))

    async def symbol_info(self, symbol):
        s = await self.conn.get_symbol_specification(symbol)
        if not s:
            raise BrokerError(f"Symbol '{symbol}' not found on this account")
        self._spec[symbol] = s
        digits = s.get("digits", 2)
        point = 10 ** -digits
        return dict(digits=digits, point=point, vol_min=s.get("minVolume", 0.01),
                    vol_max=s.get("maxVolume", 100), vol_step=s.get("volumeStep", 0.01),
                    stops_level=s.get("stopsLevel", 0) * point)

    async def price(self, symbol):
        p = await self.conn.get_symbol_price(symbol)
        return p["bid"], p["ask"]

    async def loss_per_lot(self, symbol, direction, entry, sl):
        p = await self.conn.get_symbol_price(symbol)
        spec = self._spec.get(symbol) or await self.conn.get_symbol_specification(symbol)
        tick_size = spec.get("tickSize") or 10 ** -spec.get("digits", 2)
        tick_val = p.get("lossTickValue")
        if tick_val:
            return abs(entry - sl) / tick_size * tick_val
        return abs(entry - sl) * spec.get("contractSize", 100)

    def _check(self, res):
        if isinstance(res, dict) and res.get("stringCode") not in (None, "TRADE_RETCODE_DONE",
                                                                    "TRADE_RETCODE_PLACED"):
            raise BrokerError(f"{res.get('stringCode')}: {res.get('message')}")
        return res

    async def place(self, symbol, direction, kind, volume, price, sl, tp, comment):
        c = self.conn
        opts = {"comment": comment[:31], "magic": self.magic}
        try:
            if kind == "market":
                fn = c.create_market_buy_order if direction == "BUY" else c.create_market_sell_order
                res = await fn(symbol, volume, sl, tp, opts)
            else:
                fn = {("BUY", "limit"): c.create_limit_buy_order, ("SELL", "limit"): c.create_limit_sell_order,
                      ("BUY", "stop"): c.create_stop_buy_order, ("SELL", "stop"): c.create_stop_sell_order}[(direction, kind)]
                res = await fn(symbol, volume, price, sl, tp, opts)
        except Exception as e:
            raise BrokerError(str(e)) from e
        self._check(res)
        return str(res.get("positionId") or res.get("orderId"))

    async def positions(self, symbol):
        ps = await self.conn.get_positions()
        return [dict(ticket=str(p["id"]), identifier=str(p["id"]), symbol=p["symbol"],
                     direction="BUY" if p["type"] == "POSITION_TYPE_BUY" else "SELL",
                     volume=p["volume"], open_price=p["openPrice"], sl=p.get("stopLoss") or 0,
                     tp=p.get("takeProfit") or 0, magic=p.get("magic"))
                for p in ps if p["symbol"] == symbol]

    async def orders(self, symbol):
        os_ = await self.conn.get_orders()
        return [dict(ticket=str(o["id"]), symbol=o["symbol"], price=o.get("openPrice"),
                     sl=o.get("stopLoss") or 0, tp=o.get("takeProfit") or 0,
                     volume=o.get("currentVolume", o.get("volume")), magic=o.get("magic"))
                for o in os_ if o["symbol"] == symbol]

    async def modify_position(self, ticket, symbol, sl, tp):
        try:
            self._check(await self.conn.modify_position(ticket, sl, tp))
        except BrokerError:
            raise
        except Exception as e:
            raise BrokerError(str(e)) from e

    async def modify_order(self, ticket, symbol, price, sl, tp):
        try:
            self._check(await self.conn.modify_order(ticket, price, sl, tp))
        except BrokerError:
            raise
        except Exception as e:
            raise BrokerError(str(e)) from e

    async def close_position(self, ticket, symbol, volume=None):
        try:
            if volume:
                self._check(await self.conn.close_position_partially(ticket, volume))
            else:
                self._check(await self.conn.close_position(ticket))
        except BrokerError:
            raise
        except Exception as e:
            raise BrokerError(str(e)) from e

    async def cancel_order(self, ticket):
        try:
            self._check(await self.conn.cancel_order(ticket))
        except BrokerError:
            raise
        except Exception as e:
            raise BrokerError(str(e)) from e


def make_broker(cfg: Dict, magic: int) -> BaseBroker:
    backend = (cfg.get("backend") or "mt5").lower()
    if backend == "mt5":
        return MT5Broker(cfg.get("mt5", {}), magic)
    if backend == "metaapi":
        return MetaApiBroker(cfg.get("metaapi", {}), magic)
    raise BrokerError(f"Unknown broker backend '{backend}' (use mt5 or metaapi)")
