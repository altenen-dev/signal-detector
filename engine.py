"""
Trade engine: turns parsed signals into orders and manages them afterwards.

Rules (all configurable in config.yaml -> trading):
  * Two entry prices in a signal ("SELL 3909 - 3911.5", "Buy 3898 & 3895",
    "sell limit 4020 and 4024") = TWO orders, one at each price ("legs").
    Leg 1 = the price the market reaches first, leg 2 = the deeper price.
    With split_risk on, risk_percent is shared between the legs, so if both
    fill and hit SL the total loss is still risk_percent.
  * Lot size from % risk and each leg's distance entry -> SL.
  * Take profit = LAST TP of the signal for every leg, with equal partial
    closes at each TP on the way (TP1, TP2 ...). A leg too small to split
    (0.01 lot) closes fully at TP1 instead.
  * Protection ("insurance"): when price reaches TP1 each leg's SL moves to its
    own entry (+ small offset). Ladder: TP2 reached -> SL to TP1, TP3 -> SL to TP2 ...
    A signal with a single TP moves SL to entry at single_tp_trigger_pct % of the way.
  * Leg entry: market if price is at the leg's price (± tolerance) or better,
    otherwise a limit order at that price.
  * Follow-up messages (reply or not): SL to BE, move SL to X, close, close half, cancel.
  * Safety: demo-only unless allow_live, dry-run mode, spread / price sanity
    checks, max open signals, pending-order expiry.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from brokers import BaseBroker, BrokerError
from signal_parser import Command, Signal, parse_message

log = logging.getLogger("engine")

DEFAULTS = {
    "symbol": "XAUUSD",
    "risk_percent": 1.0,
    "risk_basis": "balance",
    "max_lot": 1.0,
    "min_lot_max_risk_percent": 2.0,
    "magic": 20261104,
    "dry_run": True,
    "allow_live": False,
    "two_entries": True,
    "split_risk": True,
    "max_spread": 0.6,
    "entry_tolerance": 0.5,
    "when_price_worse": "limit",          # limit | market | skip   (first leg only)
    "pending_expiry_minutes": 25,
    "cancel_pending_if_tp1_hit": True,
    "max_price_deviation_pct": 1.5,
    "max_open_signals": 3,
    "require_symbol_word": False,
    "breakeven": {"enabled": True, "offset": 0.3, "ladder": True, "single_tp_trigger_pct": 50},
    "partial_close": {"enabled": True},
    # place an unfinished signal straight away ("GOLD SELL LIMIT 4121", "GOLD BUY  Entry: 4052 & 4049
    # SL: TP:") and fill in SL / TP when the admin edits the message
    "early_entry": {"enabled": True, "temp_sl_distance": 6.0, "allow_market": True,
                    "complete_minutes": 5},
    "commands": {"enabled": True, "apply_to_latest_if_not_reply": True,
                 "obey_partial_close": False, "reenter_max_hours": 12},
    "edit_retry_minutes": 10,
    "max_message_age_seconds": 120,
    "fix_hundreds_typo": False,
    "allowed_sender_ids": [],
    "poll_seconds": 2,
}

ACTIVE = ("pending", "open")


def _merge(d, u):
    out = dict(d)
    for k, v in (u or {}).items():
        out[k] = _merge(d[k], v) if isinstance(v, dict) and isinstance(d.get(k), dict) else v
    return out


class Journal:
    FIELDS = ["time", "event", "msg_id", "leg", "direction", "kind", "volume", "entry", "sl", "tp",
              "ticket", "details"]

    def __init__(self, path):
        self.path = path
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self.FIELDS)

    def write(self, event, **kw):
        row = {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "event": event, **kw}
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, self.FIELDS, extrasaction="ignore").writerow(row)


class State:
    def __init__(self, path):
        self.path = path
        self.data = {"signals": {}, "processed": []}
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                self.data = json.load(f)

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)
        os.replace(tmp, self.path)

    @property
    def signals(self) -> Dict[str, Dict]:
        return self.data["signals"]

    def processed(self, msg_id) -> bool:
        return str(msg_id) in self.data["processed"]

    def mark(self, msg_id):
        self.data["processed"].append(str(msg_id))
        self.data["processed"] = self.data["processed"][-2000:]

    def active(self, statuses=ACTIVE) -> List[Dict]:
        return sorted([s for s in self.signals.values() if s["status"] in statuses],
                      key=lambda s: s["created"])


def _refresh(s):
    """signal status from its legs"""
    if s["status"] == "dry":
        return
    st = [l["status"] for l in s["legs"]]
    s["status"] = "open" if "open" in st else "pending" if "pending" in st else "closed"


class Engine:
    def __init__(self, trading_cfg: Dict, broker: BaseBroker, state_path="state.json",
                 journal_path="trades_journal.csv"):
        self.c = _merge(DEFAULTS, trading_cfg)
        self.b = broker
        self.sym = self.c["symbol"]
        self.state = State(state_path)
        self.journal = Journal(journal_path)
        self.lock = asyncio.Lock()
        self.info: Dict = {}
        self.trading_enabled = False

    # ------------------------------------------------------------ startup
    async def start(self):
        acct = await self.b.account_info()
        self.info = await self.b.symbol_info(self.sym)
        mode = "DEMO" if acct["is_demo"] else "LIVE (REAL MONEY)"
        log.info("Connected: account %s on %s | %s | balance %.2f %s",
                 acct.get("login"), acct.get("server"), mode, acct["balance"], acct["currency"])
        if self.c["dry_run"]:
            log.warning("DRY RUN is ON - signals will be read and logged, NO orders will be sent.")
            self.trading_enabled = False
        elif not acct["is_demo"] and not self.c["allow_live"]:
            log.error("This is a LIVE account and allow_live is false. Refusing to trade. "
                      "Test on demo first; set trading.allow_live: true only when you are sure.")
            self.trading_enabled = False
        else:
            self.trading_enabled = True
            log.info("Trading is ENABLED on %s, risk %.2f%% per signal.", self.sym, self.c["risk_percent"])

    # ------------------------------------------------------------ helpers
    def _r(self, x):
        return round(float(x), int(self.info.get("digits", 2)))

    def _lots(self, raw):
        step = self.info.get("vol_step", 0.01) or 0.01
        lots = math.floor(raw / step + 1e-9) * step
        lots = min(lots, float(self.c["max_lot"]), self.info.get("vol_max", 100))
        return round(lots, 8)

    @staticmethod
    def _better(direction, new_sl, cur_sl):
        if not cur_sl:
            return True
        return new_sl > cur_sl + 1e-9 if direction == "BUY" else new_sl < cur_sl - 1e-9

    def _sl_valid(self, direction, sl, bid, ask):
        gap = self.info.get("stops_level", 0)
        return sl < bid - gap if direction == "BUY" else sl > ask + gap

    def _trigger_levels(self, s, entry):
        tps = s["tps"]
        if len(tps) >= 2:
            return tps[:-1]
        pct = self.c["breakeven"]["single_tp_trigger_pct"]
        if not pct:
            return []
        return [entry + (tps[0] - entry) * pct / 100.0]

    def _sl_for_stage(self, s, entry, stage):
        """stage 1 = break-even, stage n>1 = SL to TP(n-1)."""
        off = float(self.c["breakeven"]["offset"])
        if stage <= 0:
            return None
        if stage == 1:
            return entry + off if s["direction"] == "BUY" else entry - off
        return s["tps"][stage - 2]

    @staticmethod
    def _legs_active(s):
        return [l for l in s["legs"] if l["status"] in ACTIVE and l.get("ticket")]

    # ------------------------------------------------------------ messages
    async def on_message(self, msg_id, text, reply_to=None, sender_id=None, edited=False, msg_time=None):
        allowed = [int(x) for x in (self.c.get("allowed_sender_ids") or [])]
        if allowed and sender_id is not None and int(sender_id) not in allowed:
            return
        if msg_time is not None:
            age = (datetime.now(timezone.utc) - msg_time).total_seconds()
            if age > float(self.c["max_message_age_seconds"]):
                # delivered late (bot was stopped / restarting): too old to act on safely
                if any(w in (text or "").upper() for w in ("BUY", "SELL", "SL", "TP")) or len(text or "") < 120:
                    log.info("Ignored msg %s: %.0f s old (received after a restart)", msg_id, age)
                return
        async with self.lock:
            try:
                await self._handle(str(msg_id), text or "", reply_to, edited)
            except BrokerError as e:
                log.error("Broker error on message %s: %s", msg_id, e)
                self.journal.write("error", msg_id=msg_id, details=str(e))
            except Exception:
                log.exception("Unexpected error on message %s", msg_id)
            finally:
                self.state.save()

    async def _handle(self, msg_id, text, reply_to, edited):
        res = parse_message(text, self.c["require_symbol_word"], is_reply=reply_to is not None)

        if res.signal:
            if msg_id in self.state.signals:
                if edited:
                    await self._apply_edit(self.state.signals[msg_id], res.signal)
                return
            if self.state.processed(msg_id):
                # a skipped signal that the admin corrects soon after (typo fixed) gets another chance
                t0 = self.state.data.setdefault("skipped", {}).get(msg_id)
                if not (edited and t0 and time.time() - t0 < float(self.c["edit_retry_minutes"]) * 60):
                    if edited and t0:
                        log.warning("Signal %s was corrected %.0f min after it was skipped - too late "
                                    "(edit_retry_minutes = %s), not traded", msg_id, (time.time() - t0) / 60,
                                    self.c["edit_retry_minutes"])
                    return
                del self.state.data["skipped"][msg_id]
                log.info("Signal %s was edited after being skipped - checking it again", msg_id)
            self.state.mark(msg_id)
            sig = res.signal
            log.info("SIGNAL %s: %s entry %s SL %s TPs %s", msg_id, sig.direction,
                     "market" if sig.entry_low is None else
                     (sig.entry_low if sig.entry_low == sig.entry_high else f"{sig.entry_low} & {sig.entry_high}"),
                     sig.sl, sig.tps)
            await self._open(msg_id, sig)
            return

        if res.draft and self.c["early_entry"]["enabled"]:
            s = self.state.signals.get(msg_id)
            if s is not None:
                if s.get("draft"):
                    await self._update_draft(s, res.draft)
                return
            if self.state.processed(msg_id):
                return
            self.state.mark(msg_id)
            await self._open(msg_id, res.draft, draft=True)
            return

        if res.commands and self.c["commands"]["enabled"] and not edited:
            if self.state.processed(msg_id):
                return
            self.state.mark(msg_id)
            if any(c.action == "reenter" for c in res.commands):
                await self._reenter(reply_to, msg_id)
                return
            targets = self._targets(reply_to, any(c.all_trades for c in res.commands))
            if not targets:
                log.info("Command %s in msg %s but no active trade to apply it to",
                         [c.action for c in res.commands], msg_id)
                return
            for cmd in res.commands:
                for s in targets:
                    await self._command(cmd, s, msg_id)
                    _refresh(s)
            return

        if any(w in text.upper() for w in ("BUY", "SELL")):
            log.info("Ignored msg %s (%s): %r", msg_id, res.reason, text[:80])

    def _targets(self, reply_to, all_trades):
        ok = ACTIVE if self.trading_enabled else ACTIVE + ("dry",)
        active = self.state.active(ok)
        if all_trades:
            return active
        if reply_to is not None and str(reply_to) in self.state.signals:
            s = self.state.signals[str(reply_to)]
            return [s] if s["status"] in ok else []
        if self.c["commands"]["apply_to_latest_if_not_reply"] and active:
            return [active[-1]]
        return []

    # ------------------------------------------------------------ open trade
    def _entry_prices(self, sig: Signal) -> List[Optional[float]]:
        """None = market. Returned in the order the market reaches them."""
        buy = sig.direction == "BUY"
        if sig.entry_low is None:
            return [None]
        lo, hi = sig.entry_low, sig.entry_high
        first, second = (hi, lo) if buy else (lo, hi)
        if lo == hi or not self.c["two_entries"]:
            return [None] if sig.market and sig.order_kind is None else [first]
        if sig.market and sig.order_kind is None:
            return [None, second]        # "buy now 3910 - 3908": one now, one deeper
        return [first, second]

    def _decide_leg(self, sig: Signal, idx, price, bid, ask):
        buy = sig.direction == "BUY"
        px = ask if buy else bid
        if price is None:
            return "market", px
        if sig.order_kind == "STOP":
            if (buy and price > ask) or (not buy and price < bid):
                return "stop", price
            return "market", px
        tol = float(self.c["entry_tolerance"])
        reached = (buy and px <= price + tol) or (not buy and px >= price - tol)
        if reached:
            return "market", px
        if idx > 0 or sig.order_kind == "LIMIT" or self.c["when_price_worse"] == "limit":
            return "limit", price
        if self.c["when_price_worse"] == "market":
            return "market", px
        return "skip", px

    async def _open(self, msg_id, sig: Signal, draft=False, rec=None, skip=()):
        """Place a signal. draft=True: SL/TPs not written yet - pending orders (and market if
        allowed) go in with a temporary SL and no TP. rec/skip: finishing a draft - add the
        entries that are not in the trade yet to the existing record."""
        bid, ask = await self.b.price(self.sym)
        buy = sig.direction == "BUY"
        px = ask if buy else bid
        if draft:
            sig = self._draft_signal(msg_id, sig, px)
            if sig is None:
                return
        j = dict(msg_id=msg_id, direction=sig.direction, sl=sig.sl, tp=sig.tps[-1] if sig.tps else 0)
        if rec is None and len(self.state.active()) >= int(self.c["max_open_signals"]):
            return self._reject(msg_id, "max_open_signals reached", j)

        ref = sig.entry_mid if sig.entry_mid is not None else sig.sl
        dev = abs(ref - px) / px * 100
        if dev > float(self.c["max_price_deviation_pct"]) and self.c["fix_hundreds_typo"]:
            # admin typed 40xx instead of 41xx on every line ("SELL LIMIT 4021 / SL 4026 ..." at 4117):
            # shift the whole signal by 100 if that puts it right next to the market
            for shift in (100.0, -100.0):
                if abs(ref + shift - px) / px * 100 <= 0.3:
                    log.warning("Signal %s: all prices look 100 off the market (typo) - using them +%s: "
                                "entry %s SL %s", msg_id, shift, ref + shift, sig.sl + shift)
                    sig = Signal(direction=sig.direction, sl=sig.sl + shift, tps=[x + shift for x in sig.tps],
                                 entry_low=None if sig.entry_low is None else sig.entry_low + shift,
                                 entry_high=None if sig.entry_high is None else sig.entry_high + shift,
                                 order_kind=sig.order_kind, market=sig.market)
                    j = dict(msg_id=msg_id, direction=sig.direction, sl=sig.sl, tp=sig.last_tp)
                    ref, dev = ref + shift, abs(ref + shift - px) / px * 100
                    break
        if dev > float(self.c["max_price_deviation_pct"]):
            return self._reject(msg_id, f"signal price {ref} is {dev:.2f}% away from market {px}", j)

        past_tp1 = False
        if sig.tps:
            tp1, tp_last = sig.tps[0], sig.tps[-1]
            past_tp1 = (buy and bid >= tp1) or (not buy and ask <= tp1)
            if ((buy and bid >= tp_last) or (not buy and ask <= tp_last)) and sig.order_kind != "LIMIT":
                return self._reject(msg_id, f"price already past the last TP ({tp_last})", j)

        prices = self._entry_prices(sig)
        plan = []
        for i, p in enumerate(prices):
            if i in skip:
                continue
            kind, entry = self._decide_leg(sig, i, p, bid, ask)
            if draft and kind == "market" and not self.c["early_entry"]["allow_market"]:
                log.info("Signal %s leg %d: price is at the entry but SL/TP are not written yet - "
                         "waiting for them (early_entry.allow_market is off)", msg_id, i + 1)
                continue
            if kind == "skip":
                log.info("Signal %s leg %d skipped: price %s away from %s (when_price_worse=skip)",
                         msg_id, i + 1, px, p)
                continue
            if kind != "market" and ((buy and not sig.sl < entry) or (not buy and not sig.sl > entry)):
                return self._reject(msg_id, f"SL on wrong side of entry {entry}", j)
            plan.append((i, kind, entry))
        if not plan:
            if rec is not None or draft:
                return
            return self._reject(msg_id, "no entry left to place", j)
        # entering at market when the move to TP1 has already happened = too late.
        # A limit order that waits for a pullback (SELL LIMIT 4121, TP1 4119, price 4117) is fine.
        if past_tp1 and any(k == "market" for _, k, _ in plan):
            if rec is not None:
                plan = [x for x in plan if x[1] != "market"]
                if not plan:
                    return log.info("Signal %s: remaining entry not taken - price already past TP1", msg_id)
            else:
                return self._reject(msg_id, f"price already past TP1 ({sig.tps[0]})", j)

        if any(k == "market" for _, k, _ in plan):
            spread = ask - bid
            if spread > float(self.c["max_spread"]):
                return self._reject(msg_id, f"spread {spread:.2f} > max_spread", j)
            if not self._sl_valid(sig.direction, sig.sl, bid, ask):
                return self._reject(msg_id, "price is already at/through the stop loss", j)

        n_share = len(prices) if self.c["split_risk"] else 1
        sl = self._r(sig.sl)
        if rec is None:
            rec = dict(msg_id=msg_id, direction=sig.direction, tps=sig.tps, signal_sl=sig.sl,
                       entry_low=sig.entry_low, entry_high=sig.entry_high, created=time.time(),
                       status="dry" if not self.trading_enabled else "pending", legs=[],
                       sig=sig.to_dict(), origin=getattr(sig, "origin", msg_id),
                       draft=draft, temp_sl=getattr(sig, "temp_sl", False))

        for i, kind, entry in plan:
            entry = self._r(entry)
            lots, note = await self._size(sig, entry, n_share)
            # a trade too small to split (0.01 lot) closes fully at TP1
            tp1_only = (self.c["partial_close"]["enabled"] and len(sig.tps) > 1
                        and lots > 0 and not self._splittable(lots))
            tp = (self._r(sig.tps[0]) if tp1_only else self._r(sig.last_tp)) if sig.tps else 0.0
            leg = dict(n=i + 1, kind=kind, entry=entry, volume=lots, ticket=None, status="skipped",
                       stage=0, missing=0, sl_fail=0, created=time.time(),
                       tp_mode="tp1" if tp1_only else "last", tp_done=0, best=None)
            rec["legs"].append(leg)
            plan_txt = (self._plan_text(lots, sig.tps, tp1_only) if sig.tps else
                        ("TEMPORARY SL, no TP yet - waiting for the admin to finish the signal"
                         if getattr(sig, "temp_sl", False) else "no TP yet - waiting for the admin"))
            note = f"{note}; {plan_txt}"
            lj = dict(j, leg=i + 1, kind=kind, volume=lots, entry=entry, sl=sl, tp=tp)
            if lots <= 0:
                log.warning("Signal %s leg %d not placed: %s", msg_id, i + 1, note)
                self.journal.write("skipped", details=note, **lj)
                continue
            if not self.trading_enabled:
                log.info("[DRY RUN] leg %d: would place %s %s %.2f lots @ %s SL %s TP %s (%s)",
                         i + 1, sig.direction, kind, lots, entry, sl, tp, note)
                leg["status"] = "dry"
                self.journal.write("dry_run", details=note, **lj)
                continue
            try:
                leg["ticket"] = await self.b.place(self.sym, sig.direction, kind, lots, entry, sl, tp,
                                                   f"tg:{msg_id}:{i + 1}")
            except BrokerError as e:
                log.error("Signal %s leg %d order failed: %s", msg_id, i + 1, e)
                self.journal.write("error", details=str(e), **lj)
                continue
            leg["status"] = "open" if kind == "market" else "pending"
            log.info("PLACED leg %d: %s %s %.2f lots @ %s SL %s TP %s ticket %s (%s)",
                     i + 1, sig.direction, kind, lots, entry, sl, tp, leg["ticket"], note)
            self.journal.write("placed", ticket=leg["ticket"], details=note, **lj)

        self.state.signals[msg_id] = rec
        _refresh(rec)

    # ------------------------------------------------------------ unfinished signals
    def _draft_signal(self, msg_id, d: Signal, px):
        """Draft -> placeable signal: drop entries far from the market (typos like 5049),
        temporary SL when the admin has not written one yet."""
        dev_max = float(self.c["max_price_deviation_pct"])
        entries = [e for e in {d.entry_low, d.entry_high} if e is not None and abs(e - px) / px * 100 <= dev_max]
        if not entries:
            log.info("Unfinished signal %s: entry %s is far from the market %s - waiting for the full signal",
                     msg_id, d.entry_low, px)
            self.state.data.setdefault("skipped", {})[msg_id] = time.time()
            return None
        lo, hi = min(entries), max(entries)
        buy = d.direction == "BUY"
        temp = d.sl is None
        dist = float(self.c["early_entry"]["temp_sl_distance"])
        first = hi if buy else lo
        sl = d.sl if not temp else (first - dist if buy else first + dist)
        if (buy and sl >= lo) or (not buy and sl <= hi):
            return None
        sig = Signal(direction=d.direction, sl=sl, tps=[], entry_low=lo, entry_high=hi,
                     order_kind=d.order_kind, market=d.market)
        sig.temp_sl = temp
        log.info("UNFINISHED signal %s: %s entry %s SL %s (%s), no TP yet - placing it now",
                 msg_id, d.direction, lo if lo == hi else f"{lo} & {hi}", self._r(sl),
                 "temporary" if temp else "from the group")
        return sig

    async def _update_draft(self, s, d: Signal):
        """the admin added the SL but no TPs yet: put the real SL on the orders"""
        if d.sl is None or d.sl == s["signal_sl"] or d.direction != s["direction"]:
            return
        s["signal_sl"], s["temp_sl"] = d.sl, False
        positions, orders = await self.b.positions(self.sym), await self.b.orders(self.sym)
        bid, ask = await self.b.price(self.sym)
        for leg in self._legs_active(s):
            pos, order = self._find(leg, positions, orders)
            try:
                if order:
                    await self.b.modify_order(order["ticket"], self.sym, order["price"], self._r(d.sl), order["tp"])
                elif pos and self._sl_valid(s["direction"], d.sl, bid, ask):
                    await self.b.modify_position(pos["ticket"], self.sym, self._r(d.sl), pos["tp"])
                log.info("Signal %s leg %d: SL from the group set to %s", s["msg_id"], leg["n"], self._r(d.sl))
            except BrokerError as e:
                log.warning("Signal %s leg %d: could not set SL %s: %s", s["msg_id"], leg["n"], d.sl, e)

    async def _finish_draft(self, s, sig: Signal):
        """The admin finished the signal (SL + TPs): fix every entry already placed and add the rest.
        Waiting orders are replaced (right price, lot size, SL, TP). Entries already in the trade
        get the real SL/TP, and their lot is reduced if the real SL makes it risk too much."""
        msg_id = s["msg_id"]
        log.info("Signal %s is complete now: %s entry %s SL %s TPs %s - updating the orders",
                 msg_id, sig.direction, sig.entry_low if sig.entry_low == sig.entry_high
                 else f"{sig.entry_low} & {sig.entry_high}", sig.sl, sig.tps)
        positions, orders = await self.b.positions(self.sym), await self.b.orders(self.sym)
        bid, ask = await self.b.price(self.sym)
        if sig.direction != s["direction"]:
            for leg in self._legs_active(s):
                pos, order = self._find(leg, positions, orders)
                if order:
                    await self.b.cancel_order(order["ticket"])
                if pos:
                    await self.b.close_position(pos["ticket"], self.sym)
                leg["status"] = "closed"
            log.warning("Signal %s changed direction - old orders removed", msg_id)
            del self.state.signals[msg_id]
            return await self._open(msg_id, sig)

        prices = self._entry_prices(sig)
        n_share = len(prices) if self.c["split_risk"] else 1
        filled = []
        for leg in self._legs_active(s):
            pos, order = self._find(leg, positions, orders)
            if order:
                await self.b.cancel_order(order["ticket"])
                leg["status"] = "replaced"
                self.journal.write("replaced", msg_id=msg_id, leg=leg["n"], ticket=order["ticket"],
                                   details="unfinished signal completed - order placed again")
            elif pos:
                filled.append((leg, pos))

        s.update(tps=sig.tps, signal_sl=sig.sl, entry_low=sig.entry_low, entry_high=sig.entry_high,
                 sig=sig.to_dict(), draft=False, temp_sl=False)
        used = set()
        step, vmin = self._step(), self.info.get("vol_min", 0.01)
        for leg, pos in filled:
            # which entry of the finished signal is this trade?
            cands = [(abs((p if p is not None else pos["open_price"]) - pos["open_price"]), i)
                     for i, p in enumerate(prices) if i not in used]
            idx = min(cands)[1] if cands else 0
            used.add(idx)
            leg["n"] = idx + 1
            if not self._sl_valid(s["direction"], sig.sl, bid, ask):
                await self.b.close_position(pos["ticket"], self.sym)
                leg["status"] = "closed"
                log.warning("Signal %s leg %d: price is already past the real SL %s - trade closed",
                            msg_id, leg["n"], sig.sl)
                self.journal.write("closed", msg_id=msg_id, leg=leg["n"], ticket=pos["ticket"],
                                   details="past the real SL when the signal was completed")
                continue
            target, note = await self._size(sig, pos["open_price"], n_share)
            volume = pos["volume"]
            if target <= 0:
                await self.b.close_position(pos["ticket"], self.sym)
                leg["status"] = "closed"
                log.warning("Signal %s leg %d closed: with the real SL %s %s", msg_id, leg["n"], sig.sl, note)
                continue
            excess = round(math.floor((volume - target) / step + 1e-9) * step, 8)
            if excess >= vmin:
                await self.b.close_position(pos["ticket"], self.sym, excess)
                volume = round(volume - excess, 8)
                log.info("Signal %s leg %d: real SL is wider than the temporary one - closed %.2f lots "
                         "to keep the risk at the planned %%", msg_id, leg["n"], excess)
            leg["volume"] = volume
            tp1_only = (self.c["partial_close"]["enabled"] and len(sig.tps) > 1 and not self._splittable(volume))
            leg["tp_mode"] = "tp1" if tp1_only else "last"
            tp = self._r(sig.tps[0]) if tp1_only else self._r(sig.last_tp)
            sl = self._r(sig.sl) if leg["stage"] == 0 else pos["sl"]
            await self.b.modify_position(pos["ticket"], self.sym, sl, tp)
            log.info("Signal %s leg %d updated: %.2f lots, SL %s, TP %s (%s)", msg_id, leg["n"], volume, sl, tp,
                     self._plan_text(volume, sig.tps, tp1_only))
            self.journal.write("completed", msg_id=msg_id, leg=leg["n"], volume=volume, sl=sl, tp=tp,
                               ticket=pos["ticket"])
        s["legs"] = [l for l in s["legs"] if l["status"] != "replaced"]
        await self._open(msg_id, sig, rec=s, skip=used)
        _refresh(s)

    async def _size(self, sig: Signal, entry, n_share):
        acct = await self.b.account_info()
        basis = acct["equity"] if self.c["risk_basis"] == "equity" else acct["balance"]
        risk_pct = float(self.c["risk_percent"]) / n_share
        lpl = await self.b.loss_per_lot(self.sym, sig.direction, entry, sig.sl)
        if lpl <= 0:
            return 0, "could not compute loss per lot"
        lots = self._lots(basis * risk_pct / 100.0 / lpl)
        vmin = self.info.get("vol_min", 0.01)
        if lots < vmin:
            min_risk_pct = lpl * vmin / basis * 100
            allowed = float(self.c["min_lot_max_risk_percent"]) / n_share
            if min_risk_pct <= allowed:
                return vmin, (f"min lot {vmin} used, real risk {min_risk_pct:.2f}% "
                              f"(SL distance too wide for {risk_pct:.2f}%)")
            return 0, f"even the minimum lot {vmin} would risk {min_risk_pct:.2f}% (> {allowed:.2f}% allowed)"
        return lots, f"risk {lots * lpl:.2f} {acct['currency']} = {lots * lpl / basis * 100:.2f}%"

    def _reject(self, msg_id, why, j):
        log.warning("SKIPPED signal %s: %s", msg_id, why)
        self.journal.write("skipped", details=why, **j)
        self.state.data.setdefault("skipped", {})[msg_id] = time.time()

    # ------------------------------------------------------------ re-enter
    async def _reenter(self, reply_to, msg_id):
        """'نرجع ندخل بنفس المعطيات' / 'بنكرر' - place the same signal again."""
        cands = [s for s in self.state.signals.values() if s.get("sig")]
        if reply_to is not None and str(reply_to) in self.state.signals:
            base = self.state.signals[str(reply_to)]
            base = self.state.signals.get(base.get("origin", base["msg_id"]), base)
        elif cands:
            base = max(cands, key=lambda x: x["created"])
            base = self.state.signals.get(base.get("origin", base["msg_id"]), base)
        else:
            return log.info("Re-enter (msg %s) but no earlier signal is known", msg_id)
        origin = base.get("origin", base["msg_id"])
        family = [s for s in self.state.signals.values() if s.get("origin", s["msg_id"]) == origin]
        if any(s["status"] in ACTIVE for s in family):
            return log.info("Re-enter (msg %s): signal %s still has open trades/orders - ignored", msg_id, origin)
        if time.time() - base["created"] > float(self.c["commands"]["reenter_max_hours"]) * 3600:
            return log.info("Re-enter (msg %s): signal %s is too old - ignored", msg_id, origin)
        sig = Signal(**base["sig"])
        sig.origin = origin
        new_id = f"{origin}-r{len(family)}"
        log.info("RE-ENTER signal %s as %s (msg %s)", origin, new_id, msg_id)
        await self._open(new_id, sig)

    # ------------------------------------------------------------ edits
    async def _apply_edit(self, s, sig: Signal):
        if s.get("draft"):
            return await self._finish_draft(s, sig)
        if s["status"] not in ACTIVE or sig.direction != s["direction"]:
            return
        if sig.tps == s["tps"] and sig.sl == s["signal_sl"]:
            return
        new_sl, new_tp = self._r(sig.sl), self._r(sig.last_tp)
        log.info("Signal %s was edited: SL %s TP %s", s["msg_id"], new_sl, new_tp)
        s["tps"], s["signal_sl"] = sig.tps, sig.sl
        positions, orders = await self.b.positions(self.sym), await self.b.orders(self.sym)
        bid, ask = await self.b.price(self.sym)
        for leg in self._legs_active(s):
            pos, order = self._find(leg, positions, orders)
            leg_tp = self._r(sig.tps[0]) if leg.get("tp_mode") == "tp1" else new_tp
            if leg.get("tp_mode") == "entry" and pos:
                leg_tp = pos["tp"]                                  # keep "exit at entry"
            same = lambda x, y: abs(float(x or 0) - float(y or 0)) < 1e-6
            if pos:
                sl = new_sl if leg["stage"] == 0 else pos["sl"]     # never undo protection
                if same(sl, pos["sl"]) and same(leg_tp, pos["tp"]):
                    continue                                        # nothing to change for this entry
                if self._sl_valid(s["direction"], sl, bid, ask):
                    await self.b.modify_position(pos["ticket"], self.sym, sl, leg_tp)
            elif order:
                if same(new_sl, order["sl"]) and same(leg_tp, order["tp"]):
                    continue
                await self.b.modify_order(order["ticket"], self.sym, order["price"], new_sl, leg_tp)
            else:
                continue
            log.info("Signal %s leg %d updated after edit: SL %s TP %s", s["msg_id"], leg["n"],
                     sl if pos else new_sl, leg_tp)
            self.journal.write("edited", msg_id=s["msg_id"], leg=leg["n"], sl=new_sl, tp=leg_tp,
                               ticket=leg["ticket"])

    # ------------------------------------------------------------ commands
    async def _command(self, cmd: Command, s: Dict, msg_id):
        tag = f"{cmd.action} (msg {msg_id}) on signal {s['msg_id']}"
        if s["status"] == "dry" or not self.trading_enabled:
            log.info("[DRY RUN] would apply %s", tag)
            self.journal.write("dry_run_cmd", msg_id=s["msg_id"], details=tag)
            return
        positions, orders = await self.b.positions(self.sym), await self.b.orders(self.sym)
        bid, ask = await self.b.price(self.sym)
        if cmd.action == "close_part" and not self.c["commands"]["obey_partial_close"]:
            return log.info("%s: 'close part' message noted; the bot's own partial closes at each TP apply "
                            "(set commands.obey_partial_close: true to also close half here)", tag)
        if cmd.action == "new_tp":
            buy = s["direction"] == "BUY"
            ref = s["entry_high"] if buy else s["entry_low"]
            if ref is not None and ((buy and cmd.value <= ref) or (not buy and cmd.value >= ref)):
                return log.warning("%s: new TP %s is on the wrong side of the entry - ignored", tag, cmd.value)
            s["tps"] = [x for x in s["tps"] if (x < cmd.value if buy else x > cmd.value)] + [cmd.value]
        legs = self._legs_active(s)
        if cmd.action in ("cancel_leg", "close_leg") and not cmd.all_trades:
            legs = self._pick_legs(s, legs, cmd)
            if not legs:
                return log.info("%s: no matching entry", tag)
        for leg in legs:
            pos, order = self._find(leg, positions, orders)
            ltag = f"{tag} leg {leg['n']}"
            try:
                await self._command_leg(cmd, s, leg, pos, order, bid, ask, ltag)
            except BrokerError as e:
                log.error("%s failed: %s", ltag, e)

    @staticmethod
    def _pick_legs(s, legs, cmd):
        if cmd.value is not None:
            near = sorted(legs, key=lambda l: abs(l["entry"] - cmd.value))
            return near[:1] if near and abs(near[0]["entry"] - cmd.value) <= 3 else []
        if cmd.leg:
            return [l for l in legs if l["n"] == cmd.leg]
        return legs

    async def _command_leg(self, cmd, s, leg, pos, order, bid, ask, tag):
        buy = s["direction"] == "BUY"
        if cmd.action == "close_part":
            cmd = Command("close_half")
        if cmd.action == "cancel_leg":
            cmd = Command("cancel")
        if cmd.action == "close_leg":
            if order:                                   # not filled yet -> just remove it
                await self.b.cancel_order(order["ticket"])
                leg["status"] = "closed"
                log.info("%s -> pending order cancelled", tag)
                self.journal.write("cancelled", msg_id=s["msg_id"], leg=leg["n"], ticket=order["ticket"])
                return
            if not pos:
                return
            in_profit = (bid >= pos["open_price"]) if buy else (ask <= pos["open_price"])
            if cmd.at_entry and not in_profit:
                # exit at the entry price when price comes back: TP = entry
                tp = self._r(pos["open_price"])
                await self.b.modify_position(pos["ticket"], self.sym, pos["sl"], tp)
                leg["tp_mode"] = "entry"
                log.info("%s -> will close at its entry %s (TP set to entry)", tag, tp)
                self.journal.write("close_at_entry", msg_id=s["msg_id"], leg=leg["n"], tp=tp, ticket=pos["ticket"])
                return
            cmd = Command("close")
        if cmd.action == "new_tp":
            if leg.get("tp_mode") not in (None, "last"):
                return
            tp = self._r(cmd.value)
            if pos:
                await self.b.modify_position(pos["ticket"], self.sym, pos["sl"], tp)
            elif order:
                await self.b.modify_order(order["ticket"], self.sym, order["price"], order["sl"], tp)
            log.info("%s -> last TP changed to %s", tag, tp)
            self.journal.write("new_tp", msg_id=s["msg_id"], leg=leg["n"], tp=tp, ticket=leg["ticket"])
            return
        if cmd.action in ("breakeven", "move_sl"):
            if pos:
                new = (self._sl_for_stage(s, pos["open_price"], 1) if cmd.action == "breakeven"
                       else cmd.value)
                if not self._sl_valid(s["direction"], new, bid, ask):
                    log.warning("%s: SL %s is not valid at current price %s/%s", tag, new, bid, ask)
                    return
                await self.b.modify_position(pos["ticket"], self.sym, self._r(new), pos["tp"])
                leg["stage"] = max(leg["stage"], 1)
                log.info("%s -> SL %s", tag, self._r(new))
                self.journal.write(cmd.action, msg_id=s["msg_id"], leg=leg["n"], sl=self._r(new),
                                   ticket=pos["ticket"])
            elif order and cmd.action == "move_sl":
                await self.b.modify_order(order["ticket"], self.sym, order["price"], self._r(cmd.value), order["tp"])
                self.journal.write("move_sl", msg_id=s["msg_id"], leg=leg["n"], sl=cmd.value,
                                   ticket=order["ticket"])

        elif cmd.action == "close":
            if pos:
                await self.b.close_position(pos["ticket"], self.sym)
                log.info("%s -> closed", tag)
                self.journal.write("closed_by_group", msg_id=s["msg_id"], leg=leg["n"], ticket=pos["ticket"])
            if order:
                await self.b.cancel_order(order["ticket"])
                log.info("%s -> pending order cancelled", tag)
                self.journal.write("cancelled", msg_id=s["msg_id"], leg=leg["n"], ticket=order["ticket"])
            leg["status"] = "closed"

        elif cmd.action == "close_half":
            if pos:
                step, vmin = self.info.get("vol_step", 0.01), self.info.get("vol_min", 0.01)
                half = math.floor(pos["volume"] / 2 / step + 1e-9) * step
                if half < vmin or pos["volume"] - half < vmin - 1e-9:
                    log.warning("%s: volume %.2f too small to split, kept open", tag, pos["volume"])
                    return
                await self.b.close_position(pos["ticket"], self.sym, round(half, 8))
                log.info("%s -> closed %.2f lots", tag, half)
                self.journal.write("closed_half", msg_id=s["msg_id"], leg=leg["n"], volume=half,
                                   ticket=pos["ticket"])

        elif cmd.action == "cancel":
            if order:
                await self.b.cancel_order(order["ticket"])
                leg["status"] = "closed"
                log.info("%s -> pending order cancelled", tag)
                self.journal.write("cancelled", msg_id=s["msg_id"], leg=leg["n"], ticket=order["ticket"])
            elif pos:
                log.info("%s: already open, 'cancel' ignored (send 'close' to exit)", tag)

    # ------------------------------------------------------------ monitoring
    @staticmethod
    def _find(leg, positions, orders):
        t = str(leg["ticket"])
        pos = next((p for p in positions if t in (p["ticket"], p["identifier"])), None)
        order = next((o for o in orders if o["ticket"] == t), None)
        return pos, order

    async def monitor_loop(self):
        while True:
            try:
                async with self.lock:
                    await self.monitor_once()
                    self.state.save()
            except BrokerError as e:
                log.error("Monitor broker error: %s", e)
            except Exception:
                log.exception("Monitor error")
            await asyncio.sleep(float(self.c["poll_seconds"]))

    async def monitor_once(self):
        active = [s for s in self.state.active() if self._legs_active(s)]
        if not active:
            return
        positions = await self.b.positions(self.sym)
        orders = await self.b.orders(self.sym)
        bid, ask = await self.b.price(self.sym)
        for s in active:
            for leg in self._legs_active(s):
                pos, order = self._find(leg, positions, orders)
                try:
                    if pos:
                        leg["missing"] = 0
                        if leg["status"] == "pending":
                            leg["status"] = "open"
                            log.info("Signal %s leg %d FILLED at %s", s["msg_id"], leg["n"], pos["open_price"])
                            self.journal.write("filled", msg_id=s["msg_id"], leg=leg["n"],
                                               entry=pos["open_price"], ticket=leg["ticket"])
                        await self._partials(s, leg, pos, bid, ask)
                        await self._protect(s, leg, pos, bid, ask)
                    elif order:
                        leg["missing"] = 0
                        await self._manage_pending(s, leg, order, bid, ask)
                    else:
                        leg["missing"] += 1
                        if leg["missing"] >= 3:
                            leg["status"] = "closed"
                            log.info("Signal %s leg %d closed (TP/SL hit or closed manually)",
                                     s["msg_id"], leg["n"])
                            self.journal.write("closed", msg_id=s["msg_id"], leg=leg["n"], ticket=leg["ticket"])
                except BrokerError as e:
                    log.error("Signal %s leg %d: %s", s["msg_id"], leg["n"], e)
            _refresh(s)

    # ------------------------------------------------------------ partial closes
    def _step(self):
        return self.info.get("vol_step", 0.01) or 0.01

    def _splittable(self, lots):
        return lots >= self.info.get("vol_min", 0.01) + self._step() - 1e-9

    @staticmethod
    def _split_units(units, m):
        """Share `units` lot-steps over the m remaining TPs as equally as possible.
        Extra steps go to the earliest TPs; the last TP always keeps at least one."""
        if m <= 1:
            return [units]
        if units < m:
            return [1] * (units - 1) + [0] * (m - units) + [1]
        base, extra = divmod(units, m)
        return [base + (1 if k < extra else 0) for k in range(m)]

    def _plan_text(self, lots, tps, tp1_only):
        if lots <= 0:
            return ""
        if tp1_only:
            return "too small to split -> closes fully at TP1"
        if not self.c["partial_close"]["enabled"] or len(tps) < 2:
            return "runs to last TP"
        step = self._step()
        parts = self._split_units(int(round(lots / step)), len(tps))
        return "closes " + ", ".join(f"{p * step:.2f}@TP{k + 1}" for k, p in enumerate(parts) if p)

    async def _partials(self, s, leg, pos, bid, ask):
        """At each TP except the last, close an equal share of what is left."""
        tps = s["tps"]
        if (not self.c["partial_close"]["enabled"] or leg.get("tp_mode") in ("tp1", "entry")
                or len(tps) < 2):
            return
        buy = s["direction"] == "BUY"
        px = bid if buy else ask
        leg["best"] = px if leg.get("best") is None else (max(leg["best"], px) if buy else min(leg["best"], px))
        step, vmin = self._step(), self.info.get("vol_min", 0.01)
        volume = pos["volume"]
        while leg["tp_done"] < len(tps) - 1:
            k = leg["tp_done"]                      # TP index to process (0 = TP1)
            if not ((buy and leg["best"] >= tps[k]) or (not buy and leg["best"] <= tps[k])):
                break
            units = int(round(volume / step))
            share = self._split_units(units, len(tps) - k)[0]
            close = round(share * step, 8)
            if close <= 0 or volume - close < vmin - 1e-9:
                log.info("Signal %s leg %d: TP%d reached, nothing to close (%.2f lots left run on)",
                         s["msg_id"], leg["n"], k + 1, volume)
                leg["tp_done"] = k + 1
                continue
            try:
                await self.b.close_position(pos["ticket"], self.sym, close)
            except BrokerError as e:
                leg["pc_fail"] = leg.get("pc_fail", 0) + 1
                log.warning("Signal %s leg %d: partial close at TP%d failed: %s", s["msg_id"], leg["n"], k + 1, e)
                if leg["pc_fail"] >= 5:
                    leg["tp_done"] = k + 1
                    leg["pc_fail"] = 0
                break
            volume = round(volume - close, 8)
            leg["tp_done"] = k + 1
            leg["pc_fail"] = 0
            log.info("PARTIAL signal %s leg %d: TP%d reached, closed %.2f lots, %.2f lots left",
                     s["msg_id"], leg["n"], k + 1, close, volume)
            self.journal.write("partial_close", msg_id=s["msg_id"], leg=leg["n"], volume=close,
                               tp=tps[k], ticket=pos["ticket"], details=f"TP{k + 1}, {volume:.2f} left")

    async def _protect(self, s, leg, pos, bid, ask):
        be = self.c["breakeven"]
        if not be["enabled"] or not s["tps"]:
            return
        buy = s["direction"] == "BUY"
        px = bid if buy else ask
        entry = pos["open_price"]
        levels = self._trigger_levels(s, entry)
        max_stage = len(levels) if be["ladder"] else min(1, len(levels))
        stage = leg["stage"]
        while stage < max_stage:
            lvl = levels[stage]
            if (buy and px >= lvl) or (not buy and px <= lvl):
                stage += 1
            else:
                break
        if stage > leg["stage"]:
            log.info("Signal %s leg %d reached protection stage %d (price %s)", s["msg_id"], leg["n"], stage, px)
            leg["stage"] = stage
        want = self._sl_for_stage(s, entry, leg["stage"])
        if want is None or not self._better(s["direction"], want, pos["sl"]):
            return
        if not self._sl_valid(s["direction"], want, bid, ask):
            return          # price pulled back too close; retry on the next tick
        if leg.get("sl_fail", 0) >= 10:
            return
        try:
            await self.b.modify_position(pos["ticket"], self.sym, self._r(want), pos["tp"])
            leg["sl_fail"] = 0
            what = "entry (break-even)" if leg["stage"] == 1 else f"TP{leg['stage'] - 1}"
            log.info("PROTECT signal %s leg %d: SL moved to %s = %s", s["msg_id"], leg["n"], what, self._r(want))
            self.journal.write("sl_moved", msg_id=s["msg_id"], leg=leg["n"], sl=self._r(want),
                               ticket=pos["ticket"], details=f"stage {leg['stage']}: SL to {what}")
        except BrokerError as e:
            leg["sl_fail"] = leg.get("sl_fail", 0) + 1
            log.warning("Could not move SL for signal %s leg %d: %s", s["msg_id"], leg["n"], e)

    async def _manage_pending(self, s, leg, order, bid, ask):
        age_min = (time.time() - leg["created"]) / 60
        buy = s["direction"] == "BUY"
        why = None
        if age_min > float(self.c["pending_expiry_minutes"]):
            why = f"not filled after {self.c['pending_expiry_minutes']} min"
        elif s.get("draft") or not s["tps"]:
            if age_min > float(self.c["early_entry"]["complete_minutes"]):
                why = (f"the admin did not finish the signal (SL/TP) within "
                       f"{self.c['early_entry']['complete_minutes']} min")
        else:
            tp1 = s["tps"][0]
            # A waiting limit order is only "missed" if price actually came to the entry (within
            # entry_tolerance) and then ran to TP1 without filling it, or if another entry of the
            # same signal is already in the trade. Price wobbling up to TP1 from just above the
            # entry is not a missed move - the order keeps waiting for the pullback.
            tol = float(self.c["entry_tolerance"])
            if (buy and ask <= order["price"] + tol) or (not buy and bid >= order["price"] - tol):
                leg["touched"] = True
            past_tp1 = (buy and bid >= tp1) or (not buy and ask <= tp1)
            past_last = (buy and bid >= s["tps"][-1]) or (not buy and ask <= s["tps"][-1])
            started = any(l["status"] == "open" for l in s["legs"])
            if self.c["cancel_pending_if_tp1_hit"] and past_tp1 and (started or leg.get("touched")):
                why = ("TP1 reached while another entry of this signal is in the trade" if started
                       else "price came to the entry and ran to TP1 without filling it")
            elif past_last:
                why = "price reached the last TP without this entry filling - the move is over"
        if why:
            await self.b.cancel_order(order["ticket"])
            leg["status"] = "closed"
            log.info("Cancelled pending leg %d of signal %s: %s", leg["n"], s["msg_id"], why)
            self.journal.write("cancelled", msg_id=s["msg_id"], leg=leg["n"], ticket=order["ticket"], details=why)
