"""
Offline self-test: runs the parser and the whole trade engine against a fake
broker (no Telegram, no MT5 needed).   Run:  python selftest.py
"""
import asyncio
import os
import tempfile

from brokers import BaseBroker
from engine import Engine
from signal_parser import parse_message

FAILS = []


def check(name, cond):
    print(("  PASS " if cond else "  FAIL ") + name)
    if not cond:
        FAILS.append(name)


class FakeBroker(BaseBroker):
    """XAUUSD, 100 oz contract, USD account, demo."""
    def __init__(self, bid=2650.0, spread=0.3, balance=10000):
        self.bid, self.spread, self.balance = bid, spread, balance
        self.pos, self.ord, self.n = {}, {}, 1000

    @property
    def ask(self):
        return self.bid + self.spread

    async def connect(self): pass
    async def account_info(self):
        return dict(balance=self.balance, equity=self.balance, currency="USD", is_demo=True, login=1, server="fake")
    async def symbol_info(self, s):
        return dict(digits=2, point=0.01, vol_min=0.01, vol_max=100, vol_step=0.01, stops_level=0.1)
    async def price(self, s): return self.bid, self.ask
    async def loss_per_lot(self, s, d, e, sl): return abs(e - sl) * 100

    async def place(self, s, d, kind, vol, price, sl, tp, comment):
        self.n += 1
        t = str(self.n)
        if kind == "market":
            self.pos[t] = dict(ticket=t, identifier=t, direction=d, volume=vol,
                               open_price=self.ask if d == "BUY" else self.bid, sl=sl, tp=tp)
        else:
            self.ord[t] = dict(ticket=t, direction=d, kind=kind, price=price, sl=sl, tp=tp, volume=vol)
        return t

    async def positions(self, s): return [dict(p) for p in self.pos.values()]
    async def orders(self, s): return [dict(o) for o in self.ord.values()]
    async def modify_position(self, t, s, sl, tp): self.pos[t].update(sl=sl, tp=tp)
    async def modify_order(self, t, s, price, sl, tp): self.ord[t].update(price=price, sl=sl, tp=tp)
    async def close_position(self, t, s, volume=None):
        if volume: self.pos[t]["volume"] = round(self.pos[t]["volume"] - volume, 2)
        else: self.pos.pop(t, None)
    async def cancel_order(self, t): self.ord.pop(t, None)

    def tick(self, bid):
        """move price, fill limit orders, hit SL/TP"""
        self.bid = bid
        for t, o in list(self.ord.items()):
            buy = o["direction"] == "BUY"
            hit = (self.ask <= o["price"]) if (buy and o["kind"] == "limit") else \
                  (self.bid >= o["price"]) if (not buy and o["kind"] == "limit") else False
            if hit:
                self.pos[t] = dict(ticket=t, identifier=t, direction=o["direction"], volume=o["volume"],
                                   open_price=o["price"], sl=o["sl"], tp=o["tp"])
                del self.ord[t]
        for t, p in list(self.pos.items()):
            if p["direction"] == "BUY" and (self.bid <= p["sl"] or self.bid >= p["tp"]):
                del self.pos[t]
            if p["direction"] == "SELL" and (self.ask >= p["sl"] or self.ask <= p["tp"]):
                del self.pos[t]


def new_engine(b, **over):
    d = tempfile.mkdtemp()
    cfg = dict(dry_run=False, risk_percent=1.0)
    cfg.update(over)
    return Engine(cfg, b, os.path.join(d, "s.json"), os.path.join(d, "j.csv"))


async def run():
    print("Parser")
    r = parse_message("XAUUSD BUY NOW 2650\nSL 2640\nTP1 2655\nTP2 2660\nTP3 2670")
    check("buy signal parsed", r.signal and r.signal.sl == 2640 and r.signal.tps == [2655, 2660, 2670])
    check("chat message ignored", not parse_message("be careful, close to news").signal
          and not parse_message("be careful, close to news").commands)

    check("'close 50%' is a half close, not a full close",
          [c.action for c in parse_message("close 50%").commands] == ["close_half"])

    print("Market buy, risk sizing, TP = last TP, SL ladder")
    b = FakeBroker(bid=2650.0)
    e = new_engine(b); await e.start()
    await e.on_message(1, "XAUUSD BUY NOW 2650\nSL 2640\nTP1 2655\nTP2 2660\nTP3 2670")
    p = list(b.pos.values())[0]
    # risk 1% of 10000 = 100 USD; entry 2650.3 - SL 2640 = 10.3 * 100 = 1030/lot -> 0.09 lots
    check("lot size 0.09 for 1% risk", abs(p["volume"] - 0.09) < 1e-9)
    check("TP is last TP 2670", p["tp"] == 2670)
    check("SL is 2640", p["sl"] == 2640)
    b.tick(2655.5); await e.monitor_once()
    check("TP1 hit -> SL moved to entry + offset", abs(b.pos[p["ticket"]]["sl"] - (p["open_price"] + 0.3)) < 1e-6)
    b.tick(2661); await e.monitor_once()
    check("TP2 hit -> SL moved to TP1", b.pos[p["ticket"]]["sl"] == 2655)
    b.tick(2656); await e.monitor_once()
    check("price pulls back -> SL never moves down", b.pos[p["ticket"]]["sl"] == 2655)
    b.tick(2654.9)
    for _ in range(3): await e.monitor_once()
    check("stopped out at TP1 level -> trade marked closed", e.state.signals["1"]["status"] == "closed")

    print("Sell zone -> two entries, fill, follow-up commands")
    b = FakeBroker(bid=2660.0)
    e = new_engine(b); await e.start()
    await e.on_message(2, "GOLD SELL @ 2665-2668\nSL: 2675\nTP: 2660\nTP: 2650")
    prices = sorted(o["price"] for o in b.ord.values())
    check("two sell limits at 2665 and 2668", prices == [2665, 2668])
    b.tick(2668.2); await e.monitor_once()
    check("both legs filled -> open", all(l["status"] == "open" for l in e.state.signals["2"]["legs"]))
    t1, t2 = [l["ticket"] for l in e.state.signals["2"]["legs"]]
    await e.on_message(3, "Move SL to 2672", reply_to=2)
    check("'Move SL to 2672' applied to both legs", b.pos[t1]["sl"] == 2672 and b.pos[t2]["sl"] == 2672)
    before = b.pos[t1]["volume"]
    await e.on_message(4, "close half")
    check(f"'close half' closed 50% of each leg ({before} -> {b.pos[t1]['volume']})",
          0 < b.pos[t1]["volume"] < before)
    await e.on_message(5, "Close all now")
    check("'close all now' closed both legs", not b.pos and e.state.signals["2"]["status"] == "closed")

    print("Safety checks")
    b = FakeBroker(bid=2650.0)
    e = new_engine(b); await e.start()
    await e.on_message(6, "BUY GOLD 2800 SL 2790 TP 2810")
    check("typo/far price rejected", not b.pos and not b.ord)
    await e.on_message(7, "BUY GOLD 2640 SL 2630 TP1 2645 TP2 2650")
    check("price already past TP1 rejected", not b.pos and not b.ord)
    b.spread = 2.0
    await e.on_message(8, "BUY GOLD NOW 2650 SL 2640 TP 2660")
    check("wide spread rejected", not b.pos)
    b.spread = 0.3
    await e.on_message(8, "BUY GOLD NOW 2650 SL 2640 TP 2660")
    check("same message id not traded twice", not b.pos)
    await e.on_message(9, "BUY GOLD NOW 2650 SL 2640 TP 2660")
    t = e.state.signals["9"]["legs"][0]["ticket"]
    b.tick(2655.5); await e.monitor_once()
    check("single TP: SL to entry at 50% of the way", b.pos[t]["sl"] > 2650)
    await e.on_message(9, "BUY GOLD NOW 2650 SL 2640 TP 2665", edited=True)
    check("edited signal -> new TP applied, protection kept", b.pos[t]["tp"] == 2665 and b.pos[t]["sl"] > 2650)

    print("Dry run & live guard")
    b = FakeBroker(bid=2650.0)
    e = new_engine(b, dry_run=True); await e.start()
    await e.on_message(10, "BUY GOLD NOW 2650 SL 2640 TP 2660")
    check("dry run places nothing", not b.pos and not b.ord)
    class Live(FakeBroker):
        async def account_info(self):
            a = await super().account_info(); a["is_demo"] = False; return a
    b = Live(bid=2650.0)
    e = new_engine(b); await e.start()
    await e.on_message(11, "BUY GOLD NOW 2650 SL 2640 TP 2660")
    check("live account blocked while allow_live=false", not b.pos)

    print("Small balance")
    b = FakeBroker(bid=2650.0, balance=300)
    e = new_engine(b); await e.start()
    await e.on_message(12, "BUY GOLD NOW 2650 SL 2640 TP 2660")
    check("min lot refused when it would risk > 2%", not b.pos)
    b = FakeBroker(bid=2650.0, balance=600)
    e = new_engine(b); await e.start()
    await e.on_message(13, "BUY GOLD NOW 2650 SL 2645 TP 2660")
    check("min lot 0.01 used when risk stays <= 2%", b.pos and list(b.pos.values())[0]["volume"] == 0.01)

    print("Real signals from the group")
    G1 = "Gold sell limit 4020\nAnd 4024\n\nSl 4028\n\nTp 4018\nTp 4016\nTp 4014\nTp 4010"
    G2 = "SELL 3909 - 3911.5\nSL 3915\n\nTP 3907\nTP 3905\nTP 3902\nTP 3898"
    G3 = "GOLD NUY 3910 - 3908\n\nSL 3907\n\nTP 3912\nTP 3914\nTP 3917\nTP 3920"
    G4 = "Buy gold 3898 & 3895\n\nSL 3892\n\nTP 3900\nTP 3902\nTP 3905\nTP 3908"
    G5 = "GOLD BUY NOW 3906.50\n\nSL:3903\n\nTP:3908\n\nTP:3910\n\nTP:3912\n\nTP:3914\n\nTP:3916"
    s = parse_message(G1).signal
    check("G1 sell limit 4020/4024, SL 4028, last TP 4010",
          s and s.direction == "SELL" and (s.entry_low, s.entry_high) == (4020, 4024) and s.sl == 4028 and s.last_tp == 4010)
    s = parse_message(G2).signal
    check("G2 sell 3909-3911.5, SL 3915, last TP 3898", s and s.entry_high == 3911.5 and s.last_tp == 3898)
    s = parse_message(G3).signal
    check("G3 typo 'NUY' read as BUY 3908-3910, SL 3907, last TP 3920",
          s and s.direction == "BUY" and (s.entry_low, s.entry_high) == (3908, 3910) and s.last_tp == 3920)
    s = parse_message(G4).signal
    check("G4 buy 3895 & 3898, SL 3892, last TP 3908", s and (s.entry_low, s.entry_high) == (3895, 3898) and s.last_tp == 3908)
    s = parse_message(G5).signal
    check("G5 buy now, SL 3903, 5 TPs, last 3916", s and s.market and s.sl == 3903 and len(s.tps) == 5 and s.last_tp == 3916)

    def legs(e, mid):
        return e.state.signals[str(mid)]["legs"]

    # G1 at 4019: sell limit 4020 AND sell limit 4024, 0.5% risk each
    b = FakeBroker(bid=4019.0)
    e = new_engine(b); await e.start()
    await e.on_message(101, G1)
    o = sorted(b.ord.values(), key=lambda x: x["price"])
    check("G1 @4019: two sell limits at 4020 and 4024, TP 4010",
          [x["price"] for x in o] == [4020, 4024] and all(x["tp"] == 4010 and x["sl"] == 4028 for x in o))
    # leg1: 50 USD / (8 * 100) = 0.0625 -> 0.06 ; leg2: 50 / (4 * 100) = 0.125 -> 0.12
    check(f"G1 lots split 0.5% each: 4020 -> {o[0]['volume']}, 4024 -> {o[1]['volume']}",
          abs(o[0]["volume"] - 0.06) < 1e-9 and abs(o[1]["volume"] - 0.12) < 1e-9)
    b.tick(4020.0); await e.monitor_once()
    t1, t2 = legs(e, 101)[0]["ticket"], legs(e, 101)[1]["ticket"]
    check("G1 price 4020 -> first entry filled, second still waiting", t1 in b.pos and t2 in b.ord)
    b.tick(4017.7); await e.monitor_once()
    check("G1 TP1 4018 hit -> SL to entry 4019.70", abs(b.pos[t1]["sl"] - 4019.7) < 1e-6)
    check("G1 TP1 hit -> unfilled 4024 entry cancelled", t2 not in b.ord and legs(e, 101)[1]["status"] == "closed")
    b.tick(4015.7); await e.monitor_once()
    check("G1 TP2 4016 hit -> SL to TP1 4018", b.pos[t1]["sl"] == 4018)

    # G2 at 3910: sell 3909 now (reached) + sell limit 3911.5; both fill, each protected at own entry
    b = FakeBroker(bid=3910.0)
    e = new_engine(b); await e.start()
    await e.on_message(102, G2)
    L = legs(e, 102)
    check("G2 @3910: 3909 entry at market, 3911.5 as sell limit",
          L[0]["kind"] == "market" and L[1]["kind"] == "limit" and L[1]["entry"] == 3911.5)
    b.tick(3911.6); await e.monitor_once()
    check("G2 price 3911.6 -> second entry filled", all(l["status"] == "open" for l in L))
    b.tick(3906.7); await e.monitor_once()
    check("G2 TP1 hit -> leg1 SL 3909.70, leg2 SL 3911.20",
          abs(b.pos[L[0]["ticket"]]["sl"] - 3909.7) < 1e-6 and abs(b.pos[L[1]["ticket"]]["sl"] - 3911.2) < 1e-6)
    check("G2 both legs TP = 3898", all(b.pos[l["ticket"]]["tp"] == 3898 for l in L))

    # G3 ("NUY") at 3909: buy 3910 now (reached), buy limit 3908
    b = FakeBroker(bid=3909.0)
    e = new_engine(b); await e.start()
    await e.on_message(103, G3)
    L = legs(e, 103)
    check("G3 @3909: buy at market + buy limit 3908, SL 3907, TP 3920",
          L[0]["kind"] == "market" and L[1]["kind"] == "limit" and L[1]["entry"] == 3908
          and all(x["sl"] == 3907 and x["tp"] == 3920 for x in list(b.pos.values()) + list(b.ord.values())))

    # G4 at 3899: price above both -> buy limits at 3898 and 3895
    b = FakeBroker(bid=3899.0)
    e = new_engine(b); await e.start()
    await e.on_message(104, G4)
    check("G4 @3899: buy limits at 3898 and 3895",
          sorted(o["price"] for o in b.ord.values()) == [3895, 3898] and not b.pos)

    # G5 single entry "BUY NOW" -> one market order, full 1% risk
    b = FakeBroker(bid=3906.5)
    e = new_engine(b); await e.start()
    await e.on_message(105, G5)
    p = list(b.pos.values())
    # 100 / ((3906.8 - 3903) * 100) = 0.263 -> 0.26
    check("G5 single entry: one market buy 0.26 lots (full 1%)", len(p) == 1 and abs(p[0]["volume"] - 0.26) < 1e-9)
    b = FakeBroker(bid=3908.2)
    e = new_engine(b); await e.start()
    await e.on_message(106, G5)
    check("G5 skipped when price already past TP1", not b.pos and not b.ord)

    # split_risk off -> each entry gets the full 1%
    b = FakeBroker(bid=4019.0)
    e = new_engine(b, split_risk=False); await e.start()
    await e.on_message(107, G1)
    o = sorted(b.ord.values(), key=lambda x: x["price"])
    check("split_risk false -> 1% on each entry (0.12 and 0.25 lots)",
          abs(o[0]["volume"] - 0.12) < 1e-9 and abs(o[1]["volume"] - 0.25) < 1e-9)

    print("Partial closes at each TP")
    # G5 with 10,000 balance -> 0.26 lots, 5 TPs -> 0.06 / 0.05 / 0.05 / 0.05, last 0.05 runs to TP5
    b = FakeBroker(bid=3906.5)
    e = new_engine(b); await e.start()
    await e.on_message(201, G5)
    t = legs(e, 201)[0]["ticket"]
    vols = []
    for price in (3908.1, 3910.1, 3912.1, 3914.1):
        b.tick(price); await e.monitor_once(); vols.append(b.pos[t]["volume"])
    check(f"0.26 lots, 5 TPs: left after TP1-TP4 = {vols}", vols == [0.2, 0.15, 0.1, 0.05])
    check("runner keeps TP 3916, SL ladder at TP3 after TP4", b.pos[t]["tp"] == 3916 and b.pos[t]["sl"] == 3912)
    b.tick(3913.0); await e.monitor_once()
    check("price dips back -> no extra partial close", b.pos[t]["volume"] == 0.05)

    # price jumps straight from entry past TP2 -> both TP1 and TP2 shares closed at once
    b = FakeBroker(bid=3906.5)
    e = new_engine(b); await e.start()
    await e.on_message(202, G5)
    t = legs(e, 202)[0]["ticket"]
    b.tick(3910.2); await e.monitor_once()
    check(f"jump past TP1+TP2 -> 0.11 closed, {b.pos[t]['volume']} left", b.pos[t]["volume"] == 0.15)

    # 0.02 lots, 4 TPs -> 0.01 at TP1, last 0.01 runs to TP4
    S4 = "BUY GOLD NOW 2650 SL 2640 TP1 2652 TP2 2654 TP3 2656 TP4 2660"
    b = FakeBroker(bid=2650.0, balance=2100)
    e = new_engine(b); await e.start()
    await e.on_message(203, S4)
    t = legs(e, 203)[0]["ticket"]
    check("0.02 lots opened with TP4 2660", b.pos[t]["volume"] == 0.02 and b.pos[t]["tp"] == 2660)
    b.tick(2652.1); await e.monitor_once()
    check("TP1 -> 0.01 closed, 0.01 left", b.pos[t]["volume"] == 0.01)
    b.tick(2654.1); await e.monitor_once()
    check("TP2 -> nothing more to close, runner 0.01 stays", b.pos[t]["volume"] == 0.01 and b.pos[t]["sl"] == 2652)

    # 0.01 lot -> TP set to TP1, closes fully there
    b = FakeBroker(bid=2650.0, balance=1050)
    e = new_engine(b); await e.start()
    await e.on_message(204, S4)
    p = list(b.pos.values())
    check("0.01 lot -> TP set to TP1 2652", len(p) == 1 and p[0]["volume"] == 0.01 and p[0]["tp"] == 2652)
    b.tick(2652.1)
    for _ in range(3): await e.monitor_once()
    check("0.01 lot closed fully at TP1", not b.pos and legs(e, 204)[0]["status"] == "closed")

    # two entries: each leg does its own partials
    b = FakeBroker(bid=3910.0)
    e = new_engine(b); await e.start()
    await e.on_message(205, G2)
    b.tick(3911.6); await e.monitor_once()
    L = legs(e, 205)
    v0 = [b.pos[l["ticket"]]["volume"] for l in L]
    b.tick(3906.7); await e.monitor_once()
    v1 = [b.pos[l["ticket"]]["volume"] for l in L]
    check(f"two entries {v0} -> after TP1 {v1}: each closed ~1/4", all(a > c > 0 for a, c in zip(v0, v1)))

    print("Real group messages (Arabic + English) - parser")
    def acts(text, reply=False):
        r = parse_message(text, is_reply=reply)
        return [c.action for c in r.commands] if not r.signal else ["SIGNAL"]
    cases = [
        ("نامن الصفقة", ["breakeven"]), ("التامين اجباري الحين", ["breakeven"]),
        ("لا ننسى التأمين شباب", ["breakeven"]), ("هدفف اوول نأمن", ["breakeven"]),
        ("شباب التامين يكون عقب الهدف الاول ممنوع التامين قبل الهدف", []),
        ("شباب ما بنأمن بنواصل بدون تأمين", []), ("ضرب تامين", []),
        ("عطانا هدفين و رجع تامين", []), ("خذينا 50 نقطة وطلعنا تامين", []),
        ("اللي حاب يامن يقدر يامن", []), ("اذا وصل 4033 راح نامن دخولنا", []),
        ("امن و احذف التعزيز", ["cancel_leg", "breakeven"]),
        ("تأمين + حذف التعزيز", ["cancel_leg", "breakeven"]),
        ("نلغي الأمر", ["cancel"]), ("إلغاء الأمر طار", ["cancel"]), ("نمسح الأمر جاب الهدف", ["cancel"]),
        ("خلاص ماراح نحذف الامر مره ثانيه", []),
        ("نصك الدخول الاول على الدخول", ["close_leg"]),
        ("نغلق صفقة 4316 على الدخول\nونأمن صفقة 4318", ["close_leg", "breakeven"]),
        ("اغلاق كامل", ["close"]), ("اغلاق جميع الصفقات", ["close"]),
        ("اغلاق ربع ساعه بيكون سيئ", []),
        ("نغلق جزء ونأمن", ["close_part", "breakeven"]), ("احجز وامن", ["close_part", "breakeven"]),
        ("نرفع ستوبنا الى 4469.50", ["move_sl"]),
        ("شباب هدف الذهب الأخير 4067", ["new_tp"]),
        ("نرجع ندخل بنفس المعطيات", ["reenter"]),
        ("صفقات اليوم لحد الآن\n\nBuy 20 pips ✅\nBuy (Breakeven)\nBuy 20 pips ❌\nTotal: 140 pips", []),
        ("Eurusd جابت الهدف الاول\nنأمن الصفقات على الدخول", []),
        ("TP1✅", []), ("SL ❌", []), ("تفعلت", []),
        ("بيع الذهب امر معلق\n\n4438\n4441\n\nالاستوب\n4449\n\nالاهداف\n\n4433\n4426\n4421", ["SIGNAL"]),
        ("GOLD SELLNOW 4123-4122\n\nSL:4126\n\nTP:4120\n\nTP:4118.80", ["SIGNAL"]),
        ("تعزيز 4378\n\nSL 4382\n\nTP 4373.8\nTP 4370.6", ["SIGNAL"]),
    ]
    bad = [(t, acts(t), want) for t, want in cases if sorted(acts(t)) != sorted(want)]
    for t, got, want in bad:
        print(f"    {t[:40]!r}: got {got}, want {want}")
    check(f"{len(cases) - len(bad)}/{len(cases)} real follow-up messages read correctly", not bad)
    s = parse_message("Gold buy 4130 & 4127\n\nSl 4124\n\nTp 4132\nTp 4334\nTp 4136\nTp 4140").signal
    check("typo TP 4334 dropped, last TP 4140", s and s.tps == [4132, 4136, 4140])
    s = parse_message("GOLD SELL 4045\n\nSL:4050\n\nTP:4043\n\nTP:4041.80\n\nTP:4039.60\n\nTP:4037.70\n\nTP:4045\n\nTP:4043").signal
    check("repeated TPs at the end ignored", s and s.tps == [4043, 4041.8, 4039.6, 4037.7])

    print("Real group messages - trading flow")
    # 2 entries: buy 3907 now + 3903 limit, price drops, "close first at entry", "BE + delete reinforcement"
    b = FakeBroker(bid=3906.7)
    e = new_engine(b); await e.start()
    await e.on_message(301, "GOLD BUY NOW 3907 - 3903\n\nSL:3897\n\nTP: 3909\n\nTP: 3911\n\nTP: 3913\n\nTP: 3915\n\nTP: 3920")
    L = legs(e, 301)
    check("BUY NOW 3907 - 3903: market buy + buy limit 3903", L[0]["kind"] == "market" and L[1]["entry"] == 3903)
    b.tick(3902.6); await e.monitor_once()
    check("price 3902.6 -> second entry filled", L[1]["status"] == "open")
    await e.on_message(302, "بنصك الدخول الاول على الدخول  و نواصل بالتعزيز")
    check("'close first at entry' -> first trade TP set to its entry",
          b.pos[L[0]["ticket"]]["tp"] == round(b.pos[L[0]["ticket"]]["open_price"], 2) and b.pos[L[1]["ticket"]]["tp"] == 3920)
    b.tick(3907.1)
    for _ in range(3): await e.monitor_once()
    check("price back at 3907 -> first trade closed at entry, second still open",
          L[0]["status"] == "closed" and L[1]["status"] == "open")

    b = FakeBroker(bid=3906.7)
    e = new_engine(b); await e.start()
    await e.on_message(311, "GOLD BUY NOW 3907 - 3903\n\nSL:3897\n\nTP: 3909\n\nTP: 3911\n\nTP: 3913\n\nTP: 3915\n\nTP: 3920")
    L = legs(e, 311)
    b.tick(3908.0); await e.monitor_once()
    await e.on_message(312, "امن و احذف التعزيز")
    check("'امن و احذف التعزيز' -> 3903 order deleted, first trade SL to entry",
          L[1]["status"] == "closed" and not b.ord and b.pos[L[0]["ticket"]]["sl"] > 3906)

    # cancel an unfilled order, then re-enter with the same details
    b = FakeBroker(bid=4019.0)
    e = new_engine(b); await e.start()
    await e.on_message(321, G1)
    await e.on_message(322, "نلغي الأمر", reply_to=321)
    check("'نلغي الأمر' -> both sell limits cancelled", not b.ord and e.state.signals["321"]["status"] == "closed")
    await e.on_message(323, "نرجع ندخل بنفس المعطيات", reply_to=321)
    check("'نرجع ندخل بنفس المعطيات' -> same two sell limits placed again",
          sorted(o["price"] for o in b.ord.values()) == [4020, 4024] and "321-r1" in e.state.signals)
    await e.on_message(324, "نرجع ندخل بنفس المعطيات", reply_to=321)
    check("second re-enter while orders are still open -> ignored", len(b.ord) == 2)

    # new last TP, move SL, ignore "BE forbidden before target"
    b = FakeBroker(bid=3910.0)
    e = new_engine(b); await e.start()
    await e.on_message(331, G2)
    t1 = legs(e, 331)[0]["ticket"]
    await e.on_message(332, "شباب التامين يكون عقب الهدف الاول ممنوع التامين قبل الهدف")
    check("'BE forbidden before target' -> SL unchanged", b.pos[t1]["sl"] == 3915)
    await e.on_message(333, "شباب هدف الذهب الأخير 3890", reply_to=331)
    check("'هدف الذهب الأخير 3890' -> last TP moved to 3890", b.pos[t1]["tp"] == 3890 and e.state.signals["331"]["tps"][-1] == 3890)
    await e.on_message(334, "نرفع ستوبنا الى 3913")
    check("'نرفع ستوبنا الى 3913' -> SL 3913", b.pos[t1]["sl"] == 3913)
    b.tick(3908.0)
    await e.on_message(335, "التامين اجباري")
    check("'التامين اجباري' -> SL to entry", b.pos[t1]["sl"] < 3910)

    print("Typo signals (msg 3121, 5 Nov)")
    TYPO = "GOLD SELL LIMIT 4021\n\nSL:4026\n\nTP:4019\n\nTP:4017.80\n\nTP:4015.70\n\nTP:4013.60\n\nTP:4011.50"
    FIXED = TYPO.replace("40", "41")
    b = FakeBroker(bid=4116.87)
    e = new_engine(b, fix_hundreds_typo=True); await e.start()
    await e.on_message(3121, TYPO)
    o = list(b.ord.values())
    check("all prices 100 off -> traded as sell limit 4121, SL 4126, TP 4111.5",
          len(o) == 1 and o[0]["price"] == 4121 and o[0]["sl"] == 4126 and o[0]["tp"] == 4111.5)
    b = FakeBroker(bid=4116.87)
    e = new_engine(b, fix_hundreds_typo=False); await e.start()
    await e.on_message(3121, TYPO)
    check("typo fix off (default) -> skipped", not b.ord)
    b2 = FakeBroker(bid=4116.87)
    e2 = new_engine(b2, fix_hundreds_typo=False); await e2.start()
    await e2.on_message(3130, FIXED)
    check("corrected signal at 4116.87 (below TP1 4119) -> sell limit 4121 waits", len(b2.ord) == 1)
    await e2.monitor_once()
    check("limit is NOT cancelled just because price started below TP1", len(b2.ord) == 1)
    b2.tick(4120.0); await e2.monitor_once(); b2.tick(4118.5); await e2.monitor_once()
    check("price rose to 4120 then fell to TP1 without filling -> cancelled (move missed)", not b2.ord)
    e.state.data["skipped"]["3121"] -= 5 * 60           # admin corrects it 5 minutes later
    await e.on_message(3121, FIXED, edited=True)
    check("corrected 5 min later -> checked again and placed", len(b.ord) == 1 and list(b.ord.values())[0]["price"] == 4121)
    b = FakeBroker(bid=4116.87)
    e = new_engine(b, fix_hundreds_typo=False); await e.start()
    await e.on_message(3122, TYPO)
    e.state.data["skipped"]["3122"] -= 15 * 60
    await e.on_message(3122, FIXED, edited=True)
    check("corrected 15 min later -> too late, not traded", not b.ord)

    print("\nALL TESTS PASSED" if not FAILS else f"\n{len(FAILS)} FAILED: {FAILS}")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.WARNING, format="    %(levelname)s %(message)s")
    asyncio.run(run())
