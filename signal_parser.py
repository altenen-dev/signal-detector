"""
Reads Telegram messages (English and Arabic) and turns them into structured
gold (XAUUSD) signals or follow-up commands.

Built and tested against the real history of the signal group, e.g.

    GOLD SELL NOW 4033            Gold sell limit 4020          بيع الذهب امر معلق
    SL:4038.60                    And 4024                      4438
    TP:4031                       Sl 4028                       4441
    TP:4029.80                    Tp 4018                       الاستوب 4449
    ...                           ...                           الاهداف 4433 4426 4421

and follow-ups such as  "TP1 hit, SL to BE",  "نامن الصفقة",  "حذف التعزيز",
"نصك الدخول الاول على الدخول",  "نلغي الأمر",  "نرفع الستوب الى 4092",
"هدف الذهب الأخير 4067",  "نرجع ندخل بنفس المعطيات".
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional

# ================================================================ normalising
_AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
_AR_DIACRITICS = re.compile(r"[ً-ْٰـ]")      # harakat + tatweel


def normalize(text: str) -> str:
    t = (text or "").translate(_AR_DIGITS).upper()
    t = _AR_DIACRITICS.sub("", t)
    t = re.sub("[أإآٱ]", "ا", t).replace("ة", "ه").replace("ى", "ي").replace("ؤ", "و").replace("ئ", "ي")
    t = re.sub(r"([ء-ي])\1{2,}", r"\1", t)          # هدفففف -> هدف
    t = re.sub(r"(ا|و)\1+", r"\1", t)                          # اوول / ثااني -> اول / ثاني
    t = re.sub(r"([نفل])\1(?![ء-ي])", r"\1", t)  # نامنن / هدفف / اولل -> نامن / هدف / اول
    t = re.sub(r"(?<=\d),(?=\d{3}(?!\d))", "", t)                    # 4,012.50 -> 4012.50
    t = t.replace("،", " ").replace("؛", " ")
    t = re.sub(r"[^\w\s./:@\-%,+&]", " ", t)                          # drop emojis & symbols
    t = t.replace("_", " ")
    # common English typos from admins typing fast
    t = re.sub(r"\b(?:NUY|BYU|BUU|BIY|VUY|BHY|BUYY|BBUY)\b", "BUY", t)
    t = re.sub(r"\b(?:SEL|SELLL+|SSELL|SWLL|SRLL|SEEL|SEKL)\b", "SELL", t)
    t = re.sub(r"\b(BUY|SELL)(NOW|LIMIT|STOP)\b", r"\1 \2", t)      # SELLNOW -> SELL NOW
    return t


def _w(pattern: str) -> str:
    """whole-word wrapper that works for Arabic and English"""
    return rf"(?<![\w]){pattern}(?![\w])"


# ================================================================ patterns
GOLD_RE = re.compile(r"\b(?:XAU\s*/?\s*USD\w*|XAU|GOLD|GIOD|GOLS|GLOD)\b|" + _w("(?:ال)?ذهب"))
OTHER_SYMBOL_RE = re.compile(
    r"\b(?:EUR|GBP|USD|JPY|AUD|NZD|CAD|CHF)\s*/?\s*(?:EUR|GBP|USD|JPY|AUD|NZD|CAD|CHF)\b"
    r"|\b(?:BTC\w*|ETH\w*|US30|US100|NAS100|NASDAQ|SPX\w*|US500|USOIL|WTI|XAG\w*|SILVER|DXY|DJI|GER40|DAX)\b"
    r"|" + _w("(?:ال)?(?:يورو|باوند|بتكوين|البيتكوين|فضه|نفط|ين)")
)
_SELL_AR = _w(r"(?:و|ف)?(?:ال|ب|بن|ن|ا|با|سن)?بيع(?:ه)?") + "|" + _w(r"(?:و)?بعت")
_BUY_AR = _w(r"(?:و|ف)?(?:ال)?(?:شراء|اشتر|اشتري|بشتري|نشتري|بنشتري|شريت)")
DIR_RE = re.compile(rf"\b(BUY|SELL)\b(?:\s*(?:NOW\s*)?(LIMIT|STOP))?|(?P<sar>{_SELL_AR})|(?P<bar>{_BUY_AR})")
SL_RE = re.compile(r"\b(?:SL|S\s*/\s*L|S\.L\.?|STOP\s*-?\s*LOSS|STOPLOSS)(?![A-Z])|\bS\s*:(?=\s*\d)|"
                   + _w(r"(?:و)?(?:ال)?(?:ا)?ستوب(?:ي|نا|ك)?") + r"|وقف\s*الخساره")
TP_RE = re.compile(r"\b(?:TP\s?\d?|T\s*/\s*P|T\.P\.?|TAKE\s*-?\s*PROFIT\s?\d?|TARGETS?\s?\d?)(?!\d)|"
                   + _w(r"(?:و)?(?:ال)?(?:هدف|اهداف|هداف)(?:ي|نا)?"))
ENTRY_RE = re.compile(r"\bENTRY\b|" + _w(r"(?:و)?(?:ال)?(?:دخول|تعزيز)"))
MARKET_RE = re.compile(r"\b(?:NOW|MARKET|CMP|INSTANT|ASAP)\b|" + _w(r"(?:ال)?(?:حالي|حاليا|الان|الحين)"))
PENDING_AR_RE = re.compile(r"امر\s*معلق|اوامر\s*معلقه")
NUM_RE = re.compile(r"(?<![\d.])\d{1,6}(?:\.\d+)?(?![\d])")

PLAUSIBLE_PCT = 10.0  # every price in a signal must be within 10 % of the SL


@dataclass
class Signal:
    direction: str                      # "BUY" or "SELL"
    sl: float
    tps: List[float]                    # nearest -> furthest
    entry_low: Optional[float] = None   # None = market
    entry_high: Optional[float] = None
    order_kind: Optional[str] = None    # None / "LIMIT" / "STOP"
    market: bool = False                # "buy now"

    @property
    def last_tp(self) -> float:
        return self.tps[-1]

    @property
    def entry_mid(self) -> Optional[float]:
        if self.entry_low is None:
            return None
        return (self.entry_low + self.entry_high) / 2

    def to_dict(self):
        return dict(direction=self.direction, sl=self.sl, tps=list(self.tps), entry_low=self.entry_low,
                    entry_high=self.entry_high, order_kind=self.order_kind, market=self.market)


@dataclass
class Command:
    action: str
    # breakeven | move_sl | close | close_half | close_part | cancel | cancel_leg |
    # close_leg | new_tp | reenter
    value: Optional[float] = None       # price for move_sl / new_tp / cancel_leg / close_leg
    all_trades: bool = False
    leg: Optional[int] = None           # 1 = first entry, 2 = second entry ("التعزيز")
    at_entry: bool = False              # close_leg: exit at the entry price instead of now


@dataclass
class ParseResult:
    signal: Optional[Signal] = None
    commands: List[Command] = field(default_factory=list)
    reason: str = ""
    # an unfinished signal: direction + entry price(s), SL and/or TPs not written yet
    # ("GOLD BUY  Entry: 4052 & 4049  SL:  TP:"). sl is None when no SL yet, tps is [].
    draft: Optional[Signal] = None


# ================================================================ helpers
def _numbers(segment: str) -> List[float]:
    return [float(n) for n in NUM_RE.findall(segment)]


def _plausible(nums: List[float], ref: float) -> List[float]:
    return [n for n in nums if abs(n - ref) / ref * 100 <= PLAUSIBLE_PCT]


def _dir_of(m) -> str:
    if m.group(1):
        return m.group(1)
    return "SELL" if m.group("sar") else "BUY"


# ================================================================ signals
def parse_signal(text: str, require_symbol_word: bool = False) -> ParseResult:
    t = normalize(text)

    has_gold = bool(GOLD_RE.search(t))
    if not has_gold and OTHER_SYMBOL_RE.search(t):
        return ParseResult(reason="not a gold signal (other symbol mentioned)")
    if require_symbol_word and not has_gold:
        return ParseResult(reason="no gold/XAU word in message")

    dirs = list(DIR_RE.finditer(t))
    if len({_dir_of(d) for d in dirs}) > 1:
        return ParseResult(reason="both BUY and SELL in one message - ambiguous")

    marks = [(m.start(), m.end(), "SL") for m in SL_RE.finditer(t)]
    marks += [(m.start(), m.end(), "TP") for m in TP_RE.finditer(t)]
    marks += [(m.start(), m.end(), "ENT") for m in ENTRY_RE.finditer(t)]
    if dirs:
        marks.append((dirs[0].start(), dirs[0].end(), "DIR"))
    marks.sort()
    segs = {"DIR": [], "SL": [], "TP": [], "ENT": []}
    for i, (s, e, kind) in enumerate(marks):
        nxt = marks[i + 1][0] if i + 1 < len(marks) else len(t)
        segs[kind].append(t[e:nxt])

    sl = None
    for seg in segs["SL"]:
        nums = [n for n in _numbers(seg) if n >= 100]
        if nums:
            sl = nums[0]
            break
    if sl is None:
        if not dirs:
            return ParseResult(reason="no BUY/SELL")
        return ParseResult(reason="no stop loss price", draft=_draft(t, dirs, segs, None))

    entries = []
    for seg in segs["DIR"] + segs["ENT"]:
        entries += _plausible(_numbers(seg), sl)
    tps_raw: List[float] = []
    for seg in segs["TP"]:
        tps_raw += _plausible(_numbers(seg), sl)

    if dirs:
        direction = _dir_of(dirs[0])
    else:
        # no direction word: only when an entry label is present, infer from SL vs TPs
        if not segs["ENT"] or not entries or not tps_raw:
            return ParseResult(reason="no BUY/SELL")
        if all(x > sl for x in tps_raw):
            direction = "BUY"
        elif all(x < sl for x in tps_raw):
            direction = "SELL"
        else:
            return ParseResult(reason="no BUY/SELL")

    order_kind = dirs[0].group(2) if dirs and dirs[0].group(1) else None
    if order_kind is None and PENDING_AR_RE.search(t):
        order_kind = "LIMIT"
    head = ((segs["DIR"][0] if segs["DIR"] else "") + " " + t[:dirs[0].start()]) if dirs else ""
    market = bool(MARKET_RE.search(head)) or bool(re.search(r"(?:ال)?سعر\s*(?:ال)?(?:حالي|حاليا)", t))

    entry_low = entry_high = None
    uniq = []
    for x in entries:
        if x not in uniq:
            uniq.append(x)
    if uniq:
        a = uniq[0]
        b = uniq[1] if len(uniq) > 1 else a
        entry_low, entry_high = min(a, b), max(a, b)

    buy = direction == "BUY"
    ref = entry_low if buy else entry_high
    base = ref if ref is not None else sl
    sl_dist = abs(base - sl)
    tps: List[float] = []
    for x in tps_raw:                       # message order; must move away from entry
        if buy and not (x > sl and (ref is None or x > ref)):
            continue
        if not buy and not (x < sl and (ref is None or x < ref)):
            continue
        if ref is not None and abs(x - base) > max(12 * sl_dist, base * 0.005):
            continue                        # typo far away, e.g. "Tp 4334" in a 4130 signal
        if tps and ((buy and x <= tps[-1]) or (not buy and x >= tps[-1])):
            continue                        # duplicates / stray numbers going backwards
        tps.append(x)
    if not tps:
        return ParseResult(reason="no valid take profit",
                           draft=_draft(t, dirs, segs, sl) if dirs else None)

    if entry_low is not None:
        if buy and not (sl < entry_low):
            return ParseResult(reason="BUY but SL is not below entry")
        if not buy and not (sl > entry_high):
            return ParseResult(reason="SELL but SL is not above entry")

    return ParseResult(signal=Signal(direction=direction, sl=sl, tps=tps,
                                     entry_low=entry_low, entry_high=entry_high,
                                     order_kind=order_kind, market=market or entry_low is None))


def _draft(t, dirs, segs, sl) -> Optional[Signal]:
    """direction + entry price(s) but SL/TPs still missing. Entries are checked against the
    market by the engine (a typo such as 5049 in a 4050 signal is dropped there)."""
    entries = []
    for seg in segs["DIR"] + segs["ENT"]:
        for n in _numbers(seg):
            if n >= 100 and n not in entries and (sl is None or abs(n - sl) / sl * 100 <= PLAUSIBLE_PCT):
                entries.append(n)
    if not entries:
        return None
    # only a signal being typed counts, not chat such as "نرجع نشتري 3903":
    # it needs an order type (LIMIT / STOP / NOW / امر معلق), an "Entry" label, or SL / TP lines
    typed = bool(dirs[0].group(1) and dirs[0].group(2)) or bool(PENDING_AR_RE.search(t)) \
        or bool(MARKET_RE.search((segs["DIR"][0] if segs["DIR"] else "") + " " + t[:dirs[0].start()])) \
        or bool(segs["ENT"]) or bool(segs["SL"]) or bool(segs["TP"])
    if not typed:
        return None
    direction = _dir_of(dirs[0])
    if sl is not None:
        entries = [e for e in entries if (e > sl if direction == "BUY" else e < sl)]
        if not entries:
            return None
    order_kind = dirs[0].group(2) if dirs[0].group(1) else None
    if order_kind is None and PENDING_AR_RE.search(t):
        order_kind = "LIMIT"
    head = (segs["DIR"][0] if segs["DIR"] else "") + " " + t[:dirs[0].start()]
    return Signal(direction=direction, sl=sl, tps=[], entry_low=min(entries[:2]), entry_high=max(entries[:2]),
                  order_kind=order_kind, market=bool(MARKET_RE.search(head)))


# ================================================================ commands (English)
_BE_RE = re.compile(
    r"\b(?:SL|STOP\s*LOSS)\s*(?:TO|AT|@|ON|=)?\s*(?:BE|B/E|BREAK\s*-?\s*EVEN|BREAKEVEN|ENTRY|ENTRY\s*POINT|OPEN\s*PRICE)\b"
    r"|\b(?:SET|MOVE|PUT|KEEP)\s*(?:IT|TRADES?|POSITIONS?)?\s*(?:TO|AT|ON)?\s*(?:BE|B/E|BREAK\s*-?\s*EVEN|BREAKEVEN)\b"
    r"|\bBREAK\s*-?\s*EVEN\b|\bBREAKEVEN\b|\bB/E\b|\bRISK\s*-?\s*FREE\b"
)
_MOVE_SL_RE = re.compile(
    r"\b(?:MOVE|SET|NEW|CHANGE|UPDATE|PUT)\s*(?:THE\s*)?(?:SL|STOP\s*LOSS)\s*(?:TO|@|AT|:|=)?\s*(\d{3,6}(?:\.\d+)?)"
    r"|\bSL\s*(?:MOVED|CHANGED|UPDATED)\s*(?:TO|@|AT)?\s*(\d{3,6}(?:\.\d+)?)"
)
_HALF_RE = re.compile(r"\b(?:CLOSE|TAKE|SECURE)\s*(?:THE\s*)?(?:HALF\b|50\s*%|PARTIALS?\b)|\bPARTIAL\s*CLOSE\b|\bCLOSE\s*1\s*/\s*2\b")
_CLOSE_RE = re.compile(
    r"\b(?:CLOSE|EXIT)\s*(?:ALL|NOW|IT|THE\s*TRADES?|TRADES?|GOLD|XAU\w*|POSITIONS?|BUYS?|SELLS?|HERE|EARLY|FULL|FULLY|MANUALLY|AT\s*MARKET)\b"
    r"|\b(?:CLOSED|EXITED)\s*(?:ALL|NOW|IT|TRADES?|GOLD|XAU\w*|HERE|MANUALLY)\b"
)
_CANCEL_RE = re.compile(r"\b(?:CANCEL|CANCELLED|CANCELED|DELETE|REMOVE)\b")
_NEW_TP_EN = re.compile(r"\b(?:LAST|FINAL|NEW)\s*(?:TP|TARGET)\s*(?:IS|AT|:|@|=)?\s*(\d{4,6}(?:\.\d+)?)")
_REENTER_EN = re.compile(r"\b(?:RE-?ENTER|SAME\s*(?:SIGNAL|ENTRY|SETUP)|ENTER\s*AGAIN)\b")


# ================================================================ commands (Arabic)
_COND = re.compile(_w(r"(?:و)?(?:اذا|لو)") + r"|في\s*حال")
_OPTIONAL = re.compile(_w("(?:و)?اللي") + r"\s*(?:حاب|يحب|يبي|يبغي|اكتفي|مو\s*حاب|ماحاب|يخاف|خايف)|" + _w("ممكن") + "|الخيار")
_BE_AR = re.compile(_w(r"(?:و|ف)?(?:ن|ي|ت|ب|بن|ا|نب)?امن(?:و|وا|ها|ه|ك|نا)?")
                    + "|" + _w(r"(?:و|ف)?(?:ال|لل|ل)?تامين(?:ك|ي|نا)?")
                    + r"|(?:ال)?ستوب\s*(?:علي|عند)\s*(?:ال)?دخول")
_BE_INFO = re.compile(r"(?:ضرب\w*|رجع\w*|طلع\w*|طار|طقت|ماضرب|راحت|وراحت)\s*(?:علي\s*|لل|ال)?(?:ال)?تامين")
_BE_NEG = re.compile(r"ممنوع\s*(?:ال)?تامين|ما\s*ب?ن?امن|مانامن|ما\s*راح\s*نامن|بدون\s*(?:ال)?تامين|"
                     r"(?:ال)?تامين\s*(?:الحين\s*)?غلط|بعد\s*(?:ال)?هدف")
# reports and stories ("we took 2 targets then BE", "today's results", "I forgot to ...")
_REPORT_AR = re.compile(_w(r"(?:و)?(?:عطانا|عطتنا|عطاني|خذينا|اخذنا|خدينا|نسيت|كنت)"))
_RECAP = re.compile(r"\bTOTAL\b|صافي|صفقات\s*(?:ال)?(?:يوم|اسبوع)|نتايج")
_PART_AR = re.compile(r"(?:نغلق|اغلق|اغلاق|بغلق|بنغلق|نصك|يصك|بنصك|صك|نسكر|يسكر|نقفل)\s*(?:ال)?(?:جزء|جزا|نص|نصف|\d+\s*بالمي\w*)"
                      + "|" + _w(r"(?:و)?(?:احجز|نحجز|يحجز|حجز|نجني|جني|يجني|تجني)"))
_FIRST_LEG_AR = re.compile(
    r"(?:نصك|بنصك|نغلق|بنغلق|نسكر|نسكرها|اغلاق|نغلقه|نقفل|بنقفل|صك|نغلقها|سكر)\s*(?:ال)?(?:دخول|صفقه)?\s*(?:ال)?(?:اول|اولي)"
    r"|اغلاق\s*اول\s*دخول")
_LEG_BY_PRICE_AR = re.compile(r"(?:نصك|بنصك|نغلق|بنغلق|نسكر|اغلاق|نقفل)\s*(?:ال)?(?:صفقه|دخول)?\s*(?:اللي|الي)?\s*(?:من)?\s*(\d{4}(?:\.\d+)?)")
_AT_ENTRY_AR = re.compile(r"علي\s*(?:ال)?دخول")
_FULL_CLOSE_AR = re.compile(
    r"(?:اغلاق|نغلق|بنغلق|اغلق|سكرنا|نسكر|بنسكر|نقفل|بنقفل|صكيت|نصك|بنصك)\s*(?:ال)?(?:كامل|كلي|جميع|كل|الكل|صفقه|صفقات|صفقاتكم|صفقتنا)")
_NOT_A_CLOSE_AR = re.compile(r"اغلاق\s*(?:ال)?(?:ساعه|ربع|شمعه|يومي|الشهري|شهري|اسبوع\w*|4|1)|اسبوع|بيسكر|السوق")
_CANCEL_AR = re.compile(_w(r"(?:و)?(?:الغاء|نلغي|نلغيها|بنلغي|الغي|الغيه|نمسح|امسح|بنمسح|احذف|نحذف|بنحذف|حذف|ملغي|ملغيه)"))
_CANCEL_NEG_AR = re.compile(r"ما\s*حذف|ماحذف|ما\s*راح\s*ن?حذف|ماراح\s*ن?حذف|ما\s*مسح|مسحنا|حذفنا|ما\s*نلغي")
_CANCEL_PRICE_AR = re.compile(r"(\d{4}(?:\.\d+)?)\s*ملغي")
_SECOND_LEG_AR = re.compile(_w(r"(?:ال)?تعزيز"))
_MOVE_SL_AR = re.compile(
    r"(?:نرفع|ارفع|بنرفع|نزل|ننزل|بننزل|غيرت|نغير|غير|نعدل|نخلي|نحط|حط|خل)\s*(?:ال)?(?:ا)?ستوب\w*\s*(?:ال)?(?:تامين)?\s*"
    r"(?:الي|ل|علي|عند|=|:)?\s*(?:حوالي|تقريبا)?\s*(?:اغلاق\s*اعلي|اعلي)?\s*(\d{4}(?:\.\d+)?)")
_SHORT_SL = re.compile(r"^\s*(?:SL|(?:ال)?(?:ا)?ستوب\w*)\s*:?\s*(\d{4}(?:\.\d+)?)\s*$")
_NEW_TP_AR = re.compile(r"(?:ال)?هدف\s*(?:ال)?(?:ذهب)?\s*(?:ال)?(?:اخير|نهايي)\s*:?\s*(?:عند)?\s*(\d{4}(?:\.\d+)?)"
                        r"|اخر\s*هدف\s*(?:عند)?\s*:?\s*(\d{4}(?:\.\d+)?)")
_REENTER_AR = re.compile(r"(?:ب|ن)?نفس\s*(?:ال)?(?:معطيات|دخول|ستوب|اهداف)")
_REENTER_REPLY_AR = re.compile(r"(?:نرجع|بنرجع|راح\s*نرجع)\s*(?:ندخل|نبيع|نشتري|نعيد)|" + _w(r"(?:ب|و)?نكرر"))


def parse_commands(text: str, is_reply: bool = False) -> List[Command]:
    t = normalize(text)
    words = t.split()
    cmds: List[Command] = []
    if OTHER_SYMBOL_RE.search(t) and not GOLD_RE.search(t):
        return cmds                     # about EURUSD, the pound, bitcoin ... not gold
    all_trades =bool(re.search(r"\bALL\b|" + _w(r"(?:ال)?(?:كل|جميع)") + r"|(?:ال)?صفقتين|(?:ال)?صفقات", t))
    cond, optional = bool(_COND.search(t)), bool(_OPTIONAL.search(t))
    if _RECAP.search(t) or len(re.findall(r"PIPS|نقطه", t)) >= 3:
        return cmds                     # a results summary, not an instruction
    if re.search(r"\bكنت\b|نسيت", t):
        return cmds                     # a story about the past
    report = bool(_REPORT_AR.search(t))

    def add(c: Command):
        if not any(x.action == c.action and x.leg == c.leg and x.value == c.value for x in cmds):
            cmds.append(c)

    # ---------- re-enter the same signal
    if _REENTER_AR.search(t) and (_REENTER_REPLY_AR.search(t) or is_reply or "نفس" in t) \
            or (is_reply and _REENTER_REPLY_AR.search(t)) or _REENTER_EN.search(t):
        add(Command("reenter"))
        return cmds

    # ---------- new last TP
    m = _NEW_TP_AR.search(t) or _NEW_TP_EN.search(t)
    if m:
        add(Command("new_tp", float(next(g for g in m.groups() if g))))

    # ---------- move SL to a price
    m = _MOVE_SL_RE.search(t) or _MOVE_SL_AR.search(t)
    if m:
        add(Command("move_sl", float(next(g for g in m.groups() if g))))
    elif is_reply and _SHORT_SL.match(t):
        add(Command("move_sl", float(_SHORT_SL.match(t).group(1))))

    # ---------- close one entry (usually the first) - optionally at its entry price
    at_entry = bool(_AT_ENTRY_AR.search(t))
    m_first = _FIRST_LEG_AR.search(t)
    m_price = _LEG_BY_PRICE_AR.search(t)
    if (m_first or m_price) and not optional:
        if m_price and not m_first:
            add(Command("close_leg", value=float(m_price.group(1)), at_entry=at_entry or cond))
        else:
            add(Command("close_leg", leg=1, at_entry=at_entry or cond))
    elif not cond and not optional and _FULL_CLOSE_AR.search(t) and not _NOT_A_CLOSE_AR.search(t):
        if at_entry:
            add(Command("close_leg", at_entry=True, all_trades=True))
        else:
            add(Command("close", all_trades=all_trades))

    # ---------- partial close ("close part / book profit")
    if _HALF_RE.search(t):
        add(Command("close_half", all_trades=all_trades))
    elif _PART_AR.search(t) and not cond and not optional and not report \
            and not any(c.action.startswith("close") for c in cmds):
        add(Command("close_part", all_trades=all_trades))

    # ---------- breakeven
    if _BE_RE.search(t) and not re.search(r"\b(?:BUY|SELL)\s*[:(]?\s*(?:BE|BREAKEVEN|B/E)\b", t):
        add(Command("breakeven", all_trades=all_trades))   # "Buy (Breakeven)" is a result line, not an order
    elif _BE_AR.search(t) and not _BE_INFO.search(t) and not _BE_NEG.search(t) and not cond and not optional \
            and not report:
        add(Command("breakeven", all_trades=all_trades))

    # ---------- English full close
    if not any(c.action in ("close", "close_half", "close_leg") for c in cmds):
        if _CLOSE_RE.search(t) or (len(words) <= 4 and re.search(r"\b(?:CLOSE|EXIT)\b", t)):
            add(Command("close", all_trades=all_trades))

    # ---------- cancel waiting orders
    if not cond and not _CANCEL_NEG_AR.search(t):
        m = _CANCEL_PRICE_AR.search(t)
        if m:
            add(Command("cancel_leg", value=float(m.group(1))))
        elif _CANCEL_AR.search(t):
            if _SECOND_LEG_AR.search(t):
                add(Command("cancel_leg", leg=2))
            else:
                add(Command("cancel", all_trades=all_trades))
        elif _CANCEL_RE.search(t) and not any(c.action == "close" for c in cmds):
            add(Command("cancel", all_trades=all_trades))
    return cmds


def parse_message(text: str, require_symbol_word: bool = False, is_reply: bool = False) -> ParseResult:
    """Signal first; if the message is not a full signal, look for commands."""
    if not text or not text.strip():
        return ParseResult(reason="empty / image-only message")
    res = parse_signal(text, require_symbol_word)
    if res.signal:
        return res
    cmds = parse_commands(text, is_reply)
    if cmds:
        return ParseResult(commands=cmds)
    return res
