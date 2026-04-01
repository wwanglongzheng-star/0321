import time
import requests
import pandas as pd
import numpy as np
import talib as ta
from datetime import datetime, timedelta
from dataclasses import dataclass

from config import *
from logger import log

# ====================== 全局状态 ======================
PRE_ZT_PUSHED_MORNING = set()
PRE_ZT_PUSHED_AFTERNOON = set()
_PREV_Z = set()
_LHB = set()
_CONCEPT = {}
_NORTH = 0.0
_MARKET_BREADTH = {"rise": 0, "fall": 0, "limit_up": 0}

@dataclass
class Signal:
    code: str
    name: str
    price: float
    chg: float
    zt_price: float
    gap: float
    vol_ratio: float
    turnover: float
    main_in: float
    concept: str
    sector_strength: str
    lhb: bool
    north: bool
    leader: int
    reclose_prob: int
    rsi: float
    macd_strong: bool
    stop_loss: float
    score: float
    reason: str

# ====================== 时间 ======================
def now():
    return datetime.utcnow() + timedelta(hours=8)

def is_trading_day():
    return now().weekday() < 5

def phase():
    hm = now().hour * 100 + now().minute
    if 925 <= hm < 1030:
        return "morning"
    elif 1300 <= hm < 1400:
        return "afternoon"
    elif hm >= 1500:
        return "closed"
    return "other"

# ====================== 推送 ======================
def send(title, content):
    if not SENDKEY:
        log.warning("无 SENDKEY")
        return False
    try:
        url = f"https://sctapi.ftqq.com/{SENDKEY}.send"
        requests.post(url, data={"title": title, "desp": content}, timeout=TIMEOUT)
        log.info(f"已推送: {title}")
        return True
    except Exception as e:
        log.error(f"推送失败: {e}")
        return False

# ====================== 数据加载（全部免费接口） ======================
def load_all_data():
    global _PREV_Z, _LHB, _CONCEPT, _NORTH, _MARKET_BREADTH
    try:
        r = requests.get("https://push2.eastmoney.com/api/qt/ulist/get?fs=m:0+t:80&fields=f12,f14", timeout=TIMEOUT).json()
        _PREV_Z = {str(x["f12"]).zfill(6) for x in r.get("data", {}).get("diff", [])}

        r2 = requests.get("https://push2.eastmoney.com/api/qt/ulist/get?fs=b:kd&fields=f12", timeout=TIMEOUT).json()
        _LHB = {str(x["f12"]).zfill(6) for x in r2.get("data", {}).get("diff", [])}

        r3 = requests.get("https://push2.eastmoney.com/api/qt/ulist/get?fs=m:0+t:80,m:1+t:2&fields=f12,f13", timeout=TIMEOUT).json()
        _CONCEPT = {str(x["f12"]).zfill(6): x.get("f13", "无") for x in r3.get("data", {}).get("diff", [])}

        r4 = requests.get("https://push2.eastmoney.com/api/qt/stock/get?secid=1.000001&fields=f124", timeout=TIMEOUT).json()
        _NORTH = float(r4.get("data", {}).get("f124", 0)) / 100000000

        r5 = requests.get("https://push2.eastmoney.com/api/qt/stock/trends2/get?secid=1.000001&fields=f1,f2,f3", timeout=TIMEOUT).json()
        _MARKET_BREADTH["rise"] = r5.get("data", {}).get("data", {}).get("f1", 0)
        _MARKET_BREADTH["fall"] = r5.get("data", {}).get("data", {}).get("f2", 0)
        _MARKET_BREADTH["limit_up"] = r5.get("data", {}).get("data", {}).get("f3", 0)

        log.info(f"北向: {_NORTH:.1f}亿 | 涨跌家数: {_MARKET_BREADTH['rise']}/{_MARKET_BREADTH['fall']}")
    except Exception as e:
        log.error(f"数据加载异常: {e}")

# ====================== 行情 ======================
def get_quotes():
    try:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1, "pz": 5000,
            "fs": "m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f12,f14,f2,f18,f17,f10,f6,f8,f15,f5,f124"
        }
        r = requests.get(url, params=params, timeout=TIMEOUT).json()
        rows = []
        for item in r.get("data", {}).get("diff", []):
            code = str(item.get("f12", "")).zfill(6)
            name = item.get("f14", "")
            if "ST" in name or "退" in name:
                continue
            rows.append({
                "code": code, "name": name,
                "price": float(item.get("f2", 0)),
                "pre": float(item.get("f18", 0)),
                "open": float(item.get("f17", 0)),
                "vol_ratio": float(item.get("f10", 0)),
                "amount": float(item.get("f6", 0)),
                "cap": float(item.get("f8", 0)) * 1e8,
                "high": float(item.get("f15", 0)),
                "turnover": float(item.get("f5", 0)),
                "main_in": float(item.get("f124", 0)),
            })
        df = pd.DataFrame(rows)
        if not df.empty:
            df["atr"] = ta.ATR(df["high"], df["price"]*0.99, df["price"], timeperiod=14)
        return df
    except Exception as e:
        log.error(f"行情异常: {e}")
        return pd.DataFrame()

# ====================== 选股核心（全免费增强） ======================
def scan():
    ph = phase()
    max_num = PRE_ZT_MAX_MORNING if ph == "morning" else PRE_ZT_MAX_AFTERNOON
    pushed = PRE_ZT_PUSHED_MORNING if ph == "morning" else PRE_ZT_PUSHED_AFTERNOON
    if len(pushed) >= max_num:
        return []

    df = get_quotes()
    if df.empty:
        return []

    rise_ratio = _MARKET_BREADTH["rise"] / max(_MARKET_BREADTH["fall"], 1)
    if rise_ratio < 0.7 and _MARKET_BREADTH["limit_up"] < 30:
        log.info("市场情绪弱，暂停选股")
        return []

    signals = []
    for _, row in df.iterrows():
        try:
            code = row.code
            if code in pushed:
                continue

            price = row.price
            pre = row.pre
            if pre <= 0:
                continue

            chg = (price - pre) / pre
            if not (PRE_ZT_MIN_CHG <= chg <= PRE_ZT_MAX_CHG):
                continue

            if row.amount < PRE_ZT_MIN_AUCTION_AMOUNT:
                continue
            if row.vol_ratio < PRE_ZT_MIN_VOL_RATIO:
                continue
            if row.turnover < MIN_TURNOVER:
                continue
            if not (MIN_MKT_CAP <= row.cap <= MAX_MKT_CAP):
                continue

            close_arr = np.array([price*0.99, price, price*1.01])
            rsi = ta.RSI(close_arr, timeperiod=14)[-1] if len(close_arr)>=14 else 50
            if rsi >= 75:
                continue

            macd_val = ta.MACD(close_arr)[0][-1] if len(close_arr)>=26 else 0
            macd_strong = macd_val > 0

            atr = row.atr if not np.isnan(row.atr) else price * 0.03
            stop_loss = price - atr * STOP_LOSS_ATR_MULTIPLIER
            stop_loss = max(stop_loss, price * (1 - MAX_STOP_LOSS_PCT))
            stop_loss = min(stop_loss, price * (1 - MIN_STOP_LOSS_PCT))

            zt_price = round(pre * 1.2, 2) if code.startswith(("300","688")) else round(pre * 1.1, 2)
            gap = (zt_price - price) / zt_price if zt_price > price else 0
            if gap < PRE_ZT_GAP_MIN:
                continue

            lhb_flag = code in _LHB
            concept = _CONCEPT.get(code, "无")[:18]
            sector_strength = "强" if "概念" in concept or "板块" in concept else "中"

            reclose = 50
            if row.main_in > 0: reclose += 25
            if row.vol_ratio >= 6: reclose += 15
            if macd_strong: reclose += 10
            reclose = min(reclose, 99)

            score = 65
            score += 15 if row.main_in > 0 else 0
            score += 10 if lhb_flag else 0
            score += 8 if _NORTH > 0 else 0
            score += 5 if code in _PREV_Z else 0
            score += int(reclose * 0.1)
            score += 3 if macd_strong else 0
            score += 3 if rsi < 60 else 0

            if score < PRE_ZT_MIN_SCORE:
                continue

            s = Signal(
                code=code, name=row.name, price=round(price,2),
                chg=round(chg*100,2), zt_price=zt_price, gap=round(gap*100,2),
                vol_ratio=round(row.vol_ratio,1), turnover=round(row.turnover,1),
                main_in=round(row.main_in/10000,0), concept=concept,
                sector_strength=sector_strength, lhb=lhb_flag, north=_NORTH>0,
                leader=4 if code in _PREV_Z else 0, reclose_prob=reclose,
                rsi=round(rsi,1), macd_strong=macd_strong, stop_loss=round(stop_loss,2),
                score=round(score,1), reason=f"回封概率{reclose}%"
            )
            signals.append(s)
            if len(signals) >= max_num:
                break
        except:
            continue

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:max_num]

# ====================== 推送格式 ======================
def fmt(s, i):
    lhb = "✅龙虎" if s.lhb else ""
    north = "💹北向" if s.north else ""
    leader = f"龙{s.leader}" if s.leader else ""
    macd = "📈MACD强" if s.macd_strong else ""
    return (
        f"#{i} {s.name}({s.code})\n"
        f"现价:{s.price} | 涨幅:{s.chg}% | 空间:{s.gap}%\n"
        f"量比:{s.vol_ratio} | 换手:{s.turnover}% | 回封:{s.reclose_prob}%\n"
        f"主力:{s.main_in}万 | RSI:{s.rsi} | {lhb} {north} {leader} {macd}\n"
        f"题材:{s.concept} | 板块:{s.sector_strength}\n"
        f"评分:{s.score} | 动态止损:{s.stop_loss}\n"
        f"{s.reason}"
    )

def push(signals):
    if not signals:
        return
    ph = phase()
    title = f"【早盘起爆】{len(signals)}只" if ph == "morning" else f"【午盘强势】{len(signals)}只"
    content = f"时间:{now().strftime('%H:%M')}\n\n"
    content += "\n---\n".join([fmt(s, i+1) for i,s in enumerate(signals)])
    if send(title, content):
        for s in signals:
            if phase() == "morning":
                PRE_ZT_PUSHED_MORNING.add(s.code)
            else:
                PRE_ZT_PUSHED_AFTERNOON.add(s.code)

# ====================== 主程序 ======================
def main():
    log.info("=== 免费增强版量化打板系统启动 ===")
    if not SENDKEY:
        log.error("未配置SENDKEY")
        return
    if not is_trading_day():
        send("系统", "非交易日")
        return

    send("系统启动", "9:25即推 | 全免费因子 | 动态止损")
    load_all_data()

    while True:
        ph = phase()
        if ph == "closed":
            m = len(PRE_ZT_PUSHED_MORNING)
            a = len(PRE_ZT_PUSHED_AFTERNOON)
            send("收盘", f"早盘:{m}只 | 午盘:{a}只")
            log.info("交易结束")
            break

        if ph in ["morning", "afternoon"]:
            sigs = scan()
            push(sigs)

        time.sleep(20)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"崩溃: {e}")
        send("系统异常", str(e))
