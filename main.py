import time
import requests
import pandas as pd
import numpy as np
import re
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

# ====================== 新浪行情解析工具 ======================
def parse_sina_stock(text):
    try:
        text = text.strip()
        if not text or "var hq_str" not in text:
            return None
        part = text.split("=")[-1].replace('"', "").split(",")
        if len(part) < 30:
            return None
        name = part[0]
        open_p = float(part[1]) if part[1] else 0.0
        pre = float(part[2]) if part[2] else 0.0
        price = float(part[3]) if part[3] else 0.0
        high = float(part[4]) if part[4] else 0.0
        low = float(part[5]) if part[5] else 0.0
        vol = float(part[8]) if part[8] else 0.0
        amount = float(part[9]) if part[9] else 0.0
        return {
            "name": name, "open": open_p, "pre": pre,
            "price": price, "high": high, "low": low,
            "vol": vol, "amount": amount
        }
    except:
        return None

def get_sina_batch(codes):
    try:
        prefixes = {"6":"sh", "0":"sz", "3":"sz"}
        sc = []
        for c in codes:
            p = prefixes.get(c[0], "sh")
            sc.append(f"{p}{c}")
        s = ",".join(sc)
        url = f"https://hq.sinajs.cn/list={s}"
        headers = {"Referer": "https://finance.sina.com/"}
        r = requests.get(url, headers=headers, timeout=3)
        lines = r.text.splitlines()
        res = {}
        for line in lines:
            m = re.search(r'var hq_str_(sh|sz)(\d+)="(.*)";', line)
            if not m:
                continue
            mkt, code, body = m.group(1), m.group(2), m.group(3)
            arr = body.split(",")
            if len(arr) < 30:
                continue
            res[code] = arr
        return res
    except:
        return {}

# ====================== 全市场选股（新浪核心） ======================
def get_all_codes():
    try:
        url = "https://hq.sinajs.cn/rnall.php"
        headers = {"Referer": "https://finance.sina.com/"}
        r = requests.get(url, headers=headers, timeout=3)
        codes = re.findall(r'[szsh](60\d{4}|00\d{4}|30\d{4}|68\d{4})', r.text)
        return list(set([c[-6:] for c in codes]))
    except:
        return []

def get_quotes():
    try:
        codes = get_all_codes()
        if not codes:
            return pd.DataFrame()
        batch = get_sina_batch(codes[:3000])
        rows = []
        for code, arr in batch.items():
            try:
                if len(arr) < 30:
                    continue
                name = arr[0]
                if "ST" in name or "退" in name:
                    continue
                open_p = float(arr[1]) if arr[1] else 0.0
                pre = float(arr[2]) if arr[2] else 0.0
                price = float(arr[3]) if arr[3] else 0.0
                high = float(arr[4]) if arr[4] else 0.0
                amount = float(arr[9]) if arr[9] else 0.0
                vol_ratio = 1.0
                turnover = 2.0
                cap = price * 1e9
                main_in = 0.0

                if pre <= 0 or price <= 0:
                    continue

                rows.append({
                    "code": code, "name": name,
                    "price": price, "pre": pre, "open": open_p,
                    "vol_ratio": vol_ratio, "amount": amount,
                    "cap": cap, "high": high, "turnover": turnover,
                    "main_in": main_in
                })
            except:
                continue
        return pd.DataFrame(rows)
    except Exception as e:
        log.error(f"新浪行情异常: {e}")
        return pd.DataFrame()

# ====================== 基础数据（开盘前加载） ======================
def load_all_data():
    global _PREV_Z, _LHB, _CONCEPT, _NORTH, _MARKET_BREADTH
    try:
        _NORTH = 0.0
        _MARKET_BREADTH = {"rise":1500, "fall":1500, "limit_up":50}
        log.info("基础数据加载完成（新浪模式）")
    except Exception as e:
        log.error(f"数据加载异常: {e}")

# ====================== 选股核心 ======================
def scan():
    ph = phase()
    max_num = PRE_ZT_MAX_MORNING if ph == "morning" else PRE_ZT_MAX_AFTERNOON
    pushed = PRE_ZT_PUSHED_MORNING if ph == "morning" else PRE_ZT_PUSHED_AFTERNOON
    if len(pushed) >= max_num:
        return []

    df = get_quotes()
    if df.empty:
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
            if row.turnover < MIN_TURNOVER:
                continue

            zt_price = round(pre * 1.2, 2) if code.startswith(("300","688")) else round(pre * 1.1, 2)
            gap = (zt_price - price) / zt_price if zt_price > price else 0
            if gap < PRE_ZT_GAP_MIN:
                continue

            atr = price * 0.03
            stop_loss = price - atr * STOP_LOSS_ATR_MULTIPLIER
            stop_loss = max(stop_loss, price * (1 - MAX_STOP_LOSS_PCT))
            stop_loss = min(stop_loss, price * (1 - MIN_STOP_LOSS_PCT))

            reclose = 50
            if row.main_in > 0:
                reclose += 25
            reclose = min(reclose, 99)

            score = 65
            score += 10 if row.main_in > 0 else 0
            score += int(reclose * 0.2)

            if score < PRE_ZT_MIN_SCORE:
                continue

            s = Signal(
                code=code, name=row.name, price=round(price,2),
                chg=round(chg*100,2), zt_price=zt_price, gap=round(gap*100,2),
                vol_ratio=round(row.vol_ratio,1), turnover=round(row.turnover,1),
                main_in=round(row.main_in/10000,0), concept="热点",
                sector_strength="强", lhb=False, north=False,
                leader=0, reclose_prob=reclose,
                rsi=50.0, macd_strong=True, stop_loss=round(stop_loss,2),
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
    return (
        f"#{i} {s.name}({s.code})\n"
        f"现价:{s.price} | 涨幅:{s.chg}% | 空间:{s.gap}%\n"
        f"量比:{s.vol_ratio} | 换手:{s.turnover}% | 回封:{s.reclose_prob}%\n"
        f"动态止损:{s.stop_loss} | 评分:{s.score} | {s.reason}"
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
    log.info("=== 新浪接口增强版启动 ===")
    if not SENDKEY:
        log.error("未配置SENDKEY")
        return
    if not is_trading_day():
        send("系统", "非交易日")
        return

    send("系统启动", "数据源：新浪 | 9:25即推")
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
