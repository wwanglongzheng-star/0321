import time
import requests
import re
from datetime import datetime
from dataclasses import dataclass

# 强制北京时间（无需pytz，100%生效）
def now():
    return datetime.utcnow()
def beijing_now():
    utc_now = now()
    ts = utc_now.timestamp() + 8 * 3600
    return datetime.fromtimestamp(ts)

from config import *
from logger import log

PUSHED_TODAY = set()

@dataclass
class Signal:
    code: str
    name: str
    price: float
    chg: float
    open_p: float
    pre: float
    low: float
    high: float
    amount: float
    main_power: float
    float_cap: float
    sector: str
    profit_mode: str
    push_title: str
    stop_loss: float
    take_profit: float
    score: float
    reason: str

def is_trading_day():
    return beijing_now().weekday() < 5

def phase():
    dt = beijing_now()
    h, m = dt.hour, dt.minute
    hm = h * 100 + m
    if 925 <= hm < 930:
        return "pre_open"
    elif 930 <= hm < 1130:
        return "morning"
    elif 1300 <= hm < 1457:
        return "afternoon"
    elif 1457 <= hm < 1500:
        return "close_auction"
    else:
        return "closed"

def send(title, content):
    if not SENDKEY:
        return False
    try:
        url = f"https://sctapi.ftqq.com/{SENDKEY}.send"
        requests.post(url, data={"title": title, "desp": content}, timeout=3)
        return True
    except Exception as e:
        log.warning(f"推送失败: {e}")
        return False

def get_all_codes():
    try:
        r = requests.get(
            "https://hq.sinajs.cn/rnall.php",
            headers={"Referer": "https://finance.sina.com"},
            timeout=3
        )
        codes = re.findall(r"[szsh](60|00|30|68)\d{4}", r.text)
        return list({c[-6:] for c in codes})
    except:
        return []

def get_sina_batch(codes):
    try:
        m = {"6":"sh","0":"sz","3":"sz"}
        syms = [f"{m.get(c[0],'sh')}{c}" for c in codes[:800]]
        url = f"https://hq.sinajs.cn/list={','.join(syms)}"
        r = requests.get(
            url,
            headers={"Referer": "https://finance.sina.com"},
            timeout=3
        )
        res = {}
        for line in r.text.splitlines():
            g = re.search(r'var hq_str_(sh|sz)(\d+)="(.*)";', line)
            if not g:
                continue
            _, code, body = g.groups()
            arr = body.split(",")
            if len(arr) < 10:
                continue
            res[code] = arr
        return res
    except:
        return {}

def calc_main_power(price, open_p, pre, amount):
    try:
        trend = (price - pre) / pre
        mom = (price - open_p) / open_p
        return (abs(trend) + abs(mom)) * amount / 10000
    except:
        return 0

def is_smooth_rise(price, high, low):
    try:
        return (high - price) < (high - low) * 0.3
    except:
        return True

def est_float_cap(code, price):
    try:
        base = {"6":50,"0":40,"3":25,"68":20}.get(code[:2], 30)
        return base * price * 100
    except:
        return 50

def scan_all():
    codes = get_all_codes()
    if not codes:
        return []
    data = get_sina_batch(codes)
    signals = []
    for code, arr in data.items():
        try:
            name = arr[0]
            op = float(arr[1])
            pre = float(arr[2])
            p = float(arr[3])
            h = float(arr[4])
            l = float(arr[5])
            amt = float(arr[9])
            if pre <= 0 or "ST" in name or "退" in name:
                continue
            chg = (p - pre) / pre
            smooth = is_smooth_rise(p, h, l)
            mp = calc_main_power(p, op, pre, amt)
            cap = est_float_cap(code, p)
            if mp < 80 or amt < 20000000:
                continue
            zt = pre * (1.2 if code.startswith(("3","68")) else 1.1)
            ztgap = (zt - p) / zt
            sl, tp, title, mode = 0,0,"",""
            if ztgap >= 0.005 and ztgap <= 0.08 and chg >= 0.05:
                sl,tp,title,mode = p*0.95, zt*0.98, "【首板·高胜率】", "首板"
            elif chg >= 0.04 and (h-l)/pre >= 0.03 and mp >= 100:
                sl,tp,title,mode = p*0.95, p*1.08, "【趋势·稳健】", "趋势"
            else:
                continue
            score = round(75 + min(30, mp/30), 1)
            if score < 80:
                continue
            signals.append(Signal(
                code=code,name=name,price=p,chg=round(chg*100,2),open_p=op,pre=pre,low=l,high=h,amount=amt,
                main_power=round(mp,1),float_cap=cap,sector="",profit_mode=mode,push_title=title,
                stop_loss=round(sl,2),take_profit=round(tp,2),score=score,reason="资金强势"
            ))
        except:
            continue
    return [s for s in signals if s.code not in PUSHED_TODAY]

def push_grouped(signals):
    groups = {}
    for s in signals:
        groups.setdefault(s.push_title, []).append(s)
    for title, lst in groups.items():
        if not lst:
            continue
        body = beijing_now().strftime("%H:%M") + "\n\n"
        for i, s in enumerate(sorted(lst, key=lambda x:x.score, reverse=True)):
            body += f"{title[:-1]} #{i+1} {s.name}({s.code})\n"
            body += f"现:{s.price} 涨:{s.chg}% 主:{s.main_power}万\n"
            body += f"损:{s.stop_loss} 盈:{s.take_profit} 分:{s.score}\n---\n"
        send(title, body.strip())
        for s in lst:
            PUSHED_TODAY.add(s.code)

def main():
    log.info("=== 最终强制北京时间版 ===")
    log.info(f"当前北京时间: {beijing_now()}")
    if not is_trading_day():
        log.info("非交易日")
        return
    log.info("系统启动成功，开始扫描")
    while True:
        current = phase()
        if current == "closed":
            log.info("非交易时段，等待开盘")
            time.sleep(60)
            continue
        try:
            sigs = scan_all()
            if sigs:
                push_grouped(sigs)
            log.info(f"【{beijing_now().strftime('%H:%M')}】扫描完成 | 信号: {len(sigs)}")
        except Exception as e:
            log.error(f"异常: {e}")
        time.sleep(25)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"崩溃: {e}")
