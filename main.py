import time
import requests
import re
from datetime import datetime
from dataclasses import dataclass

from config import *
from logger import log

PUSHED_TODAY = set()
JJPUSH = False
OPEN_PUSH = False

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

def now():
    return datetime.now()

def is_trading_day():
    return now().weekday() < 5

def phase():
    h, m = now().hour, now().minute
    hm = h * 100 + m
    if 925 <= hm < 930:
        return "call_auction"
    elif 930 <= hm < 931:
        return "open_minute"
    elif 931 <= hm < 1130:
        return "morning"
    elif 1300 <= hm < 1430:
        return "afternoon"
    elif 1430 <= hm < 1500:
        return "endgame"
    return "closed"

def send(title, content):
    if not SENDKEY:
        return False
    try:
        url = f"https://sctapi.ftqq.com/{SENDKEY}.send"
        requests.post(url, data={"title": title, "desp": content}, timeout=3)
        return True
    except:
        return False

def get_all_codes():
    try:
        r = requests.get("https://hq.sinajs.cn/rnall.php", headers={"Referer": "https://finance.sina.com/"}, timeout=2)
        codes = re.findall(r"[szsh](60|00|30|68)\d{4}", r.text)
        return list({c[-6:] for c in codes})
    except:
        return []

def get_sina_batch(codes):
    try:
        m = {"6":"sh","0":"sz","3":"sz"}
        syms = [f"{m.get(c[0],'sh')}{c}" for c in codes[:1200]]
        url = f"https://hq.sinajs.cn/list={','.join(syms)}"
        r = requests.get(url, headers={"Referer": "https://finance.sina.com/"}, timeout=2)
        res = {}
        for line in r.text.splitlines():
            g = re.search(r'var hq_str_(sh|sz)(\d+)="(.*)";', line)
            if not g:
                continue
            _, code, body = g.groups()
            arr = body.split(",")
            if len(arr) < 30:
                continue
            res[code] = arr
        return res
    except:
        return {}

def calc_main_power(price, open_p, pre, amount, high, low):
    try:
        trend = (price - pre) / pre
        mom = (price - open_p) / open_p
        sign = 1 if trend > -0.015 and mom > -0.02 else -1
        return sign * (abs(trend) + abs(mom)) * amount / 10000
    except:
        return 0

def is_smooth_rise(price, high, low):
    try:
        return (high - price) < (high - low) * 0.3
    except:
        return False

def est_float_cap(code, price):
    try:
        base = {"6":50,"0":40,"3":25,"6":20}.get(code[0],30)
        return base * price * 100
    except:
        return 50

def scan_call_auction():
    codes = get_all_codes()
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

            if pre <= 0 or "ST" in name or "退" in name or "N" in name:
                continue

            chg = (p - pre) / pre
            og = (op - pre) / pre
            smooth = is_smooth_rise(p, h, l)
            mp = calc_main_power(p, op, pre, amt, h, l)
            cap = est_float_cap(code, p)

            if not (og >= 0.03 and chg >= 0.05 and amt >= 30000000 and mp >= 200 and smooth and 20 <= cap <= 200):
                continue

            zt = pre * (1.2 if code.startswith(("3","68")) else 1.1)
            score = round(90 + (12 if code[0] in ("3","6") else 4) + min(15, mp/50), 1)
            if score < 94:
                continue

            signals.append(Signal(
                code=code, name=name, price=p, chg=round(chg*100,2),
                open_p=op, pre=pre, low=l, high=h, amount=amt,
                main_power=round(mp,1), float_cap=cap, sector="竞价强势", profit_mode="竞价直线冲板",
                push_title="【9:25竞价·直线冲板】",
                stop_loss=round(p*0.92,2), take_profit=round(zt*0.98,2), score=score, reason="平滑拉升+量价齐升+市值适中"
            ))
        except:
            continue
    return [s for s in signals if s.code not in PUSHED_TODAY]

def scan_open_minute():
    codes = get_all_codes()
    data = get_sina_batch(codes)
    signals = []
    for code, arr in data.items():
        try:
            name = arr[0]
            op = float(arr[1])
            pre = float(arr[2])
            p = float(arr[3])
            amt = float(arr[9])

            if pre <= 0 or "ST" in name or "退" in name:
                continue

            chg = (p - pre) / pre
            mp = calc_main_power(p, op, pre, amt, p, p)
            cap = est_float_cap(code, p)

            if not (chg >= 0.07 and p > op * 1.01 and amt >= 50000000 and mp >= 300 and 20 <= cap <= 200):
                continue

            zt = pre * (1.2 if code.startswith(("3","68")) else 1.1)
            score = round(92 + min(18, mp/40), 1)
            if score < 95:
                continue

            signals.append(Signal(
                code=code, name=name, price=p, chg=round(chg*100,2),
                open_p=op, pre=pre, low=p, high=p, amount=amt,
                main_power=round(mp,1), float_cap=cap, sector="开盘爆量", profit_mode="开盘瞬间爆拉",
                push_title="【9:30开盘·瞬间爆拉】",
                stop_loss=round(p*0.91,2), take_profit=round(zt*0.98,2), score=score, reason="9:30秒拉+真资金+市值健康"
            ))
        except:
            continue
    return [s for s in signals if s.code not in PUSHED_TODAY]

def scan_all_mode():
    current_phase = phase()
    codes = get_all_codes()
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
            og = (op - pre) / pre
            smooth = is_smooth_rise(p, h, l)
            mp = calc_main_power(p, op, pre, amt, h, l)
            cap = est_float_cap(code, p)

            if mp < 150 or amt < 40000000 or not (20 <= cap <= 200):
                continue

            zt = pre * (1.2 if code.startswith(("3","68")) else 1.1)
            ztgap = (zt - p) / zt

            sl, tp, title, mode = 0,0,"",""

            if ztgap >= 0.005 and ztgap <= 0.05 and chg >= 0.07 and p > h * 0.985 and smooth:
                sl,tp,title,mode = p*0.92, zt*0.98, "【首板·高胜率】", "首板"
            elif og >= 0.025 and (op-p)/op >= 0.02 and (p-l)/l >= 0.012 and -0.01 <= chg <= 0.07 and smooth:
                sl,tp,title,mode = p*0.90, p*1.15, "【连板妖股·高爆发】", "连板"
            elif chg >= 0.05 and (h-l)/pre >= 0.04 and mp >= 200 and (h-p)/p <= 0.03:
                sl,tp,title,mode = p*0.93, p*1.10, "【大肉趋势·稳健】", "趋势"
            elif current_phase == "endgame" and chg >= 0.04 and mp >= 250 and p > op * 0.995:
                sl,tp,title,mode = p*0.94, p*1.08, "【尾盘回封·弱转强】", "尾盘"
            else:
                continue

            score = round(85 + (12 if code[0] in ("3","68") else 5) + min(20, mp/50) + (8 if mode in ("首板","连板") else 3), 1)
            if score < 93:
                continue

            signals.append(Signal(
                code=code, name=name, price=p, chg=round(chg*100,2),
                open_p=op, pre=pre, low=l, high=h, amount=amt,
                main_power=round(mp,1), float_cap=cap, sector="主线热点", profit_mode=mode,
                push_title=title, stop_loss=round(sl,2), take_profit=round(tp,2), score=score,
                reason=f"{mode}+主力资金+平滑上涨"
            ))
        except:
            continue
    return [s for s in signals if s.code not in PUSHED_TODAY]

def push_grouped(signals):
    groups = {}
    for s in signals:
        if s.push_title not in groups:
            groups[s.push_title] = []
        groups[s.push_title].append(s)

    for title, group in groups.items():
        if not group:
            continue
        body = f"{now().strftime('%H:%M')}\n\n"
        for i, s in enumerate(sorted(group, key=lambda x: x.score, reverse=True)):
            body += f"{title[:-1]} #{i+1} {s.name}({s.code})\n"
            body += f"现:{s.price} 涨:{s.chg}% 主:{s.main_power}万\n"
            body += f"损:{s.stop_loss} 盈:{s.take_profit} 分:{s.score}\n"
            body += f"{s.reason}\n---\n"
        send(title, body.strip("---\n"))
        for s in group:
            PUSHED_TODAY.add(s.code)

def main():
    global JJPUSH, OPEN_PUSH
    log.info("=== 终极全优化·全分类实盘系统 ===")
    if not is_trading_day():
        log.info("非交易日，退出")
        return
    send("系统启动", "全模式+全优化+分类推送·高胜率低炸板")
    log.info("系统启动成功")

    while True:
        current = phase()
        if current == "closed":
            send("收盘", "今日交易结束")
            log.info("收盘退出")
            break

        if current == "call_auction" and not JJPUSH:
            push_grouped(scan_call_auction())
            JJPUSH = True
            time.sleep(2)
            continue

        if current == "open_minute" and not OPEN_PUSH:
            push_grouped(scan_open_minute())
            OPEN_PUSH = True
            time.sleep(2)
            continue

        push_grouped(scan_all_mode())
        time.sleep(22)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"异常: {e}")
        send("系统异常", str(e))
