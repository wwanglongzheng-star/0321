import time
import requests
import re
from datetime import datetime
from dataclasses import dataclass

def now():
    return datetime.utcnow()

def beijing_now():
    return datetime.fromtimestamp(datetime.utcnow().timestamp() + 8 * 3600)

from config import *
from logger import log

PUSHED_TODAY = set()

@dataclass
class Signal:
    code: str
    name: str
    price: float
    chg: float
    amount: float
    push_title: str
    stop_loss: float
    take_profit: float
    reason: str

def is_trading_day():
    return beijing_now().weekday() < 5

def phase():
    dt = beijing_now()
    hm = dt.hour * 100 + dt.minute
    if 930 <= hm < 1130 or 1300 <= hm < 1457:
        return "trading"
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

# 只抓常见热门票，保证稳定出票
def get_stock_list():
    symbols = [
        "sh600000","sh600036","sh601318","sh600900","sh600104",
        "sz000001","sz000858","sz000568","sz002594","sz300750"
    ]
    try:
        url = f"https://hq.sinajs.cn/list={','.join(symbols)}"
        r = requests.get(url, timeout=3)
        res = {}
        for line in r.text.splitlines():
            g = re.search(r'hq_str_(sh|sz)(\d+)="(.*)"', line)
            if not g: continue
            _, code, body = g.groups()
            arr = body.split(",")
            if len(arr) < 10: continue
            res[code] = arr
        return res
    except:
        return {}

def scan():
    data = get_stock_list()
    signals = []
    for code, arr in data.items():
        try:
            name = arr[0]
            pre_close = float(arr[2])
            price = float(arr[3])
            amount = float(arr[9])

            if pre_close <= 0:
                continue
            if "ST" in name or "退" in name or "N" in name:
                continue

            chg = (price - pre_close) / pre_close * 100

            # ========== 宽松策略，保证出票 ==========
            if chg > 2 and amount > 5000000:
                sl = round(price * 0.96, 2)
                tp = round(price * 1.06, 2)
                signals.append(Signal(
                    code=code,
                    name=name,
                    price=round(price, 2),
                    chg=round(chg, 2),
                    amount=round(amount/10000, 2),
                    push_title="【短线信号】",
                    stop_loss=sl,
                    take_profit=tp,
                    reason=f"涨幅{chg:.1f}%，放量活跃"
                ))
        except:
            continue
    return [s for s in signals if s.code not in PUSHED_TODAY]

def push(signals):
    if not signals:
        return
    msg = f"{beijing_now().strftime('%H:%M')}\n\n"
    for s in signals[:3]:
        msg += f"{s.name}({s.code})\n"
        msg += f"价格：{s.price}  涨幅：{s.chg}%\n"
        msg += f"止损：{s.stop_loss}  止盈：{s.take_profit}\n"
        msg += "---\n"
        PUSHED_TODAY.add(s.code)
    send("A股短线信号", msg)

def main():
    log.info("=== 宽松可出票版 A股监控 ===")
    log.info(f"北京时间：{beijing_now()}")
    if not is_trading_day():
        log.info("非交易日")
        return

    while True:
        if phase() != "trading":
            log.info("非交易时段，等待...")
            time.sleep(60)
            continue

        sigs = scan()
        if sigs:
            push(sigs)
        log.info(f"扫描完成，信号数量：{len(sigs)}")
        time.sleep(30)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"异常：{e}")
