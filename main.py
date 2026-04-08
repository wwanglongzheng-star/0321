import time
import requests
import re
from datetime import datetime
from dataclasses import dataclass

# 强制北京时间
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
    reason: str

def is_trading_day():
    return beijing_now().weekday() < 5

def is_trading_time():
    dt = beijing_now()
    hm = dt.hour * 100 + dt.minute
    return (930 <= hm <= 1130) or (1300 <= hm <= 1457)

# 推送
def send(title, content):
    if not SENDKEY:
        return False
    try:
        url = f"https://sctapi.ftqq.com/{SENDKEY}.send"
        requests.post(url, data={"title": title, "desp": content}, timeout=3)
        return True
    except:
        return False

# 获取全市场股票列表
def get_stock_codes():
    try:
        r = requests.get("https://hq.sinajs.cn/rnall.php", headers={"Referer": "https://finance.sina.com"}, timeout=3)
        codes = re.findall(r'(sh|sz)(60|00|30)\d{4}', r.text)
        return [f"{x}{y}" for x,y in codes][:300]
    except:
        return []

# 获取行情
def get_batch_data(symbols):
    try:
        url = f"https://hq.sinajs.cn/list={','.join(symbols)}"
        r = requests.get(url, headers={"Referer": "https://finance.sina.com"}, timeout=3)
        res = {}
        for line in r.text.splitlines():
            g = re.search(r'hq_str_(sh|sz)(\d+)="(.*)";', line)
            if not g:
                continue
            _, code, body = g.groups()
            arr = body.split(",")
            if len(arr) >= 10:
                res[code] = arr
        return res
    except:
        return {}

# 宽松扫描策略：必出票
def scan():
    symbols = get_stock_codes()
    if not symbols:
        return []
    data = get_batch_data(symbols)
    signals = []

    for code, arr in data.items():
        try:
            name = arr[0]
            pre = float(arr[2])
            price = float(arr[3])
            amount = float(arr[9])

            if pre <= 0:
                continue
            if "ST" in name or "退" in name or "N" in name:
                continue

            chg = (price - pre) / pre * 100

            # 极宽松条件：涨幅>1% 且 成交额>100万
            if chg > 1 and amount > 1000000:
                signals.append(Signal(
                    code=code,
                    name=name,
                    price=round(price, 2),
                    chg=round(chg, 2),
                    amount=round(amount/10000, 2),
                    reason=f"涨幅{chg:.1f}%，放量"
                ))
        except:
            continue

    return [s for s in signals if s.code not in PUSHED_TODAY]

# 推送信号
def push_signals(signals):
    if not signals:
        return
    msg = f"【{beijing_now().strftime('%H:%M')} 短线信号】\n\n"
    count = 0
    for s in signals:
        if count >= 3:
            break
        msg += f"{s.name}({s.code})\n"
        msg += f"价格：{s.price} | 涨幅：{s.chg}%\n"
        msg += f"成交额：{s.amount} 万\n"
        msg += "---\n"
        PUSHED_TODAY.add(s.code)
        count += 1
    send("A股量化信号", msg)

def main():
    log.info("=== 宽松必出票版量化监控 ===")
    log.info(f"当前北京时间：{beijing_now()}")

    if not is_trading_day():
        log.info("非交易日，退出")
        return

    while True:
        if not is_trading_time():
            log.info("非交易时段，等待...")
            time.sleep(60)
            continue

        signals = scan()
        push_signals(signals)
        log.info(f"扫描完成，本次信号数：{len(signals)}")
        time.sleep(30)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"异常：{e}")
