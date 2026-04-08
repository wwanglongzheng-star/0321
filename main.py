import time
import requests
import re
from datetime import datetime
from dataclasses import dataclass

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

def send(title, content):
    if not SENDKEY:
        return False
    try:
        url = f"https://sctapi.ftqq.com/{SENDKEY}.send"
        requests.post(url, data={"title": title, "desp": content}, timeout=3)
        return True
    except:
        return False

# ======================
# 获取全市场 A 股代码
# ======================
def get_all_a_share_codes():
    try:
        r = requests.get(
            "https://hq.sinajs.cn/rnall.php",
            headers={"Referer": "https://finance.sina.com"},
            timeout=5
        )
        # 匹配所有 A 股：60/00/30 开头
        symbols = re.findall(r'(sh60\d{4}|sz00\d{4}|sz30\d{4})', r.text)
        return list(set(symbols))  # 去重
    except Exception as e:
        log.error(f"获取股票列表失败: {e}")
        return []

# ======================
# 批量获取全市场行情
# ======================
def get_market_data(symbols, batch_size=800):
    data = {}
    try:
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            url = f"https://hq.sinajs.cn/list={','.join(batch)}"
            r = requests.get(
                url,
                headers={"Referer": "https://finance.sina.com"},
                timeout=5
            )
            for line in r.text.splitlines():
                g = re.search(r'hq_str_(sh|sz)(\d+)="(.*)";', line)
                if not g:
                    continue
                _, code, body = g.groups()
                arr = body.split(",")
                if len(arr) >= 10:
                    data[code] = arr
            time.sleep(0.5)
    except Exception as e:
        log.error(f"行情获取异常: {e}")
    return data

# ======================
# 全市场扫描策略
# ======================
def scan_whole_market():
    symbols = get_all_a_share_codes()
    if not symbols:
        return []

    data = get_market_data(symbols)
    signals = []

    for code, arr in data.items():
        try:
            name = arr[0]
            pre_close = float(arr[2])
            price = float(arr[3])
            amount = float(arr[9])

            # 过滤异常
            if pre_close <= 0 or price <= 0:
                continue
            # 过滤风险股
            if "ST" in name or "退" in name or "N" in name:
                continue

            chg = (price - pre_close) / pre_close * 100

            # ========= 正常实盘策略，不苛刻也不乱来 =========
            if chg > 2 and amount > 20000000:  # 涨幅>2% + 成交额>2000万
                signals.append(Signal(
                    code=code,
                    name=name,
                    price=round(price, 2),
                    chg=round(chg, 2),
                    amount=round(amount / 10000, 2),
                    reason=f"涨幅{chg:.1f}%，放量走强"
                ))
        except:
            continue

    return [s for s in signals if s.code not in PUSHED_TODAY]

# ======================
# 推送信号（每天每只只推一次）
# ======================
def push(signals):
    if not signals:
        return

    msg = f"【全市场量化信号 {beijing_now().strftime('%H:%M')}】\n\n"
    cnt = 0
    for s in signals:
        if cnt >= 3:
            break
        msg += f"{s.name} ({s.code})\n"
        msg += f"价格：{s.price}  涨幅：{s.chg}%\n"
        msg += f"成交额：{s.amount} 万\n"
        msg += "---\n"
        PUSHED_TODAY.add(s.code)
        cnt += 1

    send("A股全市场信号", msg)

# ======================
# 主程序
# ======================
def main():
    log.info("=== A 股全市场量化扫描系统 ===")
    log.info(f"当前北京时间: {beijing_now()}")

    if not is_trading_day():
        log.info("非交易日，退出")
        return

    log.info("进入交易时段监控")

    while True:
        if not is_trading_time():
            log.info("非交易时间，等待...")
            time.sleep(60)
            continue

        signals = scan_whole_market()
        push(signals)

        log.info(f"全市场扫描完成 | 总信号: {len(signals)}")
        time.sleep(60)  # 1分钟扫一次

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"系统异常: {e}")
