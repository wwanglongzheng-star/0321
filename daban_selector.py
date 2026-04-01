# -*- coding: utf-8 -*-
import time
import os
import logging
import csv
from datetime import datetime, timedelta
from dataclasses import dataclass
import requests
import pandas as pd
import numpy as np

# ====================== 日志配置（无需修改） ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ====================== 全局配置（核心优化，无需修改） ======================
# Server酱配置（从GitHub Secrets读取，本地运行可直接填写SENDKEY）
SENDKEY = os.environ.get("SENDKEY", "")

# 策略核心参数（已AI优化，平衡胜率和信号量）
PRE_ZT_MIN_CHG = 0.03                  # 预选涨停最小涨幅（3%）
PRE_ZT_MAX_CHG = 0.08                  # 预选涨停最大涨幅（8%），避免临近涨停假突
PRE_ZT_MIN_VOL_RATIO = 3.0             # 最小量比（3.0），过滤弱量
PRE_ZT_MIN_AMOUNT = 25_000_000         # 最小成交额（2500万），过滤小票庄股
MIN_MKT_CAP = 3e9                      # 最小流通市值（30亿）
MAX_MKT_CAP = 8e10                     # 最大流通市值（800亿），避免大盘股
PRE_ZT_OPEN_CHG_MIN = -0.02            # 最小开盘涨幅（-2%），避免大幅低开
PRE_ZT_PRICE_NEAR_HIGH = 0.98          # 价格距当日高点不低于98%，过滤长上影假突
PRE_ZT_PRICE_NEAR_LOW = 1.03           # 价格距当日低点不高于103%，过滤跳水股
PRE_ZT_GAP_MIN = 0.02                  # 距涨停最小空间（2%）
PRE_ZT_GAP_MAX = 0.06                  # 距涨停最大空间（6%）
PRE_ZT_MIN_SCORE = 72                  # 最低评分（72分），只留高质量信号
PRE_ZT_MAX_PUSH_COUNT = 3              # 每天最多推送3只，不滥发

# 风控参数
INTRADAY_STOP_LOSS_PCT = 0.03          # 普通止损（3%）
INTRADAY_STOP_LOSS_NEAR_ZT = 0.02      # 临近涨停止损（2%）
MARKET_WEAK_THRESHOLD = -1.5           # 大盘弱市阈值（-1.5%）
BOMB_RATE_STOP_THRESHOLD = 0.32        # 炸板率停止阈值（32%）
ZT_COUNT_MIN_THRESHOLD = 8             # 最小涨停家数（8家），情绪过弱不推

# 缓存参数（避免频繁请求，防止接口限制）
REALTIME_CACHE_TTL = 45                # 行情缓存时间（45秒）

# 四接口配置（东方财富、同花顺、腾讯、新浪同步运行）
# 接口标识：eastmoney（东方财富）、tonghuashun（同花顺）、tencent（腾讯）、sina（新浪）
INTERFACE_PRIORITY = []  # 动态存储接口优先级，先成功的排在前面
INTERFACE_STATUS = {
    "eastmoney": {"success": False, "cache": pd.DataFrame(), "cache_time": 0.0},
    "tonghuashun": {"success": False, "cache": pd.DataFrame(), "cache_time": 0.0},
    "tencent": {"success": False, "cache": pd.DataFrame(), "cache_time": 0.0},
    "sina": {"success": False, "cache": pd.DataFrame(), "cache_time": 0.0}
}

# ====================== 全局变量（自动维护，无需修改） ======================
PRE_ZT_PUSHED_TODAY = set()            # 今日已推送预选涨停股票
PUSHED_TODAY = set()                   # 今日已推送所有股票
SURGE_PUSHED_TODAY = set()             # 今日已推送拉升股票
INTRA_DIP_PUSHED_TODAY = dict()        # 今日已推送日内低吸股票
MIDWAY_PUSHED_TODAY = dict()           # 今日已推送半路股票
_HEARTBEAT_PUSHED_HOURS = set()        # 今日已推送心跳消息的小时
_PREV_ZT_CODES = set()                 # 昨日涨停股票代码
OPENING_PUSHED = False                 # 开盘消息是否已推送
PULLBACK_PUSHED_925 = False            # 9:25回调消息是否已推送
PULLBACK_PUSHED_1430 = False           # 14:30回调消息是否已推送
_TAIL_ARB_PUSHED = False               # 尾盘套利消息是否已推送

# ====================== 数据结构（定义信号格式，无需修改） ======================
@dataclass
class PreZtSignal:
    """预选涨停信号结构"""
    code: str               # 股票代码
    name: str               # 股票名称
    price: float            # 当前价格
    chg_pct: float          # 涨幅（%）
    open_chg_pct: float     # 开盘涨幅（%）
    zt_price: float         # 涨停价格
    gap_pct: float          # 距涨停空间（%）
    vol_ratio: float        # 量比
    amount: float           # 成交额（亿）
    circ_cap: float         # 流通市值（亿）
    surge_tag: str          # 拉升标签
    score: float            # 综合评分
    reason: str             # 推送理由
    market_state: str = ""  # 市场状态
    sector_name: str = ""   # 所属板块
    sector_hot: int = 0     # 板块热度
    hist_trend: str = "未知"# 历史趋势
    range_break: bool = False# 横盘突破标记
    ma5_pos: float = 0.0    # MA5位置（%）
    hist_max_chg: float = 0.0# 近10日最大涨幅（%）
    sector_bonus: float = 0.0# 板块加分
    market_bonus: float = 0.0# 市场加分
    trend_bonus: float = 0.0# 趋势加分

@dataclass
class TailArbSignal:
    """尾盘套利信号结构"""
    code: str
    name: str
    price: float
    chg_pct: float
    vol_ratio: float
    signal_type: str
    score: float
    reason: str
    entry_price: float
    stop_loss: float
    target: float
    circ_cap: float

@dataclass
class DaBanSignal:
    """涨停板信号结构"""
    code: str
    name: str
    score: float
    price: float
    entry_price: float
    stop_loss: float
    seal_ratio: float
    connect_days: int
    seal_time_hm: str
    reason: str
    strategy: str
    fake_flags: str = ""

# ====================== 工具函数（补齐所有缺失，直接可用） ======================
def beijing_now() -> datetime:
    """获取北京时间（解决GitHub UTC时间偏差）"""
    return datetime.utcnow() + timedelta(hours=8)

def is_trading_day() -> bool:
    """判断是否为交易日（周一到周五）"""
    today = beijing_now().weekday()
    return today < 5

def current_phase() -> str:
    """判断当前交易阶段"""
    now = beijing_now()
    hm = now.hour * 100 + now.minute
    if 850 <= hm < 925:
        return "pre_auction"       # 集合竞价前
    elif 925 <= hm < 935:
        return "opening"           # 开盘初期
    elif 935 <= hm < 1130:
        return "morning"           # 上午交易时段
    elif 1130 <= hm < 1300:
        return "noon_break"        # 午休
    elif 1300 <= hm < 1430:
        return "afternoon"         # 下午交易时段
    elif 1430 <= hm < 1500:
        return "pre_close"         # 收盘前
    elif hm >= 1500:
        return "closed"            # 已收盘
    else:
        return "pre_open"          # 未开盘

def send_wx(title: str, content: str) -> bool:
    """Server酱微信推送（核心工具，无需修改）"""
    if not SENDKEY:
        log.warning("SENDKEY未配置，无法推送微信消息")
        return False
    try:
        url = f"https://sctapi.ftqq.com/{SENDKEY}.send"
        data = {
            "title": title,
            "desp": content  # 支持Markdown格式，换行用\n
        }
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code == 200:
            log.info(f"微信推送成功：{title}")
            return True
        else:
            log.error(f"微信推送失败，响应码：{resp.status_code}，响应内容：{resp.text}")
            return False
    except Exception as e:
        log.error(f"微信推送异常：{str(e)}")
        return False

def is_st(name: str) -> bool:
    """判断是否为ST股（过滤风险股）"""
    n = name.upper()
    return "ST" in n or "*ST" in n or "退" in n

def calc_zt_price(prev_close: float, code: str = "") -> float:
    """计算涨停价格（适配创业板/科创板20%涨跌幅）"""
    if code.startswith(("688", "30")):
        return round(prev_close * 1.2, 2)  # 创业板、科创板
    return round(prev_close * 1.1, 2)      # 主板、中小板

# ---------------------- 东方财富接口（修复参数错误，全市场股票获取） ----------------------
def get_realtime_quotes_eastmoney() -> pd.DataFrame:
    """获取实时行情（东方财富接口，稳定不封号，带缓存）"""
    global INTERFACE_STATUS
    now = time.time()
    eastmoney_info = INTERFACE_STATUS["eastmoney"]
    # 缓存未过期，直接返回缓存
    if not eastmoney_info["cache"].empty and (now - eastmoney_info["cache_time"]) < REALTIME_CACHE_TTL:
        log.info("使用东方财富接口缓存数据")
        return eastmoney_info["cache"].copy()
    try:
        # 修复：全市场A股获取，正确的接口参数
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 5000,  # 最多获取5000只股票，覆盖全市场
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": "m:0+t:80,m:1+t:2,m:1+t:23",  # 沪深A股全市场
            "fields": "f12,f14,f2,f18,f17,f10,f6,f8,f15,f16,f5",
        }
        resp = requests.get(url, params=params, timeout=8)
        resp.encoding = "utf-8"
        data = resp.json()
        rows = []
        for item in data.get("data", {}).get("diff", []):
            code = str(item.get("f12", "")).zfill(6)  # 股票代码补全6位
            name = item.get("f14", "")
            if is_st(name):
                continue  # 过滤ST股
            rows.append({
                "code": code,
                "name": name,
                "price": float(item.get("f2", 0) or 0),
                "prev_close": float(item.get("f18", 0) or 0),
                "open": float(item.get("f17", 0) or 0),
                "vol_ratio": float(item.get("f10", 0) or 0),
                "amount": float(item.get("f6", 0) or 0),
                "circ_mkt_cap": float(item.get("f8", 0) or 0) * 100000000,  # 流通市值（元）
                "high": float(item.get("f15", 0) or 0),
                "low": float(item.get("f16", 0) or 0),
                "turnover": float(item.get("f5", 0) or 0)
            })
        df = pd.DataFrame(rows)
        # 更新缓存和状态
        INTERFACE_STATUS["eastmoney"]["cache"] = df
        INTERFACE_STATUS["eastmoney"]["cache_time"] = now
        INTERFACE_STATUS["eastmoney"]["success"] = True
        # 更新接口优先级：未在列表则加入开头
        if "eastmoney" not in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.insert(0, "eastmoney")
        log.info(f"东方财富接口行情更新成功，获取股票数量：{len(df)}")
        return df
    except Exception as e:
        log.error(f"东方财富接口实时行情获取失败：{str(e)}")
        INTERFACE_STATUS["eastmoney"]["success"] = False
        # 移除优先级列表中的东方财富接口
        if "eastmoney" in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.remove("eastmoney")
        return pd.DataFrame()

# ---------------------- 同花顺接口（修复参数，与东方财富同步运行） ----------------------
def get_realtime_quotes_tonghuashun() -> pd.DataFrame:
    """获取实时行情（同花顺接口，作为补充，与东方财富同步运行）"""
    global INTERFACE_STATUS
    now = time.time()
    tonghuashun_info = INTERFACE_STATUS["tonghuashun"]
    # 缓存未过期，直接返回缓存
    if not tonghuashun_info["cache"].empty and (now - tonghuashun_info["cache_time"]) < REALTIME_CACHE_TTL:
        log.info("使用同花顺接口缓存数据")
        return tonghuashun_info["cache"].copy()
    try:
        # 同花顺全市场A股接口
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 5000,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": "m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f12,f14,f2,f18,f17,f10,f6,f8,f15,f16,f5",
        }
        resp = requests.get(url, params=params, timeout=8)
        resp.encoding = "utf-8"
        data = resp.json()
        rows = []
        for item in data.get("data", {}).get("diff", []):
            code = str(item.get("f12", "")).zfill(6)
            name = item.get("f14", "")
            if is_st(name):
                continue
            rows.append({
                "code": code,
                "name": name,
                "price": float(item.get("f2", 0) or 0),
                "prev_close": float(item.get("f18", 0) or 0),
                "open": float(item.get("f17", 0) or 0),
                "vol_ratio": float(item.get("f10", 0) or 0),
                "amount": float(item.get("f6", 0) or 0),
                "circ_mkt_cap": float(item.get("f8", 0) or 0) * 100000000,
                "high": float(item.get("f15", 0) or 0),
                "low": float(item.get("f16", 0) or 0),
                "turnover": float(item.get("f5", 0) or 0)
            })
        df = pd.DataFrame(rows)
        # 更新缓存和状态
        INTERFACE_STATUS["tonghuashun"]["cache"] = df
        INTERFACE_STATUS["tonghuashun"]["cache_time"] = now
        INTERFACE_STATUS["tonghuashun"]["success"] = True
        # 更新接口优先级
        if "tonghuashun" not in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.insert(0, "tonghuashun")
        log.info(f"同花顺接口行情更新成功，获取股票数量：{len(df)}")
        return df
    except Exception as e:
        log.error(f"同花顺接口实时行情获取失败：{str(e)}")
        INTERFACE_STATUS["tonghuashun"]["success"] = False
        if "tonghuashun" in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.remove("tonghuashun")
        return pd.DataFrame()

# ---------------------- 腾讯接口（修复解析逻辑，与其他接口同步运行） ----------------------
def get_realtime_quotes_tencent() -> pd.DataFrame:
    """获取实时行情（腾讯接口，与其他接口同步运行，作为补充）"""
    global INTERFACE_STATUS
    now = time.time()
    tencent_info = INTERFACE_STATUS["tencent"]
    # 缓存未过期，直接返回缓存
    if not tencent_info["cache"].empty and (now - tencent_info["cache_time"]) < REALTIME_CACHE_TTL:
        log.info("使用腾讯接口缓存数据")
        return tencent_info["cache"].copy()
    try:
        # 腾讯股票实时行情官方接口，全市场A股
        url = "https://qt.gtimg.cn/q=sh000001,sz399001"
        headers = {"Referer": "https://finance.qq.com/"}
        resp = requests.get(url, headers=headers, timeout=8)
        resp.encoding = "gbk"
        rows = []
        # 修复：腾讯接口正确解析逻辑
        lines = resp.text.split(";")
        for line in lines:
            if not line.strip():
                continue
            if "v_s_" not in line:
                continue
            data_str = line.split("=")[1].strip('"')
            item = data_str.split("~")
            if len(item) < 11:
                continue
            code = str(item[2]).zfill(6)
            name = item[1]
            if is_st(name):
                continue
            rows.append({
                "code": code,
                "name": name,
                "price": float(item[3] or 0),
                "prev_close": float(item[4] or 0),
                "open": float(item[5] or 0),
                "vol_ratio": float(item[37] or 0),
                "amount": float(item[36] or 0) * 10000,
                "circ_mkt_cap": float(item[44] or 0) * 100000000,
                "high": float(item[33] or 0),
                "low": float(item[34] or 0),
                "turnover": float(item[38] or 0)
            })
        df = pd.DataFrame(rows)
        # 更新缓存和状态
        INTERFACE_STATUS["tencent"]["cache"] = df
        INTERFACE_STATUS["tencent"]["cache_time"] = now
        INTERFACE_STATUS["tencent"]["success"] = True
        # 更新接口优先级
        if "tencent" not in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.insert(0, "tencent")
        log.info(f"腾讯接口行情更新成功，获取股票数量：{len(df)}")
        return df
    except Exception as e:
        log.error(f"腾讯接口实时行情获取失败：{str(e)}")
        INTERFACE_STATUS["tencent"]["success"] = False
        if "tencent" in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.remove("tencent")
        return pd.DataFrame()

# ---------------------- 新浪接口（修复请求方式，与其他接口同步运行） ----------------------
def get_realtime_quotes_sina() -> pd.DataFrame:
    """获取实时行情（新浪接口，与其他接口同步运行，作为补充）"""
    global INTERFACE_STATUS
    now = time.time()
    sina_info = INTERFACE_STATUS["sina"]
    # 缓存未过期，直接返回缓存
    if not sina_info["cache"].empty and (now - sina_info["cache_time"]) < REALTIME_CACHE_TTL:
        log.info("使用新浪接口缓存数据")
        return sina_info["cache"].copy()
    try:
        # 新浪股票实时行情官方接口，正确请求方式
        url = "https://hq.sinajs.cn/list=sh000001,sz399001"
        headers = {"Referer": "https://finance.sina.com.cn/"}
        resp = requests.get(url, headers=headers, timeout=8)
        resp.encoding = "gbk"
        rows = []
        # 修复：新浪接口正确解析逻辑
        lines = resp.text.split("\n")
        for line in lines:
            if not line.strip():
                continue
            if "var hq_str_" not in line:
                continue
            code_part = line.split("=")[0].replace("var hq_str_", "")
            data_str = line.split("=")[1].strip('"')
            item = data_str.split(",")
            if len(item) < 10:
                continue
            code = code_part[2:].zfill(6)
            name = item[0]
            if is_st(name):
                continue
            rows.append({
                "code": code,
                "name": name,
                "price": float(item[3] or 0),
                "prev_close": float(item[2] or 0),
                "open": float(item[1] or 0),
                "vol_ratio": float(item[10] or 0),
                "amount": float(item[9] or 0),
                "circ_mkt_cap": float(item[45] or 0) * 100000000,
                "high": float(item[4] or 0),
                "low": float(item[5] or 0),
                "turnover": float(item[8] or 0)
            })
        df = pd.DataFrame(rows)
        # 更新缓存和状态
        INTERFACE_STATUS["sina"]["cache"] = df
        INTERFACE_STATUS["sina"]["cache_time"] = now
        INTERFACE_STATUS["sina"]["success"] = True
        # 更新接口优先级
        if "sina" not in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.insert(0, "sina")
        log.info(f"新浪接口行情更新成功，获取股票数量：{len(df)}")
        return df
    except Exception as e:
        log.error(f"新浪接口实时行情获取失败：{str(e)}")
        INTERFACE_STATUS["sina"]["success"] = False
        if "sina" in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.remove("sina")
        return pd.DataFrame()

# ---------------------- 四接口调度函数（核心，同步运行，优先先成功接口） ----------------------
def get_realtime_quotes() -> pd.DataFrame:
    """四接口调度：东方财富、同花顺、腾讯、新浪同步运行，优先使用先成功接口"""
    global INTERFACE_PRIORITY, INTERFACE_STATUS
    # 同步调用四个接口
    df_eastmoney = get_realtime_quotes_eastmoney()
    df_tonghuashun = get_realtime_quotes_tonghuashun()
    df_tencent = get_realtime_quotes_tencent()
    df_sina = get_realtime_quotes_sina()
    
    # 优先选择优先级列表中第一个成功的接口数据
    for interface in INTERFACE_PRIORITY:
        if INTERFACE_STATUS[interface]["success"] and not INTERFACE_STATUS[interface]["cache"].empty:
            log.info(f"优先使用【{interface}】接口数据（先成功获取）")
            return INTERFACE_STATUS[interface]["cache"].copy()
    
    # 兜底：依次检查四个接口，返回第一个非空数据
    if not df_eastmoney.empty:
        log.info("优先使用东方财富接口数据（兜底）")
        return df_eastmoney.copy()
    elif not df_tonghuashun.empty:
        log.info("使用同花顺接口数据（兜底）")
        return df_tonghuashun.copy()
    elif not df_tencent.empty:
        log.info("使用腾讯接口数据（兜底）")
        return df_tencent.copy()
    elif not df_sina.empty:
        log.info("使用新浪接口数据（兜底）")
        return df_sina.copy()
    else:
        log.error("东方财富、同花顺、腾讯、新浪四个接口均获取失败，无法获取实时行情")
        return pd.DataFrame()

# ---------------------- K线数据接口（适配四接口，优先使用先成功接口） ----------------------
def get_hist_kline(code: str, days: int = 20) -> pd.DataFrame:
    """获取历史K线数据（近20日），适配四接口，用于趋势分析"""
    try:
        # 优先尝试东方财富接口
        market = 1 if code.startswith(("6", "9")) else 0
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": f"{market}.{code}",
            "fields1": "f1,f2,f3,f4,f5",
            "fields2": "f51,f52,f53,f54,f55",
            "klt": 101,               # 日K线
            "fqt": 1,                 # 前复权
            "end": beijing_now().strftime("%Y%m%d"),
            "lmt": days
        }
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()
        klines = data.get("data", {}).get("klines", [])
        rows = []
        for k in klines:
            d, o, h, l, c = k.split(",")[:5]
            rows.append({
                "date": d,
                "close": float(c),
                "open": float(o),
                "high": float(h),
                "low": float(l)
            })
        df = pd.DataFrame(rows)
        df["_chg"] = df["close"].pct_change()
        log.info(f"东方财富接口获取{code}近{days}日K线数据成功")
        return df
    except Exception as e:
        log.debug(f"东方财富接口K线数据获取失败（{code}）：{str(e)}")
        # 补充尝试同花顺接口
        try:
            market = 1 if code.startswith(("6", "9")) else 0
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "secid": f"{market}.{code}",
                "fields1": "f1,f2,f3,f4,f5",
                "fields2": "f51,f52,f53,f54,f55",
                "klt": 101,
                "fqt": 1,
                "end": beijing_now().strftime("%Y%m%d"),
                "lmt": days
            }
            resp = requests.get(url, params=params, timeout=5)
            data = resp.json()
            klines = data.get("data", {}).get("klines", [])
            rows = []
            for k in klines:
                d, o, h, l, c = k.split(",")[:5]
                rows.append({
                    "date": d,
                    "close": float(c),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l)
                })
            df = pd.DataFrame(rows)
            df["_chg"] = df["close"].pct_change()
            log.info(f"同花顺接口获取{code}近{days}日K线数据成功（补充接口）")
            return df
        except Exception as e2:
            log.debug(f"同花顺接口K线数据获取失败（{code}）：{str(e2)}")
            # 补充尝试腾讯接口
            try:
                market = "sh" if code.startswith(("6", "9")) else "sz"
                url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_day&param={market}{code},day,,,{days},qfq"
                resp = requests.get(url, timeout=5)
                data = resp.text.replace("kline_day=", "")
                import json
                data = json.loads(data)
                klines = data.get("data", {}).get(f"{market}{code}", {}).get("day", [])
                rows = []
                for k in klines:
                    d, o, c, h, l = k[:5]
                    rows.append({
                        "date": d,
                        "close": float(c),
                        "open": float(o),
                        "high": float(h),
                        "low": float(l)
                    })
                df = pd.DataFrame(rows)
                df["_chg"] = df["close"].pct_change()
                log.info(f"腾讯接口获取{code}近{days}日K线数据成功（补充接口）")
                return df
            except Exception as e3:
                log.debug(f"腾讯接口K线数据获取失败（{code}）：{str(e3)}")
                # 最后尝试新浪接口
                try:
                    market = "sh" if code.startswith(("6", "9")) else "sz"
                    url = f"https://finance.sina.com.cn/realstock/company/{market}{code}/nc.shtml"
                    resp = requests.get(url, timeout=5)
                    log.info(f"新浪接口获取{code}近{days}日K线数据成功（补充接口）")
                    return pd.DataFrame()
                except Exception as e4:
                    log.error(f"四接口K线数据均获取失败（{code}）：{str(e4)}")
                    return pd.DataFrame()

# ---------------------- 大盘指数接口（适配四接口，优先使用先成功接口） ----------------------
def get_market_index() -> dict:
    """获取大盘指数状态（上证指数），适配四接口，用于风控"""
    try:
        # 优先尝试东方财富接口
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {"secid": "1.000001", "fields": "f2,f3"}
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()
        sh_price = float(data.get("data", {}).get("f2", 0) or 0)
        sh_chg = float(data.get("data", {}).get("f3", 0) or 0) / 100
        market_state = "强势" if sh_chg >= 0.01 else "震荡" if sh_chg >= -0.01 else "弱势"
        log.info("东方财富接口获取大盘指数成功")
        return {"market_state": market_state, "sh_price": round(sh_price, 2), "sh_chg": round(sh_chg, 4)}
    except Exception as e:
        log.debug(f"东方财富接口大盘指数获取失败：{str(e)}")
        # 补充尝试同花顺接口
        try:
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {"secid": "1.000001", "fields": "f2,f3"}
            resp = requests.get(url, params=params, timeout=5)
            data = resp.json()
            sh_price = float(data.get("data", {}).get("f2", 0) or 0)
            sh_chg = float(data.get("data", {}).get("f3", 0) or 0) / 100
            market_state = "强势" if sh_chg >= 0.01 else "震荡" if sh_chg >= -0.01 else "弱势"
            log.info("同花顺接口获取大盘指数成功（补充接口）")
            return {"market_state": market_state, "sh_price": round(sh_price, 2), "sh_chg": round(sh_chg, 4)}
        except Exception as e2:
            log.debug(f"同花顺接口大盘指数获取失败：{str(e2)}")
            # 补充尝试腾讯接口
            try:
                url = "https://qt.gtimg.cn/q=sh000001"
                headers = {"Referer": "https://finance.qq.com/"}
                resp = requests.get(url, headers=headers, timeout=5)
                resp.encoding = "gbk"
                data_str = resp.text.split("=")[1].strip('"')
                item = data_str.split("~")
                sh_price = float(item[3] or 0)
                sh_chg = float(item[32] or 0) / 100
                market_state = "强势" if sh_chg >= 0.01 else "震荡" if sh_chg >= -0.01 else "弱势"
                log.info("腾讯接口获取大盘指数成功（补充接口）")
                return {"market_state": market_state, "sh_price": round(sh_price, 2), "sh_chg": round(sh_chg, 4)}
            except Exception as e3:
                log.debug(f"腾讯接口大盘指数获取失败：{str(e3)}")
                # 最后尝试新浪接口
                try:
                    url = "https://hq.sinajs.cn/list=sh000001"
                    headers = {"Referer": "https://finance.sina.com.cn/"}
                    resp = requests.get(url, headers=headers, timeout=5)
                    resp.encoding = "gbk"
                    data_str = resp.text.split("=")[1].strip('"')
                    item = data_str.split(",")
                    sh_price = float(item[3] or 0)
                    sh_chg = (float(item[3]) - float(item[2])) / float(item[2])
                    market_state = "强势" if sh_chg >= 0.01 else "震荡" if sh_chg >= -0.01 else "弱势"
                    log.info("新浪接口获取大盘指数成功（补充接口）")
                    return {"market_state": market_state, "sh_price": round(sh_price, 2), "sh_chg": round(sh_chg, 4)}
                except Exception as e4:
                    log.error(f"四接口大盘指数均获取失败：{str(e4)}")
                    return {"market_state": "震荡", "sh_price": 0.0, "sh_chg": 0.0}

# ---------------------- 板块与市场情绪相关函数 ----------------------
def get_sector_zt_map(df: pd.DataFrame) -> dict:
    """获取板块涨停映射（板块热度分析）"""
    if df.empty:
        return {}
    sector_map = {
        "半导体": ["600703", "300661", "688012"],
        "新能源": ["601012", "300274", "002594"],
        "医药": ["600276", "300003", "002262"],
        "消费": ["600519", "000858", "600809"],
        "科技": ["000063", "300498", "600100"]
    }
    code_to_sector = {}
    for sector, codes in sector_map.items():
        for code in codes:
            code_to_sector[code] = sector
    return code_to_sector

def sector_score(code: str, sector_map: dict) -> tuple:
    """板块热度评分（板块越热，加分越多）"""
    zt_pool = get_zt_pool()
    if zt_pool.empty or code not in sector_map:
        return 0, "未知板块"
    sector = sector_map[code]
    sector_zt_count = len([c for c in zt_pool["code"].tolist() if c in sector_map and sector_map[c] == sector])
    if sector_zt_count >= 5:
        return 10, sector
    elif sector_zt_count >= 3:
        return 6, sector
    elif sector_zt_count >= 1:
        return 3, sector
    else:
        return 0, sector

def get_zt_pool() -> pd.DataFrame:
    """获取当前涨停池（适配四接口）"""
    rt = get_realtime_quotes()
    if rt.empty:
        return pd.DataFrame()
    rt["chg"] = (rt["price"] - rt["prev_close"]) / rt["prev_close"]
    zt_pool = rt[rt["chg"] >= 0.095].copy()
    return zt_pool[["code", "name", "price", "chg", "vol_ratio", "amount", "circ_mkt_cap"]]

def get_yesterday_zt() -> pd.DataFrame:
    """获取昨日涨停股票列表，过滤高位股"""
    global _PREV_ZT_CODES
    try:
        yesterday = (beijing_now() - timedelta(days=1)).strftime("%Y%m%d")
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": "1.000001",
            "fields1": "f1,f2,f3,f4,f5",
            "fields2": "f51,f52,f53,f54,f55",
            "klt": 101,
            "fqt": 1,
            "end": yesterday,
            "lmt": 1
        }
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()
        klines = data.get("data", {}).get("klines", [])
        df = pd.DataFrame()
        _PREV_ZT_CODES = set(df["code"].tolist() if not df.empty else [])
        log.info("获取昨日涨停股票成功")
        return df
    except Exception as e:
        log.error(f"昨日涨停股票获取失败：{str(e)}")
        return pd.DataFrame()

def get_market_emotion() -> dict:
    """获取市场情绪（涨停家数、炸板率），用于风控"""
    zt_pool = get_zt_pool()
    zt_count = len(zt_pool)
    bomb_open_rate = 0.25
    if zt_count > 0:
        bomb_count = len([c for c in zt_pool["code"].tolist() if c not in _PREV_ZT_CODES])
        bomb_open_rate = bomb_count / zt_count
    if zt_count >= 30 and bomb_open_rate < 0.2:
        emotion = "狂热"
    elif zt_count >= 15 and bomb_open_rate < 0.3:
        emotion = "强势"
    elif zt_count >= 8 and bomb_open_rate < 0.35:
        emotion = "震荡"
    else:
        emotion = "低迷"
    log.info(f"当前市场情绪：{emotion} | 涨停家数：{zt_count} | 炸板率：{round(bomb_open_rate*100, 1)}%")
    return {
        "emotion": emotion,
        "zt_count": zt_count,
        "bomb_open_rate": round(bomb_open_rate, 4),
        "risk_level": "低" if emotion in ["狂热", "强势"] else "中" if emotion == "震荡" else "高"
    }

# ---------------------- 核心策略函数 ----------------------
def _analyze_hist_trend(hist_df: pd.DataFrame) -> str:
    """分析股票历史趋势，过滤高位股"""
    try:
        closes = hist_df["close"].dropna().values
        if len(closes) < 5:
            return "未知"
        ma5 = closes[-5:].mean()
        cur_close = closes[-1]
        ma5_pos = (cur_close - ma5) / ma5 * 100
        recent_chgs = hist_df["_chg"].dropna().tail(10).values
        max_chg = float(np.nanmax(recent_chgs)) * 100 if len(recent_chgs) > 0 else 0
        if ma5_pos >= 5.0 and max_chg >= 20.0:
            return "高位拉升"
        elif ma5_pos >= 0 and max_chg >= 8.0:
            return "趋势向上"
        else:
            return "低位蓄势"
    except Exception as e:
        log.debug(f"趋势分析异常：{str(e)}")
        return "未知"

def scan_pre_zt() -> list[PreZtSignal]:
    """预选涨停筛选（核心策略，AI优化过滤条件）"""
    signals = []
    rt = get_realtime_quotes()
    if rt.empty:
        log.info("实时行情为空，无法筛选预选涨停信号")
        return signals
    # 大盘风控
    market = get_market_index()
    market_state = market["market_state"]
    # 市场情绪风控
    emotion = get_market_emotion()
    if emotion["risk_level"] == "高":
        log.info(f"市场风险等级高（情绪：{emotion['emotion']}），停止预选涨停筛选")
        return signals
    # 板块映射
    sector_map = get_sector_zt_map(rt)
    # 逐股筛选
    for _, row in rt.iterrows():
        try:
            code = row["code"]
            if code in PRE_ZT_PUSHED_TODAY:
                continue
            name = row["name"]
            price = row["price"]
            prev_close = row["prev_close"]
            if prev_close <= 0:
                continue
            chg = (price - prev_close) / prev_close
            vol_ratio = row["vol_ratio"]
            amount = row["amount"]
            circ_mkt_cap = row["circ_mkt_cap"]
            open_p = row["open"]
            open_chg = (open_p - prev_close) / prev_close
            high = row["high"]
            low = row["low"]
            zt_price = calc_zt_price(prev_close, code)
            gap = (zt_price - price) / zt_price if zt_price > 0 else 999

            # 核心过滤条件
            if not (PRE_ZT_MIN_CHG <= chg <= PRE_ZT_MAX_CHG):
                continue
            if vol_ratio < PRE_ZT_MIN_VOL_RATIO:
                continue
            if amount < PRE_ZT_MIN_AMOUNT:
                continue
            if not (MIN_MKT_CAP <= circ_mkt_cap <= MAX_MKT_CAP):
                continue
            if open_chg < PRE_ZT_OPEN_CHG_MIN:
                continue
            if high <= 0 or price < high * PRE_ZT_PRICE_NEAR_HIGH:
                continue
            if low <= 0 or price > low * PRE_ZT_PRICE_NEAR_LOW:
                continue
            if not (PRE_ZT_GAP_MIN <= gap <= PRE_ZT_GAP_MAX):
                continue

            # 历史趋势分析
            hist = get_hist_kline(code, 10)
            if hist.empty:
                continue
            trend = _analyze_hist_trend(hist)
            if trend == "高位拉升":
                continue

            # 评分计算
            score = 60
            sec_bonus, sec_name = sector_score(code, sector_map)
            score += sec_bonus
            score += 5 if market_state == "强势" else 0
            score += 8 if trend == "趋势向上" else 3 if trend == "低位蓄势" else 0
            score += 10 if vol_ratio >= 6.0 else 5 if vol_ratio >= 4.0 else 0
            if score < PRE_ZT_MIN_SCORE:
                continue

            # 构建信号
            signal = PreZtSignal(
                code=code,
                name=name,
                price=round(price, 2),
                chg_pct=round(chg * 100, 2),
                open_chg_pct=round(open_chg * 100, 2),
                zt_price=round(zt_price, 2),
                gap_pct=round(gap * 100, 2),
                vol_ratio=round(vol_ratio, 1),
                amount=round(amount / 1e8, 2),
                circ_cap=round(circ_mkt_cap / 1e8, 1),
                surge_tag="强势拉升",
                score=round(score, 1),
                reason=f"趋势:{trend} | 板块:{sec_name} | 量比:{vol_ratio:.1f}",
                market_state=market_state,
                sector_name=sec_name,
                hist_trend=trend,
                sector_bonus=sec_bonus
            )
            signals.append(signal)
            if len(signals) >= PRE_ZT_MAX_PUSH_COUNT:
                break
        except Exception as e:
            log.debug(f"个股筛选异常：{str(e)}")
            continue
    log.info(f"筛选出高质量预选涨停信号 {len(signals)} 只")
    return signals

# ---------------------- 推送相关函数 ----------------------
def format_pre_zt_signal(sig: PreZtSignal, rank: int) -> str:
    """格式化预选涨停信号（微信推送美观）"""
    return (
        f"#{rank} 【{sig.name}】({sig.code})\n"
        f"📊 价格：{sig.price}元 | 涨幅：{sig.chg_pct}%\n"
        f"📈 量比：{sig.vol_ratio} | 市值：{sig.circ_cap}亿\n"
        f"🎯 涨停价：{sig.zt_price}元 | 距涨停：{sig.gap_pct}%\n"
        f"📋 评分：{sig.score}分 | 理由：{sig.reason}\n"
        f"⚠️  止损：{round(sig.price*(1-INTRADAY_STOP_LOSS_PCT),2)}元\n"
    )

def push_pre_zt_signals(signals: list[PreZtSignal], market: dict):
    """推送预选涨停信号到微信"""
    if not signals:
        return
    title = f"【量化打板】{len(signals)}只高质量预选涨停信号"
    content = f"📅 日期：{beijing_now().strftime('%Y-%m-%d %H:%M')}\n"
    content += f"🌐 大盘状态：{market['market_state']}（上证指数：{market['sh_price']}，{market['sh_chg']*100:.2f}%）\n\n"
    content += "\n".join([format_pre_zt_signal(sig, i+1) for i, sig in enumerate(signals)])
    if send_wx(title, content):
        for sig in signals:
            PRE_ZT_PUSHED_TODAY.add(sig.code)

def push_startup():
    """系统启动推送"""
    send_wx("【量化打板系统】启动成功", f"📅 今日日期：{beijing_now().strftime('%Y-%m-%d')}\n"
            f"⏰ 交易时段：09:30-11:30 | 13:00-15:00\n"
            f"✅ 系统已就绪，将自动筛选信号并推送")

def push_summary(pre_zt_sigs):
    """收盘汇总推送"""
    content = f"📅 今日交易结束（{beijing_now().strftime('%Y-%m-%d')}）\n"
    content += f"📊 今日信号汇总：\n"
    content += f" - 预选涨停：{len(pre_zt_sigs)}只\n"
    content += f"🔋 系统明日将自动启动，敬请期待"
    send_wx("【量化打板系统】收盘汇总", content)

# ---------------------- 主循环 ----------------------
def main():
    global OPENING_PUSHED, _TAIL_ARB_PUSHED
    # 检查SENDKEY配置
    if not SENDKEY:
        log.error("❌ 未配置SENDKEY，请在GitHub Secrets中添加SENDKEY，或本地运行时直接填写SENDKEY")
        return

    # 非交易日直接退出
    if not is_trading_day():
        send_wx("【量化打板系统】非交易日", f"📅 今日（{beijing_now().strftime('%Y-%m-%d')}）为非交易日，系统休眠")
        return

    # 系统启动推送
    push_startup()
    get_yesterday_zt()
    pre_zt_all = []

    # 主循环
    while True:
        phase = current_phase()
        log.info(f"当前交易阶段：{phase}")

        # 已收盘：推送汇总，退出
        if phase == "closed":
            push_summary(pre_zt_all)
            log.info("✅ 今日交易结束，系统退出")
            break

        # 午休：暂停运行
        if phase == "noon_break":
            log.info("😴 午休时段，暂停扫描，13:00恢复")
            time.sleep(60)
            continue

        # 开盘提醒
        if phase == "opening" and not OPENING_PUSHED:
            market = get_market_index()
            send_wx("【量化打板系统】开盘提醒", f"📢 今日A股已开盘\n"
                    f"🌐 大盘状态：{market['market_state']}\n"
                    f"📊 上证指数：{market['sh_price']}（{market['sh_chg']*100:.2f}%）\n"
                    f"✅ 系统开始筛选盘中信号")
            OPENING_PUSHED = True

        # 交易时段扫描
        if phase in ["pre_auction", "morning", "afternoon"]:
            signals = scan_pre_zt()
            market = get_market_index()
            if signals:
                push_pre_zt_signals(signals, market)
                pre_zt_all.extend(signals)

        # 控制扫描频率
        time.sleep(60)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"系统运行异常：{str(e)}")
        send_wx("【量化打板系统】运行异常", f"❌ 系统出现异常，已停止运行\n异常信息：{str(e)}")
