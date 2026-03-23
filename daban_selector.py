"""
打板策略选股系统 - GitHub Actions 云端版（全功能增强版）
===========================================================
支持五种打板策略（兼容运行）：
  1. 首板策略    - 捕捉第一个涨停，题材+量能+位置三重确认
  2. 连板策略    - 跟踪连续涨停，封板强度+换手率过滤
  3. 竞价打板    - 集合竞价阶段高开信号（08:50~09:25）
  4. 跌停反包    - 跌停次日竞价反包，情绪修复逻辑
  5. 洗盘抄底 ★  - 日线级别：上涨趋势中量缩回调至均线支撑的主力洗盘信号
  6. 日内抄底 ★  - 日内级别：T字板低吸 / 均价回踩 / 半T字（昨板今低开企稳）

★ 新增功能模块（v3.0）：
  7. 长下影线信号  - 日内/昨日出现长下影线，结合位置+趋势分析，发出参与信号
  8. 盘中异动拉升  - 量价齐升，实时监测突破+放量，第一时间释放信号
  9. 外围+大盘综合 - A股三大指数+北向资金+市场宽度，综合研判多空状态
  10. 冲高回落/炸板 - 昨今冲高回落或炸板未回封，9:25/14:30 定时推送风险提示
  11. 消息联动推送 - 接入东方财富财经快讯，股价与消息联动第一时间推送

★ 市场情绪过滤系统（全局）：
  研究结论：涨停家数 10~30 家时胜率最高（40~55%），
            <10 家或 >50 家时胜率仅 20~30%。
  【硬过滤】当日全市场涨停数 < EMOTION_MIN_ZT（默认5）→ 市场冷清，暂停推送
  【硬过滤】当日全市场涨停数 > EMOTION_MAX_ZT（默认80）→ 过热/政策风险，降级推送
  【软警告】涨跌比 < EMOTION_MIN_RATIO（默认1.5）→ 弱市提醒，信号降级

★ 板块效应过滤（全局）：
  研究结论：同板块涨停数 ≥3 家时，胜率显著高于孤立涨停（+12%）。
  【加分项】同行业涨停数 ≥3 家：+15分
  【加分项】同行业涨停数 =2 家：+8分
  【提示】孤立涨停（同板块仅1家）：-5分

★ 假涨停识别系统（核心升级，不推送假信号）：
  【硬过滤 - 以下任意一条命中直接丢弃，不推送】
  1. 炸板记录：历史近30日炸板次数≥2次（反复炸板主力出货标志）
  2. 尾盘拉停：14:50后才封板（主力尾盘拉停，次日大概率开板）
  3. 封板资金净流入为负（流出>流入，封板不牢）
  4. 单次封板时长<30分钟（封板时间太短，说明没人跟进）
  5. 涨停时成交量异常放大（>昨日5倍）但无题材支撑（主力出货）
  6. 连板溢价为负（昨日涨停今日竞价低开，情绪不持续）
  7. 历史炸板后当日低收（收盘回落>3%，主力骗炮）

  【评分扣分 - 以下因素降低推荐评分，不完全过滤】
  - 午后才封板（13:00后封）：-15分
  - 量比>5（过度爆量，明显主力出货）：-10分（量比>5即触发，上限降至5x）
  - 换手率>20%（高换手=分歧大）：-10分
  - 历史有1次炸板记录：-20分
  - 涨停前30分钟有大单流出信号：-15分

★ 无效信号最终过滤系统 ★（v3.2新增）：
  【推送前最后一关 - 确保只推送有利润预期的信号】
  1. 评分低于策略门槛（首板<45/连板<40/竞价<35）→ 不推送
  2. 假涨停风险扣分>35分（综合风险太高）→ 不推送
  3. 近20日日均振幅<0.8%（死股，打板没弹性，必亏）→ 不推送
  4. 历史打板次日高开率<30%（胜率太差，期望值为负）→ 不推送

★ 洗盘抄底策略 ★（v3.2新增）：
  【有效洗盘六要素：同时满足才推送】
  1. 均线多头排列（MA5 > MA10 > MA20）
  2. 近30日内有≥10%涨幅（有趋势基础，非死股）
  3. 从近期高点回调5%~25%（真洗盘区间）
  4. 近3日均量 ≤ 近10日均量的75%（缩量确认，非出货）
  5. 回踩到MA5/MA10/MA20之一的支撑位（±2%容忍）
  6. 今日K线企稳（收阳/下影线/非大阴线）

  【无效洗盘（直接过滤）】
  - 下降趋势（MA5 < MA10）→ 不是洗盘是出货
  - 放量大阴线下跌 → 主力减仓，非洗盘
  - 跌破MA20下方5%以上 → 支撑失效

★ 日内抄底策略 ★（v3.3新增）：
  【三种日内低吸信号，实时盘中捕捉】

  ① T字板低吸（最强信号）：
  - 今日曾触及涨停（高点≥涨停价×99%）
  - 当前炸板，价格在涨停下方3%~8%
  - 当前价接近日内低点（企稳止跌）
  - 量能萎缩（vol_ratio<1.0，缩量洗盘而非出货）
  逻辑：主力拉停洗盘→打开→缩量→再次攻击，T字板是最高性价比的低吸时机

  ② 日内均价回踩低吸：
  - 日内最高涨幅≥4%（有足够上涨动能）
  - 从高点回落2%~6%（正常回踩，未失速）
  - 当前价格在VWAP均价或开盘价附近±1.5%（支撑位）
  逻辑：快速拉升后回踩整理，均价是最强支撑，是主力洗盘的理想低吸区

  ③ 半T字（昨板今低开企稳）：
  - 昨日涨停（通过历史数据确认）
  - 今日低开2%~6%（情绪回落但主力未放弃）
  - 开盘后企稳，当前价格未跌破前收3%以上
  逻辑：昨日涨停次日低开洗盘，若企稳则主力护盘意愿明显

  【无效信号（直接过滤）】
  - 已封死涨停（不需要低吸，直接打板）
  - 今日跌停（下降趋势，不做抄底）
  - 放量下跌（vol_ratio>3.0，出货特征）
  - 评分<50分（低质量信号不推送）


  【买得进去过滤 - 以下情形直接过滤，不推送】
  1. 一字板：开盘即涨停从不打开，无法买入
  2. 连续一字≥3天：超高连板一字，打板难度极大（除非竞价策略）
  3. 流通市值<30亿且日均成交额<3000万：流动性太差，买不进去
  4. 股价>200元：高价股打板成本极高，资金需求量大
  5. 涨停开板次数过少（今日从未开板）：封死一字无法介入

  【股性评分指标】
  - 活跃度（近20日平均涨跌幅绝对值）：越高弹性越好
  - 历史打板成功率（封板后次日高开率）：越高越值得打
  - 日均换手率（近20日均值）：3%~15%最优，代表流动性好
  - 近20日最大振幅：高振幅代表容易获利
  - 一字比例（近30日一字板天数/涨停天数）：>70%说明买不进去

★ Kelly仓位建议系统（新增）：
  基于 Kelly 公式：最优仓位 = 胜率 - (1-胜率)/盈亏比
  按信号评分高低，推送中给出三档仓位建议：
  - 高分（≥80）：Kelly仓位 × 1.0（约20~25%）
  - 中分（60~79）：Kelly仓位 × 0.5（约10~15%）
  - 低分（<60）：Kelly仓位 × 0.3（约5~8%）

★ v8.4 灵活仓位与出场优化说明（同步自 daban_backtest.py v8.4）：
  ① 量比分策略差异化：
     竞价/洗盘抄底/日内抄底：量比硬过滤上限放宽至8.0（高量比=强度信号）
     首板/反包/回调缩量：保留5.0上限（高量比首板多为出货拉停）
  ② 第2天强制止损机制：
     入场后第2天收盘浮亏>-2.0% 且非缩量洗盘 → 第3天开盘必须止损出场（★v8.5收紧至-2.0%）
     推送消息中明确标注止损纪律，防止持续坐穿板凳
  ③ 弱/强月份仓位联动（★v8.5重校准）：
     弱势月份（3/6/7/11/12月）仓位系数×0.5（移出8/10月，新增12月）
     强势月份（9月）仓位系数×1.2（实测优势有限，从1.5降至1.2）
  ④ 强势延长持仓提示：
     连续2天收盘接近最高价（close_pos>0.85）→ 推送提示可适当延长持仓
     最多额外延长3天，让强势趋势充分奔跑

★ v8.5 基于回测数据重校准（同步自 daban_backtest.py v8.5）：
  ① 首板推送门槛大幅提升至75分（原45分）：
     首板实际胜率21.8%/-2.16%，Kelly公式=-23%（应空仓），策略基本无效
     高门槛近乎暂停首板信号，等待市场条件改善
  ② 洗盘抄底推送门槛提升至60分（原55分）：
     洗盘胜率25.9%/-1.27%，11253笔是整体亏损主力，提高门槛减少低质信号
  ③ 月份仓位重校准（修正反向操作）：
     移出8月（胜率30.4%，各月最高之一，之前错误列为弱势月）
     移出10月（胜率30.7%，各月最高，之前错误列为弱势月）
     新增12月（胜率20.8%/-2.13%，与3月并列最差，之前遗漏）

★ v9.4/v9.5 参数同步（基于v9.4全量498笔回测·实盘就绪版）：
  ① 停用竞价-连板-极限推送（AUCTION_WHITELIST移除）：
     v9.4全量回测胜率45.2%/+2.42%，胜率<50%实盘心理难以承受，高方差不适合执行
  ② 新增 SKIP_MONTHS=[6,11]（仅影响仓位建议层，信号本身照常推送供参考）：
     6月实测43.8%/+0.00%，11月50.0%/+0.21%；出现信号仍推送，仓位建议返回0%
     推送逻辑：如当月在SKIP_MONTHS中，仓位建议返回0%，警示本月不入场
  ③ WEAK_MONTHS更新至[2,3,7,12]，新增EXTREME_WEAK_MONTHS=[1]（v9.3/v9.4）：
     1月极端弱势（胜率59.3%但均收仅+2.33%），仓位系数降至0.3
  ④ PUSH_MIN_SCORE_AUCTION_STRONG=65.0（v9.4）：
     竞价-首板-强势专用门槛（高开5~7%已消耗利润空间，低分强势信号无期望值）
  ⑤ KELLY_PARAMS竞价更新为真实回测数据（v9.4）：
     竞价：win=75.3%，ratio=2.74x（基于498笔真实回测），Kelly全仓≈38.6%

★ v9.0 重大重构（同步自 daban_backtest.py v9.0）：
  ★★★ 核心战略转变：只推送竞价盈利信号，停用所有负期望值策略 ★★★
  ① 策略开关重置：
     首板策略 ← 完全停用（Kelly=-23%，策略无效）
     洗盘抄底 ← 完全停用（11253笔贡献80%亏损）
     回调缩量 ← 完全停用（样本太少且负期望值）
     日内抄底 ← 完全停用（日线近似误差大）
     竞价策略 ← 保留，但仅推送盈利子策略
  ② 竞价子策略白名单（仅推送盈利方向）：
     竞价-首板-温和（56.8%/+2.37%）✅ 推送
     竞价-首板-强势（41.8%/+1.34%）✅ 推送
     竞价-连板-极限（60.0%/+5.27%）✅ 推送
     竞价-连板-温和（34.6%/-0.73%）❌ 停用
     竞价-连板-强势（38.7%/-1.23%）❌ 停用
  ③ 竞价专用止损：入场当天低开>2%立即止损（更严格）
  ④ 大盘情绪硬过滤：市场涨停家数<20则当天暂停所有信号

运行方式：
  - GitHub Actions 每个交易日 08:50 启动，全天守护
  - 微信推送信号，手动下单
  - 支持手动触发（随时测试）
"""

import akshare as ak
import pandas as pd
import numpy as np
import requests
import re
import time
import datetime
import logging
import warnings
import os
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

warnings.filterwarnings("ignore")

# ================================================================
# 配置区
# ================================================================
SENDKEY     = os.environ.get("SENDKEY", "")    # ★ 主推送 key（GitHub Secrets: SENDKEY）
SENDKEY2    = os.environ.get("SENDKEY2", "")   # ★ 备用推送 key（GitHub Secrets: SENDKEY2），主 key 失败时自动切换
MAX_WORKERS = 20
TZ_OFFSET   = 8

# 打板过滤条件
MIN_MKT_CAP      = 3_000_000_000    # 最小流通市值 30亿（原50亿→放宽，覆盖更多首板）
MAX_MKT_CAP      = 150_000_000_000  # 最大流通市值 1500亿（原1000亿→放宽大市值机会）
MIN_AMOUNT       = 30_000_000       # 昨日成交额下限 3000万（原5000万→放宽）
MAX_PRICE        = 150.0            # 股价上限（原100→放宽）
MIN_SEAL_RATIO   = 0.3              # 封板强度 ≥ 30%
MAX_CONNECT_DAYS = 6                # 连板天数上限（原5→适度放宽）
MIN_TURNOVER     = 2.0              # 首板最小换手率（原3%→放宽至2%）
MAX_TURNOVER_ZT  = 25.0             # 涨停时换手率上限（原20%→放宽）
DT_RECOVER_THRESH = 0.03            # 跌停反包竞价涨幅门槛

# ── ★ 市场情绪过滤阈值（全局，基于研究结论） ────────────────────────────────
# 研究结论：涨停家数 10~30 最优，胜率 40~55%
# ★ v3.1修复：降低冷清阈值（避免弱市错过信号）
EMOTION_MIN_ZT      = 5     # 全市场涨停数下限（原10→降至5，只在极端冷清时停推）
EMOTION_MAX_ZT      = 80    # 全市场涨停数上限（原60→提高至80，更多信号进入）
EMOTION_MIN_RATIO   = 1.5   # 涨停/跌停比最低值（原2.5→降至1.5，弱市警告但不停推）
# ★ v9.2 热度指数最优区间（内部算法用，取代之前被删除的EMOTION_BEST_LOW/HIGH）
_EMOTION_BEST_LOW   = 10    # 热度指数算法内部用：最优涨停家数下限
_EMOTION_BEST_HIGH  = 50    # 热度指数算法内部用：最优涨停家数上限

# ── ★ v9.2 新增：炸板打开率过滤（公开研究：打开率>60%为弱市，竞价胜率骤降）──────
BOMB_OPEN_RATE_ENABLE   = True   # 是否启用炸板打开率过滤
BOMB_OPEN_RATE_WARN     = 0.50   # 打开率≥50%：市场弱势，警告
BOMB_OPEN_RATE_STOP     = 0.70   # 打开率≥70%：市场极弱，竞价信号暂停推送

# ── ★ v9.2 新增：龙虎榜净买入加分（机构/知名游资净买=极强认可）────────────────
LHB_BONUS_ENABLE        = True   # 是否启用龙虎榜因子加分
LHB_NET_BUY_BONUS       = 12     # 龙虎榜净买入个股竞价评分加分（+12分）
LHB_CACHE_TTL           = 3600   # 龙虎榜缓存 1 小时（当日数据，无需频繁刷新）

# ── ★ v9.2 新增：高度板风险扣分（市场最高连板≥5板时末端跟风扣分）──────────────
HEIGHT_BOARD_RISK_ENABLE = True  # 是否启用高度板风险识别
HEIGHT_BOARD_RISK_MIN    = 5     # 市场最高板高度≥此值时，高板位标的风险加大
HEIGHT_BOARD_RISK_PENALTY = -10  # 竞价信号属于高板位（连板≥4）时扣分

# ── ★ v9.2 新增：封单量占流通盘比例直接从涨停池读（比自算更准）─────────────────
# 东方财富涨停池"封单换手率"字段直接代表封单量占比，阈值：
ZT_POOL_SEAL_TURNOVER_MIN = 0.30  # 封单换手率≥0.30%认为封单有力（<0.30%警告）

# ── ★ Kelly 仓位建议（基于公式：胜率 - (1-胜率)/盈亏比）────────────────────
# ★ v3.4修复：按策略独立配置胜率/盈亏比，避免用首板参数错误计算连板仓位
# ★ v9.4更新：基于v9.4全量498笔回测真实数据校准竞价/反包参数
#   竞价（总）：胜率75.3%，盈亏比2.74x → Kelly≈38.6% → 高分半Kelly≈39%
#   首板：胜率21.8%/-2.16%，Kelly=-23%（完全停用，参数不再使用）
KELLY_PARAMS = {
    "首板":  {"win": 0.55, "ratio": 1.85},   # 理论值，实盘停用
    "连板":  {"win": 0.35, "ratio": 2.20},   # 高风险，谨慎
    "竞价":  {"win": 0.753, "ratio": 2.74},  # ★v9.4: 基于498笔真实回测（75.3%/盈亏比2.74x）
    "反包":  {"win": 0.50, "ratio": 4.11},   # ★v9.4: 2笔样本（50%/均盈13.61%/均亏-3.31x）
    "洗盘":  {"win": 0.50, "ratio": 1.50},
    "日内":  {"win": 0.48, "ratio": 1.40},   # T字板/均价回踩等
}
# 默认参数（未匹配策略时）
KELLY_WIN_RATE  = 0.50
KELLY_PNL_RATIO = 1.60
_kelly_full = max(0.0, KELLY_WIN_RATE - (1 - KELLY_WIN_RATE) / KELLY_PNL_RATIO)
KELLY_HIGH  = round(_kelly_full * 1.0 * 100)
KELLY_MID   = round(_kelly_full * 0.5 * 100)
KELLY_LOW   = round(_kelly_full * 0.3 * 100)

# ★ v9.4/v9.5：月份仓位系数（基于v9.4全量498笔回测数据）
# v9.4: 移出6月/11月（均收趋零不如直接跳过），新增2月（胜率63.2%/+1.59%偏弱）
# v9.3: 1月极端弱势（加入EXTREME_WEAK_MONTHS）
WEAK_MONTH_POSITION_ENABLE  = True          # 启用弱势月份仓位减半
WEAK_MONTHS                 = [2, 3, 7, 12] # ★v9.4: 对齐backtest（移出6/11，新增2月）
EXTREME_WEAK_MONTHS         = [1]           # ★v9.3: 1月极端弱势，仓位×0.3
EXTREME_WEAK_MONTH_POS_RATIO = 0.3          # 极端弱势月仓位系数（仅3成）
WEAK_MONTH_POSITION_RATIO   = 0.5           # 弱势月份Kelly结果×0.5
SKIP_MONTHS                 = [6, 11]       # ★v9.4/v9.5: 仓位建议返回0%（信号仍推送，供参考）
STRONG_MONTH_POSITION_ENABLE = True         # 启用强势月份加仓
STRONG_MONTHS               = [9]           # 强势月份：9月（v9.4回测 87.5%/+6.27%）
STRONG_MONTH_POSITION_RATIO = 1.2           # ★v8.5: 从1.5降至1.2（实测优势有限）

# ── 真假涨停识别阈值 ────────────────────────────────────────────────────────
# 炸板：历史N日内炸板次数（涨停后收盘跌出涨停）超此值直接过滤
FAKE_MAX_BOMB_30D  = 2          # 近30日炸板次数上限（原1→2，允许1~2次历史炸板）
# 尾盘拉停：封板时间在 14:50 之后认为是尾盘拉停，直接过滤
FAKE_TAIL_SEAL_HM  = 1450       # 时间格式HHMM，≥此值视为尾盘封板
# 封板强度：封板资金/流通市值，低于此值视为封板不牢（假涨停概率高）
# ★ v3.1修复：封板强度硬过滤从25%降至15%（很多优质首板封板强度不高）
FAKE_MIN_SEAL      = 0.15       # 硬过滤：封板强度<15%直接过滤（原25%→15%）
# 量比爆量上限（量比>此值且无明确题材，可能是出货拉停）
# ★ v8.4：全局统一用8.0（极端爆量才硬过滤，低于此值在策略内部用分策略标准）
FAKE_MAX_VOL_RATIO = 8.0        # 量比>8直接过滤（原5.0→8.0）

# ★ v8.4：量比分策略差异化硬过滤上限
# 竞价/洗盘抄底/日内抄底：高量比=强度信号，放宽至8.0
# 首板/反包/回调缩量：高量比出货拉停特征明显，保持5.0
VOL_RATIO_MAX_CONSERVATIVE = 5.0   # 首板/反包/回调缩量 量比上限
VOL_RATIO_MAX_AGGRESSIVE   = 8.0   # 竞价/洗盘/日内抄底 量比上限
# 连板溢价阈值：今日竞价相对昨日涨停收盘的溢价，负值说明情绪不持续
FAKE_CONNECT_PREMIUM_MIN = -0.03  # 连板次日竞价溢价<-3%才过滤（原-2%→-3%更宽松）
# 炸板历史：近30日内曾炸板且当日收盘回落幅度（收盘-涨停价）/涨停价 < 此值
FAKE_BOMB_BACK_THRESH = -0.03   # 炸板后回落>3%的历史记录算一次"质量差炸板"

# ── 股性识别阈值 ─────────────────────────────────────────────────────────────
# 买得进去硬过滤
MAX_PRICE_ENTRY    = 200.0      # 股价≥200元直接过滤（打板成本极高）
MIN_DAILY_AMOUNT   = 20_000_000 # 近20日日均成交额下限（原3000万→2000万，放宽流动性）
MAX_YIZI_RATIO     = 0.80       # 一字板比例≥80%（原70%→80%，放宽一字判定）
MAX_YIZI_STREAK    = 5          # 当前连续一字≥此值直接过滤（原3→5，更宽松）
# 股性评分参数
STOCK_CHAR_DAYS    = 20         # 股性计算使用近N日数据
ACTIVE_SCORE_THRESH = 1.5       # 近20日日均振幅≥1.5%认为活跃（弹性好）

# ── ★ 长下影线信号配置 ────────────────────────────────────────────────────────
# 长下影线定义：下影线长度 / 实体长度 ≥ 此倍数 且 下影线 / 当日振幅 ≥ 此比例
LOWER_SHADOW_RATIO   = 2.0      # 下影线/实体 ≥ 2倍（经典长下影线定义）
LOWER_SHADOW_AMP     = 0.30     # 下影线/当日振幅 ≥ 30%（影线需有分量）
LOWER_SHADOW_MIN_AMP = 0.02     # 当日总振幅 ≥ 2%（过小的振幅无效）
LOWER_SHADOW_MIN_AMOUNT = 30_000_000  # 成交额下限（过滤垃圾票）
# 位置判断：低位定义为距近60日最低点的距离
LOWER_SHADOW_LOW_POS = 0.35     # 当前价在60日区间低于35%时为低位（低吸机会）
LOWER_SHADOW_HIGH_POS = 0.65    # 高于65%时为相对高位（突破形态）

# ── ★ 盘中异动拉升信号配置 ─────────────────────────────────────────────────
SURGE_MIN_CHG       = 0.03      # 触发盘中异动的涨幅门槛（3%）
SURGE_VOL_RATIO     = 2.0       # 放量倍数门槛（相对5日均量）
SURGE_MIN_AMOUNT    = 20_000_000 # 单笔统计最低成交额
SURGE_SCAN_INTERVAL = 120       # 盘中异动扫描间隔（秒）

# ── ★ 大盘与外围配置 ──────────────────────────────────────────────────────────
MARKET_SCAN_INTERVAL = 600      # 大盘情绪刷新间隔（秒）
# 大盘下跌警戒（上证/深证/创业板同时跌超此值视为弱市）
MARKET_WEAK_THRESH  = -0.015    # -1.5% 为弱市警戒线
MARKET_STRONG_THRESH = 0.005    # +0.5% 为多头市场线
# 北向资金
NORTH_WARN_THRESH   = -2_000_000_000  # 北向净流出 ≥ 20亿时发出警告（元）

# ── ★ 冲高回落/炸板定时推送配置 ──────────────────────────────────────────────
# 每天推送两次：09:25 和 14:30
PULLBACK_PUSH_TIMES = [(9, 25), (14, 30)]   # (时, 分)
# 冲高回落定义：当日最高涨幅 ≥ 此值 但 当前价相对最高价回落 ≥ 此值
PULLBACK_HIGH_THRESH  = 0.05    # 最高涨幅 ≥ 5%（曾经冲高）
PULLBACK_FALL_THRESH  = 0.03    # 从最高价回落 ≥ 3%（确认回落）
# 炸板未回封定义：当日触及涨停后开板，且当前价低于涨停价 N%
BOMB_UNFIXED_THRESH  = 0.02     # 炸板后低于涨停价 ≥ 2% 认为未回封

# ── ★ 洗盘抄底信号配置 ──────────────────────────────────────────────────────
# 洗盘定义：处于上升趋势(MA5>MA10>MA20)中，短期回调后缩量企稳，具备买入价值
# 有效洗盘条件：
#   1. 均线多头排列（MA5 > MA10 > MA20）
#   2. 近N日涨幅足够（有趋势，不是死股）
#   3. 当前处于回调中（距近期高点回落5%~25%，太少=没洗，太多=趋势破坏）
#   4. 量能萎缩（近3日均量 < 10日均量，缩量回调为真洗盘特征）
#   5. 今日K线企稳信号（小阳/十字星/锤子线，开始止跌）
#   6. 回踩到关键均线支撑（MA5/MA10/MA20之一）
WASHOUT_TREND_DAYS   = 30       # 洗盘前趋势持续天数（近N日内有上涨趋势）
WASHOUT_RISE_MIN     = 0.10     # 近30日内涨幅至少10%（有趋势基础）
WASHOUT_PULLBACK_MIN = 0.05     # 回调幅度下限（至少回调5%，才叫洗盘）
WASHOUT_PULLBACK_MAX = 0.25     # 回调幅度上限（超过25%认为趋势破坏，非洗盘）
WASHOUT_VOL_SHRINK   = 0.75     # 缩量系数：近3日均量 / 近10日均量 ≤ 此值（缩量确认）
WASHOUT_MA_TOUCH_PCT = 0.02     # 均线触碰容忍度：价格在均线上下2%内视为触碰支撑
WASHOUT_MIN_SCORE    = 60       # ★v8.5: 从55提升至60（洗盘胜率25.9%/-1.27%，11253笔是亏损主力，同步backtest的58取整至60）

# ── ★ v9.0 策略开关（完全停用负期望值策略）────────────────────────────────
# 基于v8.5回测数据：只有竞价策略是盈利的（45.7%胜率/+0.90%）
# 其他所有策略Kelly公式为负，实盘不应该交易
STRATEGY_ENABLE_FIRST_BOARD    = False   # ★v9.0: 首板Kelly=-23%，完全停用
STRATEGY_ENABLE_WASHOUT        = False   # ★v9.0: 洗盘抄底贡献80%亏损，完全停用
STRATEGY_ENABLE_ZT_PULLBACK    = False   # ★v9.0: 回调缩量样本无效且负收益，停用
STRATEGY_ENABLE_INTRADAY       = False   # ★v9.0: 日内抄底日线近似误差大，停用
STRATEGY_ENABLE_AUCTION        = True    # ★v9.0: 竞价是唯一盈利策略，保留
STRATEGY_ENABLE_DT_RECOVER     = True    # 跌停反包保留（独立验证，样本不足）

# ── ★ v9.0 竞价子策略白名单（只推送盈利子策略）──────────────────────────────
AUCTION_WHITELIST_ENABLE = True
AUCTION_WHITELIST = [
    "竞价-首板-温和",    # v9.4全量: 77.7% / +4.20% ← 主力，核心策略
    "竞价-首板-强势",    # v9.4全量: 64.3% / +3.12% ← 次优，保留
    # "竞价-连板-极限",  # ★v9.4停用: 45.2% / +2.42% ← 胜率<50%实盘心理难承受，方差过大
    # "竞价-连板-温和",  # 34.6% / -0.73% ← 停用
    # "竞价-连板-强势",  # 38.7% / -1.23% ← 停用
]

# ── ★ v9.0 竞价专用参数──────────────────────────────────────────────────────
AUCTION_STOP_LOSS_PCT  = -0.030  # 竞价专用止损-3%（比通用止损更紧）
AUCTION_GAP_ABORT_PCT  = -0.020  # 入场当天低开>2%立即止损（比通用3%更严）

# ── ★ v9.0 大盘情绪硬过滤────────────────────────────────────────────────────
MARKET_ZT_FILTER_ENABLE  = True  # 启用大盘涨停密度过滤
MARKET_ZT_MIN_COUNT      = 20    # 当日全市场涨停家数<20则暂停所有信号

# ── ★ v9.1 连板梯队仓位差异化（实盘推送时注明建议仓位）──────────────────────
# 思路：2连板胜率>60%，是最佳介入点；3连板开始折价风险，降低仓位
AUCTION_ZT_STREAK_POS_ENABLE   = True    # 是否展示连板梯队仓位建议
AUCTION_ZT_2_POS_RATIO         = 1.5    # 2连板：胜率高，建议仓位1.5倍
AUCTION_ZT_3_POS_RATIO         = 0.7    # 3连板：折价风险，建议仓位0.7倍
AUCTION_ZT_4PLUS_ENABLE        = False   # 4+连板竞价是否推送（默认不推送）

# ── ★ v9.1 封板力度过滤（实盘push时额外检查）────────────────────────────────
SEAL_SCORE_STRICT_FILTER       = True    # True=封板不稳（close_pos<0.60）的连板竞价直接跳过
SEAL_SCORE_WEAK_THRESH         = 0.60   # close_pos低于此值认为封板不稳

# ── ★ v9.1 分时量比持续性过滤 ────────────────────────────────────────────────
INTRADAY_VOL_SUSTAIN_ENABLE    = True    # 启用盘中量比持续性检测
INTRADAY_VOL_SUSTAIN_RATIO     = 0.75   # 日量/5日均量 < 0.75 = 无人接盘，不推送

# ── ★ v9.1 竞价-连板-极限进化（超强信号，更严格入场）────────────────────────
AUCTION_EXTREME_PREV_CLOSE_POS  = 0.90  # 前一日close_pos必须>0.90（封板极稳）
AUCTION_EXTREME_MAX_STREAK      = 3     # 最多连板天数（4+连板极限不推）


WASHOUT_MIN_AMOUNT   = 50_000_000  # 洗盘个股成交额下限（流动性门槛）

# ── ★ 日内抄底信号配置 ──────────────────────────────────────────────────────
# 覆盖三类日内机会：T字板低吸、日内回踩均价低吸、昨板今低开企稳（半T字）
#
# T字板低吸：今日曾触及涨停但炸板，当前在涨停价下方4%~8%企稳止跌，缩量
#   特征：炸板→下跌→在某低点止跌形成"T"字，等待再次冲板
INTRA_TBOARD_ZT_UP_MAX   = 0.08    # T字低吸区间上限：低于涨停价8%以内（距涨停不远）
INTRA_TBOARD_ZT_DOWN_MIN = 0.03    # T字低吸区间下限：低于涨停价至少3%（真炸板）
INTRA_TBOARD_STABLE_PCT  = 0.015   # 企稳判断：当前价距日内低点不超过1.5%（止跌）
INTRA_TBOARD_VOL_SHRINK  = 0.70    # 缩量系数：当前价格在低位时量能萎缩至盘中均量70%
#
# 日内均价回踩低吸：日内涨幅超过5%后回调至均价/开盘价附近，量缩，形成支撑
#   特征：早盘快速拉升→回踩均价→量缩止跌，等待再次启动
INTRA_VWAP_RISE_MIN      = 0.04    # 触发回踩的日内最高涨幅下限（至少涨过4%）
INTRA_VWAP_PULLBACK_MAX  = 0.06    # 从日内高点回落不超过6%（太多=失速）
INTRA_VWAP_TOUCH_PCT     = 0.015   # 价格在均价/开盘价±1.5%内视为触碰支撑
#
# 半T字（昨板今低开企稳）：昨日涨停今日低开，但快速企稳在前收±2%内，显示主力护盘
SEMI_T_OPEN_MAX          = -0.02   # 今日开盘涨幅 < -2%（低开）
SEMI_T_OPEN_MIN          = -0.06   # 今日开盘涨幅 > -6%（不能低开太多，太多=主力放弃）
SEMI_T_STABLE_PCT        = 0.02    # 当前价与今日开盘价差异在±2%内（企稳，未继续下砸）
SEMI_T_PRICE_NOT_BELOW   = -0.03   # 当前价不能低于前收3%以上（失守前收=无效）
#
INTRA_DIP_MIN_AMOUNT     = 30_000_000  # 日内抄底信号成交额门槛
INTRA_DIP_MIN_SCORE      = 50          # 日内抄底最低评分门槛

# ── ★ 直线拉升预警配置（v3.9 重构：在拉升途中尽早预警，保留足够利润空间）──────
# 核心逻辑：捕捉量价齐升的"直线拉升"态势，在冲向涨停的途中尽早推送。
# 设计原则：宁早勿晚——涨4%时推，比涨8%时推多出约6%的利润空间。
PRE_ZT_MIN_CHG         = 0.040   # 最低触发涨幅（4%，此时距涨停还有约6%利润空间）
PRE_ZT_MAX_CHG         = 0.092   # 最高涨幅上限（9.2%，快封板了再推没意义）
PRE_ZT_VOL_RATIO       = 3.0     # 量比门槛（3x，明确的主力买入信号，比2.5更严格）
PRE_ZT_MIN_AMOUNT      = 50_000_000   # 成交额门槛（5000万，比之前宽松，尽量早抓信号）
PRE_ZT_PRICE_NEAR_HIGH = 0.012        # 当前价距日内最高价≤1.2%（价格不能回落，需持续攻击）
PRE_ZT_OPEN_CHG_MIN    = 0.0          # 开盘涨幅最低要求（≥0，不接受低开，低开=主力不想拉）
PRE_ZT_MIN_SCORE       = 55           # 最低推送评分门槛

# ── ★ 涨停回调缩量再启动信号配置 ────────────────────────────────────────────
# 对应 daban_backtest.py 的 backtest_zt_pullback() 策略，实盘实时扫描版本
# 核心逻辑：近3~10日内出现涨停 → 价格回调3%~8% → 回调期间缩量 → 当日企稳 → 次日启动
ZT_PULLBACK_LOOKBACK      = 10   # 最多往前找10日的涨停（太远动能耗尽）
ZT_PULLBACK_MIN_LOOKBACK  = 3    # 最少往前找3日（刚涨停不做）
ZT_PULLBACK_MIN_DROP      = 0.03 # 回调幅度下限3%
ZT_PULLBACK_MAX_DROP      = 0.08 # 回调幅度上限8%（超过=出货，不做）
ZT_PULLBACK_VOL_RATIO     = 0.60 # 回调期均量/涨停日量 < 0.60（缩量洗盘确认）
ZT_PULLBACK_STABLE_THRESH = 0.005 # 收盘高于近期最低价0.5%以上确认企稳
ZT_PULLBACK_MIN_AMOUNT    = 50_000_000  # 成交额门槛（流动性过滤）
ZT_PULLBACK_MIN_SCORE     = 55   # 最低评分门槛

# ── ★ 无效信号过滤（最终推送前评分门槛）───────────────────────────────────
# 不同策略设置独立最低推送分，低于此分值的信号直接丢弃
# ★v8.5: 首板门槛从45大幅提升至75（首板21.8%胜率/Kelly公式=-23%，策略无效，近乎暂停）
PUSH_MIN_SCORE = {
    "首板": 75,    # ★v8.5: 从45提升至75（首板Kelly公式证明无效，高门槛近乎暂停）
    "连板": 40,    # 连板最低40分
    "竞价": 35,    # 竞价信号相对宽松
    "反包": 30,    # 反包信号难度大，门槛低一些
    "洗盘": 60,    # ★v8.5: 从55提升至60（洗盘25.9%胜率/-1.27%，提高门槛减少低质信号）
}
# ★v9.4: 竞价-首板-强势 专用更高评分门槛（64.3%胜率<温和77.7%，高开5~7%已消耗空间，低分无利润预期）
PUSH_MIN_SCORE_AUCTION_STRONG = 65.0   # 竞价-首板-强势 最低推送评分（温和档保持35分）
# 综合负面判断：以下几项同时出现时，直接认定为亏损概率高，强制过滤
LOSS_FILTER_CONDITIONS = {
    "max_penalty":  35,    # 假涨停扣分>35，直接过滤
    "min_amplitude": 0.8,  # 近20日日均振幅<0.8%（死股，打板没弹性）
    "min_open_rate": 30.0, # 历史打板次日高开率<30%（胜率太低）
}

# ── ★ 消息联动配置 ────────────────────────────────────────────────────────────
NEWS_SCAN_INTERVAL  = 60        # 消息扫描间隔（秒）
NEWS_PRICE_LINK_MIN = 0.02      # 消息发布后价格涨幅 ≥ 2% 认为联动
NEWS_MAX_AGE_MIN    = 30        # 只处理30分钟内的新消息（避免重复推送旧消息）
# 消息关键词过滤（含以下词的消息优先级高）
NEWS_KEYWORDS_HIGH  = [
    "重大合同", "重大资产", "并购", "收购", "战略合作", "研发突破",
    "中标", "获批", "定增", "回购", "增持", "业绩预增", "扭亏",
    "新品发布", "技术突破", "独家", "首创", "专利授权"
]
NEWS_KEYWORDS_LOW   = ["风险提示", "处罚", "立案", "调查", "退市", "亏损"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("daban")


# 全局状态
PUSHED_TODAY:  set  = set()
OPENING_PUSHED: bool = False   # 9:25 强制推送标记

# ★ 新增全局状态
PULLBACK_PUSHED_925:  bool = False   # 09:25 冲高回落推送标记
PULLBACK_PUSHED_1430: bool = False   # 14:30 冲高回落推送标记
NEWS_PUSHED_IDS: set = set()         # 已推送的消息ID集合（避免重复）
SURGE_PUSHED_TODAY: set = set()      # 今日已推送的异动股票代码
# ★ v3.8：临近涨停预判去重（每只每天只推一次）
PRE_ZT_PUSHED_TODAY: set = set()     # 今日已推送的临近涨停预判代码
# ★ v3.3：日内抄底独立推送集合（不受打板/洗盘PUSHED_TODAY影响，避免互相屏蔽）
# 同一只股票同一信号类型当天只推一次；不同类型独立计数
INTRA_DIP_PUSHED_TODAY: dict = {}    # {code: set(signal_type)} 今日已推的日内抄底信号
# 昨日涨停池缓存（半T字判断用，避免逐股请求历史K线拖慢扫描）
_PREV_ZT_CODES: set  = set()         # 昨日涨停股代码集合（每日开盘前加载一次）
_PREV_ZT_DATE:  str  = ""            # 已加载的日期标记
_PREV_ZT_DF: pd.DataFrame = pd.DataFrame()  # ★v9.2：昨日涨停池完整DF（含连板数字段，供竞价扫描用）
_market_cache: dict = {}             # 大盘指数缓存
_market_cache_time: float = 0.0
# ★ v3.4：全市场行情全局缓存（每轮扫描只拉一次，各子扫描函数复用）
# 解决：主循环每轮对 get_realtime_quotes() 发出5~6次重复请求的性能问题
_realtime_cache: pd.DataFrame = pd.DataFrame()
_realtime_cache_time: float = 0.0
REALTIME_CACHE_TTL: float = 55.0    # 行情缓存有效期55秒（略小于90秒扫描间隔）
# ★ v3.7：腾讯行情接口全市场代码列表缓存（一次获取后长期复用，避免开盘封锁时反复请求）
_TXZQ_CODES_CACHE: list = []         # 全市场股票代码列表缓存（6位字符串）
# ★ v3.1：定时心跳推送（每小时推一次「系统在线+市场状态」，确认系统正常）
_HEARTBEAT_PUSHED_HOURS: set = set() # 已推送心跳的小时集合（9,10,11,13,14）
# ★ v4.0：尾盘套利每日只推一次标记
_TAIL_ARB_PUSHED: bool = False


# ================================================================
# 时间工具
# ================================================================
def beijing_now() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=TZ_OFFSET)

def is_trading_day() -> bool:
    """
    判断今天是否为交易日：排除周末，并通过检查涨停池是否有数据来确认是否为节假日。
    轻量方案：周末直接排除；非周末时若涨停池接口返回空则视为节假日（懒检查，
    真正影响在 main() 启动时的 get_zt_pool() 结果，此处仅做周末快速排除）。
    """
    today = beijing_now()
    # 1. 周末
    if today.weekday() >= 5:
        return False
    # 2. 法定节假日：尝试用 chinese_calendar 库（可选依赖），没装就只排周末
    try:
        import chinese_calendar
        return chinese_calendar.is_workday(today.date())
    except ImportError:
        pass
    return True

def current_phase() -> str:
    """
    返回当前交易阶段：
      pre_open     开市前 (< 08:50)
      pre_auction  集合竞价 08:50~09:25
      opening      开盘缓冲 09:25~09:35
      morning      上午盘 09:35~11:30
      noon_break   午休 11:30~13:00
      afternoon    下午盘 13:00~14:50
      pre_close    尾盘 14:50~15:05
      closed       收盘后
    """
    t = beijing_now()
    hm = t.hour * 100 + t.minute
    if hm < 850:             return "pre_open"
    if 850 <= hm < 925:      return "pre_auction"
    if 925 <= hm < 935:      return "opening"
    if 935 <= hm < 1130:     return "morning"
    if 1130 <= hm < 1300:    return "noon_break"
    if 1300 <= hm < 1450:    return "afternoon"
    if 1450 <= hm < 1505:    return "pre_close"
    return "closed"


# ================================================================
# 微信推送（Server酱 v9.5 增强版）
# ================================================================

# ── 推送去重集合：防止同一股票同一信号在同一阶段重复推送 ──────────
_push_dedup_set: set = set()


def _sc_post(key: str, title: str, content: str, timeout: int = 8) -> bool:
    """
    向单个 Server酱 key 发送一次请求。
    兼容 SCT 开头的新版 Turbo key 和旧版 SCKEY。
    返回 True=成功，False=失败（不抛异常）。
    """
    if key.startswith("SCT"):
        url = f"https://sctapi.ftqq.com/{key}.send"
    else:
        url = f"https://sc.ftqq.com/{key}.send"
    try:
        r = requests.post(
            url,
            data={"title": title[:64], "desp": content},
            timeout=timeout,
        )
        result = r.json()
        if r.status_code == 200 and (
            result.get("data", {}).get("errno", -1) == 0
            or result.get("errmsg", "") == "success"
        ):
            return True
        log.warning(f"Server酱返回异常 key={key[:8]}***: {r.text[:200]}")
        return False
    except Exception as e:
        log.warning(f"Server酱请求异常 key={key[:8]}***: {e}")
        return False


def send_wx(title: str, content: str, retry: int = 3, urgent: bool = False) -> bool:
    """
    推送到 Server酱，失败自动重试 retry 次。
    ★ v9.5 升级：
      - 双 SENDKEY 支持（主 key 失败自动切换备用 SENDKEY2）
      - urgent=True 超时缩短至 5s，失败等待 1s，主 key 失败立即切备用（无等待）
      - 普通模式：第1次失败等 2s，第2次等 4s（快速退避）
      - 两个 key 均失败时打 ERROR 日志，便于排查
    返回 True=任一 key 推送成功，False=全部失败。
    """
    keys = [k for k in [SENDKEY, SENDKEY2] if k]
    if not keys:
        log.warning("SENDKEY 未配置，跳过推送")
        return False

    _timeout = 5 if urgent else 8

    for kidx, key in enumerate(keys):
        key_label = "主key" if kidx == 0 else "备用key"
        for attempt in range(1, retry + 1):
            ok = _sc_post(key, title, content, timeout=_timeout)
            if ok:
                log.info(f"推送成功（{key_label} 第{attempt}次）: {title[:30]}")
                return True
            log.warning(f"推送失败（{key_label} 第{attempt}/{retry}次）: {title[:30]}")
            if attempt < retry:
                time.sleep(1 if urgent else 2 * attempt)
        if kidx == 0 and len(keys) > 1:
            log.warning("主 SENDKEY 推送全部失败，立即切换备用 SENDKEY2")

    log.error(f"推送最终失败（所有 key 均已重试 {retry} 次）: {title[:30]}")
    return False


def send_wx_urgent(title: str, content: str) -> bool:
    """
    ★ 高优先级信号快速推送通道（v9.5）：
      超时 5s、失败等 1s、主 key 失败立即切备用，重试 2 次。
    适用于：竞价-连板-极限 / 评分≥80 / 大盘崩溃预警 等时效性最强的信号。
    """
    return send_wx(title, content, retry=2, urgent=True)


def push_dedup_key(code: str, strategy: str, phase: str) -> str:
    """生成推送去重 key：今日日期+股票代码+策略+阶段"""
    return f"{beijing_now().strftime('%Y%m%d')}|{code}|{strategy}|{phase}"


def is_already_pushed(code: str, strategy: str, phase: str) -> bool:
    """检查该信号今日该阶段是否已推送过，防止重复轰炸"""
    return push_dedup_key(code, strategy, phase) in _push_dedup_set


def mark_pushed(code: str, strategy: str, phase: str) -> None:
    """标记该信号已推送"""
    _push_dedup_set.add(push_dedup_key(code, strategy, phase))


# ================================================================
# 数据层
# ================================================================
def get_zt_pool() -> pd.DataFrame:
    """获取今日涨停板股票池"""
    try:
        df = ak.stock_zt_pool_em(date=beijing_now().strftime("%Y%m%d"))
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        log.error(f"获取涨停池失败: {e}")
        return pd.DataFrame()

def get_yesterday_zt() -> pd.DataFrame:
    """获取最近一个交易日涨停池。
    区分「接口成功但无数据（节假日/非交易日）」和「接口调用失败（网络/格式变化）」。
    调用失败时返回一个带 _fetch_error=True 标记的空 DataFrame，上层可据此告警。
    """
    last_error = None
    for i in range(1, 7):
        d = (beijing_now() - datetime.timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = ak.stock_zt_pool_em(date=d)
            if df is not None and len(df) > 0:
                log.info(f"昨日涨停池日期: {d}，共 {len(df)} 只")
                return df
            # 接口正常但返回空（非交易日），继续往前找
        except Exception as e:
            last_error = e
            log.debug(f"昨日涨停池 {d} 请求失败: {e}")
            continue
    if last_error is not None:
        log.warning(f"⚠️ 昨日涨停池所有日期请求均失败，最后错误: {last_error}。"
                    f"连板/首板判断可能不准，请检查网络或akshare版本。")
        empty = pd.DataFrame()
        empty._fetch_error = True   # type: ignore[attr-defined]
        return empty
    return pd.DataFrame()

def get_yesterday_dt() -> pd.DataFrame:
    """获取最近一个交易日跌停池"""
    last_error = None
    for i in range(1, 7):
        d = (beijing_now() - datetime.timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = ak.stock_zt_pool_dtgc_em(date=d)
            if df is not None and len(df) > 0:
                log.info(f"昨日跌停池日期: {d}，共 {len(df)} 只")
                return df
        except Exception as e:
            last_error = e
            log.debug(f"昨日跌停池 {d} 请求失败: {e}")
            continue
    if last_error is not None:
        log.warning(f"⚠️ 昨日跌停池所有日期请求均失败: {last_error}")
    return pd.DataFrame()

def _em_make_session(headers: dict) -> requests.Session:
    """创建独立 Session，配置连接池大小与重试适配器，避免多线程共享连接池溢出。"""
    from requests.adapters import HTTPAdapter
    sess = requests.Session()
    sess.headers.update(headers)
    # pool_connections/pool_maxsize 与并发线程数匹配，避免 "Connection pool is full" 警告
    adapter = HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=0)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


def _em_fetch_page_isolated(url: str, params: dict, headers: dict, pn: int, retry: int = 1) -> list:
    """
    每次调用独立创建 Session（不共享），用完即关，彻底消除连接池竞争。
    失败后自动重试 retry 次（间隔 0.5s）。
    """
    import time as _t
    last_exc = None
    for attempt in range(retry + 1):
        sess = _em_make_session(headers)
        try:
            r = sess.get(url, params=dict(params, pn=str(pn)), timeout=20)
            r.raise_for_status()
            j = r.json()
            return j["data"]["diff"] if j.get("data") and j["data"].get("diff") else []
        except Exception as e:
            last_exc = e
            if attempt < retry:
                _t.sleep(0.5)
        finally:
            sess.close()
    raise last_exc  # type: ignore[misc]


def _em_build_df(all_rows: list) -> pd.DataFrame:
    """将东方财富原始行列表转为标准化 DataFrame（两个节点共用）。"""
    df = pd.DataFrame(all_rows)
    df = df.rename(columns={
        "f12": "code",    "f14": "name",       "f2": "price",
        "f3":  "chg_pct", "f6":  "amount",      "f20": "circ_mkt_cap",
        "f8":  "turnover","f9":  "vol_ratio",   "f17": "open",
        "f15": "high",    "f16": "low",         "f18": "prev_close",
    })
    keep = [c for c in ("code","name","price","chg_pct","amount","circ_mkt_cap",
                         "turnover","vol_ratio","open","high","low","prev_close")
            if c in df.columns]
    df = df[keep].copy()
    for col in ("price","chg_pct","amount","circ_mkt_cap","turnover","vol_ratio",
                "open","high","low","prev_close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _fetch_spot_em() -> pd.DataFrame:
    """
    主接口：akshare stock_zh_a_spot_em()，内部携带东方财富合法 Cookie/Token，
    不会被 push2 直连封锁。字段映射为标准化列名。
    """
    df = ak.stock_zh_a_spot_em()
    if df is None or df.empty:
        raise ValueError("stock_zh_a_spot_em 返回空数据")
    # akshare 返回的列名（中文）→ 标准化英文列名
    col_map = {
        "代码":     "code",
        "名称":     "name",
        "最新价":   "price",
        "涨跌幅":   "chg_pct",
        "成交额":   "amount",
        "流通市值": "circ_mkt_cap",
        "换手率":   "turnover",
        "量比":     "vol_ratio",
        "今开":     "open",
        "最高":     "high",
        "最低":     "low",
        "昨收":     "prev_close",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    keep = [c for c in ("code","name","price","chg_pct","amount","circ_mkt_cap",
                         "turnover","vol_ratio","open","high","low","prev_close")
            if c in df.columns]
    df = df[keep].copy()
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)
    for col in ("price","chg_pct","amount","circ_mkt_cap","turnover","vol_ratio",
                "open","high","low","prev_close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _fetch_spot_em_backup() -> pd.DataFrame:
    """
    备用接口：东方财富 push2 直连，每页 500 条串行请求（约12页），
    单连接串行不触发服务端限流，akshare 主接口失败时自动切换。
    """
    _EM_URL = "https://82.push2.eastmoney.com/api/qt/clist/get"
    _EM_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    _EM_PARAMS_BASE = {
        "pz": "500", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f12,f14,f2,f3,f6,f20,f8,f9,f17,f15,f16,f18",
    }

    sess = _em_make_session(_EM_HEADERS)
    all_rows: list = []
    try:
        pn = 1
        while True:
            r = sess.get(_EM_URL, params=dict(_EM_PARAMS_BASE, pn=str(pn)), timeout=20)
            r.raise_for_status()
            j = r.json()
            if not j.get("data") or not j["data"].get("diff"):
                break
            rows = j["data"]["diff"]
            all_rows.extend(rows)
            total = int(j["data"].get("total", 0))
            pz_int = int(_EM_PARAMS_BASE["pz"])
            if len(all_rows) >= total or len(rows) < pz_int:
                break
            pn += 1
            time.sleep(0.3)   # 串行请求间隔，避免触发限流
    finally:
        sess.close()

    if not all_rows:
        raise ValueError("东方财富备用节点返回空数据")
    return _em_build_df(all_rows)


def _txzq_build_fallback_codes() -> list:
    """
    规则生成全市场A股代码列表（完全离线，不依赖任何网络请求）。
    只枚举真实存在的号段，约6200个，减少无效请求批次。
    腾讯接口对无效代码返回空数据行（不报错），少量无效代码无影响。
    """
    codes = []
    # 沪市主板：600000-606999（实际上市到约6069xx）
    codes += [f"{i:06d}" for i in range(600000, 607000)]
    # 沪市科创板：688000-688999
    codes += [f"{i:06d}" for i in range(688000, 689000)]
    # 深市主板：000001-000999
    codes += [f"{i:06d}" for i in range(1, 1000)]
    # 深市中小板：001000-002999（含001xxx、002xxx）
    codes += [f"{i:06d}" for i in range(1000, 3000)]
    # 深市创业板：300001-301599
    codes += [f"{i:06d}" for i in range(300001, 301600)]
    # 北交所：830000-835999、920000-920999
    codes += [f"{i:06d}" for i in range(830000, 836000)]
    codes += [f"{i:06d}" for i in range(920000, 921000)]
    return codes


def _fetch_spot_txzq() -> pd.DataFrame:
    """
    第三接口：腾讯证券行情直连（qt.gtimg.cn），与东方财富/新浪完全独立的第三数据源。
    腾讯行情接口反爬宽松，开盘高峰期稳定性强。
    每批150个股票代码，串行请求（全市场约5000只，约34批）。
    ★ v3.7：代码列表优先用全局缓存，缓存为空时尝试 akshare 获取，
             akshare 也失败则用规则离线生成兜底，彻底脱离对 akshare 的依赖。

    腾讯返回格式（GBK编码）：
      v_sh600000="1~浦发银行~600000~现价~昨收~今开~最高~最低~买一~卖一~成交量~成交额(万元)~...~涨跌幅~换手率~...";
    字段索引（0-based，~分割）：
      [0]=标志  [1]=名称  [2]=代码  [3]=现价  [4]=昨收  [5]=今开
      [6]=最高  [7]=最低  [11]=成交额(万元)  [34]=涨跌幅  [37]=换手率
    """
    import re as _re
    import time as _t
    global _TXZQ_CODES_CACHE

    # 1. 获取全市场代码列表（缓存 → akshare → 离线规则兜底）
    all_codes: list = []
    if _TXZQ_CODES_CACHE:
        all_codes = _TXZQ_CODES_CACHE
        log.debug(f"[腾讯行情] 使用缓存代码列表（{len(all_codes)} 只）")
    else:
        try:
            code_df = ak.stock_info_a_code_name()
            all_codes = code_df["code"].astype(str).str.zfill(6).tolist()
            if all_codes:
                _TXZQ_CODES_CACHE = all_codes   # 成功则写入缓存
                log.info(f"[腾讯行情] 代码列表已缓存（{len(all_codes)} 只）")
        except Exception as e:
            log.warning(f"[腾讯行情] akshare 代码列表获取失败（{e}），改用离线规则生成")
            all_codes = _txzq_build_fallback_codes()
            _TXZQ_CODES_CACHE = all_codes   # 离线生成同样写入缓存，避免下次重复生成
            log.info(f"[腾讯行情] 离线规则生成代码 {len(all_codes)} 个（已缓存）")

    if not all_codes:
        raise ValueError("全市场代码列表为空")

    # 2. 腾讯行情接口：sh/sz前缀 + 代码批量查询
    def _code_to_txsym(c: str) -> str:
        return ("sh" if c.startswith(("6", "9")) else "sz") + c

    _TX_URL = "https://qt.gtimg.cn/q={}"
    _TX_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":    "https://gu.qq.com/",
        "Accept":     "text/plain,*/*",
    }
    # 匹配每只股票的数据行：v_sh000001="..." 或 v_sz000001="..."
    _LINE_RE = _re.compile(r'v_\w+="([^"]+)"')

    def _safe_float(s: str) -> float:
        try:
            return float(s) if s.strip() else float("nan")
        except ValueError:
            return float("nan")

    _BATCH       = 150
    _TX_TIMEOUT  = 8      # 单批超时缩短至8s（原15s），减少网络不通时的等待
    _MAX_CONSEC_FAIL = 5  # 连续失败超过5批视为网络不通，立即放弃（不再等剩余批次）
    all_rows = []
    consec_fail = 0       # 连续失败批次计数
    sess = _em_make_session(_TX_HEADERS)
    try:
        for i in range(0, len(all_codes), _BATCH):
            batch = all_codes[i: i + _BATCH]
            syms  = ",".join(_code_to_txsym(c) for c in batch)
            try:
                r = sess.get(_TX_URL.format(syms), timeout=_TX_TIMEOUT)
                r.encoding = "gbk"
                text = r.text
                consec_fail = 0   # 成功则重置连续失败计数
            except Exception:
                consec_fail += 1
                if consec_fail >= _MAX_CONSEC_FAIL:
                    # 连续5批都失败 = 网络不通，立即放弃，不再等待后续批次
                    raise ValueError(
                        f"腾讯行情接口连续 {consec_fail} 批失败，判定为网络不通，跳转下一数据源"
                    )
                _t.sleep(0.2)
                continue
            for m in _LINE_RE.finditer(text):
                f = m.group(1).split("~")
                if len(f) < 38:
                    continue
                code = f[2].zfill(6) if f[2] else ""
                if not code:
                    continue
                all_rows.append({
                    "code":         code,
                    "name":         f[1],
                    "price":        _safe_float(f[3]),
                    "prev_close":   _safe_float(f[4]),
                    "open":         _safe_float(f[5]),
                    "high":         _safe_float(f[6]),   # [6]=最高
                    "low":          _safe_float(f[7]),   # [7]=最低
                    "amount":       _safe_float(f[11]) * 10000,  # [11]=成交额(万元)→元
                    "chg_pct":      _safe_float(f[34]),  # [34]=涨跌幅%
                    "turnover":     _safe_float(f[37]),  # [37]=换手率%
                    "circ_mkt_cap": float("nan"),        # 腾讯接口无此字段
                    "vol_ratio":    float("nan"),        # 腾讯接口无此字段
                })
            if i + _BATCH < len(all_codes):
                _t.sleep(0.05)   # 50ms 间隔，避免触发限流
    finally:
        sess.close()

    if not all_rows:
        raise ValueError("腾讯行情接口返回空数据")

    df = pd.DataFrame(all_rows)
    for col in ("price","chg_pct","amount","circ_mkt_cap","turnover","vol_ratio",
                "open","high","low","prev_close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _fetch_spot_tdx() -> pd.DataFrame:
    """
    第四接口：通达信行情服务器（mootdx），走 TCP 协议，与所有 HTTP 接口完全独立。
    不受东方财富/腾讯开盘反爬封锁影响，实时行情延迟约 3 秒。
    ★ v3.7：作为最终兜底接口，全部 HTTP 接口失败时自动切换。

    mootdx 返回字段：code/name/open/high/low/pre_close/price/volume/amount
    需自行计算：chg_pct = (price - pre_close) / pre_close * 100
    无法获取：circ_mkt_cap / vol_ratio / turnover（填 nan）
    """
    try:
        from mootdx.quotes import Quotes as _Quotes
    except ImportError:
        raise ImportError("mootdx 未安装（pip install mootdx），跳过通达信接口")

    client = _Quotes.factory(market='std', multithread=True, heartbeat=False)
    try:
        df = client.stocks()
    finally:
        try:
            client.close()
        except Exception:
            pass

    if df is None or df.empty:
        raise ValueError("mootdx 通达信接口返回空数据")

    # 字段重命名：pre_close → prev_close
    col_map = {
        "pre_close":  "prev_close",
        "last_close": "prev_close",   # 部分版本字段名不同
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # 过滤非A股（只保留 000/001/002/003/300/301/600/601/603/605/688/689/8/43/92 开头）
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)
        df = df[df["code"].str.match(r'^(00[0-3]|30[01]|60[0135]|688|689|8[3-9]|43|92)')].copy()

    # 自行计算涨跌幅
    if "price" in df.columns and "prev_close" in df.columns:
        pc = pd.to_numeric(df["prev_close"], errors="coerce").replace(0, float("nan"))
        pr = pd.to_numeric(df["price"],      errors="coerce")
        df["chg_pct"] = (pr - pc) / pc * 100
    else:
        df["chg_pct"] = float("nan")

    # 补全缺失字段（circ_mkt_cap / vol_ratio / turnover 通达信无此数据）
    for col in ("circ_mkt_cap", "vol_ratio", "turnover"):
        if col not in df.columns:
            df[col] = float("nan")

    keep = [c for c in ("code","name","price","chg_pct","amount","circ_mkt_cap",
                         "turnover","vol_ratio","open","high","low","prev_close")
            if c in df.columns]
    df = df[keep].copy()

    for col in ("price","chg_pct","amount","open","high","low","prev_close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.reset_index(drop=True)


def get_realtime_quotes(codes: list = None) -> pd.DataFrame:
    """
    获取实时行情（v3.11 真正并发竞速版）。

    ★ 并发设计：4个接口同时启动，任何一个成功立刻返回，不等其余接口。
      用 queue.Queue 传递结果，主线程阻塞在 queue.get() 上，第一个成功的线程
      put 结果后主线程立刻拿到并返回，其余线程在后台自然结束（daemon线程）。

    ★ 优先级机制：queue 里放 (priority, source, df)，主线程收到第一个成功结果
      后再等 0.3s 捞一次队列，如果有更高优先级（字段更完整）的结果也已就绪则
      优先用它（东财有 vol_ratio/circ_mkt_cap，腾讯/通达信缺失这两个字段）。

    ★ v3.4 缓存：全市场行情 TTL=55s，同一轮多次调用只发一次请求。
    ★ v3.6 降级：全部失败时容忍 5 分钟内旧缓存，应对开盘瞬间短暂断连。
    """
    import time as _time
    import queue
    import threading
    global _realtime_cache, _realtime_cache_time
    now_ts = _time.time()

    if codes is None:
        if not _realtime_cache.empty and (now_ts - _realtime_cache_time) < REALTIME_CACHE_TTL:
            return _realtime_cache.copy()

    fetchers = [
        (0, "akshare(东方财富)",      _fetch_spot_em),
        (1, "东方财富直连(大页串行)",  _fetch_spot_em_backup),
        (2, "腾讯证券(独立数据源)",    _fetch_spot_txzq),
        (3, "通达信TCP(mootdx)",      _fetch_spot_tdx),
    ]
    total = len(fetchers)

    ok_q:   queue.Queue = queue.Queue()   # (priority, source, df)
    err_q:  queue.Queue = queue.Queue()   # (source, exc)

    def _worker(priority: int, source: str, fetcher):
        try:
            df = fetcher()
            ok_q.put((priority, source, df))
        except ImportError as e:
            # 依赖未安装（如 mootdx），静默跳过，不打 WARNING
            log.debug(f"[行情] {source} 跳过（依赖未安装）: {e}")
            err_q.put((source, e))
        except Exception as e:
            log.warning(f"[行情] {source} 接口失败: {e}")
            err_q.put((source, e))

    # 全部作为 daemon 线程启动（主线程退出时自动杀掉，不阻塞进程）
    for priority, source, fetcher in fetchers:
        t = threading.Thread(target=_worker, args=(priority, source, fetcher), daemon=True)
        t.start()

    # 等待：收到第一个成功结果后，再额外等 0.3s 捞优先级更高的结果
    best: tuple | None = None          # (priority, source, df)
    finished_errors = 0

    deadline = _time.time() + 60.0     # 最长等待 60s（4个接口里最慢的通达信约需 30s）
    while _time.time() < deadline:
        # 先把所有已到达的成功结果捞出来
        while True:
            try:
                item = ok_q.get_nowait()   # (priority, source, df)
                if best is None or item[0] < best[0]:
                    best = item
            except queue.Empty:
                break

        # 统计已报错数量
        while True:
            try:
                err_q.get_nowait()
                finished_errors += 1
            except queue.Empty:
                break

        if best is not None:
            # 已有成功结果；若优先级最高的（priority=0）已到或全部接口已结束，立刻返回
            all_done = (finished_errors + (1 if best else 0)) >= total
            if best[0] == 0 or all_done:
                break
            # 否则再等 0.3s，看有没有更高优先级的结果
            _time.sleep(0.3)
            continue

        if finished_errors >= total:
            break   # 全部失败，不用再等

        _time.sleep(0.05)   # 短暂让出 CPU，避免空转

    if best is not None:
        _, best_source, df = best
        log.info(f"[行情] 采用: {best_source}，行数={len(df)}")
        if codes is None:
            _realtime_cache      = df.copy()
            _realtime_cache_time = _time.time()
        if codes:
            df = df[df["code"].isin([str(c).zfill(6) for c in codes])].reset_index(drop=True)
        return df

    # 全部接口失败 → 降级使用缓存
    log.error("[行情] 所有接口均失败")
    _CACHE_FALLBACK_TTL = 300.0
    if codes is None and not _realtime_cache.empty:
        cache_age = now_ts - _realtime_cache_time
        if cache_age < _CACHE_FALLBACK_TTL:
            log.warning(f"[行情] 降级使用缓存数据（缓存年龄 {cache_age:.0f}s）")
            return _realtime_cache.copy()
        else:
            log.error(f"[行情] 缓存已过期（{cache_age:.0f}s），返回空数据")
    return pd.DataFrame()

def get_hist_kline(code: str, days: int = 60) -> pd.DataFrame:
    """获取历史日线（带炸板统计字段）"""
    try:
        start = (beijing_now() - datetime.timedelta(days=days * 2)).strftime("%Y%m%d")
        end   = beijing_now().strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(symbol=code[:6], period="daily",
                                 start_date=start, end_date=end, adjust="qfq")
        if df is None or len(df) < 10:
            return pd.DataFrame()
        df = df.tail(days).reset_index(drop=True)
        # 计算每日涨跌幅和涨停/炸板标志
        if "收盘" in df.columns and "昨收" in df.columns:
            df["_prev"] = df["昨收"]
        elif "收盘" in df.columns:
            df["_prev"] = df["收盘"].shift(1)
        else:
            return df
        df["_chg"] = (df["收盘"] - df["_prev"]) / df["_prev"].replace(0, np.nan)
        # ★ v3.4修复：过滤除权日异常涨跌幅（复权价突变导致假涨停/假跌停）
        # 当日振幅>20%极可能是除权日数据异常，将其置为NaN避免影响炸板统计
        df.loc[df["_chg"].abs() > 0.20, "_chg"] = np.nan
        # 当日最高涨幅≥9.5%（曾触及涨停）但收盘<9.5%（炸板）
        if "最高" in df.columns:
            df["_hi_chg"]   = (df["最高"]  - df["_prev"]) / df["_prev"].replace(0, np.nan)
            df["_bomb"]     = (df["_hi_chg"] >= 0.095) & (df["_chg"] < 0.09)
            df["_bomb_bad"] = df["_bomb"] & ((df["收盘"] - df["最高"]) / df["最高"].replace(0, np.nan) < FAKE_BOMB_BACK_THRESH)
        return df
    except Exception as e:
        log.debug(f"{code} 历史数据失败: {e}")
        return pd.DataFrame()


def get_intraday_kline(code: str) -> pd.DataFrame:
    """获取当日分时数据（用于封板时间判断）"""
    try:
        df = ak.stock_zh_a_hist_min_em(
            symbol=code[:6], period="1",
            start_date=beijing_now().strftime("%Y-%m-%d 09:00:00"),
            end_date=beijing_now().strftime("%Y-%m-%d 15:10:00"),
            adjust="qfq"
        )
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        log.debug(f"{code} 分时数据失败: {e}")
        return pd.DataFrame()


# ================================================================
# ★ 市场情绪指标（新增）
# ================================================================

# 全局缓存，每轮扫描只拉一次（小写避免被识别为常量）
_emotion_cache: dict = {}
_emotion_cache_time: float = 0.0
_emotion_cache_ttl  = 300   # 5分钟刷新一次


def get_market_emotion() -> dict:
    """
    获取当日市场情绪数据（★ v5.0 增加赚钱效应指数）：
      zt_count      : 今日全市场涨停家数
      dt_count      : 今日全市场跌停家数
      zt_dt_ratio   : 涨停/跌停比
      emotion       : "热" / "正常" / "冷" / "过热"
      in_best       : 是否处于最优区间
      warn_msg      : 警告信息
      -- 新增赚钱效应字段 --
      max_height    : 今日最高连板高度（几连板）
      height2_count : 2板及以上家数（连板梯队宽度）
      heat_index    : 赚钱效应综合热度指数（0~100）
      heat_level    : "爆热"(80+) / "热" (60~80) / "正常"(40~60) / "冷"(20~40) / "极冷"(<20)
      pos_coeff     : 仓位系数建议（0.3~1.5，乘以Kelly基础仓位）

    数据来源：东方财富实时行情（涨跌幅≥9.5%计为涨停）
    """
    global _emotion_cache, _emotion_cache_time
    import time as _time
    now_ts = _time.time()
    if _emotion_cache and (now_ts - _emotion_cache_time) < _emotion_cache_ttl:
        return _emotion_cache

    result = {
        "zt_count":    0,
        "dt_count":    0,
        "zt_dt_ratio": 0.0,
        "emotion":     "未知",
        "in_best":     True,     # 默认放行（获取失败时不误杀）
        "warn_msg":    "",
        # 赚钱效应
        "max_height":    0,
        "height2_count": 0,
        "heat_index":    50,   # 未知时给中性值
        "heat_level":    "正常",
        "pos_coeff":     1.0,
    }

    try:
        # 优先从今日涨停池获取家数（最准确）
        zt_df = ak.stock_zt_pool_em(date=beijing_now().strftime("%Y%m%d"))
        zt_count = len(zt_df) if zt_df is not None else 0

        # 跌停池
        dt_df = ak.stock_zt_pool_dtgc_em(date=beijing_now().strftime("%Y%m%d"))
        dt_count = len(dt_df) if dt_df is not None else 0

        ratio = zt_count / max(dt_count, 1)
        result["zt_count"]    = zt_count
        result["dt_count"]    = dt_count
        result["zt_dt_ratio"] = round(ratio, 2)

        # ── ★ 赚钱效应：从涨停池提取连板高度信息 ──────────────────────
        max_height    = 0
        height2_count = 0
        if zt_df is not None and not zt_df.empty:
            # 东方财富涨停池中"连板数"字段名可能是"连板数"/"连续涨停天数"等，兼容处理
            height_col = None
            for col in ["连板数", "连续涨停天数", "涨停天数", "连涨天数"]:
                if col in zt_df.columns:
                    height_col = col
                    break
            if height_col:
                heights = pd.to_numeric(zt_df[height_col], errors="coerce").fillna(1)
                max_height    = int(heights.max())
                height2_count = int((heights >= 2).sum())

        result["max_height"]    = max_height
        result["height2_count"] = height2_count

        # ── ★ v9.2 新增：炸板打开率（第四维度）──────────────────────────
        bomb_open_rate = 0.0  # 今日炸板打开率（炸板池家数/涨停池家数）
        try:
            zb_df = ak.stock_zt_pool_zb_em(date=beijing_now().strftime("%Y%m%d"))
            zb_count = len(zb_df) if zb_df is not None else 0
            # 炸板打开率 = 炸板家数 / (涨停家数 + 炸板家数)，分母保护
            bomb_open_rate = round(zb_count / max(zt_count + zb_count, 1), 3)
            result["bomb_open_rate"] = bomb_open_rate
            result["zb_count"]       = zb_count
            log.info(f"炸板打开率: {bomb_open_rate:.1%}（炸板{zb_count}家/涨停{zt_count}家）")
        except Exception:
            result["bomb_open_rate"] = 0.0
            result["zb_count"]       = 0

        # ★ v9.2 炸板打开率对市场情绪的修正
        if BOMB_OPEN_RATE_ENABLE and bomb_open_rate >= BOMB_OPEN_RATE_STOP:
            result["warn_msg"] = str(result.get("warn_msg", "")) + (
                f"  ⚠️炸板打开率{bomb_open_rate:.0%}（≥{BOMB_OPEN_RATE_STOP:.0%}），"
                f"市场极弱，暂停竞价推送"
            )
            result["bomb_open_rate_stop"] = True   # 标记：需要暂停推送
        elif BOMB_OPEN_RATE_ENABLE and bomb_open_rate >= BOMB_OPEN_RATE_WARN:
            result["warn_msg"] = str(result.get("warn_msg", "")) + (
                f"  ⚠️炸板打开率{bomb_open_rate:.0%}（≥{BOMB_OPEN_RATE_WARN:.0%}），市场偏弱"
            )
            result["bomb_open_rate_stop"] = False
        else:
            result["bomb_open_rate_stop"] = False

        # ── ★ 赚钱效应热度指数算法（0~100）─────────────────────────────
        # 四个维度加权：
        #   1. 涨停家数得分（占35分）：最优区间[10,50]满分，两侧递减
        #   2. 涨跌比得分（占25分）：比值≥3满分，<1得0分
        #   3. 连板高度得分（占30分）：最高连板数×5分（上限20分）+ 宽度（上限10分）
        #   4. 炸板打开率扣分（占10分）：打开率越高扣分越多（市场弱势惩罚）

        # 1. 涨停家数得分
        if _EMOTION_BEST_LOW <= zt_count <= _EMOTION_BEST_HIGH:
            zt_score = 35
        elif zt_count < _EMOTION_BEST_LOW:
            zt_score = max(0, 35 * zt_count / max(_EMOTION_BEST_LOW, 1))
        else:
            # 超过最优上限，过热递减
            zt_score = max(8, 35 - (zt_count - _EMOTION_BEST_HIGH) * 0.3)

        # 2. 涨跌比得分
        ratio_score = min(25, ratio * 8.3)

        # 3. 连板高度得分
        height_score = min(20, max_height * 5)
        width_score  = min(10, (height2_count // 5) * 5)
        board_score  = height_score + width_score

        # 4. 炸板打开率扣分（打开率0%=满10分，打开率100%=0分）
        bomb_score = round(10 * max(0.0, 1.0 - bomb_open_rate * 1.5))

        heat_index = round(zt_score + ratio_score + board_score + bomb_score)
        heat_index = max(0, min(100, heat_index))

        # 热度等级 & 仓位系数
        if heat_index >= 80:
            heat_level = "爆热"
            pos_coeff  = 1.4    # 赚钱效应爆棚，可以适度激进
        elif heat_index >= 65:
            heat_level = "热"
            pos_coeff  = 1.2
        elif heat_index >= 45:
            heat_level = "正常"
            pos_coeff  = 1.0
        elif heat_index >= 25:
            heat_level = "冷"
            pos_coeff  = 0.6
        else:
            heat_level = "极冷"
            pos_coeff  = 0.3    # 市场极冷，出手频次和仓位都要大幅收缩

        result["heat_index"] = heat_index
        result["heat_level"] = heat_level
        result["pos_coeff"]  = pos_coeff

        # ── 原有情绪状态判断（保持兼容）──────────────────────────────────
        if zt_count < EMOTION_MIN_ZT:
            result["emotion"]  = "冷"
            result["in_best"]  = False
            result["warn_msg"] = (f"⚠️今日涨停仅{zt_count}家（<{EMOTION_MIN_ZT}），"
                                  f"市场冷清，打板胜率偏低")
        elif zt_count > EMOTION_MAX_ZT:
            result["emotion"]  = "过热"
            result["in_best"]  = False
            result["warn_msg"] = (f"⚠️今日涨停{zt_count}家（>{EMOTION_MAX_ZT}），"
                                  f"市场过热/政策风险，注意仓位")
        elif _EMOTION_BEST_LOW <= zt_count <= _EMOTION_BEST_HIGH:
            result["emotion"]  = "正常"
            result["in_best"]  = True
        else:
            result["emotion"]  = "热"
            result["in_best"]  = True

        if ratio < EMOTION_MIN_RATIO:
            result["warn_msg"] = str(result["warn_msg"]) + (
                f"  涨跌比{ratio:.1f}x（<{EMOTION_MIN_RATIO}），"
                f"弱市氛围，降低仓位"
            )

        log.info(f"市场情绪：涨停{zt_count}家/跌停{dt_count}家 "
                 f"炸板打开率{bomb_open_rate:.0%} 比值{ratio:.1f}x 状态={result['emotion']} "
                 f"赚钱效应={heat_level}({heat_index}) 最高{max_height}板 "
                 f"2板+{height2_count}家 仓位系数×{pos_coeff}")

    except Exception as e:
        log.debug(f"情绪数据获取失败（不影响主流程）: {e}")
        result["in_best"] = True  # 获取失败默认放行

    _emotion_cache      = result
    _emotion_cache_time = now_ts
    return result


# ================================================================
# ★ 板块聚合（新增，用于板块效应评分）
# ================================================================

# 全局缓存，每轮扫描只算一次（小写避免被识别为常量）
_sector_cache: dict = {}
_sector_cache_time: float = 0.0
_sector_cache_ttl  = 300   # 5分钟刷新一次


# ================================================================
# ★ v9.2 龙虎榜因子（新增）
# ================================================================
_lhb_cache: set = set()     # 今日龙虎榜净买入代码集合
_lhb_cache_time: float = 0.0


def get_lhb_net_buy_codes() -> set:
    """
    获取今日龙虎榜净买入个股代码集合（机构/游资席位净买≥0）。
    ★ 研究结论：龙虎榜净买入个股的次日竞价高开胜率提升 8~12%
    数据来源：ak.stock_lhb_detail_em(date=今日)
    缓存 1 小时（盘中不频繁变化）。
    """
    global _lhb_cache, _lhb_cache_time
    import time as _t
    now_ts = _t.time()
    if _lhb_cache and (now_ts - _lhb_cache_time) < LHB_CACHE_TTL:
        return _lhb_cache
    if not LHB_BONUS_ENABLE:
        return set()
    try:
        today_str = beijing_now().strftime("%Y%m%d")
        df = ak.stock_lhb_detail_em(date=today_str)
        if df is None or df.empty:
            return set()
        # 字段兼容：东方财富龙虎榜可能返回中文或英文字段名
        code_col = None
        for c in ["代码", "股票代码", "code"]:
            if c in df.columns:
                code_col = c
                break
        net_col = None
        for c in ["净额", "净买入额", "net", "净买入"]:
            if c in df.columns:
                net_col = c
                break
        if code_col is None:
            return set()
        codes: set = set()
        if net_col is not None:
            # 按代码聚合净额，取净额>0的代码
            df[net_col] = pd.to_numeric(df[net_col], errors="coerce").fillna(0)
            grp = df.groupby(code_col)[net_col].sum()
            codes = set(str(c).zfill(6) for c, v in grp.items() if v > 0)
        else:
            # 无净额字段：只要上榜即加分
            codes = set(str(c).zfill(6) for c in df[code_col].unique())
        _lhb_cache      = codes
        _lhb_cache_time = now_ts
        log.info(f"龙虎榜净买入标的：{len(codes)} 只")
        return codes
    except Exception as e:
        log.debug(f"龙虎榜数据获取失败（不影响主流程）: {e}")
        return set()


def get_sector_zt_map(zt_df: pd.DataFrame) -> dict:
    """
    计算当日涨停池中每个行业的涨停家数，返回 {股票代码: 同行业涨停数} 映射。

    研究结论：同板块涨停数 ≥3 家时，胜率显著高于孤立涨停（+12%）
    数据来源：涨停池中的行业字段（所属行业 / 行业）
    """
    global _sector_cache, _sector_cache_time
    import time as _time
    now_ts = _time.time()
    if _sector_cache and (now_ts - _sector_cache_time) < _sector_cache_ttl:
        return _sector_cache

    code_sector_count: dict = {}

    if zt_df.empty:
        return code_sector_count

    # 尝试多个可能的行业字段名
    sector_col = None
    for col in ["所属行业", "行业", "sector", "industry"]:
        if col in zt_df.columns:
            sector_col = col
            break

    if sector_col is None:
        # 无行业字段，尝试从 akshare 单独获取
        try:
            industry_df = ak.stock_board_industry_name_em()
            # 这需要逐股查询，成本太高，此处仅做软退化：返回空字典（不加分也不扣分）
            log.debug("涨停池无行业字段，板块效应评分跳过")
            _sector_cache      = {}
            _sector_cache_time = now_ts
            return {}
        except:
            return {}

    # 统计各行业涨停家数
    sector_counts: dict = {}
    code_to_sector: dict = {}
    for _, row in zt_df.iterrows():
        code   = str(row.get("代码", "")).zfill(6)
        sector = str(row.get(sector_col, "未知")).strip()
        if not sector or sector in ("nan", "None", ""):
            sector = "未知"
        code_to_sector[code] = sector
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    # 映射：每只股票→同行业涨停家数
    for code, sector in code_to_sector.items():
        code_sector_count[code] = sector_counts.get(sector, 1)

    _sector_cache      = code_sector_count
    _sector_cache_time = now_ts
    return code_sector_count


def sector_score(code: str, sector_zt_map: dict) -> tuple:
    """
    计算板块效应评分。
    返回 (score_delta: float, sector_msg: str)
      score_delta > 0：加分（同板块热度高）
      score_delta < 0：扣分（孤立涨停）
    """
    count = sector_zt_map.get(code, 0)
    if count == 0:
        return 0.0, ""   # 无数据，不评分
    if count >= 3:
        return 15.0, f"板块共振{count}家↑(+15分)"
    if count == 2:
        return 8.0, f"板块联动{count}家(+8分)"
    # count == 1：孤立涨停
    return -5.0, "孤立涨停⚠️(-5分)"


# ================================================================
# ★ Kelly 仓位建议（新增）
# ================================================================

def kelly_position_advice(score: float, strategy: str = "", market_state: str = "",
                          emotion: dict = None) -> str:
    """
    根据信号评分、策略类型、市场状态和赚钱效应热度给出Kelly仓位建议。
    ★ v5.0：新增 emotion 参数，纳入赚钱效应热度系数（pos_coeff）。
    ★ v8.4：新增月份仓位系数（弱势月份×0.5，强势月份×1.5）。
    公式：Kelly = 胜率 - (1-胜率)/盈亏比，再乘评分系数 × 月份系数 × 市场折扣 × 赚钱效应系数
    """
    # 按策略取对应参数
    params = KELLY_PARAMS.get(strategy, {"win": KELLY_WIN_RATE, "ratio": KELLY_PNL_RATIO})
    win   = params["win"]
    ratio = params["ratio"]
    kelly_full = max(0.0, win - (1 - win) / ratio)

    # 按评分取档位系数
    if score >= 80:
        coeff = 1.0
        label = "高置信"
    elif score >= 60:
        coeff = 0.5
        label = "中置信"
    else:
        coeff = 0.3
        label = "低置信"

    pct_raw = kelly_full * coeff * 100

    # ★ v9.4/v9.5：月份仓位系数（弱势月份减半，极端弱势更低，跳过月份不推送）
    month_note = ""
    _cur_month = beijing_now().month
    if WEAK_MONTH_POSITION_ENABLE and _cur_month in SKIP_MONTHS:
        return f"💰仓位建议：**0%**（{_cur_month}月属于跳过月份，本月不入场）"
    elif WEAK_MONTH_POSITION_ENABLE and _cur_month in EXTREME_WEAK_MONTHS:
        pct_raw *= EXTREME_WEAK_MONTH_POS_RATIO
        month_note = f"极端弱势月({_cur_month}月)×{EXTREME_WEAK_MONTH_POS_RATIO}"
    elif WEAK_MONTH_POSITION_ENABLE and _cur_month in WEAK_MONTHS:
        pct_raw *= WEAK_MONTH_POSITION_RATIO
        month_note = f"弱势月({_cur_month}月)×{WEAK_MONTH_POSITION_RATIO}"
    elif STRONG_MONTH_POSITION_ENABLE and _cur_month in STRONG_MONTHS:
        pct_raw *= STRONG_MONTH_POSITION_RATIO
        month_note = f"强势月({_cur_month}月)×{STRONG_MONTH_POSITION_RATIO}"

    # ★ 弱市打折：崩溃时建议0%，弱势时乘0.4，震荡时乘0.7
    market_note = ""
    if market_state in ("崩溃",):
        return f"💰仓位建议：**0%**（市场崩溃，建议空仓观望）"
    elif market_state in ("弱势",):
        pct_raw *= 0.4
        market_note = "弱市×0.4"
    elif market_state in ("震荡",):
        pct_raw *= 0.7
        market_note = "震荡×0.7"

    # ★ v5.0：赚钱效应热度系数
    heat_note = ""
    if emotion:
        pos_coeff  = emotion.get("pos_coeff", 1.0)
        heat_level = emotion.get("heat_level", "正常")
        heat_index = emotion.get("heat_index", 50)
        max_height = emotion.get("max_height", 0)
        if pos_coeff != 1.0:
            pct_raw  *= pos_coeff
            heat_icon = {"爆热": "🔥🔥", "热": "🔥", "正常": "", "冷": "🧊", "极冷": "🧊🧊"}.get(heat_level, "")
            heat_note = f"{heat_icon}{heat_level}({heat_index}分,{max_height}板)×{pos_coeff}"

    notes = " | ".join(filter(None, [month_note, market_note, heat_note]))
    notes_str = f"，{notes}" if notes else ""

    pct = max(1, round(pct_raw))
    # 极冷市场单笔上限 3%，防止在无效市场中过度开仓
    if emotion and emotion.get("heat_level") == "极冷":
        pct = min(pct, 3)
        notes_str += "，极冷上限3%"

    return f"💰仓位建议：**{pct}%**（{strategy or '默认'}/{label}，Kelly公式{notes_str}，严格止损）"


# ================================================================
# 打板信号结构
# ================================================================
@dataclass
class DaBanSignal:
    code:         str
    name:         str
    strategy:     str    # 首板 / 连板 / 竞价 / 反包
    price:        float
    connect_days: int
    seal_ratio:   float
    turnover:     float
    circ_mkt_cap: float  # 亿
    score:        float
    reason:       str
    stop_loss:    float
    entry_price:  float
    # 新增：真假涨停判断结果
    seal_time_hm: int    = 0      # 封板时间 HHMM（0=未知）
    fake_penalty: float  = 0.0   # 假涨停扣分（已反映在score中）
    fake_flags:   str    = ""     # 假涨停风险提示（非空=有风险）
    # 新增：股性指标
    stock_char:   str    = ""     # 股性标签（活跃/普通/迟钝）
    char_score:   float  = 0.0   # 股性评分（0~30，已计入score）
    avg_amplitude: float = 0.0   # 近20日日均振幅%
    open_rate:    float  = 0.0   # 历史打板次日高开率%
    buyable:      bool   = True  # 是否可以买进（买不进去的不推送）
    sub_strategy: str    = ""    # 竞价子策略标签（竞价-连板-极限 等），用于白名单过滤和紧急推送识别


# ================================================================
# 工具函数
# ================================================================
def is_st(name: str) -> bool:
    return "ST" in str(name).upper() or "退" in str(name)

def calc_zt_price(prev_close: float) -> float:
    return round(prev_close * 1.10, 2)

def position_score(hist: pd.DataFrame) -> float:
    if hist.empty or len(hist) < 20:
        return 0.5
    col = "收盘" if "收盘" in hist.columns else hist.columns[4]
    close = hist[col].values
    high60 = np.max(close[-min(60, len(close)):])
    low60  = np.min(close[-min(60, len(close)):])
    cur    = close[-1]
    if high60 == low60:
        return 0.5
    pos = (cur - low60) / (high60 - low60)
    if pos < 0.3:   return 0.8   # 低位
    if pos > 0.85:  return 0.9   # 高位突破
    return 0.5

def ma_trend_up(hist: pd.DataFrame) -> bool:
    if hist.empty or len(hist) < 20:
        return False
    col = "收盘" if "收盘" in hist.columns else hist.columns[4]
    close = hist[col].values
    return float(np.mean(close[-5:])) > float(np.mean(close[-10:])) > float(np.mean(close[-20:]))

def mkt_cap_score(circ_cap_yuan: float) -> float:
    """
    市值评分（研究结论：10~100亿最优）
    circ_cap_yuan：流通市值，单位元
    返回 0~1 分
    """
    cap_yi = circ_cap_yuan / 1e8
    if cap_yi < 5:    return 0.0   # 太小，流动性差
    if cap_yi < 10:   return 0.5
    if cap_yi <= 50:  return 1.0   # 最优区间 10~50亿
    if cap_yi <= 100: return 0.85  # 次优
    if cap_yi <= 200: return 0.5
    return 0.2   # >200亿大市值，封板难度大

def vol_ratio_score(vol_ratio: float) -> float:
    """
    量比评分（研究结论：1.5~3.0最优）
    """
    if vol_ratio < 1.0:   return 0.1
    if vol_ratio < 1.5:   return 0.5
    if vol_ratio <= 3.0:  return 1.0  # 最优区间
    if vol_ratio <= 5.0:  return 0.6  # 量比过高，短期爆炒风险
    return 0.3

def turnover_score_first(turnover: float) -> float:
    """首板换手率评分（5%~15%最优）"""
    if turnover < 3:    return 0.1
    if turnover < 5:    return 0.5
    if turnover <= 15:  return 1.0
    if turnover <= 20:  return 0.6
    return 0.2


# ================================================================
# ★ 股性识别系统（新增）
# ================================================================

def analyze_stock_character(hist: pd.DataFrame, code: str) -> dict:
    """
    分析个股股性，返回各项指标和综合评分。

    股性指标体系：
      1. 活跃度  ─ 近20日日均振幅（高低差/昨收），反映弹性
      2. 流动性  ─ 近20日日均成交额，反映买卖容易程度
      3. 打板胜率 ─ 历史涨停次日高开（≥+2%）比例，反映打板收益率
      4. 趋势性  ─ 近20日涨幅，反映持续走强能力
      5. 一字比例 ─ 历史涨停中一字板占比，>70%说明买不进去

    返回 dict 包含：
      buyable        bool  是否可以买进
      no_buy_reason  str   不可买原因（空=可买）
      char_label     str   股性标签
      char_score     float 股性加分（0~30分，加入总评分）
      avg_amplitude  float 近20日日均振幅%
      avg_amount     float 近20日日均成交额（元）
      open_rate      float 历史打板次日高开率%
      yizi_ratio     float 近30日一字板比例（0~1）
      yizi_streak    int   当前连续一字板天数
    """
    result = {
        "buyable":       True,
        "no_buy_reason": "",
        "char_label":    "普通",
        "char_score":    10.0,
        "avg_amplitude": 0.0,
        "avg_amount":    0.0,
        "open_rate":     0.0,
        "yizi_ratio":    0.0,
        "yizi_streak":   0,
    }

    if hist.empty or len(hist) < 5:
        return result

    # ── 基础列检查 ──────────────────────────────────────────────
    need_cols = {"收盘", "最高", "最低", "成交量"}
    if not need_cols.issubset(set(hist.columns)):
        return result

    has_open   = "开盘" in hist.columns
    has_amount = "成交额" in hist.columns
    has_prev   = "_prev" in hist.columns

    close  = hist["收盘"].values.astype(float)
    high   = hist["最高"].values.astype(float)
    low    = hist["最低"].values.astype(float)
    n      = len(close)
    days20 = min(20, n)
    days30 = min(30, n)

    # ── 计算前收盘（用于振幅基准）───────────────────────────────
    if has_prev:
        prev_close = hist["_prev"].values.astype(float)
    else:
        prev_close = np.concatenate([[close[0]], close[:-1]])

    prev_close = np.where(prev_close == 0, close, prev_close)  # 防零除

    # ── 1. 日均振幅（近20日 (最高-最低)/前收）───────────────────
    amplitude_arr = (high[-days20:] - low[-days20:]) / prev_close[-days20:] * 100
    avg_amplitude = float(np.nanmean(amplitude_arr))
    result["avg_amplitude"] = round(avg_amplitude, 2)

    # ── 2. 日均成交额（近20日）──────────────────────────────────
    if has_amount:
        amt_arr = hist["成交额"].values[-days20:].astype(float)
        avg_amount = float(np.nanmean(amt_arr))
    else:
        # 用成交量 × 均价估算
        avg_price = (high[-days20:] + low[-days20:] + close[-days20:]) / 3
        vol_arr   = hist["成交量"].values[-days20:].astype(float)
        avg_amount = float(np.nanmean(vol_arr * avg_price * 100))  # 股→手
    result["avg_amount"] = avg_amount

    # ── 3. 一字板识别（近30日）─────────────────────────────────
    # 涨停判定：当日涨幅≥9.5%
    chg_arr = (close[-days30:] - prev_close[-days30:]) / prev_close[-days30:]
    is_zt   = chg_arr >= 0.095   # 涨停日标志

    # 一字板：涨停 且 开盘=最高=涨停价（开盘即封）
    if has_open:
        open_arr = hist["开盘"].values[-days30:].astype(float)
        zt_price = prev_close[-days30:] * 1.10
        # 一字判定：开盘价与最高价差值<0.01元，且当日是涨停
        is_yizi  = is_zt & (np.abs(open_arr - high[-days30:]) < 0.02) & \
                   (np.abs(open_arr - zt_price) < 0.02)
    else:
        # 无开盘数据时用振幅判断：振幅<0.5%认为是一字（基本没动过）
        day_amp = (high[-days30:] - low[-days30:]) / prev_close[-days30:] * 100
        is_yizi  = is_zt & (day_amp < 0.5)

    zt_count   = int(is_zt.sum())
    yizi_count = int(is_yizi.sum())
    yizi_ratio = yizi_count / zt_count if zt_count > 0 else 0.0
    result["yizi_ratio"] = round(yizi_ratio, 3)

    # ── 当前连续一字天数（从最近一天往前数）────────────────────
    yizi_streak = 0
    for i in range(len(is_yizi) - 1, -1, -1):
        if is_yizi[i]:
            yizi_streak += 1
        else:
            break
    result["yizi_streak"] = yizi_streak

    # ── 4. 历史打板次日高开率（涨停后次日开盘 vs 今日收盘）────────
    # ★ v3.4修复：用次日开盘价而非收盘价计算，才是真正的"次日高开率"
    has_open = "开盘" in hist.columns
    open_arr = hist["开盘"].values.astype(float) if has_open else None

    open_days  = 0
    total_days = 0
    for i in range(len(close) - 1):
        if i >= len(chg_arr) - 1:
            break
        idx30 = i - (n - days30)
        if idx30 < 0:
            continue
        if is_zt[idx30]:
            total_days += 1
            if has_open and open_arr is not None and close[i] > 0:
                # 次日开盘相对今日收盘的高开幅度（正数=高开，负数=低开）
                next_open_chg = (open_arr[i + 1] - close[i]) / close[i]
                if next_open_chg >= 0.02:   # 次日高开≥2%算成功
                    open_days += 1
            else:
                # 无开盘价数据时用收盘价替代（降级）
                next_chg = (close[i + 1] - close[i]) / close[i] if close[i] > 0 else 0
                if next_chg >= 0.02:
                    open_days += 1
    open_rate = open_days / total_days * 100 if total_days > 0 else 50.0
    result["open_rate"] = round(open_rate, 1)

    # ================================================================
    # ★ 买得进去硬过滤（买不进去直接标记，外层跳过）
    # ================================================================
    no_buy_reason = ""

    # 硬过滤1：连续一字板（当前已封死N天，没有开板机会）
    if yizi_streak >= MAX_YIZI_STREAK:
        no_buy_reason = f"连续{yizi_streak}个一字板，无法买入"

    # 硬过滤2：历史一字比例过高（这只股票涨停几乎总是一字，买不进去）
    elif yizi_ratio >= MAX_YIZI_RATIO and zt_count >= 3:
        no_buy_reason = (f"历史一字比例{yizi_ratio:.0%}（近{zt_count}次涨停），"
                         f"几乎无法买入")

    # 硬过滤3：日均成交额太低（流动性不足）
    elif avg_amount < MIN_DAILY_AMOUNT:
        no_buy_reason = (f"日均成交额{avg_amount/1e4:.0f}万，"
                         f"流动性不足，买不进去")

    if no_buy_reason:
        result["buyable"]       = False
        result["no_buy_reason"] = no_buy_reason
        result["char_label"]    = "买不进去"
        result["char_score"]    = 0.0
        return result

    # ================================================================
    # ★ 股性评分（0~30分，叠加到总评分）
    # ================================================================
    score = 0.0

    # 活跃度评分（最高12分）
    if avg_amplitude >= 6.0:    score += 12   # 非常活跃，弹性极强
    elif avg_amplitude >= 4.0:  score += 10
    elif avg_amplitude >= 2.5:  score += 8
    elif avg_amplitude >= 1.5:  score += 6    # 达到活跃阈值
    elif avg_amplitude >= 1.0:  score += 3
    else:                       score += 0    # 死股，弹性极差

    # 流动性评分（最高8分）
    amt_yi = avg_amount / 1e8   # 单位亿元
    if amt_yi >= 5.0:    score += 8   # 超5亿，流动性极佳
    elif amt_yi >= 2.0:  score += 7
    elif amt_yi >= 1.0:  score += 6
    elif amt_yi >= 0.5:  score += 4
    elif amt_yi >= 0.3:  score += 2
    else:                score += 0

    # 打板胜率评分（最高6分）
    if total_days >= 3:   # 有足够历史样本才评分
        if open_rate >= 70:   score += 6
        elif open_rate >= 55: score += 4
        elif open_rate >= 40: score += 2
        else:                 score += 0
    else:
        score += 3  # 样本不足给中等分

    # 一字比例扣分（最多扣4分）
    if yizi_ratio >= 0.5:    score -= 4
    elif yizi_ratio >= 0.3:  score -= 2

    score = max(0.0, score)

    # 股性标签
    if avg_amplitude >= 4.0 and avg_amount >= 1e8:
        char_label = "超级活跃🔥"
    elif avg_amplitude >= 2.5 and avg_amount >= 5e7:
        char_label = "活跃✅"
    elif avg_amplitude >= 1.5:
        char_label = "普通"
    else:
        char_label = "迟钝⚠️"

    result["char_label"] = char_label
    result["char_score"] = round(score, 1)
    return result


# ================================================================
# ★ 真假涨停识别系统
# ================================================================

class FakeSignal(Exception):
    """用异常机制表示该信号是假涨停，需要丢弃"""
    pass


def check_fake_zt(
    code: str,
    name: str,
    seal_ratio: float,
    vol_ratio: float,
    turnover: float,
    seal_time_hm: int,        # 封板时间 HHMM（可能是回封时间），0表示未知
    hist: pd.DataFrame,
    row: pd.Series,           # 涨停池当行数据
    connect_days: int = 1,    # 连板天数，首板=1
    yesterday_close: float = 0.0,  # 昨日收盘价（用于连板溢价判断）
    auction_price: float = 0.0,    # 今日竞价价格（连板用）
    open_price: float = 0.0,       # 今日开盘价（用于精确判断高开秒板，优先于row字段）
) -> tuple:
    """
    真假涨停综合判断。
    返回 (is_fake: bool, penalty_score: float, fake_reasons: list)
      - is_fake=True  → 硬过滤，不推送
      - penalty_score → 扣分（评分体系中扣除）
      - fake_reasons  → 假涨停原因列表
    """
    is_fake      = False
    penalty      = 0.0
    fake_reasons = []

    # ── 硬过滤1：封板强度过低（封板不牢） ─────────────────────────────────
    # ★ 例外1：高开秒板（封板时间≤935 或竞价阶段<930）—— 行情接口封板资金统计严重滞后/失准
    # ★ 例外2：开盘价已高开≥8%（接近涨停价）—— 视为高开秒板，封板资金同样不准
    #           午后封板扣分同样对高开秒板豁免（is_skip_seal_check 统一控制）
    is_early_seal = (0 < seal_time_hm <= 935)   # 集合竞价/开盘5分钟内封板
    # 优先用调用方传入的 open_price（来自实时行情的精确开盘价）
    # 若未传入则从 row 字段尝试读取（涨停池数据不含今开时回退昨收推算）
    _open_p = open_price
    if _open_p <= 0:
        _open_p = float(row.get("今开", row.get("开盘价", row.get("今日开盘价", 0))) or 0)
    _yest_cls = float(row.get("昨日收盘价", row.get("昨收", 0)) or 0)
    # 若实在获取不到开盘价，则只依赖 is_early_seal 判断，不做高开推算
    _open_chg = (_open_p / _yest_cls - 1) if (_yest_cls > 0 and _open_p > 0) else 0
    is_high_open_seal = (_open_chg >= 0.08)     # 高开≥8%，视为高开秒板
    is_skip_seal_check = is_early_seal or is_high_open_seal

    if seal_ratio < FAKE_MIN_SEAL and not is_skip_seal_check:
        is_fake = True
        fake_reasons.append(f"封板弱({seal_ratio:.1%}<{FAKE_MIN_SEAL:.0%}，随时开板)")
    elif is_skip_seal_check and seal_ratio < FAKE_MIN_SEAL:
        # 高开秒板：封板资金数据失准，不过滤，但备注
        fake_reasons.append(f"高开秒板(封板资金统计滞后，不以此过滤)")

    # ── 硬过滤2：尾盘拉停（14:50后封板） ──────────────────────────────────
    # ★ 例外：高开秒板早盘已封，下午开板后回封，seal_time_hm显示的是回封时间
    # 不应被误判为尾盘拉停，is_skip_seal_check为True时跳过此过滤
    if seal_time_hm > 0 and seal_time_hm >= FAKE_TAIL_SEAL_HM and not is_skip_seal_check:
        is_fake = True
        fake_reasons.append(f"尾盘拉停({seal_time_hm//100}:{seal_time_hm%100:02d}封板)")

    # ── 硬过滤3：量比异常爆量 ───────────────────────────────────────────────
    if vol_ratio > FAKE_MAX_VOL_RATIO:
        is_fake = True
        fake_reasons.append(f"量比异常({vol_ratio:.1f}x>{FAKE_MAX_VOL_RATIO:.0f}，疑似出货)")

    # ── 硬过滤4：近30日炸板次数（历史数据）────────────────────────────────
    bomb_count = 0
    bad_bomb_count = 0
    if not hist.empty and "_bomb" in hist.columns:
        last30 = hist.tail(30)
        bomb_count     = int(last30["_bomb"].sum())
        bad_bomb_count = int(last30.get("_bomb_bad", pd.Series([False]*len(last30))).sum()) if "_bomb_bad" in last30.columns else 0

    if bomb_count > FAKE_MAX_BOMB_30D:
        is_fake = True
        fake_reasons.append(f"近30日炸板{bomb_count}次(主力反复骗板)")
    elif bomb_count == 1:
        penalty += 20
        fake_reasons.append(f"近期曾炸板1次(-20分)")

    # ── 硬过滤5：连板次日竞价溢价为负（情绪不持续） ───────────────────────
    if connect_days >= 2 and yesterday_close > 0 and auction_price > 0:
        premium = (auction_price - yesterday_close * 1.10) / (yesterday_close * 1.10)
        if premium < FAKE_CONNECT_PREMIUM_MIN:
            is_fake = True
            fake_reasons.append(f"连板溢价{premium:+.1%}(<-2%，情绪熄火)")

    # ── 评分扣分1：午后封板（13:00后封）—— 高开秒板豁免 ──────────────────
    if 0 < seal_time_hm < FAKE_TAIL_SEAL_HM and not is_skip_seal_check:
        if seal_time_hm >= 1300:
            penalty += 15
            fake_reasons.append(f"午后{seal_time_hm//100}:{seal_time_hm%100:02d}封板(-15分)")
        elif seal_time_hm >= 1130:
            penalty += 8
            fake_reasons.append(f"午盘前封板(-8分)")

    # ── 评分扣分2：量比过高（>3但<=FAKE_MAX_VOL_RATIO） ────────────────────
    if 3.0 < vol_ratio <= FAKE_MAX_VOL_RATIO:
        penalty += 10
        fake_reasons.append(f"量比偏高({vol_ratio:.1f}x,-10分)")

    # ── 评分扣分3：换手率过高 ───────────────────────────────────────────────
    if turnover > MAX_TURNOVER_ZT:
        penalty += 10
        fake_reasons.append(f"换手率高({turnover:.1f}%,-10分)")

    # ── 评分扣分4：连板天数≥4（次日折价概率高） ───────────────────────────
    if connect_days >= 4:
        penalty += 25
        fake_reasons.append(f"{connect_days}连板偏高(次日折价风险+,-25分)")

    return is_fake, penalty, fake_reasons


def get_seal_time_hm(row: pd.Series, code: str) -> int:
    """
    从涨停池数据行或分时数据中获取封板时间（HHMM整数）。
    返回 0 表示无法获取（不过滤，但不加分）。
    尝试字段：'首次封板时间' / '封板时间' / '最新封板时间'
    """
    for field in ["首次封板时间", "封板时间", "最新封板时间", "封板时间2"]:
        val = row.get(field, None)
        if val and str(val).strip() not in ("", "nan", "None", "--"):
            try:
                t_str = str(val).strip()
                # 支持格式：09:30 / 09:30:00 / 0930
                t_str = t_str.replace(":", "")[:4]
                return int(t_str)
            except:
                pass
    # 尝试从分时数据推断（取第一个涨停分钟）
    try:
        intraday = get_intraday_kline(code)
        if not intraday.empty:
            price_col = [c for c in intraday.columns if "收盘" in c or "close" in c.lower()]
            if price_col:
                pc = price_col[0]
                # 找第一个涨幅≥9.5%的分钟
                if "开盘" in intraday.columns or "昨收" in intraday.columns:
                    pass  # 分时数据中计算涨幅较复杂，暂不深入
    except:
        pass
    return 0


# ================================================================
# 策略1：首板策略（假涨停识别增强版）
# ================================================================
def scan_first_board(zt_df: pd.DataFrame, yesterday_zt_codes: set,
                     sector_zt_map: dict = None) -> list:
    """
    今日首次涨停（昨日未涨停）
    ★ 假涨停硬过滤：炸板历史/尾盘拉停/封板弱/量比异常，命中直接丢弃
    ★ 股性硬过滤：一字板/流动性差/买不进去，命中直接丢弃
    ★ 板块效应评分：同行业涨停≥3家加分，孤立涨停扣分
    """
    if zt_df.empty:
        return []
    required = {"代码", "名称", "最新价", "封板资金", "换手率", "流通市值", "成交额"}
    if not required.issubset(set(zt_df.columns)):
        log.warning(f"涨停池列名不匹配，实际列: {list(zt_df.columns)}")
        return []

    if sector_zt_map is None:
        sector_zt_map = {}

    # ── 阶段1：轻量预过滤（不做网络请求，快速缩小候选池）───────────────
    candidates = []
    for _, row in zt_df.iterrows():
        try:
            code = str(row["代码"]).zfill(6)
            name = str(row["名称"])
            if is_st(name) or code.startswith("688") or code.startswith("8"):
                continue
            if code in yesterday_zt_codes:
                continue
            price       = float(row["最新价"] or 0)
            circ_cap    = float(row["流通市值"] or 0)
            seal_amount = float(row["封板资金"] or 0)
            turnover    = float(row["换手率"] or 0)
            amount      = float(row["成交额"] or 0)
            vol_ratio   = float(row.get("量比", row.get("vol_ratio", 1.5)) or 1.5)
            if price <= 0 or price > MAX_PRICE:   continue
            if price > MAX_PRICE_ENTRY:           continue
            if circ_cap < MIN_MKT_CAP:            continue
            if circ_cap > MAX_MKT_CAP:            continue
            if amount < MIN_AMOUNT:               continue
            if turnover < MIN_TURNOVER:           continue
            # ★ v8.4：首板量比硬过滤上限5.0（高量比首板=出货拉停特征）
            if vol_ratio > VOL_RATIO_MAX_CONSERVATIVE:
                log.debug(f"[首板量比过滤] {name}({code}) 量比{vol_ratio:.1f}x>{VOL_RATIO_MAX_CONSERVATIVE:.0f}，疑似出货")
                continue
            seal_ratio = seal_amount / circ_cap if circ_cap > 0 else 0
            seal_time  = get_seal_time_hm(row, code)
            candidates.append(dict(row=row, code=code, name=name, price=price,
                                   circ_cap=circ_cap, seal_amount=seal_amount,
                                   turnover=turnover, amount=amount,
                                   vol_ratio=vol_ratio, seal_ratio=seal_ratio,
                                   seal_time=seal_time))
        except Exception as e:
            log.debug(f"首板预过滤异常 {row.get('代码','?')}: {e}")

    if not candidates:
        return []

    # ── 阶段2：并发拉取历史K线（核心性能优化）────────────────────────
    hist_map: dict = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates))) as ex:
        future_to_code = {ex.submit(get_hist_kline, c["code"], 60): c["code"]
                          for c in candidates}
        for fut in as_completed(future_to_code):
            code_key = future_to_code[fut]
            try:
                hist_map[code_key] = fut.result()
            except Exception as e:
                log.debug(f"首板K线并发请求失败 {code_key}: {e}")
                hist_map[code_key] = pd.DataFrame()

    # ── 阶段3：评分与信号生成 ─────────────────────────────────────────
    # 从全局实时行情缓存中提取各候选股的开盘价（用于高开秒板精确判断）
    _rt_open_map: dict = {}
    try:
        _rt_snap = get_realtime_quotes()
        if not _rt_snap.empty:
            for _, _rt_row in _rt_snap.iterrows():
                _rt_open_map[str(_rt_row.get("code", "")).zfill(6)] = float(_rt_row.get("open", 0) or 0)
    except Exception:
        pass

    signals = []
    for c in candidates:
        code        = c["code"]
        name        = c["name"]
        price       = c["price"]
        circ_cap    = c["circ_cap"]
        seal_amount = c["seal_amount"]
        turnover    = c["turnover"]
        amount      = c["amount"]
        vol_ratio   = c["vol_ratio"]
        seal_ratio  = c["seal_ratio"]
        seal_time   = c["seal_time"]
        row         = c["row"]
        hist        = hist_map.get(code, pd.DataFrame())
        open_price  = _rt_open_map.get(code, 0.0)   # 今日开盘价（来自实时行情）
        try:
            # ★ 股性识别
            char_info = analyze_stock_character(hist, code)
            if not char_info["buyable"]:
                log.info(f"[买不进去过滤] {name}({code}) | {char_info['no_buy_reason']}")
                continue

            pos_s      = position_score(hist)
            trend_up   = ma_trend_up(hist)

            # ★ 假涨停识别
            is_fake, penalty, fake_flags = check_fake_zt(
                code=code, name=name,
                seal_ratio=seal_ratio,
                vol_ratio=vol_ratio,
                turnover=turnover,
                seal_time_hm=seal_time,
                hist=hist,
                row=row,
                connect_days=1,
                open_price=open_price,
            )
            if is_fake:
                log.info(f"[假涨停过滤] {name}({code}) | {' | '.join(fake_flags)}")
                continue

            # ── 多因子加权评分 ──────────────────────────────────
            seal_s  = min(seal_ratio / 0.5, 1.0) * 30
            cap_s   = mkt_cap_score(circ_cap) * 20
            vr_s    = vol_ratio_score(vol_ratio) * 15
            to_s    = turnover_score_first(turnover) * 15
            pt_s    = pos_s * 10 + (10 if trend_up else 0)
            char_s  = char_info["char_score"]
            time_bonus = 0
            if 0 < seal_time <= 935:   time_bonus = 18
            elif 0 < seal_time < 1000: time_bonus = 15
            elif 0 < seal_time < 1130: time_bonus = 10
            elif 0 < seal_time < 1300: time_bonus = 5

            sec_delta, sec_msg = sector_score(code, sector_zt_map)
            score = seal_s + cap_s + vr_s + to_s + pt_s + char_s + time_bonus + sec_delta - penalty

            reason_parts = [
                f"封板{seal_ratio:.1%}",
                f"换手{turnover:.1f}%",
                f"流通{circ_cap/1e8:.0f}亿",
                f"量比{vol_ratio:.1f}x",
                f"股性:{char_info['char_label']}(振幅{char_info['avg_amplitude']:.1f}%)",
            ]
            if seal_time > 0:
                reason_parts.append(f"封板@{seal_time//100}:{seal_time%100:02d}")
            if trend_up:
                reason_parts.append("均线多头")
            if sec_msg:
                reason_parts.append(sec_msg)
            if fake_flags:
                reason_parts.append(f"⚠️{'; '.join(fake_flags)}")

            prev_close = round(price / 1.10, 2)
            signals.append(DaBanSignal(
                code=code, name=name, strategy="首板",
                price=price, connect_days=1,
                seal_ratio=seal_ratio, turnover=turnover,
                circ_mkt_cap=circ_cap / 1e8,
                score=round(score, 1),
                reason=" | ".join(reason_parts),
                stop_loss=round(prev_close * 0.97, 2),
                entry_price=price,
                seal_time_hm=seal_time,
                fake_penalty=round(penalty, 1),
                fake_flags=" | ".join(fake_flags),
                stock_char=char_info["char_label"],
                char_score=char_info["char_score"],
                avg_amplitude=char_info["avg_amplitude"],
                open_rate=char_info["open_rate"],
                buyable=True,
            ))
        except Exception as e:
            log.debug(f"首板评分异常 {code}: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals


# ================================================================
# 策略2：连板策略（假涨停识别增强版）
# ================================================================
def scan_connect_board(zt_df: pd.DataFrame, yesterday_zt_codes: set,
                       sector_zt_map: dict = None) -> list:
    """
    连续涨停（今日且昨日均涨停）
    ★ 额外检测：连板溢价（昨收×1.10 vs 今日竞价），情绪熄火直接过滤
    ★ 检测：≥4连板次日折价概率高（扣分不硬过滤，但评分大幅降低）
    ★ 股性硬过滤：连续一字板≥3天（封死打不进去）直接过滤
    ★ 板块效应评分：同行业涨停≥3家加分，孤立涨停扣分
    """
    if zt_df.empty:
        return []
    required = {"代码", "名称", "最新价", "封板资金", "换手率", "流通市值", "成交额"}
    if not required.issubset(set(zt_df.columns)):
        return []

    if sector_zt_map is None:
        sector_zt_map = {}

    # 获取实时行情（用于连板溢价判断，复用全局缓存）
    realtime_map = {}
    try:
        rt = get_realtime_quotes()
        if not rt.empty:
            for _, r in rt.iterrows():
                c = str(r.get("code", "")).zfill(6)
                realtime_map[c] = r
    except Exception:
        pass

    # ── 阶段1：轻量预过滤 ──────────────────────────────────────────────
    candidates = []
    for _, row in zt_df.iterrows():
        try:
            code = str(row["代码"]).zfill(6)
            name = str(row["名称"])
            if is_st(name) or code.startswith("688") or code.startswith("8"):
                continue
            if code not in yesterday_zt_codes:
                continue
            price       = float(row["最新价"] or 0)
            circ_cap    = float(row["流通市值"] or 0)
            seal_amount = float(row["封板资金"] or 0)
            turnover    = float(row["换手率"] or 0)
            amount      = float(row["成交额"] or 0)
            vol_ratio   = float(row.get("量比", row.get("vol_ratio", 1.5)) or 1.5)
            if price <= 0 or price > MAX_PRICE:  continue
            if price > MAX_PRICE_ENTRY:          continue
            if circ_cap < MIN_MKT_CAP:           continue
            if circ_cap > MAX_MKT_CAP:           continue
            if amount < MIN_AMOUNT:              continue
            if turnover > 15.0:                  continue
            # ★ v8.4：连板量比硬过滤上限5.0（高量比连板出货风险高）
            if vol_ratio > VOL_RATIO_MAX_CONSERVATIVE:
                log.debug(f"[连板量比过滤] {name}({code}) 量比{vol_ratio:.1f}x>{VOL_RATIO_MAX_CONSERVATIVE:.0f}，疑似出货")
                continue
            seal_ratio = seal_amount / circ_cap if circ_cap > 0 else 0
            if seal_ratio < 0.50:               continue
            connect_days = 2
            for col in ["连板数", "涨停统计"]:
                if col in row and row[col]:
                    try:
                        v = str(row[col]).split("/")[0]
                        connect_days = int(v)
                        break
                    except Exception:
                        pass
            if connect_days > MAX_CONNECT_DAYS:
                continue
            yesterday_close = 0.0
            auction_price   = 0.0
            rt_row = realtime_map.get(code)
            if rt_row is not None:
                try:
                    yesterday_close = float(rt_row.get("prev_close", 0) or 0)
                    auction_price   = float(rt_row.get("price", 0) or 0)
                except Exception:
                    pass
            seal_time = get_seal_time_hm(row, code)
            candidates.append(dict(row=row, code=code, name=name, price=price,
                                   circ_cap=circ_cap, seal_amount=seal_amount,
                                   turnover=turnover, amount=amount,
                                   vol_ratio=vol_ratio, seal_ratio=seal_ratio,
                                   seal_time=seal_time, connect_days=connect_days,
                                   yesterday_close=yesterday_close,
                                   auction_price=auction_price))
        except Exception as e:
            log.debug(f"连板预过滤异常 {row.get('代码','?')}: {e}")

    if not candidates:
        return []

    # ── 阶段2：并发拉取历史K线 ────────────────────────────────────────
    hist_map: dict = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates))) as ex:
        future_to_code = {ex.submit(get_hist_kline, c["code"], 60): c["code"]
                          for c in candidates}
        for fut in as_completed(future_to_code):
            code_key = future_to_code[fut]
            try:
                hist_map[code_key] = fut.result()
            except Exception as e:
                log.debug(f"连板K线并发请求失败 {code_key}: {e}")
                hist_map[code_key] = pd.DataFrame()

    # ── 阶段3：评分与信号生成 ─────────────────────────────────────────
    # 从全局实时行情缓存中提取各候选股的开盘价（用于高开秒板精确判断）
    _rt_open_map_lian: dict = {}
    try:
        _rt_snap_lian = get_realtime_quotes()
        if not _rt_snap_lian.empty:
            for _, _rt_row in _rt_snap_lian.iterrows():
                _rt_open_map_lian[str(_rt_row.get("code", "")).zfill(6)] = float(_rt_row.get("open", 0) or 0)
    except Exception:
        pass

    signals = []
    for c in candidates:
        code            = c["code"]
        name            = c["name"]
        price           = c["price"]
        circ_cap        = c["circ_cap"]
        seal_amount     = c["seal_amount"]
        turnover        = c["turnover"]
        amount          = c["amount"]
        vol_ratio       = c["vol_ratio"]
        seal_ratio      = c["seal_ratio"]
        seal_time       = c["seal_time"]
        connect_days    = c["connect_days"]
        yesterday_close = c["yesterday_close"]
        auction_price   = c["auction_price"]
        row             = c["row"]
        hist            = hist_map.get(code, pd.DataFrame())
        open_price_lian = _rt_open_map_lian.get(code, 0.0)
        try:
            char_info = analyze_stock_character(hist, code)
            if not char_info["buyable"]:
                log.info(f"[买不进去过滤] {name}({code}) {connect_days}连板 | {char_info['no_buy_reason']}")
                continue

            is_fake, penalty, fake_flags = check_fake_zt(
                code=code, name=name,
                seal_ratio=seal_ratio,
                vol_ratio=vol_ratio,
                turnover=turnover,
                seal_time_hm=seal_time,
                hist=hist,
                row=row,
                connect_days=connect_days,
                yesterday_close=yesterday_close,
                auction_price=auction_price,
                open_price=open_price_lian,
            )
            if is_fake:
                log.info(f"[假涨停过滤] {name}({code}) {connect_days}连板 | {' | '.join(fake_flags)}")
                continue

            seal_s      = min(seal_ratio / 0.8, 1.0) * 35
            cap_s       = mkt_cap_score(circ_cap) * 20
            vr_s        = vol_ratio_score(vol_ratio) * 10
            board_bonus = {2: 15, 3: 10, 4: 5}.get(connect_days, 3)
            to_s        = max(0, (15 - turnover) * 0.7)
            char_s      = char_info["char_score"]
            time_bonus  = 0
            if 0 < seal_time <= 935:   time_bonus = 15
            elif 0 < seal_time < 1000: time_bonus = 12
            elif 0 < seal_time < 1130: time_bonus = 8
            elif 0 < seal_time < 1300: time_bonus = 4

            sec_delta, sec_msg = sector_score(code, sector_zt_map)
            score = seal_s + cap_s + vr_s + board_bonus + to_s + char_s + time_bonus + sec_delta - penalty

            reason_parts = [
                f"{connect_days}连板",
                f"封板{seal_ratio:.1%}",
                f"换手{turnover:.1f}%",
                f"流通{circ_cap/1e8:.0f}亿",
                f"量比{vol_ratio:.1f}x",
                f"股性:{char_info['char_label']}(振幅{char_info['avg_amplitude']:.1f}%)",
            ]
            if seal_time > 0:
                reason_parts.append(f"封板@{seal_time//100}:{seal_time%100:02d}")
            if sec_msg:
                reason_parts.append(sec_msg)
            if fake_flags:
                reason_parts.append(f"⚠️{'; '.join(fake_flags)}")

            # ★ 连板止损：基于昨收推算，不用涨停价×0.93（修正BUG-10）
            prev_close_est = round(price / 1.10, 2) if price > 0 else 0
            stop_loss_price = round(prev_close_est * 0.97, 2)   # 昨收-3%，与首板一致

            signals.append(DaBanSignal(
                code=code, name=name, strategy="连板",
                price=price, connect_days=connect_days,
                seal_ratio=seal_ratio, turnover=turnover,
                circ_mkt_cap=circ_cap / 1e8,
                score=round(score, 1),
                reason=" | ".join(reason_parts),
                stop_loss=stop_loss_price,
                entry_price=price,
                seal_time_hm=seal_time,
                fake_penalty=round(penalty, 1),
                fake_flags=" | ".join(fake_flags),
                stock_char=char_info["char_label"],
                char_score=char_info["char_score"],
                avg_amplitude=char_info["avg_amplitude"],
                open_rate=char_info["open_rate"],
                buyable=True,
            ))
        except Exception as e:
            log.debug(f"连板评分异常 {code}: {e}")

    signals.sort(key=lambda x: (x.connect_days, x.score), reverse=True)
    return signals



# ================================================================
# 策略3：竞价打板（08:50~09:25）
# ================================================================
def scan_auction_board() -> list:
    """
    集合竞价阶段信号扫描，分两档：
      A档：高开7%~9.9%，接近涨停，强信号，建议挂涨停价排队
      B档：高开3%~7%，题材+大量，中信号，开盘后追入机会

    ★ v9.2 重大升级：
      1. 龙虎榜净买入加分（+12分）：机构/游资认可度极强
      2. 连板梯队差异化：2连板+20分，3连板+8分，4+连板直接跳过
      3. 高度板风险扣分：市场最高板≥5，本信号连板≥4 → -10分
      4. 炸板打开率过滤：打开率≥70%时跳过所有竞价信号（弱市陷阱）
      5. 封单强度从涨停池读（封单换手率字段），比自算更准确
    """
    realtime = get_realtime_quotes()
    if realtime.empty:
        return []

    # ── 预取辅助数据（龙虎榜 + 昨日涨停池 + 市场情绪） ────────────────
    prev_zt   = _PREV_ZT_CODES             # 昨日涨停代码集合
    lhb_codes = get_lhb_net_buy_codes()    # 今日龙虎榜净买入代码集合（★v9.2）
    emotion   = get_market_emotion()        # 市场情绪（含炸板打开率）

    # ★ v9.2：炸板打开率≥70%时，市场极弱，跳过所有竞价信号
    if BOMB_OPEN_RATE_ENABLE and emotion.get("bomb_open_rate_stop", False):
        log.warning(f"炸板打开率{emotion.get('bomb_open_rate',0):.0%}≥{BOMB_OPEN_RATE_STOP:.0%}，"
                    f"市场极弱，本轮竞价信号全部跳过")
        return []

    market_max_height = emotion.get("max_height", 0)  # 今日市场最高连板高度

    # ── 构建昨日涨停池连板数映射（{code: zt_streak}）──────────────────
    # 东方财富昨日涨停池包含"连板数"字段，直接读取避免重复计算
    _prev_zt_streak: dict = {}
    if hasattr(get_yesterday_zt, "_cached_df"):
        pass  # 已有缓存则直接用（下面从全局 _PREV_ZT_CODES 判断即可）
    # 简化：昨日涨停池的连板数直接从 _PREV_ZT_DF（若存在）读取
    # _PREV_ZT_DF 在主循环中缓存，此处通过全局变量读取
    _prev_zt_df = globals().get("_PREV_ZT_DF", pd.DataFrame())
    if not _prev_zt_df.empty:
        for _hcol in ["连板数", "连续涨停天数", "涨停天数"]:
            if _hcol in _prev_zt_df.columns:
                _code_col = "代码" if "代码" in _prev_zt_df.columns else _prev_zt_df.columns[0]
                for _, _r in _prev_zt_df.iterrows():
                    _c = str(_r.get(_code_col, "")).zfill(6)
                    _prev_zt_streak[_c] = int(pd.to_numeric(_r.get(_hcol, 1), errors="coerce") or 1)
                break

    signals = []
    for _, row in realtime.iterrows():
        try:
            code       = str(row.get("code", "")).zfill(6)
            name       = str(row.get("name", ""))
            price      = float(row.get("price", 0) or 0)
            prev_close = float(row.get("prev_close", 0) or 0)
            amount     = float(row.get("amount", 0) or 0)
            circ_cap   = float(row.get("circ_mkt_cap", 0) or 0)
            vol_ratio  = float(row.get("vol_ratio", 0) or 0)

            if is_st(name) or code.startswith("688") or code.startswith("8"):
                continue
            if prev_close <= 0 or price <= 0:
                continue
            if circ_cap < MIN_MKT_CAP:          continue
            if circ_cap > MAX_MKT_CAP:          continue
            if price > MAX_PRICE:               continue

            chg_pct    = (price - prev_close) / prev_close
            is_connect = code in prev_zt            # 昨日也涨停 → 今日竞价是连板信号
            zt_streak  = _prev_zt_streak.get(code, 1) if is_connect else 0  # 昨日已经是第几板

            # ★ v9.2：4+连板竞价直接跳过（折价概率>60%，backtest铁证）
            if AUCTION_ZT_4PLUS_ENABLE is False and is_connect and zt_streak >= 4:
                log.debug(f"跳过 {code}{name}：{zt_streak}连板竞价，折价风险>60%")
                continue

            # ── A档：强信号（接近涨停7%~9.9%）─────────────────────
            if 0.07 <= chg_pct < 0.099:
                if amount < 20_000_000:
                    continue
                # ★ 量比过低（<0.5）说明竞价成交极少，信号可信度低
                if 0 < vol_ratio < 0.5:
                    continue
                score = (
                    min(chg_pct / 0.10, 1.0) * 50 +
                    min(amount / 1e8, 1.0) * 30 +
                    min(circ_cap / 2e10, 1.0) * 20
                )
                reason = f"竞价A档+{chg_pct:.1%} | 竞价额{amount/1e4:.0f}万"
                if vol_ratio >= 3:
                    score += 10
                    reason += f" | 量比{vol_ratio:.1f}x🔥"

                # ★ v9.2 连板梯队差异化评分
                if is_connect:
                    if zt_streak == 2:
                        score += 20       # 2连板：胜率最高(>60%)，重仓
                        reason += f" | 2连板🔥(+20分)"
                    elif zt_streak == 3:
                        score += 8        # 3连板：折价风险开始，减分
                        reason += f" | 3连板⚡(+8分)"
                    else:
                        score += 15       # 昨日首次涨停今日连板
                        reason += " | 昨涨停🔗连板(+15分)"

                # ★ v9.2 龙虎榜净买入加分
                if LHB_BONUS_ENABLE and code in lhb_codes:
                    score += LHB_NET_BUY_BONUS
                    reason += f" | 龙虎榜净买🏆(+{LHB_NET_BUY_BONUS}分)"

                # ★ v9.2 高度板风险扣分
                if (HEIGHT_BOARD_RISK_ENABLE and market_max_height >= HEIGHT_BOARD_RISK_MIN
                        and is_connect and zt_streak >= 4):
                    score += HEIGHT_BOARD_RISK_PENALTY
                    reason += f" | 高度板风险⚠️({HEIGHT_BOARD_RISK_PENALTY}分)"

                _sub = "竞价-连板-极限" if is_connect else "竞价-首板-极限"
                signals.append(DaBanSignal(
                    code=code, name=name, strategy="竞价",
                    price=price, connect_days=zt_streak if is_connect else 0,
                    seal_ratio=0.0, turnover=0.0,
                    circ_mkt_cap=circ_cap / 1e8,
                    score=round(score, 1),
                    reason=reason,
                    stop_loss=round(prev_close * 0.97, 2),
                    entry_price=calc_zt_price(prev_close),
                    sub_strategy=_sub,
                ))

            # ── B档：中信号（量比大且高开3%~7%）────────────────────
            elif 0.03 <= chg_pct < 0.07 and vol_ratio >= 3.0:
                if amount < 30_000_000:
                    continue
                score = (
                    min(chg_pct / 0.07, 1.0) * 30 +
                    min(vol_ratio / 5.0, 1.0) * 35 +
                    min(amount / 1e8, 1.0) * 20 +
                    min(circ_cap / 3e10, 1.0) * 15
                )
                b_reason = f"竞价B档+{chg_pct:.1%} | 量比{vol_ratio:.1f}x | 竞价额{amount/1e4:.0f}万"

                # ★ v9.2 连板梯队差异化评分（B档）
                if is_connect:
                    if zt_streak == 2:
                        score += 12
                        b_reason += f" | 2连板🔥(+12分)"
                    elif zt_streak == 3:
                        score += 5
                        b_reason += f" | 3连板⚡(+5分)"
                    else:
                        score += 10
                        b_reason += " | 昨涨停🔗连板(+10分)"

                # ★ v9.2 龙虎榜净买入加分（B档同样加分）
                if LHB_BONUS_ENABLE and code in lhb_codes:
                    score += LHB_NET_BUY_BONUS
                    b_reason += f" | 龙虎榜净买🏆(+{LHB_NET_BUY_BONUS}分)"

                # ★ v9.2 高度板风险扣分（B档同样检查）
                if (HEIGHT_BOARD_RISK_ENABLE and market_max_height >= HEIGHT_BOARD_RISK_MIN
                        and is_connect and zt_streak >= 4):
                    score += HEIGHT_BOARD_RISK_PENALTY
                    b_reason += f" | 高度板风险⚠️({HEIGHT_BOARD_RISK_PENALTY}分)"

                _sub_b = "竞价-连板-强势" if is_connect else "竞价-首板-强势"
                signals.append(DaBanSignal(
                    code=code, name=name, strategy="竞价",
                    price=price, connect_days=zt_streak if is_connect else 0,
                    seal_ratio=0.0, turnover=0.0,
                    circ_mkt_cap=circ_cap / 1e8,
                    score=round(score, 1),
                    reason=b_reason,
                    stop_loss=round(prev_close * 0.96, 2),
                    entry_price=round(price * 1.01, 2),
                    sub_strategy=_sub_b,
                ))

        except Exception as e:
            log.debug(f"竞价异常: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:15]


# ================================================================
# 策略4：跌停反包
# ================================================================
def scan_dt_recover(dt_yesterday_df: pd.DataFrame) -> list:
    """昨日跌停，今日竞价高开反包"""
    if dt_yesterday_df.empty:
        return []

    code_col = "代码" if "代码" in dt_yesterday_df.columns else dt_yesterday_df.columns[0]
    dt_codes = list(dt_yesterday_df[code_col].astype(str).str.zfill(6))
    if not dt_codes:
        return []

    realtime = get_realtime_quotes(dt_codes)
    if realtime.empty:
        return []

    signals = []
    for _, row in realtime.iterrows():
        try:
            code       = str(row.get("code", "")).zfill(6)
            name       = str(row.get("name", ""))
            price      = float(row.get("price", 0) or 0)
            prev_close = float(row.get("prev_close", 0) or 0)
            circ_cap   = float(row.get("circ_mkt_cap", 0) or 0)
            amount     = float(row.get("amount", 0) or 0)

            if is_st(name) or code.startswith("688"):
                continue
            if prev_close <= 0 or price <= 0:
                continue

            chg_pct = (price - prev_close) / prev_close
            if chg_pct < DT_RECOVER_THRESH:     continue
            if circ_cap < 3_000_000_000:        continue
            if circ_cap > 30_000_000_000:       continue

            score = min(chg_pct / 0.10, 1.0) * 60 + min(circ_cap / 1e10, 1.0) * 20 + 20

            signals.append(DaBanSignal(
                code=code, name=name, strategy="反包",
                price=price, connect_days=-1,
                seal_ratio=0.0, turnover=0.0,
                circ_mkt_cap=circ_cap / 1e8,
                score=round(score, 1),
                reason=f"昨跌停反包 | 今+{chg_pct:.1%} | 流通{circ_cap/1e8:.0f}亿",
                stop_loss=round(prev_close * 0.97, 2),
                entry_price=round(price * 1.002, 2)
            ))
        except Exception as e:
            log.debug(f"反包异常: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:5]


# ================================================================
# 策略5：涨停回调缩量再启动（实盘实时版）
# ================================================================

@dataclass
class ZtPullbackSignal:
    """涨停回调缩量再启动信号"""
    code:          str
    name:          str
    price:         float
    circ_mkt_cap:  float     # 流通市值（亿）
    last_zt_date:  str       # 最近一次涨停日期
    days_since_zt: int       # 距涨停天数
    pullback_pct:  float     # 从涨停日收盘回调幅度
    vol_ratio:     float     # 回调期均量/涨停日量
    chg_pct:       float     # 今日涨跌幅
    score:         float
    reason:        str
    entry_price:   float
    stop_loss:     float
    target_price:  float


def scan_zt_pullback() -> list:
    """
    涨停回调缩量再启动扫描：
    识别近3~10日内出现涨停、随后价格缩量回调3%~8%、当日企稳的再启动机会。

    有效信号六要素：
      1. 近ZT_PULLBACK_LOOKBACK日内有过涨停（动能基础）
      2. 距涨停日已过ZT_PULLBACK_MIN_LOOKBACK日（给洗盘时间，不追涨停次日）
      3. 从涨停日收盘回调3%~8%（真洗盘区间）
      4. 今日获取60日K线，验证回调期间成交量明显萎缩（< 涨停日量×60%）
      5. 今日K线企稳（非大阴线/有下影线/收阳线）
      6. 均线未破坏（MA5不能低于MA20×95%，趋势基本完整）
    """
    signals = []
    try:
        realtime = get_realtime_quotes()
        if realtime.empty:
            return []

        for _, row in realtime.iterrows():
            try:
                code      = str(row.get("code", "")).zfill(6)
                name      = str(row.get("name", ""))
                price     = float(row.get("price",      0) or 0)
                prev_c    = float(row.get("prev_close", 0) or 0)
                open_p    = float(row.get("open",       0) or 0)
                high_p    = float(row.get("high",       0) or 0)
                low_p     = float(row.get("low",        0) or 0)
                amount    = float(row.get("amount",     0) or 0)
                circ      = float(row.get("circ_mkt_cap", 0) or 0)
                vol_ratio = float(row.get("vol_ratio",  0) or 0)
                chg_pct   = float(row.get("chg_pct",    0) or 0) / 100

                # ── 基础过滤 ───────────────────────────────────────
                if is_st(name):
                    continue
                if code.startswith("688") or code.startswith("8"):
                    continue
                if price <= 0 or prev_c <= 0:
                    continue
                if amount < ZT_PULLBACK_MIN_AMOUNT:
                    continue
                if circ < MIN_MKT_CAP or circ > MAX_MKT_CAP:
                    continue
                if price > MAX_PRICE_ENTRY:
                    continue
                # 今日本身是涨停/跌停，不做回调策略
                if abs(chg_pct) >= 0.095:
                    continue

                # ── 获取历史数据（需要60日以找到涨停点） ─────────
                hist = get_hist_kline(code, 60)
                if hist is None or hist.empty or len(hist) < ZT_PULLBACK_LOOKBACK + 3:
                    continue

                close_col = "收盘"  if "收盘"  in hist.columns else None
                high_col  = "最高"  if "最高"  in hist.columns else None
                low_col   = "最低"  if "最低"  in hist.columns else None
                vol_col   = "成交量" if "成交量" in hist.columns else None
                open_col  = "开盘"  if "开盘"  in hist.columns else None
                date_col  = "date"  if "date"  in hist.columns else None

                if close_col is None:
                    continue

                cls_arr = hist[close_col].values.astype(float)
                n_hist  = len(cls_arr)

                # ── 在近ZT_PULLBACK_LOOKBACK日内寻找最近一次涨停 ─
                last_zt_idx   = -1
                last_zt_price = 0.0
                last_zt_vol   = 0.0
                last_zt_date  = ""

                # 从最新往前找（最近的涨停优先）
                search_start = max(0, n_hist - ZT_PULLBACK_LOOKBACK - 1)
                search_end   = n_hist - ZT_PULLBACK_MIN_LOOKBACK  # 最近3日内的涨停不做

                for ki in range(search_end - 1, search_start - 1, -1):
                    krow_close = float(cls_arr[ki])
                    if ki == 0:
                        continue
                    krow_prev_c = float(cls_arr[ki - 1])
                    if krow_prev_c <= 0:
                        continue
                    krow_chg = (krow_close - krow_prev_c) / krow_prev_c
                    # 涨停判断：涨幅 ≥ 9.5%
                    if krow_chg >= 0.095:
                        last_zt_idx   = ki
                        last_zt_price = krow_close
                        last_zt_vol   = float(hist[vol_col].iloc[ki]) if vol_col else 0.0
                        if date_col:
                            last_zt_date = str(hist[date_col].iloc[ki])[:10]
                        break

                if last_zt_idx < 0 or last_zt_price <= 0:
                    continue  # 找不到近期涨停

                days_since_zt = n_hist - 1 - last_zt_idx  # 距今交易日数
                if days_since_zt < ZT_PULLBACK_MIN_LOOKBACK or days_since_zt > ZT_PULLBACK_LOOKBACK:
                    continue

                # ── 回调幅度（当前价相对涨停日收盘） ─────────────
                pullback = (last_zt_price - price) / last_zt_price
                if pullback < ZT_PULLBACK_MIN_DROP or pullback > ZT_PULLBACK_MAX_DROP:
                    continue

                # ── 回调期间缩量验证 ──────────────────────────────
                vol_shrink_ok = False
                avg_pullback_vol = 0.0
                if vol_col and last_zt_vol > 0:
                    pullback_vols = hist[vol_col].values[last_zt_idx + 1:].astype(float)
                    if len(pullback_vols) > 0:
                        avg_pullback_vol = float(np.mean(pullback_vols))
                        if avg_pullback_vol < last_zt_vol * ZT_PULLBACK_VOL_RATIO:
                            vol_shrink_ok = True
                elif vol_ratio > 0:
                    # 若无详细K线量数据，用实时量比 < 0.8 作为缩量替代判断
                    vol_shrink_ok = vol_ratio < 0.80

                if not vol_shrink_ok:
                    continue

                # ── 当日企稳（非大阴线/有下影线/收阳线）─────────
                today_body   = price - open_p if open_p > 0 else 0
                today_shadow = (open_p if open_p > 0 else price) - low_p
                stabilized = (
                    chg_pct > -0.03
                    or today_shadow > abs(today_body) * 0.5
                    or price > open_p
                )
                if not stabilized:
                    continue

                # ── 均线不破坏（趋势基本完整）────────────────────
                if len(cls_arr) >= 20:
                    ma5  = float(np.mean(cls_arr[-5:]))
                    ma20 = float(np.mean(cls_arr[-20:]))
                    if ma5 < ma20 * 0.95:
                        continue  # 均线空头排列，趋势破坏

                # ── 评分系统（0~100分） ───────────────────────────
                score = 0.0

                # 涨停后距今天数（3~5天最优，洗盘充分但动能未散）
                if 3 <= days_since_zt <= 5:     score += 25
                elif days_since_zt <= 7:        score += 18
                else:                           score += 10

                # 回调幅度（3%~6%最优黄金回调区间）
                if 0.03 <= pullback <= 0.06:    score += 25
                elif 0.06 < pullback <= 0.08:   score += 15
                else:                           score += 8

                # 缩量程度
                if last_zt_vol > 0 and avg_pullback_vol > 0:
                    vol_ratio_val = avg_pullback_vol / last_zt_vol
                    if vol_ratio_val <= 0.30:   score += 25
                    elif vol_ratio_val <= 0.45: score += 20
                    elif vol_ratio_val <= 0.60: score += 12
                    else:                       score += 5
                elif vol_ratio < 0.5:           score += 20
                elif vol_ratio < 0.8:           score += 14
                else:                           score += 5

                # 今日企稳形态
                if price > open_p and today_shadow > 0:     score += 15
                elif price > open_p:                        score += 10
                elif today_shadow > abs(today_body) * 0.5:  score += 8
                else:                                       score += 3

                # 均线趋势健康度
                if len(cls_arr) >= 20:
                    ma_gap = (ma5 - ma20) / ma20 if ma20 > 0 else 0
                    if ma_gap >= 0.05:   score += 10
                    elif ma_gap >= 0.02: score += 7
                    else:                score += 3

                score = max(0.0, min(score, 100.0))
                if score < ZT_PULLBACK_MIN_SCORE:
                    continue

                # ── 进出场参考 ────────────────────────────────────
                entry_price  = round(price * 1.005, 2)
                # 止损：距近期最低价下方2%（跌破洗盘低点出局）
                if low_col and len(hist) > 0:
                    recent_lows = hist[low_col].values[last_zt_idx + 1:].astype(float)
                    pullback_low = float(np.min(recent_lows)) if len(recent_lows) > 0 else price * 0.95
                else:
                    pullback_low = price * 0.95
                stop_loss    = round(pullback_low * 0.98, 2)
                target_price = round(last_zt_price * 1.02, 2)  # 目标：超越前次涨停高点

                vol_ratio_display = (avg_pullback_vol / last_zt_vol) if last_zt_vol > 0 and avg_pullback_vol > 0 else 0
                reason_parts = [
                    f"近{days_since_zt}日前涨停({last_zt_date})",
                    f"回调{pullback:.1%}至{price:.2f}",
                    f"缩量{vol_ratio_display:.0%}" if vol_ratio_display > 0 else f"量比{vol_ratio:.1f}x缩量",
                    f"今日{'收阳' if price > open_p else '企稳'}",
                ]

                signals.append(ZtPullbackSignal(
                    code=code, name=name, price=price,
                    circ_mkt_cap=circ / 1e8,
                    last_zt_date=last_zt_date,
                    days_since_zt=days_since_zt,
                    pullback_pct=round(pullback, 4),
                    vol_ratio=round(vol_ratio_display if vol_ratio_display > 0 else vol_ratio, 3),
                    chg_pct=round(chg_pct, 4),
                    score=round(score, 1),
                    reason=" | ".join(reason_parts),
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    target_price=target_price,
                ))

            except Exception as e:
                log.debug(f"涨停回调缩量扫描异常 {row.get('code','?')}: {e}")

    except Exception as e:
        log.error(f"涨停回调缩量扫描失败: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:10]


def format_zt_pullback_signal(sig: ZtPullbackSignal, rank: int) -> str:
    if sig.score >= 80:     grade = "⭐⭐⭐ 极佳"
    elif sig.score >= 65:   grade = "⭐⭐ 良好"
    elif sig.score >= 55:   grade = "⭐ 一般"
    else:                   grade = "普通"

    profit_pct = (sig.target_price - sig.price) / sig.price * 100 if sig.price > 0 else 0
    risk_pct   = (sig.price - sig.stop_loss) / sig.price * 100 if sig.price > 0 else 0
    rr_ratio   = round(profit_pct / risk_pct, 1) if risk_pct > 0 else 0
    shrink_pct = (1 - sig.vol_ratio) * 100

    return (
        f"### {rank}. 🔄{sig.name}（{sig.code}）\n"
        f"**评分**：{sig.score:.0f}分 {grade}\n"
        f"**现价**：{sig.price:.2f}（{sig.chg_pct:+.1%}）| **流通**：{sig.circ_mkt_cap:.0f}亿\n"
        f"**涨停**：{sig.last_zt_date}（{sig.days_since_zt}日前）| **回调**：{sig.pullback_pct:.1%}\n"
        f"**缩量**：{shrink_pct:.0f}%（回调期均量/涨停日量）\n"
        f"**参考买入**：{sig.entry_price:.2f} | **止损**：{sig.stop_loss:.2f} | **目标**：{sig.target_price:.2f}\n"
        f"**盈亏比**：{profit_pct:.1f}% / {risk_pct:.1f}% ≈ {rr_ratio}:1\n"
        f"**分析**：{sig.reason}\n"
    )


def push_zt_pullback_signals(signals: list, market: dict = None) -> None:
    """推送涨停回调缩量再启动信号"""
    if not signals:
        return

    market_line = ""
    if market:
        ms = market.get("market_state", "")
        il = market.get("index_line", "")
        market_line = f"大盘：{ms} | {il}\n\n"

    header = (
        f"# 🔄涨停回调缩量再启动\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
        f"共 **{len(signals)}只**\n\n"
        f"{market_line}"
        f"> 策略：近期涨停 → 缩量回调洗盘 → 当日企稳再启动\n"
        f"> 核心逻辑：主力涨停后缩量洗盘，低吸等待下一波拉升\n\n"
    )
    footer = (
        "\n> ⚠️ 跌破止损价坚决出局 | 仓位建议20%~30%"
        "\n> 回调期间成交量须明显萎缩，放量回调=出货，不参与"
    )

    body = ""
    for i, sig in enumerate(signals[:8], 1):
        body += format_zt_pullback_signal(sig, i) + "\n---\n"

    top = signals[0]
    title = f"🔄回调缩量:{top.name}涨停后{top.days_since_zt}日回调{top.pullback_pct:.0%} [{beijing_now().strftime('%H:%M')}]"
    send_wx(title, header + body + footer)


# ================================================================
# 策略6：洗盘抄底（主力上涨途中缩量回调支撑位企稳）
# ================================================================

@dataclass
class WashoutSignal:
    """洗盘抄底信号"""
    code:          str
    name:          str
    price:         float
    circ_mkt_cap:  float      # 流通市值（亿）
    pullback_pct:  float      # 从近期高点回调幅度
    vol_shrink:    float      # 缩量程度（近3日均量/近10日均量）
    ma_touch:      str        # 触碰支撑均线（"MA5"/"MA10"/"MA20"）
    trend_rise:    float      # 近30日涨幅
    signal_type:   str        # "日内企稳" / "昨日底部"
    score:         float
    reason:        str
    entry_price:   float
    stop_loss:     float
    target_price:  float      # 目标价（近期高点）


def scan_washout_dip() -> list:
    """
    洗盘抄底扫描：识别处于上涨趋势中的主力回调洗盘机会。

    有效洗盘六要素：
      1. 均线多头（MA5 > MA10 > MA20），趋势完整
      2. 近30日有过≥10%的涨幅（有趋势基础，非死股）
      3. 从近期高点回调5%~25%（真洗盘区间，非趋势反转）
      4. 近3日成交量萎缩至10日均量的75%以下（缩量为真洗盘特征）
      5. 回踩到MA5/MA10/MA20之一的支撑位附近（±2%）
      6. 今日K线企稳信号（非大阴线，开始止跌）

    无效洗盘（直接过滤）：
      - 下降趋势（MA5 < MA10，趋势破坏）
      - 放量下跌（主力减仓出货，非洗盘）
      - 回调超过MA20下方5%（支撑失效）
      - ST股/科创板/北交所
    """
    signals = []
    try:
        realtime = get_realtime_quotes()
        if realtime.empty:
            return []

        for _, row in realtime.iterrows():
            try:
                code      = str(row.get("code", "")).zfill(6)
                name      = str(row.get("name", ""))
                price     = float(row.get("price",      0) or 0)
                prev_c    = float(row.get("prev_close", 0) or 0)
                open_p    = float(row.get("open",       0) or 0)
                high_p    = float(row.get("high",       0) or 0)
                low_p     = float(row.get("low",        0) or 0)
                amount    = float(row.get("amount",     0) or 0)
                circ      = float(row.get("circ_mkt_cap", 0) or 0)
                vol_ratio = float(row.get("vol_ratio",  0) or 0)
                chg_pct   = float(row.get("chg_pct",    0) or 0) / 100  # akshare 返回百分比

                # ── 基础过滤 ───────────────────────────────────────
                if is_st(name):
                    continue
                if code.startswith("688") or code.startswith("8"):
                    continue
                if price <= 0 or prev_c <= 0:
                    continue
                if amount < WASHOUT_MIN_AMOUNT:
                    continue
                if circ < MIN_MKT_CAP or circ > MAX_MKT_CAP:
                    continue
                if price > MAX_PRICE_ENTRY:
                    continue

                # 涨跌停股不做洗盘判断（今日已涨停/跌停）
                if abs(chg_pct) >= 0.095:
                    continue

                # ── 获取历史数据（60日） ───────────────────────────
                hist = get_hist_kline(code, 60)
                if hist is None or hist.empty or len(hist) < 20:
                    continue

                close_col = "收盘" if "收盘" in hist.columns else None
                high_col  = "最高" if "最高" in hist.columns else None
                low_col   = "最低" if "最低" in hist.columns else None
                vol_col   = "成交量" if "成交量" in hist.columns else None
                open_col  = "开盘" if "开盘" in hist.columns else None

                if close_col is None:
                    continue

                cls_arr  = hist[close_col].values.astype(float)
                n        = len(cls_arr)

                # ── 均线计算 ───────────────────────────────────────
                if n < 20:
                    continue
                ma5  = float(np.mean(cls_arr[-5:]))
                ma10 = float(np.mean(cls_arr[-10:]))
                ma20 = float(np.mean(cls_arr[-20:]))

                # 要素1：均线多头排列（MA5 > MA10 > MA20）
                if not (ma5 > ma10 * 0.995):  # 允许1%容忍
                    continue
                if not (ma10 > ma20 * 0.99):
                    continue

                # ── 要素2：近30日趋势涨幅（用起始点而非最低点，避免大幅回调后误算） ─
                look_back = min(30, n - 1)
                start_price = float(cls_arr[-look_back])   # 30日前的起始价格
                rise_pct    = (price - start_price) / start_price if start_price > 0 else 0
                if rise_pct < WASHOUT_RISE_MIN:
                    continue  # 涨幅不足，无趋势

                # ── 要素3：回调幅度（从近20日高点回落） ─────────────
                high_20   = float(np.max(cls_arr[-20:])) if high_col is None else \
                            float(np.max(hist[high_col].values[-20:].astype(float)))
                # 也考虑今日最高
                high_20   = max(high_20, high_p) if high_p > 0 else high_20
                if high_20 <= 0:
                    continue
                pullback  = (high_20 - price) / high_20

                if pullback < WASHOUT_PULLBACK_MIN:
                    continue  # 还没跌下来，不是洗盘
                if pullback > WASHOUT_PULLBACK_MAX:
                    continue  # 跌太多，趋势可能破坏

                # ── 要素4：量能萎缩 ───────────────────────────────
                vol_shrink_ratio = 1.0
                if vol_col and vol_col in hist.columns:
                    vols = hist[vol_col].values.astype(float)
                    if len(vols) >= 10:
                        vol3  = float(np.mean(vols[-3:]))  # 近3日均量
                        vol10 = float(np.mean(vols[-10:])) # 近10日均量
                        if vol10 > 0:
                            vol_shrink_ratio = vol3 / vol10
                elif vol_ratio > 0:
                    # 用量比估算：量比<0.7视为缩量
                    vol_shrink_ratio = min(vol_ratio / 1.0, 1.5)

                # 缩量确认（近3日均量 ≤ 近10日均量的75%）
                if vol_shrink_ratio > WASHOUT_VOL_SHRINK:
                    # 放量下跌：硬过滤（主力出货）
                    if chg_pct < -0.02 and vol_shrink_ratio > 1.2:
                        continue
                    # 量没缩但也没放，给较低评分，不直接过滤

                # ── 要素5：触碰均线支撑 ──────────────────────────
                ma_touch = ""
                tol = WASHOUT_MA_TOUCH_PCT
                if abs(price - ma5) / ma5 <= tol:
                    ma_touch = "MA5"
                elif abs(price - ma10) / ma10 <= tol:
                    ma_touch = "MA10"
                elif abs(price - ma20) / ma20 <= tol:
                    ma_touch = "MA20"
                elif price < ma20 * (1 - tol):
                    # 已经跌破MA20且不在触碰范围，洗盘无效
                    continue

                # ── 要素6：今日企稳K线 ───────────────────────────
                # 今日非大阴线（跌幅<3%），或有下影线，或收阳线
                today_body = price - open_p if open_p > 0 else 0
                today_shadow = (open_p if open_p > 0 else price) - low_p  # 下影线
                stabilized = (
                    chg_pct > -0.03                          # 没有大跌
                    or today_shadow > abs(today_body) * 0.5  # 有下影线支撑
                    or price > open_p                        # 收阳线
                )
                if not stabilized:
                    continue

                # ── 评分系统（0~100分） ───────────────────────────
                score = 0.0

                # 均线多头强度（最高20分）
                ma_gap_ratio = (ma5 - ma20) / ma20 if ma20 > 0 else 0
                if ma_gap_ratio >= 0.08:    score += 20   # MA5比MA20高8%以上，趋势强劲
                elif ma_gap_ratio >= 0.05:  score += 16
                elif ma_gap_ratio >= 0.03:  score += 12
                else:                       score += 8

                # 回调幅度评分（最高20分，5%~15%最优）
                if 0.08 <= pullback <= 0.15:   score += 20   # 黄金回调区间
                elif 0.05 <= pullback < 0.08:  score += 15   # 浅回调，洗盘轻
                elif 0.15 < pullback <= 0.20:  score += 10   # 回调偏深
                else:                          score += 5    # 边界区间

                # 触碰支撑均线（最高15分）
                if ma_touch == "MA5":      score += 15   # 回踩MA5最强
                elif ma_touch == "MA10":   score += 12   # 回踩MA10次之
                elif ma_touch == "MA20":   score += 8    # 回踩MA20，需谨慎
                else:                      score += 3    # 未明确触碰，但在均线上方

                # 量能萎缩（最高20分）
                if vol_shrink_ratio <= 0.50:    score += 20   # 极度缩量，洗盘纯正
                elif vol_shrink_ratio <= 0.65:  score += 16
                elif vol_shrink_ratio <= 0.75:  score += 12   # 达到缩量标准
                elif vol_shrink_ratio <= 0.90:  score += 6
                else:                           score += 0    # 量未缩

                # 今日企稳形态（最高15分）
                if price > open_p and today_shadow > 0:    score += 15   # 阳线+下影
                elif price > open_p:                       score += 10   # 阳线
                elif today_shadow > abs(today_body):       score += 8    # 下影线
                else:                                      score += 3

                # 趋势涨幅（最高10分，近30日涨幅越大趋势越强）
                if rise_pct >= 0.40:    score += 10
                elif rise_pct >= 0.25:  score += 8
                elif rise_pct >= 0.15:  score += 6
                else:                   score += 3

                score = max(0.0, min(score, 100.0))

                # 低分直接跳过（减少干扰推送）
                if score < WASHOUT_MIN_SCORE:
                    continue

                # ── 进出场参考 ────────────────────────────────────
                entry_price  = round(price * 1.005, 2)       # 小幅溢价买入
                # 止损：MA20下方2%（跌破均线止损）
                stop_loss    = round(ma20 * 0.98, 2)
                # ★ v3.4修复：目标价改用近期高点的80%（更保守，避免盈亏比虚高）
                # 实际目标通常是+5%~+10%，不应直接等于历史高点
                target_price = round(price * 1.08, 2)  # 目标价：当前价+8%（保守估计）

                # ── 组装信号 ──────────────────────────────────────
                ma_touch_str = ma_touch if ma_touch else "均线上方"
                shrink_pct   = (1 - vol_shrink_ratio) * 100
                reason_parts = [
                    f"上升趋势(MA5>{ma5:.2f},MA10>{ma10:.2f})",
                    f"回调{pullback:.1%}至{ma_touch_str}支撑",
                    f"量缩{shrink_pct:.0f}%",
                    f"近30日涨{rise_pct:.0f}%",
                ]
                if today_shadow > abs(today_body) * 0.5:
                    reason_parts.append("有下影线企稳")
                if price > open_p:
                    reason_parts.append("今日收阳")

                signals.append(WashoutSignal(
                    code=code, name=name, price=price,
                    circ_mkt_cap=circ / 1e8,
                    pullback_pct=round(pullback, 4),
                    vol_shrink=round(vol_shrink_ratio, 3),
                    ma_touch=ma_touch_str,
                    trend_rise=round(rise_pct, 3),
                    signal_type="日内企稳" if chg_pct >= -0.005 else "回调支撑",
                    score=round(score, 1),
                    reason=" | ".join(reason_parts),
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    target_price=target_price,
                ))

            except Exception as e:
                log.debug(f"洗盘扫描异常 {row.get('code','?')}: {e}")

    except Exception as e:
        log.error(f"洗盘抄底扫描失败: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:12]


def _washout_score_grade(score: float) -> str:
    """洗盘信号评分等级"""
    if score >= 80:   return "⭐⭐⭐ 极佳"
    if score >= 70:   return "⭐⭐ 良好"
    if score >= 55:   return "⭐ 一般"
    return "普通"


def format_washout_signal(sig: WashoutSignal, rank: int) -> str:
    grade = _washout_score_grade(sig.score)
    ma_icon = {"MA5": "🟢", "MA10": "🟡", "MA20": "🟠", "均线上方": "⚪"}.get(sig.ma_touch, "⚪")
    shrink_pct = (1 - sig.vol_shrink) * 100
    profit_space = (sig.target_price - sig.price) / sig.price * 100 if sig.price > 0 else 0
    risk_space   = (sig.price - sig.stop_loss) / sig.price * 100 if sig.price > 0 else 0
    rr_ratio     = round(profit_space / risk_space, 1) if risk_space > 0 else 0
    return (
        f"### {rank}. 🔍{sig.name}（{sig.code}）\n"
        f"**评分**：{sig.score:.0f}分 {grade} | **类型**：{sig.signal_type}\n"
        f"**现价**：{sig.price:.2f} | **流通**：{sig.circ_mkt_cap:.0f}亿\n"
        f"**支撑**：{ma_icon}{sig.ma_touch}支撑 | **回调**：{sig.pullback_pct:.1%} | **量缩**：{shrink_pct:.0f}%\n"
        f"**趋势涨幅**：近30日+{sig.trend_rise:.0%}\n"
        f"**参考买入**：{sig.entry_price:.2f} | **止损**：{sig.stop_loss:.2f} | **目标**：{sig.target_price:.2f}\n"
        f"**盈亏比**：{profit_space:.1f}% / {risk_space:.1f}% ≈ {rr_ratio}:1\n"
        f"**分析**：{sig.reason}\n"
    )


def push_washout_signals(signals: list, market: dict = None) -> None:
    """推送洗盘抄底信号"""
    if not signals:
        return

    market_line = ""
    if market:
        ms = market.get("market_state", "")
        il = market.get("index_line", "")
        market_line = f"大盘：{ms} | {il}\n\n"

    header = (
        f"# 🔍洗盘抄底信号\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
        f"共 **{len(signals)}只**\n\n"
        f"{market_line}"
        f"> 洗盘信号 = 上涨趋势中主力缩量回调至均线支撑，低吸机会\n"
        f"> 确认要素：均线多头 + 量缩 + 触碰支撑 + 今日企稳\n\n"
    )
    footer = (
        "\n> ⚠️ 洗盘信号需结合板块强弱判断，跌破止损价坚决出局\n"
        "> 盈亏比低于1:1的机会不参与，仓位建议20%~30%"
    )

    body = ""
    for i, sig in enumerate(signals[:10], 1):
        body += format_washout_signal(sig, i) + "\n---\n"

    top = signals[0]
    title = f"🔍洗盘:{top.name}回调{top.pullback_pct:.0%}+量缩 [{beijing_now().strftime('%H:%M')}]"
    send_wx(title, header + body + footer)


# ================================================================
# 策略7：日内抄底信号（T字板低吸 / 均价回踩 / 昨板今低开企稳）
# ================================================================

@dataclass
class IntraDipSignal:
    """日内抄底信号"""
    code:        str
    name:        str
    price:       float
    circ_mkt_cap: float     # 流通市值（亿）
    signal_type: str        # "T字板" / "均价回踩" / "半T字"
    # T字板专属
    zt_price:    float      # 涨停价
    dist_to_zt:  float      # 当前距涨停比例（负值=在涨停价下方）
    # 均价回踩专属
    vwap_price:  float      # 日内均价（用开盘和最高估算）
    intra_high:  float      # 日内最高价
    pullback_from_hi: float # 从日内高点回落幅度
    # 共用字段
    chg_pct:     float      # 当前涨幅
    vol_ratio:   float      # 量比
    score:       float
    reason:      str
    entry_price: float
    stop_loss:   float
    target_price: float


def _estimate_vwap(open_p: float, high_p: float, low_p: float, price: float) -> float:
    """用 OHLC 估算日内成交均价（简化 VWAP）"""
    if open_p <= 0:
        return price
    # 典型价格 = (最高+最低+收盘) / 3，与开盘加权
    typical = (high_p + low_p + price) / 3.0
    return (typical + open_p) / 2.0


def scan_intraday_dip() -> list:
    """
    日内抄底信号扫描：捕捉盘中三类低吸机会。

    ① T字板低吸（核心信号）：
       - 今日曾触及涨停（高点 ≥ 涨停价×99%）
       - 当前炸板，价格在涨停下方 3%~8%
       - 当前价接近日内低点（下跌企稳，止跌信号）
       - 量能萎缩（炸板后量缩=主力洗盘，不是出货）
       逻辑：主力拉停洗盘，打开后缩量，是再次进攻前的蓄力

    ② 日内均价回踩低吸：
       - 日内最高涨幅 ≥ 4%（有足够的上涨动能）
       - 当前价从高点回落不超过6%（没有失速）
       - 当前价格在日内均价（VWAP）或开盘价附近±1.5%（支撑位）
       - 量比适中（回调时量缩，止跌后量能恢复）
       逻辑：日内拉高后正常回踩整理，均价是强支撑

    ③ 半T字（昨板今低开企稳）：
       - 昨日涨停（今日前收 ≈ 昨日涨停价，通过涨幅判断）
       - 今日低开2%~6%（情绪有所降温，不是崩塌式低开）
       - 开盘后企稳，未继续下砸（当前价接近开盘价）
       - 当前价格不破昨日收盘（前收）3%以上
       逻辑：昨日涨停洗盘式低开，主力没放弃，等待再次封板

    过滤条件（无效信号直接丢弃）：
    - ST股/科创板/北交所
    - 今日已跌停（-10%）
    - 流动性不足（成交额<3000万）
    - 当前又是涨停状态（不需要抄，直接打板）
    """
    signals = []
    try:
        realtime = get_realtime_quotes()
        if realtime.empty:
            return []

        # ── 今日涨停池（已封死=不需要低吸）────────────────────────
        today_zt_df = get_zt_pool()
        today_zt_codes: set = set()
        if not today_zt_df.empty and "代码" in today_zt_df.columns:
            today_zt_codes = set(today_zt_df["代码"].astype(str).str.zfill(6))

        # ── 昨日涨停池（半T字判断：替代逐股历史K线请求）───────────
        # 每交易日只加载一次，大幅提升扫描速度
        global _PREV_ZT_CODES, _PREV_ZT_DATE, _PREV_ZT_DF
        today_date = beijing_now().strftime("%Y-%m-%d")
        if _PREV_ZT_DATE != today_date:
            try:
                prev_zt_df = get_yesterday_zt()   # 昨日涨停池
                if prev_zt_df is not None and not prev_zt_df.empty and "代码" in prev_zt_df.columns:
                    _PREV_ZT_CODES = set(prev_zt_df["代码"].astype(str).str.zfill(6))
                    _PREV_ZT_DF    = prev_zt_df.copy()   # ★v9.2：保存完整DF供连板数读取
                else:
                    _PREV_ZT_CODES = set()
                    _PREV_ZT_DF    = pd.DataFrame()
                _PREV_ZT_DATE = today_date
                log.info(f"加载昨日涨停池 {len(_PREV_ZT_CODES)} 只（半T字判断）")
            except Exception as e:
                log.debug(f"昨日涨停池加载失败（半T字降级）: {e}")
                _PREV_ZT_CODES = set()
                _PREV_ZT_DF    = pd.DataFrame()

        for _, row in realtime.iterrows():
            try:
                code      = str(row.get("code", "")).zfill(6)
                name      = str(row.get("name", ""))
                price     = float(row.get("price",      0) or 0)
                prev_c    = float(row.get("prev_close", 0) or 0)
                open_p    = float(row.get("open",       0) or 0)
                high_p    = float(row.get("high",       0) or 0)
                low_p     = float(row.get("low",        0) or 0)
                amount    = float(row.get("amount",     0) or 0)
                circ      = float(row.get("circ_mkt_cap", 0) or 0)
                vol_ratio = float(row.get("vol_ratio",  0) or 0)
                turnover  = float(row.get("turnover",   0) or 0)

                # ── 基础过滤 ───────────────────────────────────────
                if is_st(name):
                    continue
                if code.startswith("688") or code.startswith("8"):
                    continue
                if price <= 0 or prev_c <= 0 or open_p <= 0:
                    continue
                if amount < INTRA_DIP_MIN_AMOUNT:
                    continue
                if circ < MIN_MKT_CAP or circ > MAX_MKT_CAP:
                    continue
                if price > MAX_PRICE_ENTRY:
                    continue
                # 已封死涨停的不做低吸
                if code in today_zt_codes:
                    continue
                # 今日跌停不做低吸
                chg_pct = (price - prev_c) / prev_c if prev_c > 0 else 0
                if chg_pct <= -0.095:
                    continue

                zt_price    = round(prev_c * 1.10, 2)    # 今日涨停价
                max_chg     = (high_p - prev_c) / prev_c if prev_c > 0 else 0
                dist_to_zt  = (price - zt_price) / zt_price if zt_price > 0 else 0
                intra_high  = high_p
                vwap        = _estimate_vwap(open_p, high_p, low_p, price)
                pullback_hi = (price - high_p) / high_p if high_p > 0 else 0

                sig_type    = ""
                score       = 0.0
                reason_parts = []
                entry_price  = 0.0
                stop_loss    = 0.0
                target_price = 0.0

                # ══════════════════════════════════════════════════
                # ① T字板低吸
                # ══════════════════════════════════════════════════
                touched_zt = (high_p >= zt_price * 0.99)   # 曾触及涨停
                below_zt   = (dist_to_zt < -INTRA_TBOARD_ZT_DOWN_MIN and
                              dist_to_zt > -(INTRA_TBOARD_ZT_UP_MAX + 0.001))
                # 企稳判断：当前价在日内低点附近（回落后止跌）
                stable_at_low = (low_p > 0 and
                                 (price - low_p) / low_p <= INTRA_TBOARD_STABLE_PCT)

                if touched_zt and below_zt and stable_at_low:
                    # ★ 量能硬过滤：vol_ratio > 3.0 说明放量炸板（出货），不是真洗盘，直接跳过
                    if vol_ratio > 3.0:
                        log.debug(f"[T字板过滤] {name}({code}) 量比{vol_ratio:.1f}x>3.0，疑似出货放量炸板")
                        continue
                    sig_type = "T字板"

                    # ── T字板评分（0~100分）──────────────────────
                    # 距涨停距离（3%~5%最优：离涨停近，容易再拉）
                    abs_dist = abs(dist_to_zt)
                    if abs_dist <= 0.04:        score += 30   # 极近，3%~4%内，非常强
                    elif abs_dist <= 0.05:      score += 24
                    elif abs_dist <= 0.06:      score += 18
                    else:                       score += 10   # 偏远，6%~8%

                    # 量比评分（炸板后量缩才是真洗盘）
                    if vol_ratio <= 0.5:        score += 25   # 极度缩量，洗盘特征强烈
                    elif vol_ratio <= 0.8:      score += 20
                    elif vol_ratio <= 1.2:      score += 14
                    elif vol_ratio <= 2.0:      score += 8
                    else:                       score += 0    # 放量炸板=出货，减分
                    if vol_ratio > 3.0:         score -= 10   # 大量炸板，出货嫌疑

                    # 今日大盘涨跌幅（0~10分，用已算的chg_pct替代）
                    if chg_pct >= 0.03:         score += 10   # 炸板后仍有3%涨幅，强势
                    elif chg_pct >= 0.00:       score += 7
                    elif chg_pct >= -0.02:      score += 3
                    else:                       score += 0

                    # 企稳程度（当前价距低点越近，止跌越确认）
                    stab_gap = (price - low_p) / low_p if low_p > 0 else 0
                    if stab_gap <= 0.005:       score += 15   # 几乎踩在低点，企稳强
                    elif stab_gap <= 0.010:     score += 10
                    else:                       score += 5

                    # 流通市值加分（小盘好打）
                    cap_yi = circ / 1e8
                    if 20 <= cap_yi <= 60:      score += 10
                    elif 60 < cap_yi <= 120:    score += 7
                    else:                       score += 3

                    # 换手率适中
                    if 3 <= turnover <= 15:     score += 10
                    elif turnover > 25:         score -= 5

                    entry_price  = round(price * 1.005, 2)
                    stop_loss    = round(low_p * 0.985, 2)    # 止损：日内低点下方1.5%
                    target_price = round(zt_price, 2)          # 目标：涨停价

                    reason_parts = [
                        f"曾触涨停({zt_price:.2f})后炸板",
                        f"当前{price:.2f}(涨停下{abs_dist:.1%})",
                        f"量比{vol_ratio:.1f}x{'缩量' if vol_ratio < 1.0 else ''}",
                        f"日内低点{low_p:.2f}企稳",
                        f"涨幅{chg_pct:+.1%}",
                    ]

                # ══════════════════════════════════════════════════
                # ② 日内均价回踩低吸
                # ══════════════════════════════════════════════════
                elif (not touched_zt and
                      max_chg >= INTRA_VWAP_RISE_MIN and
                      INTRA_VWAP_PULLBACK_MAX * (-1) <= pullback_hi < -0.01):
                    # 是否在均价/开盘价支撑附近
                    near_vwap = vwap > 0 and abs(price - vwap) / vwap <= INTRA_VWAP_TOUCH_PCT
                    near_open = open_p > 0 and abs(price - open_p) / open_p <= INTRA_VWAP_TOUCH_PCT

                    if near_vwap or near_open:
                        sig_type = "均价回踩"
                        support_label = "均价" if near_vwap else "开盘价"
                        support_price = vwap if near_vwap else open_p

                        # ── 均价回踩评分（0~100分）─────────────────
                        # 日内最高涨幅（越高，回踩前动能越强）
                        if max_chg >= 0.08:     score += 25
                        elif max_chg >= 0.06:   score += 20
                        elif max_chg >= 0.05:   score += 15
                        else:                   score += 8

                        # 回踩幅度（2%~4%为黄金区间，太浅没意义，太深不安全）
                        pb = abs(pullback_hi)
                        if 0.02 <= pb <= 0.04:  score += 25
                        elif 0.04 < pb <= 0.05: score += 18
                        elif pb < 0.02:         score += 8
                        else:                   score += 5

                        # 量比（回踩时量比<1.0为缩量，好信号）
                        if vol_ratio <= 0.6:    score += 20
                        elif vol_ratio <= 0.9:  score += 15
                        elif vol_ratio <= 1.2:  score += 8
                        else:                   score += 0
                        if vol_ratio > 2.0:     score -= 10

                        # 支撑类型加分
                        if near_vwap and near_open:
                            score += 15   # 均价与开盘价双支撑，最强
                        elif near_vwap:
                            score += 10
                        else:
                            score += 7

                        # 当前有小阳/企稳（price > low_p 一定幅度）
                        if price > low_p * 1.005:
                            score += 5

                        # 流通市值
                        cap_yi = circ / 1e8
                        if 20 <= cap_yi <= 80:  score += 10
                        elif 80 < cap_yi <= 150: score += 6
                        else:                   score += 2

                        entry_price  = round(price * 1.003, 2)
                        stop_loss    = round(min(low_p, support_price * 0.985), 2)
                        target_price = round(intra_high, 2)   # 目标：冲回日内高点

                        reason_parts = [
                            f"日内最高+{max_chg:.1%}",
                            f"回踩至{support_label}({support_price:.2f})",
                            f"量比{vol_ratio:.1f}x{'缩量回调' if vol_ratio < 1.0 else ''}",
                            f"当前{price:.2f}(+{chg_pct:.1%})",
                        ]

                # ══════════════════════════════════════════════════
                # ③ 半T字（昨板今低开企稳）
                # ══════════════════════════════════════════════════
                else:
                    # 判断昨日是否涨停：历史数据验证（保险起见用chg推算）
                    # 快速判断：今日前收若是昨日涨停价，则今日前收/昨日前收≈1.10
                    # 用实时数据中 prev_close 和 open 来判断
                    open_chg = (open_p - prev_c) / prev_c if prev_c > 0 else 0
                    price_vs_open = (price - open_p) / open_p if open_p > 0 else 0
                    price_vs_prev = (price - prev_c) / prev_c if prev_c > 0 else 0

                    is_semi_t = (
                        SEMI_T_OPEN_MIN <= open_chg <= SEMI_T_OPEN_MAX and   # 低开2%~6%
                        abs(price_vs_open) <= SEMI_T_STABLE_PCT and           # 当前价企稳在开盘价附近
                        price_vs_prev >= SEMI_T_PRICE_NOT_BELOW               # 未大幅破前收
                    )

                    # 需要前一日是涨停才叫"半T字"，直接查昨日涨停池（O(1)，无网络请求）
                    if is_semi_t and code not in _PREV_ZT_CODES:
                        if not _PREV_ZT_CODES:
                            # 缓存为空：节后/加载失败，记录日志便于排查
                            log.debug(f"[半T字] {name}({code}) 昨日涨停池缓存为空（可能节后第一天或加载失败），跳过半T字判断")
                        is_semi_t = False  # 昨日未涨停，不是半T字

                    if is_semi_t:
                        sig_type = "半T字"

                        # ── 半T字评分（0~100分）──────────────────
                        # 低开幅度（2%~4%最优：足够洗盘但不崩溃）
                        abs_open_chg = abs(open_chg)
                        if 0.02 <= abs_open_chg <= 0.04:   score += 25
                        elif 0.04 < abs_open_chg <= 0.05:  score += 18
                        else:                               score += 10

                        # 企稳强度（当前价越接近开盘价越稳）
                        abs_stab = abs(price_vs_open)
                        if abs_stab <= 0.005:   score += 20
                        elif abs_stab <= 0.01:  score += 14
                        else:                   score += 7

                        # 量比（企稳时量缩最理想）
                        if vol_ratio <= 0.7:    score += 20
                        elif vol_ratio <= 1.0:  score += 14
                        elif vol_ratio <= 1.5:  score += 8
                        else:                   score += 2
                        if vol_ratio > 3.0:     score -= 10

                        # 当前涨跌幅（企稳在0附近最好）
                        if -0.01 <= price_vs_prev <= 0.02: score += 15
                        elif -0.03 <= price_vs_prev < -0.01: score += 8
                        else:                               score += 3

                        # 流通市值
                        cap_yi = circ / 1e8
                        if 20 <= cap_yi <= 80:  score += 10
                        elif 80 < cap_yi <= 150: score += 7
                        else:                   score += 2

                        # 换手率
                        if 3 <= turnover <= 15: score += 10
                        elif turnover > 25:     score -= 5

                        entry_price  = round(price * 1.003, 2)
                        stop_loss    = round(open_p * 0.97, 2)    # 止损：开盘价下3%
                        target_price = round(prev_c * 1.10, 2)    # 目标：今日涨停价

                        reason_parts = [
                            f"昨日涨停低开{open_chg:.1%}",
                            f"开盘后企稳{price:.2f}(开盘{open_p:.2f})",
                            f"量比{vol_ratio:.1f}x{'缩量护盘' if vol_ratio < 1.0 else ''}",
                            f"当前{price_vs_prev:+.1%}守住前收",
                        ]

                # ── 信号合并判断 ──────────────────────────────────
                if not sig_type:
                    continue

                score = max(0.0, min(score, 100.0))
                if score < INTRA_DIP_MIN_SCORE:
                    continue

                signals.append(IntraDipSignal(
                    code=code, name=name, price=price,
                    circ_mkt_cap=circ / 1e8,
                    signal_type=sig_type,
                    zt_price=zt_price,
                    dist_to_zt=round(dist_to_zt, 4),
                    vwap_price=round(vwap, 2),
                    intra_high=intra_high,
                    pullback_from_hi=round(pullback_hi, 4),
                    chg_pct=round(chg_pct, 4),
                    vol_ratio=round(vol_ratio, 2),
                    score=round(score, 1),
                    reason=" | ".join(reason_parts),
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    target_price=target_price,
                ))

            except Exception as e:
                log.debug(f"日内抄底扫描异常 {row.get('code','?')}: {e}")

    except Exception as e:
        log.error(f"日内抄底扫描失败: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:12]


def format_intra_dip_signal(sig: IntraDipSignal, rank: int) -> str:
    """格式化日内抄底信号推送卡片"""
    type_icon = {"T字板": "🅣", "均价回踩": "📉", "半T字": "🔁"}.get(sig.signal_type, "📌")
    score_grade = ("⭐⭐⭐极佳" if sig.score >= 80
                   else "⭐⭐良好" if sig.score >= 65
                   else "⭐一般")
    profit_pct = (sig.target_price - sig.price) / sig.price * 100 if sig.price > 0 else 0
    risk_pct   = (sig.price - sig.stop_loss) / sig.price * 100 if sig.price > 0 else 0
    rr = round(profit_pct / risk_pct, 1) if risk_pct > 0 else 0

    extra = ""
    if sig.signal_type == "T字板":
        extra = f"**涨停价**：{sig.zt_price:.2f}（距涨停{abs(sig.dist_to_zt):.1%}）\n"
    elif sig.signal_type == "均价回踩":
        extra = f"**日内高点**：{sig.intra_high:.2f}（回落{abs(sig.pullback_from_hi):.1%}）| **均价**：{sig.vwap_price:.2f}\n"
    elif sig.signal_type == "半T字":
        extra = f"**涨停目标**：{sig.target_price:.2f} | 低开后企稳\n"

    return (
        f"### {rank}. {type_icon}{sig.name}（{sig.code}）\n"
        f"**评分**：{sig.score:.0f}分 {score_grade} | **类型**：{sig.signal_type}\n"
        f"**现价**：{sig.price:.2f}（{sig.chg_pct:+.1%}） | **量比**：{sig.vol_ratio:.1f}x | **流通**：{sig.circ_mkt_cap:.0f}亿\n"
        f"{extra}"
        f"**买入参考**：{sig.entry_price:.2f} | **止损**：{sig.stop_loss:.2f} | **目标**：{sig.target_price:.2f}\n"
        f"**盈亏比**：{profit_pct:.1f}% / {risk_pct:.1f}% ≈ {rr}:1\n"
        f"**分析**：{sig.reason}\n"
    )


def push_intra_dip_signals(signals: list, market: dict = None) -> None:
    """
    推送日内抄底信号。

    ★ 实时性策略（不耽误买点）：
    - T字板：每只单独立即推送，不等凑批。T字板窗口期极短（分钟级），绝不批量延迟。
    - 均价回踩 / 半T字：批量推送（最多3只一批），时效性略宽松。
    """
    if not signals:
        return

    market_line = ""
    if market:
        ms = market.get("market_state", "")
        il = market.get("index_line", "")
        market_line = f"大盘：{ms} | {il}\n\n"

    # 分拣：T字板单独推，其他批量推
    t_board_sigs  = [s for s in signals if s.signal_type == "T字板"]
    other_sigs    = [s for s in signals if s.signal_type != "T字板"]

    footer_t = (
        "\n> ⚠️ T字板窗口极短！量缩止跌时是最佳买点，一旦量能放大再次上攻立即跟进\n"
        "> 止损明确：跌破日内低点立即出局，不恋战\n"
        "> 仓位：20%~30%（成本低于追板，风险可控）"
    )
    footer_other = (
        "\n> 均价回踩：跌破均价+开盘价双支撑立即止损\n"
        "> 半T字：跌破开盘价3%以上止损，量缩企稳是买点\n"
        "> 仓位建议：轻仓10%~20%试探"
    )

    # ── T字板：每只立即单独推送（分秒必争）────────────────────────
    for sig in t_board_sigs:
        body = format_intra_dip_signal(sig, 1)
        header = (
            f"# 🅣T字板低吸信号\n\n"
            f"时间：{beijing_now().strftime('%H:%M:%S')} | "
            f"评分：**{sig.score:.0f}分**\n\n"
            f"{market_line}"
            f"> 炸板缩量低位企稳 → 低成本候冲板时机\n\n"
        )
        title = (
            f"🅣T字:{sig.name}{sig.chg_pct:+.1%}"
            f" 离板{abs(sig.dist_to_zt):.1%}"
            f" 量比{sig.vol_ratio:.1f}x"
            f" [{beijing_now().strftime('%H:%M')}]"
        )
        send_wx(title, header + body + footer_t)
        log.info(f"🅣T字板立即推送: {sig.name}({sig.code}) 评分{sig.score:.0f}")

    # ── 均价回踩 / 半T字：批量推（最多3只一批，不堆积）────────────
    if other_sigs:
        type_count = {}
        for s in other_sigs:
            type_count[s.signal_type] = type_count.get(s.signal_type, 0) + 1
        type_summary = " | ".join([f"{k}{v}只" for k, v in type_count.items()])

        header = (
            f"# 📍日内抄底信号\n\n"
            f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
            f"共 **{len(other_sigs[:3])}只** | {type_summary}\n\n"
            f"{market_line}"
            f"> 📉均价回踩=拉升后缩量回踩支撑 | 🔁半T字=昨板今低开护盘\n\n"
        )
        body = ""
        for i, sig in enumerate(other_sigs[:3], 1):
            body += format_intra_dip_signal(sig, i) + "\n---\n"

        top = other_sigs[0]
        type_abbr = {"均价回踩": "回踩", "半T字": "半T"}.get(top.signal_type, "低吸")
        title = (
            f"📍{type_abbr}:{top.name}{top.chg_pct:+.1%}"
            f" 量比{top.vol_ratio:.1f}x"
            f" [{beijing_now().strftime('%H:%M')}]"
        )
        send_wx(title, header + body + footer_other)


# ================================================================
# 无效信号最终过滤器（推送前最后一关）
# ================================================================

def filter_invalid_signals(signals: list) -> tuple:
    """
    在推送前对所有信号做最终质量过滤，返回 (valid, filtered_out)。

    过滤规则：
    1. 评分低于策略最低门槛（PUSH_MIN_SCORE）→ 过滤
    2. 假涨停扣分过高（penalty > LOSS_FILTER.max_penalty）→ 过滤
    3. 近20日日均振幅过低（死股，弹性不足）→ 过滤
    4. 历史打板次日高开率过低（策略胜率太差）→ 过滤
    5. 综合：以上多项叠加达到阈值 → 过滤
    """
    valid       = []
    filtered_out = []

    for sig in signals:
        reject_reason = ""

        # ── ★ v9.0 策略开关过滤（完全停用负期望值策略）─────────────
        strat = sig.strategy
        if strat == "首板" and not STRATEGY_ENABLE_FIRST_BOARD:
            reject_reason = "★v9.0首板策略已停用(Kelly=-23%，负期望值)"
        elif strat == "洗盘" and not STRATEGY_ENABLE_WASHOUT:
            reject_reason = "★v9.0洗盘抄底已停用(贡献80%亏损)"
        elif strat == "回调缩量" and not STRATEGY_ENABLE_ZT_PULLBACK:
            reject_reason = "★v9.0回调缩量已停用(样本无效)"
        elif strat == "日内抄底" and not STRATEGY_ENABLE_INTRADAY:
            reject_reason = "★v9.0日内抄底已停用(日线误差大)"

        # ── ★ v9.0 竞价子策略白名单过滤──────────────────────────────
        elif strat == "竞价" and AUCTION_WHITELIST_ENABLE:
            sub_label = getattr(sig, "sub_strategy", "") or getattr(sig, "signal_type", "")
            if sub_label and sub_label not in AUCTION_WHITELIST:
                reject_reason = f"★v9.0竞价子策略{sub_label}不在白名单(历史负收益)"

        # ── ★ v9.1 竞价-连板 封板力度过滤──────────────────────────────
        if not reject_reason and strat == "竞价" and SEAL_SCORE_STRICT_FILTER:
            sub_label = getattr(sig, "sub_strategy", "") or getattr(sig, "signal_type", "")
            if sub_label and "连板" in sub_label:
                prev_close_pos = getattr(sig, "prev_close_pos", 1.0) or 1.0
                if float(prev_close_pos) < SEAL_SCORE_WEAK_THRESH:
                    reject_reason = (f"★v9.1封板不稳(前日close_pos={float(prev_close_pos):.2f}"
                                     f"<{SEAL_SCORE_WEAK_THRESH})，续板概率低")

        # ── ★ v9.1 竞价-连板-极限 4+连板控制──────────────────────────
        if not reject_reason and strat == "竞价":
            sub_label = getattr(sig, "sub_strategy", "") or getattr(sig, "signal_type", "")
            if sub_label == "竞价-连板-极限":
                zt_streak = int(getattr(sig, "zt_days", 0) or 0)
                if not AUCTION_ZT_4PLUS_ENABLE and zt_streak >= 4:
                    reject_reason = f"★v9.1连板天数={zt_streak}≥4，极限高开折价风险>60%，停用"
                elif zt_streak >= 4 and float(getattr(sig, "prev_close_pos", 1.0) or 1.0) < AUCTION_EXTREME_PREV_CLOSE_POS:
                    reject_reason = f"★v9.1极限档前日封板不牢(close_pos<{AUCTION_EXTREME_PREV_CLOSE_POS})"

        # ── ★ v9.1 分时量比持续性过滤──────────────────────────────────
        if not reject_reason and strat == "竞价" and INTRADAY_VOL_SUSTAIN_ENABLE:
            intraday_vol_sustain = float(getattr(sig, "intraday_vol_sustain", 1.0) or 1.0)
            if intraday_vol_sustain < INTRADAY_VOL_SUSTAIN_RATIO:
                reject_reason = (f"★v9.1分时量比持续性不足"
                                 f"(日量/均量={intraday_vol_sustain:.2f}<{INTRADAY_VOL_SUSTAIN_RATIO}，盘中无人接盘)")

        # ── 规则1：评分门槛 ──────────────────────────────────────
        if not reject_reason:
            min_score = PUSH_MIN_SCORE.get(sig.strategy, 40)
            if sig.score < min_score:
                reject_reason = f"评分{sig.score:.0f}<{min_score}分门槛"
            # ★v9.4: 竞价-首板-强势 专用更高门槛（避免低分强势信号入场）
            elif sig.strategy == "竞价":
                sub_label = getattr(sig, "sub_strategy", "") or getattr(sig, "signal_type", "")
                if sub_label == "竞价-首板-强势" and sig.score < PUSH_MIN_SCORE_AUCTION_STRONG:
                    reject_reason = f"★v9.4强势档评分{sig.score:.0f}<{PUSH_MIN_SCORE_AUCTION_STRONG:.0f}分专用门槛(高开已消耗空间)"

        # ── 规则2：假涨停扣分过重 ─────────────────────────────────
        if not reject_reason and sig.fake_penalty > LOSS_FILTER_CONDITIONS["max_penalty"]:
            reject_reason = (f"风险扣分{sig.fake_penalty:.0f}分过高"
                             f"(>{LOSS_FILTER_CONDITIONS['max_penalty']})")

        # ── 规则3：死股过滤（弹性不足，打板没有利润空间）────────────
        if not reject_reason and (sig.avg_amplitude > 0 and
              sig.avg_amplitude < LOSS_FILTER_CONDITIONS["min_amplitude"]):
            reject_reason = (f"近20日振幅{sig.avg_amplitude:.1f}%过低"
                             f"(<{LOSS_FILTER_CONDITIONS['min_amplitude']}%，弹性极差)")

        # ── 规则4：历史胜率太低（有足够样本才过滤）──────────────────
        if not reject_reason and (sig.open_rate > 0 and
              sig.open_rate < LOSS_FILTER_CONDITIONS["min_open_rate"]):
            reject_reason = (f"历史打板胜率{sig.open_rate:.0f}%过低"
                             f"(<{LOSS_FILTER_CONDITIONS['min_open_rate']:.0f}%)")

        if reject_reason:
            filtered_out.append((sig, reject_reason))
            log.info(f"[无效信号过滤] {sig.name}({sig.code}) {sig.strategy} | {reject_reason}")
        else:
            valid.append(sig)

    if filtered_out:
        log.info(f"本轮过滤无效信号 {len(filtered_out)} 只，保留有效信号 {len(valid)} 只")

    return valid, filtered_out


# ================================================================
# 推送格式化
# ================================================================
STRATEGY_ICON = {"首板": "🔥", "连板": "🚀", "竞价": "⚡", "反包": "🔄"}

def format_signal(sig: DaBanSignal, rank: int, market_state: str = "", emotion: dict = None) -> str:
    icon = STRATEGY_ICON.get(sig.strategy, "📌")
    board_line = ""
    if sig.strategy == "连板":
        board_line = f"**连板天数**：{sig.connect_days}板\n"
    seal_line = f"**封板强度**：{sig.seal_ratio:.1%} | " if sig.seal_ratio > 0 else ""

    # 封板时间显示
    time_line = ""
    if sig.seal_time_hm > 0:
        hh, mm = sig.seal_time_hm // 100, sig.seal_time_hm % 100
        # 高开秒板判断：fake_flags 中含"高开秒板"标记
        is_high_open_seal = "高开秒板" in (sig.fake_flags or "")
        if sig.seal_time_hm <= 935:
            quality = "🟢早盘封板（信号强）"
        elif sig.seal_time_hm < 1000:
            quality = "🟢开盘早封（信号强）"
        elif sig.seal_time_hm < 1130:
            quality = "🟡上午封板（信号正常）"
        elif sig.seal_time_hm < 1300:
            quality = "🟠午盘前封（信号偏弱）"
        elif is_high_open_seal:
            # 高开秒板 + 午后封板时间 → 实为开板后迅速回封，属强势特征
            quality = "🟢高开秒板回封（开板后快速回封，强势特征）"
        else:
            quality = "🔴午后封板（注意风险）"
        time_line = f"**封板时间**：{hh}:{mm:02d} {quality}\n"

    # 股性信息
    char_line = ""
    if sig.stock_char:
        open_info = f" | 打板胜率{sig.open_rate:.0f}%" if sig.open_rate > 0 else ""
        char_line = (f"**股性**：{sig.stock_char}"
                     f"（振幅{sig.avg_amplitude:.1f}%{open_info}）\n")

    # 假涨停风险提示
    risk_line = ""
    if sig.fake_flags:
        risk_line = f"**⚠️风险提示**：{sig.fake_flags}\n"

    # 扣分提示
    deduction_line = ""
    if sig.fake_penalty > 0:
        deduction_line = f"**风险扣分**：-{sig.fake_penalty:.0f}分（已反映在评分中）\n"

    # 出场建议（基于现价与入场价关系，实时动态）
    exit_line = ""
    if sig.price > 0 and sig.entry_price > 0:
        _advice = exit_advice(sig.entry_price, sig.price, sig.strategy, sig.connect_days)
        if _advice:
            exit_line = f"{_advice}\n"

    return (
        f"### {rank}. {icon}{sig.name}（{sig.code}）\n"
        f"**策略**：{sig.strategy} | **评分**：{sig.score:.0f}分\n"
        f"**现价**：{sig.price:.2f} | **挂单价**：{sig.entry_price:.2f} | **止损**：{sig.stop_loss:.2f}\n"
        f"{board_line}"
        f"{seal_line}**换手**：{sig.turnover:.1f}% | **流通市值**：{sig.circ_mkt_cap:.0f}亿\n"
        f"{time_line}"
        f"{char_line}"
        f"{risk_line}"
        f"{deduction_line}"
        f"{exit_line}"
        f"{kelly_position_advice(sig.score, strategy=sig.strategy, market_state=market_state, emotion=emotion)}\n"
        f"**理由**：{sig.reason}\n"
    )

def push_signals(signals: list, phase: str, emotion: dict = None) -> None:
    if not signals:
        return
    phase_name = {"pre_auction": "竞价", "opening": "开盘", "morning": "上午盘",
                  "afternoon": "下午盘", "pre_close": "尾盘"}.get(phase, phase)

    # ── ★ 无效信号最终过滤（评分低/胜率差/死股直接丢弃）────────────
    signals, dropped = filter_invalid_signals(signals)
    if not signals:
        log.info("所有信号均被无效过滤器拦截，本轮不推送")
        return

    # ── 全局按评分统一排序（优先级：策略加权 + 评分）──────────────
    def sort_key(s: DaBanSignal) -> float:
        strategy_boost = {"连板": s.connect_days * 8, "首板": 5, "竞价": 3, "反包": 2}
        return s.score + strategy_boost.get(s.strategy, 0)

    sorted_signals = sorted(signals, key=sort_key, reverse=True)

    # ── ★ v5.0 标题精简：锁屏第一眼看到「策略+股名+分+仓位+热度」────
    top1 = sorted_signals[0]
    # 仓位建议（从Kelly结果中提取数字）
    _ms  = emotion.get("market_state", "") if emotion else ""
    _kp  = kelly_position_advice(top1.score, strategy=top1.strategy,
                                  market_state=_ms, emotion=emotion)
    _pct = ""
    _m = re.search(r"\*\*(\d+)%\*\*", _kp)
    if _m:
        _pct = f" 仓{_m.group(1)}%"

    # 赚钱效应热度图标
    _heat_icon = ""
    if emotion:
        _heat_icon = {"爆热": "🔥", "热": "🌤", "正常": "", "冷": "🧊", "极冷": "❄️"}.get(
            emotion.get("heat_level", ""), "")
        _max_h = emotion.get("max_height", 0)
        if _max_h >= 3:
            _heat_icon += f"{_max_h}板"

    extra = f"+{len(sorted_signals)-1}只" if len(sorted_signals) > 1 else ""
    # 标题格式：📈⚡XX股票(85分) 仓12% 🔥5板 +2只 [09:35]
    title = (
        f"📈{STRATEGY_ICON.get(top1.strategy,'')}{top1.name}"
        f"({top1.score:.0f}分){_pct} {_heat_icon}{extra}"
        f"[{beijing_now().strftime('%H:%M')}]"
    )

    # ── ★ v5.0 情绪信息行（增加赚钱效应热度）────────────────────────
    emotion_line = ""
    if emotion:
        zt = emotion.get("zt_count", 0)
        dt = emotion.get("dt_count", 0)
        ratio = emotion.get("zt_dt_ratio", 0.0)
        emo   = emotion.get("emotion", "")
        heat_level  = emotion.get("heat_level", "")
        heat_index  = emotion.get("heat_index", 50)
        max_height  = emotion.get("max_height", 0)
        height2_cnt = emotion.get("height2_count", 0)
        pos_coeff   = emotion.get("pos_coeff", 1.0)
        emo_icon  = {"冷": "🔵", "正常": "🟢", "热": "🟡", "过热": "🔴"}.get(emo, "⚪")
        heat_icon2= {"爆热": "🔥🔥", "热": "🔥", "正常": "✅", "冷": "🧊", "极冷": "❄️"}.get(heat_level, "")
        emotion_line = (
            f"市场：{emo_icon}{emo}（涨停{zt}家/跌停{dt}家 比值{ratio:.1f}x）\n"
            f"赚钱效应：{heat_icon2}{heat_level}（热度{heat_index}分 | "
            f"最高{max_height}板 | 2板+{height2_cnt}家）\n"
            f"仓位系数：×{pos_coeff}（{'加仓' if pos_coeff>1 else '减仓' if pos_coeff<1 else '正常'}模式）\n\n"
        )
        warn = emotion.get("warn_msg", "")
        if warn:
            emotion_line += f"> {warn}\n\n"

    header = (
        f"# 打板信号 · {phase_name}\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
        f"本轮共 **{len(sorted_signals)}只** | 按综合评分排序\n\n"
        f"{emotion_line}"
    )
    footer = ("\n> ⚠️ 仅供参考，严格止损，注意仓位\n"
              "> 股性：超级活跃🔥=弹性大 / 活跃✅=正常 / 迟钝⚠️=弹性差\n"
              "> 仓位建议=Kelly×月份系数×赚钱效应系数（极冷市场自动收缩）\n"
              "> 📌 第2天止损纪律：入场后第2天收盘浮亏>-2.0%且非缩量→第3天开盘必须止损出场（★v8.5收紧）\n"
              "> ★v9.0竞价止损：低开>2%当天直接止损（更严格），涨停后断板跌破5%止损\n"
              "> ★v9.1连板梯队：2连板仓位×1.5（最佳），3连板仓位×0.7，4+连板停用\n"
              "> ★v9.1封板力度：前日close_pos<0.60的连板竞价不参与（封板不稳=次日折价高）\n"
              "> 📈 强势延仓提示：连续2天收盘接近最高价（>85%位置）→可适当延长持仓1~3天")

    # ── ★ v9.1 高优先级信号快速单推（不等批量，第一时间到达）────────
    # 条件：评分≥80 或 竞价-连板-极限 子策略，且今日该阶段尚未推送过
    URGENT_SCORE = 80
    urgent_signals = [
        s for s in sorted_signals
        if (s.score >= URGENT_SCORE or
            (s.strategy == "竞价" and s.sub_strategy == "竞价-连板-极限"))
        and not is_already_pushed(s.code, s.strategy, phase)
    ]
    for us in urgent_signals:
        _u_kp  = kelly_position_advice(us.score, strategy=us.strategy,
                                       market_state=_ms, emotion=emotion)
        _u_pct = ""
        _u_m = re.search(r"\*\*(\d+)%\*\*", _u_kp)
        if _u_m:
            _u_pct = f" 仓{_u_m.group(1)}%"
        urgent_title = (
            f"⚡{STRATEGY_ICON.get(us.strategy,'')}{us.name}"
            f"({us.score:.0f}分){_u_pct} [{beijing_now().strftime('%H:%M')}]"
        )
        urgent_body = (
            f"# ⚡ 高优先级信号\n\n"
            f"**{us.name}**（{us.code}）| {us.strategy} | {us.score:.0f}分\n\n"
            f"{format_signal(us, 1, market_state=_ms, emotion=emotion)}\n"
            f"> ⚡ 此消息为高优先级单独推送，请第一时间处理"
        )
        send_wx_urgent(urgent_title, urgent_body)
        mark_pushed(us.code, us.strategy, phase)
        log.info(f"高优先级快速推送: {us.name} {us.score:.0f}分")

    # ── 分批推送，每批不超过 8000 字，避免 Server酱截断 ───────────
    # ★ v9.1：跳过已通过紧急通道单独推送过的信号，避免重复推送
    urgent_pushed_keys = {push_dedup_key(s.code, s.strategy, phase) for s in urgent_signals}
    batch_signals = [s for s in sorted_signals
                     if push_dedup_key(s.code, s.strategy, phase) not in urgent_pushed_keys]

    BATCH_CHARS = 8000
    batch_lines: list = []
    batch_size  = 0
    batch_idx   = 1

    for rank, s in enumerate(sorted_signals, 1):
        # ★ v9.1：已单独紧急推送的信号，在批量推中跳过（避免重复），但仍保留排名序号
        if push_dedup_key(s.code, s.strategy, phase) in urgent_pushed_keys:
            continue
        # ★ v5.0：把 emotion 传给 format_signal，Kelly仓位纳入赚钱效应
        card = format_signal(s, rank, market_state=_ms, emotion=emotion) + "\n---\n"
        if batch_size + len(card) > BATCH_CHARS and batch_lines:
            content = header + "".join(batch_lines) + footer
            batch_title = title if batch_idx == 1 else f"📈打板续{batch_idx} [{beijing_now().strftime('%H:%M')}]"
            send_wx(batch_title, content)
            batch_lines = []
            batch_size  = 0
            batch_idx  += 1
        batch_lines.append(card)
        batch_size += len(card)

    if batch_lines:
        content = header + "".join(batch_lines) + footer
        batch_title = title if batch_idx == 1 else f"📈打板续{batch_idx} [{beijing_now().strftime('%H:%M')}]"
        send_wx(batch_title, content)

# ================================================================
# ★ 大盘指数 + 外围走势 + 资金综合判断模块
# ================================================================

def get_market_index() -> dict:
    """
    获取A股三大指数（上证/深证/创业板）实时行情及北向资金，
    综合研判当前市场多空状态。
    返回 dict：
      sh_chg      : 上证涨跌幅
      sz_chg      : 深证涨跌幅
      cy_chg      : 创业板涨跌幅
      north_flow  : 北向今日净流入（亿元）
      market_state: "强势" / "震荡" / "弱势" / "崩溃"
      index_line  : 格式化的指数摘要行
      warn_msg    : 风险警示（空=无）
    """
    global _market_cache, _market_cache_time
    import time as _time
    now_ts = _time.time()
    if _market_cache and (now_ts - _market_cache_time) < MARKET_SCAN_INTERVAL:
        return _market_cache

    result = {
        "sh_chg": 0.0, "sz_chg": 0.0, "cy_chg": 0.0,
        "north_flow": 0.0,
        "market_state": "震荡",
        "index_line": "",
        "warn_msg": "",
    }

    try:
        # ── 三大指数实时行情 ──────────────────────────────────────
        spot = ak.stock_zh_index_spot_em()
        if spot is not None and not spot.empty:
            # 东财字段：代码 名称 最新价 涨跌幅 ...
            chg_col = None
            for c in ["涨跌幅", "涨跌额"]:
                if c in spot.columns:
                    chg_col = c
                    break

            def _get_index_chg(code_kw: str) -> float:
                if chg_col is None:
                    return 0.0
                sub = spot[spot["代码"].astype(str).str.contains(code_kw, na=False)]
                if sub.empty:
                    # 尝试名称匹配
                    sub = spot[spot["名称"].astype(str).str.contains(code_kw, na=False)]
                if sub.empty:
                    return 0.0
                val = pd.to_numeric(sub.iloc[0][chg_col], errors="coerce")
                return float(val) if not pd.isna(val) else 0.0

            sh_chg = _get_index_chg("000001")   # 上证指数
            sz_chg = _get_index_chg("399001")   # 深证成指
            cy_chg = _get_index_chg("399006")   # 创业板指

            # 尝试获取价格用于显示
            def _get_price(code_kw: str) -> float:
                sub = spot[spot["代码"].astype(str).str.contains(code_kw, na=False)]
                if sub.empty:
                    return 0.0
                for col in ["最新价", "现价", "收盘价"]:
                    if col in sub.columns:
                        v = pd.to_numeric(sub.iloc[0][col], errors="coerce")
                        return float(v) if not pd.isna(v) else 0.0
                return 0.0

            sh_price = _get_price("000001")
            sz_price = _get_price("399001")
            cy_price = _get_price("399006")

            result["sh_chg"] = round(sh_chg, 2)
            result["sz_chg"] = round(sz_chg, 2)
            result["cy_chg"] = round(cy_chg, 2)

            def _fmt(v: float) -> str:
                icon = "🔴" if v < -1 else ("🟢" if v > 0.5 else "⚪")
                sign = "+" if v >= 0 else ""
                return f"{icon}{sign}{v:.2f}%"

            result["index_line"] = (
                f"上证{sh_price:.0f}({_fmt(sh_chg)}) | "
                f"深证({_fmt(sz_chg)}) | "
                f"创业板({_fmt(cy_chg)})"
            )

            # 市场状态判断
            chg_list = [sh_chg, sz_chg, cy_chg]
            avg_chg  = sum(chg_list) / 3
            if avg_chg >= 1.0:
                result["market_state"] = "强势"
            elif avg_chg >= MARKET_STRONG_THRESH * 100:
                result["market_state"] = "偏强"
            elif avg_chg >= MARKET_WEAK_THRESH * 100:
                result["market_state"] = "震荡"
            elif avg_chg >= -2.0:
                result["market_state"] = "弱势"
            else:
                result["market_state"] = "崩溃"
                result["warn_msg"] += "🚨大盘急跌，建议空仓观望！"

            if min(chg_list) < MARKET_WEAK_THRESH * 100:
                result["warn_msg"] += f"⚠️三大指数同步下跌，打板风险上升"

    except Exception as e:
        log.debug(f"大盘指数获取失败: {e}")

    try:
        # ── 北向资金（今日净流入）─────────────────────────────────
        north_df = ak.stock_em_hsgt_north_net_flow_in(symbol="沪深港通")
        if north_df is not None and not north_df.empty:
            today_str = beijing_now().strftime("%Y-%m-%d")
            row = north_df[north_df.iloc[:, 0].astype(str).str.startswith(today_str)]
            if row.empty:
                row = north_df.tail(1)
            if not row.empty:
                flow_val = pd.to_numeric(row.iloc[0, 1], errors="coerce")
                if not pd.isna(flow_val):
                    flow_yi = float(flow_val) / 1e8
                    result["north_flow"] = round(flow_yi, 2)
                    sign = "+" if flow_yi >= 0 else ""
                    flow_icon = "🟢" if flow_yi > 0 else "🔴"
                    result["index_line"] += f" | 北向{flow_icon}{sign}{flow_yi:.1f}亿"
                    if flow_yi * 1e8 < NORTH_WARN_THRESH:
                        result["warn_msg"] += f"⚠️北向净流出{abs(flow_yi):.1f}亿，外资撤退信号"
    except Exception as e:
        log.debug(f"北向资金获取失败: {e}")

    _market_cache      = result
    _market_cache_time = now_ts
    log.info(f"大盘状态：{result['market_state']} | {result['index_line']}")
    return result


def market_state_advice(market: dict) -> str:
    """根据大盘状态给出策略建议"""
    state = market.get("market_state", "震荡")
    advice_map = {
        "强势":  "✅大盘强势，可积极参与打板，适当提高仓位",
        "偏强":  "🟢大盘偏强，正常参与，按信号操作",
        "震荡":  "🟡大盘震荡，谨慎参与，优选强势个股",
        "弱势":  "🟠大盘偏弱，建议轻仓，仅做评分最高信号",
        "崩溃":  "🔴大盘崩溃，建议空仓，等待企稳",
    }
    return advice_map.get(state, "⚪大盘数据获取中...")


# ================================================================
# ★ 长下影线信号模块
# ================================================================

@dataclass
class LowerShadowSignal:
    code:        str
    name:        str
    price:       float
    shadow_len:  float   # 下影线长度（元）
    shadow_pct:  float   # 下影线/前收 百分比
    shadow_ratio: float  # 下影线/实体 倍数
    position:    str     # "低位" / "中位" / "高位"
    pos_pct:     float   # 当前价在60日区间的位置（0~1）
    trend:       str     # "上升趋势" / "震荡" / "下降趋势"
    signal_type: str     # "日内" / "昨日"
    score:       float
    reason:      str
    entry_price: float
    stop_loss:   float


def _detect_lower_shadow_bar(
    code: str, name: str,
    open_p: float, high_p: float, low_p: float, close_p: float,
    prev_close: float, volume: float, avg_vol5: float,
    signal_type: str,
    hist60: pd.DataFrame = None,
) -> Optional[LowerShadowSignal]:
    """
    识别单根K线的长下影线特征并评分。
    判断标准：
      1. 下影线 = min(open, close) - low  ≥ |close - open| × LOWER_SHADOW_RATIO
      2. 下影线 / 当日振幅 ≥ LOWER_SHADOW_AMP
      3. 当日振幅 ≥ LOWER_SHADOW_MIN_AMP
    位置分析：结合近60日历史K线判断支撑位、均线趋势
    """
    if low_p <= 0 or prev_close <= 0:
        return None

    body_top    = max(open_p, close_p)
    body_bottom = min(open_p, close_p)
    body_len    = body_top - body_bottom       # 实体长度
    shadow_len  = body_bottom - low_p          # 下影线长度
    amp         = high_p - low_p              # 当日振幅

    if amp < prev_close * LOWER_SHADOW_MIN_AMP:
        return None
    if shadow_len <= 0:
        return None

    # 下影线/实体 比例
    shadow_ratio = shadow_len / max(body_len, 0.01)
    # 下影线/振幅 比例
    shadow_amp_ratio = shadow_len / amp

    if shadow_ratio < LOWER_SHADOW_RATIO:
        return None
    if shadow_amp_ratio < LOWER_SHADOW_AMP:
        return None

    shadow_pct = shadow_len / prev_close * 100  # 下影线相对前收的百分比

    # ── 位置分析 ────────────────────────────────────────────────
    pos_pct  = 0.5
    position = "中位"
    trend    = "震荡"

    if hist60 is not None and not hist60.empty and len(hist60) >= 10:
        close_col = "收盘" if "收盘" in hist60.columns else hist60.columns[-1]
        cls_arr = hist60[close_col].values.astype(float)
        high_col = "最高" if "最高" in hist60.columns else None
        low_col  = "最低" if "最低" in hist60.columns else None

        high60 = float(np.max(cls_arr[-min(60, len(cls_arr)):]))
        low60  = float(np.min(cls_arr[-min(60, len(cls_arr)):]))
        if high60 > low60:
            pos_pct = (close_p - low60) / (high60 - low60)

        if pos_pct <= LOWER_SHADOW_LOW_POS:
            position = "低位"
        elif pos_pct >= LOWER_SHADOW_HIGH_POS:
            position = "高位"
        else:
            position = "中位"

        # 均线趋势
        if len(cls_arr) >= 20:
            ma5  = float(np.mean(cls_arr[-5:]))
            ma10 = float(np.mean(cls_arr[-10:]))
            ma20 = float(np.mean(cls_arr[-20:]))
            if ma5 > ma10 > ma20:
                trend = "上升趋势"
            elif ma5 < ma10 < ma20:
                trend = "下降趋势"
            else:
                trend = "震荡"

    # ── 评分系统 ────────────────────────────────────────────────
    score = 0.0

    # 下影线长度评分（越长信号越强）
    if shadow_ratio >= 5.0:    score += 30
    elif shadow_ratio >= 3.5:  score += 22
    elif shadow_ratio >= 2.5:  score += 16
    else:                       score += 10  # 刚达到门槛

    # 位置评分
    pos_score = {"低位": 25, "中位": 12, "高位": 18}.get(position, 12)
    score += pos_score

    # 趋势评分
    trend_score = {"上升趋势": 20, "震荡": 10, "下降趋势": 0}.get(trend, 10)
    score += trend_score

    # 放量加分（下影线+放量=主力积极吸筹）
    if avg_vol5 > 0 and volume > avg_vol5 * 1.5:
        score += 15
    elif avg_vol5 > 0 and volume > avg_vol5 * 1.2:
        score += 8

    # 日内信号时效性加分
    if signal_type == "日内":
        score += 10

    # 阳线下影优于阴线下影
    if close_p >= open_p:
        score += 8

    # 下降趋势减分
    if trend == "下降趋势":
        score -= 15

    score = max(0.0, score)

    # ── 建仓参考 ────────────────────────────────────────────────
    entry_price = round(close_p * 1.002, 2)   # 小幅溢价追入
    stop_loss   = round(low_p * 0.98, 2)       # 止损设在下影线低点下方2%

    pos_icon  = {"低位": "📉低位", "中位": "📊中位", "高位": "📈高位"}.get(position, "")
    trend_icon = {"上升趋势": "↗️", "震荡": "↔️", "下降趋势": "↘️"}.get(trend, "")

    reason = (
        f"长下影{shadow_pct:.1f}%({shadow_ratio:.1f}倍实体) | "
        f"{pos_icon}({pos_pct:.0%}) | "
        f"{trend_icon}{trend}"
    )

    return LowerShadowSignal(
        code=code, name=name, price=close_p,
        shadow_len=round(shadow_len, 3),
        shadow_pct=round(shadow_pct, 2),
        shadow_ratio=round(shadow_ratio, 2),
        position=position, pos_pct=round(pos_pct, 3),
        trend=trend, signal_type=signal_type,
        score=round(score, 1), reason=reason,
        entry_price=entry_price, stop_loss=stop_loss,
    )


def scan_lower_shadow() -> list:
    """
    扫描日内和昨日出现长下影线的股票，发出参与信号。
    日内：从分时数据推断当日K线形态（实时）
    昨日：从历史日线数据识别昨日长下影线（趋势延续判断）
    """
    signals = []
    try:
        realtime = get_realtime_quotes()
        if realtime.empty:
            return []

        today_date = beijing_now().strftime("%Y%m%d")
        # 前一交易日
        prev_date_obj = beijing_now() - datetime.timedelta(days=1)
        for _ in range(7):
            if prev_date_obj.weekday() < 5:
                break
            prev_date_obj -= datetime.timedelta(days=1)

        for _, row in realtime.iterrows():
            try:
                code   = str(row.get("code", "")).zfill(6)
                name   = str(row.get("name", ""))
                price  = float(row.get("price",  0) or 0)
                open_p = float(row.get("open",   0) or 0)
                high_p = float(row.get("high",   0) or 0)
                low_p  = float(row.get("low",    0) or 0)
                prev_c = float(row.get("prev_close", 0) or 0)
                amount = float(row.get("amount", 0) or 0)
                circ   = float(row.get("circ_mkt_cap", 0) or 0)

                if is_st(name):
                    continue
                if code.startswith("688") or code.startswith("8"):
                    continue
                if price <= 0 or prev_c <= 0 or open_p <= 0:
                    continue
                if amount < LOWER_SHADOW_MIN_AMOUNT:
                    continue
                if circ < MIN_MKT_CAP or circ > MAX_MKT_CAP:
                    continue

                # 获取历史数据（60日）
                hist = get_hist_kline(code, 65)

                # ★ v3.4修复：vol_ratio语义为量比，直接传入并设avg_vol5=1.0作为基准
                # 在 _detect_lower_shadow_bar 中：volume > avg_vol5 * 1.5 即量比>1.5视为放量
                # 这是正确的语义：vol_ratio>1.5→放量，vol_ratio<1.0→缩量
                avg_vol5 = 1.0  # 固定基准，配合量比使用

                # ── 日内信号（当前K线形态）──────────────────────────
                chg_pct = (price - prev_c) / prev_c
                if -0.09 < chg_pct < 0.09:  # 非涨跌停日才识别
                    sig = _detect_lower_shadow_bar(
                        code=code, name=name,
                        open_p=open_p, high_p=high_p, low_p=low_p, close_p=price,
                        prev_close=prev_c, volume=vol_ratio, avg_vol5=avg_vol5,
                        signal_type="日内", hist60=hist,
                    )
                    if sig and sig.score >= 40:
                        signals.append(sig)
                        continue  # 日内信号优先，不再看昨日

                # ── 昨日信号（历史日线最新一根）────────────────────
                if hist is not None and not hist.empty and len(hist) >= 2:
                    last = hist.iloc[-1]
                    prev = hist.iloc[-2]
                    yest_open  = float(last.get("开盘",  0) or 0)
                    yest_high  = float(last.get("最高",  0) or 0)
                    yest_low   = float(last.get("最低",  0) or 0)
                    yest_close = float(last.get("收盘",  0) or 0)
                    yest_prev  = float(prev.get("收盘",  0) or 0)
                    yest_vol   = float(last.get("成交量", 0) or 0)

                    # 近5日均量估算
                    if len(hist) >= 6:
                        avg5 = float(hist["成交量"].iloc[-6:-1].mean()) if "成交量" in hist.columns else 0.0
                    else:
                        avg5 = 0.0

                    if yest_open > 0 and yest_prev > 0:
                        sig = _detect_lower_shadow_bar(
                            code=code, name=name,
                            open_p=yest_open, high_p=yest_high,
                            low_p=yest_low, close_p=yest_close,
                            prev_close=yest_prev,
                            volume=yest_vol, avg_vol5=avg5,
                            signal_type="昨日", hist60=hist,
                        )
                        if sig and sig.score >= 45:
                            # 昨日长下影线 + 今日继续走强才推送
                            if price >= yest_close * 0.99:  # 今日不破昨日收盘
                                signals.append(sig)

            except Exception as e:
                log.debug(f"长下影线扫描异常 {row.get('code','?')}: {e}")

    except Exception as e:
        log.error(f"长下影线扫描失败: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:15]


def format_lower_shadow_signal(sig: LowerShadowSignal, rank: int) -> str:
    pos_icon  = {"低位": "📉", "中位": "📊", "高位": "📈"}.get(sig.position, "")
    trend_icon = {"上升趋势": "↗️强", "震荡": "↔️震荡", "下降趋势": "↘️弱"}.get(sig.trend, "")
    type_icon  = "🕯️日内" if sig.signal_type == "日内" else "📅昨日"
    score_stars = "⭐" * min(int(sig.score / 20), 5)
    return (
        f"### {rank}. {type_icon} {sig.name}（{sig.code}）\n"
        f"**现价**：{sig.price:.2f} | **评分**：{sig.score:.0f}分 {score_stars}\n"
        f"**位置**：{pos_icon}{sig.position}（{sig.pos_pct:.0%}） | **趋势**：{trend_icon}\n"
        f"**下影线**：{sig.shadow_pct:.1f}%（{sig.shadow_ratio:.1f}倍实体）\n"
        f"**参考买入**：{sig.entry_price:.2f} | **止损**：{sig.stop_loss:.2f}\n"
        f"**分析**：{sig.reason}\n"
    )


def push_lower_shadow_signals(signals: list, market: dict = None) -> None:
    """推送长下影线参与信号"""
    if not signals:
        return

    market_line = ""
    if market:
        ms = market.get("market_state", "")
        il = market.get("index_line", "")
        market_line = f"大盘：{ms} | {il}\n\n"

    header = (
        f"# 🕯️长下影线参与信号\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
        f"共 **{len(signals)}只**\n\n"
        f"{market_line}"
        f"> 长下影线 = 主力护盘/强力吸筹，关注反弹机会\n\n"
    )
    footer = "\n> ⚠️ 长下影线为参考信号，需结合成交量和板块强弱判断，严格止损"

    body = ""
    for i, sig in enumerate(signals[:10], 1):
        body += format_lower_shadow_signal(sig, i) + "\n---\n"

    top = signals[0]
    title = f"🕯️长下影线:{top.name}{top.code}等{len(signals)}只 [{beijing_now().strftime('%H:%M')}]"
    send_wx(title, header + body + footer)


# ================================================================
# ★ 盘中异动拉升信号模块（量价齐升）
# ================================================================

@dataclass
class SurgeSignal:
    code:       str
    name:       str
    price:      float
    chg_pct:    float   # 涨幅%
    vol_ratio:  float   # 量比（相对5日均量）
    amount:     float   # 成交额
    circ_cap:   float   # 流通市值（亿）
    score:      float
    reason:     str
    entry_price: float
    stop_loss:   float


def scan_intraday_surge() -> list:
    """
    扫描盘中量价齐升的异动股票。
    触发条件：
      1. 涨幅突破 SURGE_MIN_CHG（3%）
      2. 量比 ≥ SURGE_VOL_RATIO（2倍）
      3. 价格创日内新高（突破形态）
      4. 流通市值在合理范围内
    """
    signals = []
    try:
        realtime = get_realtime_quotes()
        if realtime.empty:
            return []

        for _, row in realtime.iterrows():
            try:
                code      = str(row.get("code", "")).zfill(6)
                name      = str(row.get("name", ""))
                price     = float(row.get("price",      0) or 0)
                prev_c    = float(row.get("prev_close", 0) or 0)
                open_p    = float(row.get("open",       0) or 0)
                high_p    = float(row.get("high",       0) or 0)
                amount    = float(row.get("amount",     0) or 0)
                circ      = float(row.get("circ_mkt_cap", 0) or 0)
                vol_ratio = float(row.get("vol_ratio",  0) or 0)
                turnover  = float(row.get("turnover",   0) or 0)

                if is_st(name):
                    continue
                if code.startswith("688") or code.startswith("8"):
                    continue
                if code in SURGE_PUSHED_TODAY:
                    continue   # 已推送过，不重复

                if prev_c <= 0 or price <= 0:
                    continue
                if amount < SURGE_MIN_AMOUNT:
                    continue
                if circ < MIN_MKT_CAP or circ > MAX_MKT_CAP:
                    continue

                chg_pct = (price - prev_c) / prev_c

                # 涨幅门槛（非涨停，有操作空间）
                if chg_pct < SURGE_MIN_CHG or chg_pct >= 0.095:
                    continue

                # 量比门槛
                if vol_ratio < SURGE_VOL_RATIO:
                    continue

                # 价格接近日内最高（确认量价齐升而非高位缩量）
                if price < high_p * 0.99:
                    continue

                # 评分
                score = 0.0

                # 涨幅评分（3%~9%，越高越强，但不能涨停）
                if chg_pct >= 0.07:   score += 25
                elif chg_pct >= 0.05: score += 20
                elif chg_pct >= 0.04: score += 15
                else:                  score += 10

                # 量比评分
                if vol_ratio >= 5.0:   score += 25
                elif vol_ratio >= 3.5: score += 20
                elif vol_ratio >= 2.5: score += 15
                else:                  score += 10

                # 开盘以来连续上涨（price > open）
                if price > open_p and open_p > prev_c:
                    score += 15

                # 换手率适中（2%~12%）
                if 2.0 <= turnover <= 12.0:
                    score += 10
                elif turnover > 15.0:
                    score -= 10  # 换手过高，可能尾部追高

                # 市值评分
                cap_yi = circ / 1e8
                if 10 <= cap_yi <= 80:
                    score += 15
                elif 80 < cap_yi <= 150:
                    score += 8

                score = max(0.0, score)

                if score < 40:
                    continue

                entry_price = round(price * 1.001, 2)
                stop_loss   = round(open_p * 0.97, 2) if open_p > 0 else round(prev_c * 0.95, 2)

                reason = (
                    f"涨幅{chg_pct:+.1%} | "
                    f"量比{vol_ratio:.1f}x | "
                    f"换手{turnover:.1f}% | "
                    f"流通{circ/1e8:.0f}亿"
                )

                signals.append(SurgeSignal(
                    code=code, name=name, price=price,
                    chg_pct=chg_pct, vol_ratio=vol_ratio,
                    amount=amount, circ_cap=circ / 1e8,
                    score=round(score, 1), reason=reason,
                    entry_price=entry_price, stop_loss=stop_loss,
                ))

            except Exception as e:
                log.debug(f"异动扫描异常 {row.get('code','?')}: {e}")

    except Exception as e:
        log.error(f"盘中异动扫描失败: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:10]


def push_surge_signals(signals: list, market: dict = None) -> None:
    """推送盘中量价齐升异动信号"""
    global SURGE_PUSHED_TODAY
    if not signals:
        return

    market_line = ""
    if market:
        ms = market.get("market_state", "")
        il = market.get("index_line", "")
        market_line = f"大盘：{ms} | {il}\n\n"

    header = (
        f"# 🚀盘中量价异动信号\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
        f"共 **{len(signals)}只**\n\n"
        f"{market_line}"
    )
    footer = "\n> ⚠️ 异动信号需快速决策，注意量能持续性，建议轻仓试探，严格止损"

    body = ""
    for i, sig in enumerate(signals, 1):
        score_icon = "🔴" if sig.score >= 70 else ("🟡" if sig.score >= 50 else "⚪")
        body += (
            f"### {i}. {score_icon}{sig.name}（{sig.code}）\n"
            f"**现价**：{sig.price:.2f}（+{sig.chg_pct:.1%}） | **评分**：{sig.score:.0f}分\n"
            f"**量比**：{sig.vol_ratio:.1f}x | **流通**：{sig.circ_cap:.0f}亿\n"
            f"**参考买入**：{sig.entry_price:.2f} | **止损**：{sig.stop_loss:.2f}\n"
            f"**理由**：{sig.reason}\n\n---\n"
        )
        SURGE_PUSHED_TODAY.add(sig.code)

    top = signals[0]
    title = f"🚀异动:{top.name}+{top.chg_pct:.1%}量比{top.vol_ratio:.0f}x [{beijing_now().strftime('%H:%M')}]"
    send_wx(title, header + body + footer)


# ================================================================
# ★ 冲高回落 / 炸板未回封 定时推送模块
# ================================================================

@dataclass
class PullbackSignal:
    code:        str
    name:        str
    price:       float
    max_chg:     float    # 日内最高涨幅
    cur_chg:     float    # 当前涨幅
    fall_from_hi: float   # 从最高价回落幅度
    signal_type: str      # "冲高回落" / "炸板未封" / "昨日炸板"
    reason:      str
    risk_level:  str      # "高危" / "注意" / "关注"


def scan_pullback_and_bomb() -> list:
    """
    扫描冲高回落和炸板未回封的股票。
    1. 冲高回落：日内最高涨幅≥5% 但当前已回落≥3%
    2. 炸板未封：今日曾触及涨停但当前已开板，且低于涨停价2%以上
    3. 昨日炸板：昨日涨停池中炸板的股票（今日需要关注是否能重新封板）
    """
    signals = []
    try:
        realtime = get_realtime_quotes()
        if realtime.empty:
            return []

        today_zt_df = get_zt_pool()
        today_zt_codes = set()
        if not today_zt_df.empty and "代码" in today_zt_df.columns:
            today_zt_codes = set(today_zt_df["代码"].astype(str).str.zfill(6))

        for _, row in realtime.iterrows():
            try:
                code   = str(row.get("code", "")).zfill(6)
                name   = str(row.get("name", ""))
                price  = float(row.get("price",      0) or 0)
                prev_c = float(row.get("prev_close", 0) or 0)
                high_p = float(row.get("high",       0) or 0)
                open_p = float(row.get("open",       0) or 0)
                amount = float(row.get("amount",     0) or 0)
                circ   = float(row.get("circ_mkt_cap", 0) or 0)

                if is_st(name):
                    continue
                if code.startswith("688") or code.startswith("8"):
                    continue
                if prev_c <= 0 or price <= 0 or high_p <= 0:
                    continue
                if amount < LOWER_SHADOW_MIN_AMOUNT:
                    continue
                if circ < MIN_MKT_CAP:
                    continue

                cur_chg    = (price - prev_c) / prev_c
                max_chg    = (high_p - prev_c) / prev_c
                fall_from_hi = (price - high_p) / high_p

                zt_price   = round(prev_c * 1.10, 2)

                # ── 类型1：冲高回落 ─────────────────────────────────
                if (max_chg >= PULLBACK_HIGH_THRESH and
                        fall_from_hi <= -PULLBACK_FALL_THRESH and
                        code not in today_zt_codes):
                    risk = "高危" if fall_from_hi <= -0.06 else "注意"
                    signals.append(PullbackSignal(
                        code=code, name=name, price=price,
                        max_chg=max_chg, cur_chg=cur_chg,
                        fall_from_hi=fall_from_hi,
                        signal_type="冲高回落",
                        reason=(f"最高涨{max_chg:+.1%}→当前{cur_chg:+.1%}"
                                f"，回落{abs(fall_from_hi):.1%}"),
                        risk_level=risk,
                    ))

                # ── 类型2：炸板未封 ─────────────────────────────────
                # ★ 豁免：当前价已回到涨停价98%以上说明已回封或正在回封中
                # 典型场景：高开秒板→下午开板→迅速回封，不应误报风险
                elif (max_chg >= 0.095 and
                      code not in today_zt_codes and
                      price < zt_price * (1 - BOMB_UNFIXED_THRESH) and
                      price < zt_price * 0.98):
                    fall_pct = (price - zt_price) / zt_price
                    risk = "高危" if fall_pct <= -0.05 else "注意"
                    signals.append(PullbackSignal(
                        code=code, name=name, price=price,
                        max_chg=max_chg, cur_chg=cur_chg,
                        fall_from_hi=fall_from_hi,
                        signal_type="炸板未封",
                        reason=(f"曾触涨停({zt_price:.2f})→当前{price:.2f}"
                                f"({fall_pct:+.1%})，炸板未回封"),
                        risk_level=risk,
                    ))

            except Exception as e:
                log.debug(f"冲高回落扫描异常: {e}")

        # ── 类型3：昨日炸板（今日关注是否回封）─────────────────
        try:
            yesterday_zt_df = get_yesterday_zt()
            if not yesterday_zt_df.empty:
                zt_col = "代码" if "代码" in yesterday_zt_df.columns else yesterday_zt_df.columns[0]
                # 获取昨日涨停池中炸板的（今日开盘跌幅大的）
                yest_codes = list(yesterday_zt_df[zt_col].astype(str).str.zfill(6))
                if yest_codes:
                    rt2 = get_realtime_quotes(yest_codes[:50])  # 限制数量
                    for _, row2 in rt2.iterrows():
                        try:
                            code2  = str(row2.get("code", "")).zfill(6)
                            name2  = str(row2.get("name", ""))
                            price2 = float(row2.get("price",      0) or 0)
                            prev2  = float(row2.get("prev_close", 0) or 0)
                            if price2 <= 0 or prev2 <= 0:
                                continue
                            chg2 = (price2 - prev2) / prev2
                            # 昨日涨停今日明显低开（炸板后续弱）
                            yt_zt_price = round(prev2 * 1.10, 2)  # 昨日涨停价即今日前收×1.1，实际是今日prev_close
                            # 今日前收即昨日收盘，昨日是涨停日则前收≈涨停价
                            if chg2 < -0.03:  # 今日跌超3%说明昨日涨停已炸
                                signals.append(PullbackSignal(
                                    code=code2, name=name2, price=price2,
                                    max_chg=0.10, cur_chg=chg2,
                                    fall_from_hi=chg2,
                                    signal_type="昨日炸板",
                                    reason=f"昨日涨停今日{chg2:+.1%}，关注能否企稳",
                                    risk_level="关注",
                                ))
                        except Exception:
                            pass
        except Exception as e:
            log.debug(f"昨日炸板扫描异常: {e}")

    except Exception as e:
        log.error(f"冲高回落扫描失败: {e}")

    # 排序：高危优先
    risk_order = {"高危": 0, "注意": 1, "关注": 2}
    signals.sort(key=lambda x: (risk_order.get(x.risk_level, 9), x.fall_from_hi))
    return signals[:20]


def push_pullback_warning(signals: list, market: dict = None, push_time: str = "9:25") -> None:
    """定时推送冲高回落/炸板警告（09:25 和 14:30）"""
    market_line = ""
    if market:
        ms = market.get("market_state", "")
        il = market.get("index_line", "")
        warn = market.get("warn_msg", "")
        market_line = f"**大盘**：{ms} | {il}\n"
        if warn:
            market_line += f"> {warn}\n"
        market_line += "\n"

    if not signals:
        content = (
            f"# ⚠️冲高回落/炸板风险提示 ({push_time})\n\n"
            f"{market_line}"
            f"当前暂无明显冲高回落或炸板未封情况，市场相对健康。\n\n"
            f"> 持续监控中，如出现异动将即时推送"
        )
        send_wx(f"⚠️打板风险提示({push_time})", content)
        return

    high_risk = [s for s in signals if s.risk_level == "高危"]
    notice    = [s for s in signals if s.risk_level == "注意"]
    watch     = [s for s in signals if s.risk_level == "关注"]

    header = (
        f"# ⚠️冲高回落/炸板风险提示 ({push_time})\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"{market_line}"
        f"共发现 **{len(signals)}只** 风险标的 "
        f"（🔴高危{len(high_risk)}只 🟡注意{len(notice)}只 🔵关注{len(watch)}只）\n\n"
    )

    type_icon = {"冲高回落": "📉", "炸板未封": "💥", "昨日炸板": "🕳️"}
    risk_icon = {"高危": "🔴", "注意": "🟡", "关注": "🔵"}

    body = ""
    for i, s in enumerate(signals[:15], 1):
        body += (
            f"**{i}. {risk_icon.get(s.risk_level,'')}{type_icon.get(s.signal_type,'')} "
            f"{s.name}（{s.code}）**\n"
            f"现价：{s.price:.2f} | 风险：{s.risk_level}\n"
            f"{s.reason}\n\n"
        )

    footer = (
        "\n---\n> ⚠️ 以上标的存在主力出货/情绪转弱风险，已持仓者注意防守，\n"
        "> 未入场者暂时回避，等待企稳再看机会"
    )

    send_wx(
        f"⚠️{len(high_risk)}只高危炸板/回落警示({push_time}) [{beijing_now().strftime('%H:%M')}]",
        header + body + footer
    )


# ================================================================
# ★ 消息联动推送模块
# ================================================================

_news_cache: list = []
_news_cache_time: float = 0.0


def fetch_financial_news() -> list:
    """
    从东方财富获取最新财经快讯/公告。
    返回 list of dict: {id, time, title, content, codes}
    """
    global _news_cache, _news_cache_time
    import time as _time
    now_ts = _time.time()
    if _news_cache and (now_ts - _news_cache_time) < NEWS_SCAN_INTERVAL:
        return _news_cache

    news_list = []
    try:
        # 东方财富电报/快讯
        df = ak.stock_news_em(symbol="")
        if df is not None and not df.empty:
            for _, row in df.head(50).iterrows():
                try:
                    title   = str(row.get("新闻标题", row.get("title", "")) or "")
                    content = str(row.get("新闻内容", row.get("content", "")) or "")
                    pub_time = str(row.get("发布时间", row.get("time", "")) or "")
                    if not title:
                        continue
                    news_id = hashlib.md5((title + pub_time).encode("utf-8")).hexdigest()[:16]
                    # 解析时间
                    try:
                        if len(pub_time) >= 16:
                            t_obj = datetime.datetime.strptime(pub_time[:16], "%Y-%m-%d %H:%M")
                        elif len(pub_time) >= 5:
                            today = beijing_now().strftime("%Y-%m-%d")
                            t_obj = datetime.datetime.strptime(f"{today} {pub_time[:5]}", "%Y-%m-%d %H:%M")
                        else:
                            t_obj = beijing_now()
                    except Exception:
                        t_obj = beijing_now()

                    news_list.append({
                        "id":      news_id,
                        "time":    t_obj,
                        "title":   title,
                        "content": content,
                        "raw_code": str(row.get("股票代码", row.get("code", "")) or ""),
                    })
                except Exception:
                    pass
    except Exception as e:
        log.debug(f"快讯获取失败: {e}")

    # 备用：个股公告扫描（仅当快讯为空时）
    if not news_list:
        try:
            ann_df = ak.stock_notice_report(symbol="全部")
            if ann_df is not None and not ann_df.empty:
                for _, row in ann_df.head(30).iterrows():
                    title   = str(row.get("公告标题", "") or "")
                    code    = str(row.get("股票代码", "") or "").zfill(6)
                    pub_time = str(row.get("公告时间", "") or "")
                    if not title:
                        continue
                    news_id = hashlib.md5((title + code + pub_time).encode("utf-8")).hexdigest()[:16]
                    try:
                        t_obj = datetime.datetime.strptime(pub_time[:16], "%Y-%m-%d %H:%M")
                    except Exception:
                        t_obj = beijing_now()
                    news_list.append({
                        "id":      news_id,
                        "time":    t_obj,
                        "title":   title,
                        "content": "",
                        "raw_code": code,
                    })
        except Exception as e:
            log.debug(f"公告获取失败: {e}")

    _news_cache      = news_list
    _news_cache_time = now_ts
    return news_list


def _extract_codes_from_text(text: str) -> list:
    """从文本中提取6位数字股票代码"""
    import re
    codes = re.findall(r'\b([036]\d{5})\b', text)
    return list(set(codes))


def _news_priority(title: str, content: str) -> tuple:
    """
    评估消息优先级。
    返回 (priority: int, tags: list)
      priority: 3=高, 2=中, 1=低, 0=负面（不推送参与信号）
    """
    text = (title + " " + content).upper()
    for kw in NEWS_KEYWORDS_LOW:
        if kw in text:
            return 0, [kw]

    tags = []
    for kw in NEWS_KEYWORDS_HIGH:
        if kw in title or kw in content:
            tags.append(kw)

    if len(tags) >= 2:
        return 3, tags
    elif len(tags) == 1:
        return 2, tags
    else:
        return 1, []


def scan_news_price_linkage() -> list:
    """
    扫描消息与股价联动：
    1. 获取最新财经快讯
    2. 过滤30分钟内的新消息
    3. 提取相关股票代码
    4. 检测股价是否同步上涨（联动判断）
    5. 高优先级消息立即推送
    返回 list of dict: 联动信号列表
    """
    global NEWS_PUSHED_IDS

    news_list = fetch_financial_news()
    if not news_list:
        return []

    now = beijing_now()
    results = []

    for news in news_list:
        try:
            news_id   = news["id"]
            if news_id in NEWS_PUSHED_IDS:
                continue

            pub_time  = news["time"]
            age_min   = (now - pub_time).total_seconds() / 60

            if age_min > NEWS_MAX_AGE_MIN:
                continue

            title    = news["title"]
            content  = news["content"]
            raw_code = news["raw_code"]

            # 消息优先级判断
            priority, tags = _news_priority(title, content)
            if priority == 0:
                continue   # 负面消息，跳过参与信号

            # 提取股票代码
            codes = []
            if raw_code:
                codes = [raw_code.zfill(6)]
            else:
                codes = _extract_codes_from_text(title + " " + content)

            if not codes:
                # 无具体股票代码，仅推送文字消息（高优先级才推）
                if priority >= 3:
                    results.append({
                        "type":     "news_only",
                        "priority": priority,
                        "tags":     tags,
                        "title":    title,
                        "content":  content,
                        "pub_time": pub_time,
                        "news_id":  news_id,
                        "code":     "",
                        "name":     "",
                        "price":    0.0,
                        "chg_pct":  0.0,
                        "linked":   False,
                    })
                continue

            # 获取相关股票实时行情，检测价格联动
            rt = get_realtime_quotes(codes)
            for _, rrow in rt.iterrows():
                try:
                    code   = str(rrow.get("code", "")).zfill(6)
                    name   = str(rrow.get("name", ""))
                    price  = float(rrow.get("price",      0) or 0)
                    prev_c = float(rrow.get("prev_close", 0) or 0)
                    amount = float(rrow.get("amount",     0) or 0)

                    if prev_c <= 0 or price <= 0:
                        continue

                    chg_pct = (price - prev_c) / prev_c
                    linked  = chg_pct >= NEWS_PRICE_LINK_MIN

                    if priority >= 2 or linked:
                        results.append({
                            "type":     "news_stock",
                            "priority": priority,
                            "tags":     tags,
                            "title":    title,
                            "content":  content,
                            "pub_time": pub_time,
                            "news_id":  news_id,
                            "code":     code,
                            "name":     name,
                            "price":    price,
                            "chg_pct":  chg_pct,
                            "linked":   linked,
                            "amount":   amount,
                        })
                except Exception:
                    pass

        except Exception as e:
            log.debug(f"消息联动扫描异常: {e}")

    # 按优先级+联动程度排序
    results.sort(key=lambda x: (x["priority"], x.get("chg_pct", 0)), reverse=True)
    return results[:10]


def push_news_signals(news_results: list) -> None:
    """推送消息联动信号"""
    global NEWS_PUSHED_IDS
    if not news_results:
        return

    for item in news_results:
        try:
            priority  = item["priority"]
            title     = item["title"]
            tags      = item["tags"]
            pub_time  = item["pub_time"]
            code      = item.get("code", "")
            name      = item.get("name", "")
            price     = item.get("price", 0.0)
            chg_pct   = item.get("chg_pct", 0.0)
            linked    = item.get("linked", False)
            news_id   = item["news_id"]

            if news_id in NEWS_PUSHED_IDS:
                continue

            pri_icon  = {3: "🔴紧急", 2: "🟠重要", 1: "🔵普通"}.get(priority, "")
            link_line = ""
            if code:
                chg_icon = "🚀" if chg_pct >= 0.05 else ("📈" if chg_pct >= 0.02 else "⚪")
                link_line = (
                    f"\n**相关股票**：{name}（{code}）"
                    f"  现价：{price:.2f}  {chg_icon}{chg_pct:+.1%}"
                )
                if linked:
                    link_line += "  ✅价格联动"

            tag_line = ""
            if tags:
                tag_line = f"\n**关键词**：{'、'.join(tags)}"

            content = (
                f"# {pri_icon}消息联动信号\n\n"
                f"**时间**：{pub_time.strftime('%H:%M') if hasattr(pub_time, 'strftime') else str(pub_time)}\n\n"
                f"**消息**：{title}\n"
                f"{tag_line}"
                f"{link_line}\n\n"
                f"> 消息发布后如价格快速上涨，需在第一时间判断是否介入\n"
                f"> 注意量能是否持续，警惕利好兑现即卖出"
            )

            push_title = f"📰{name or '市场'}消息 {title[:20]}... [{beijing_now().strftime('%H:%M')}]"
            if send_wx(push_title, content):
                NEWS_PUSHED_IDS.add(news_id)

        except Exception as e:
            log.debug(f"消息推送异常: {e}")


# ================================================================
# ★ 半路模式 — 日内追强不追高（放量突破分时均线/前高）
# ================================================================

# 半路信号去重缓存（今日已推送的股票+信号类型，避免重复推同类信号）
MIDWAY_PUSHED_TODAY: dict = {}   # {code: set(signal_type)}

@dataclass
class MidwaySignal:
    code:        str
    name:        str
    price:       float
    chg_pct:     float
    vol_ratio:   float   # 相对5日均量的放量倍数
    signal_type: str     # "突破前高" / "突破均线" / "量价共振"
    score:       float
    reason:      str
    entry_price: float
    stop_loss:   float
    target:      float
    circ_cap:    float   # 亿


def scan_midway_surge() -> list:
    """
    半路模式扫描：捕捉日内放量突破分时均线/前高的标的。

    信号类型：
      1. 突破前高：当前价突破昨日最高价，且量比≥2.0，涨幅3%~9.5%
      2. 突破均线：当前价站上5日均线且量比≥1.5，近3日涨幅≤5%（低位蓄势后启动）
      3. 量价共振：日内涨幅≥3%同时量比≥3.0，近5日涨幅≤15%（短期强势）

    过滤条件：
      - 剔除 ST / 涨停 / 跌停 / 停牌
      - 流通市值 30亿~500亿
      - 今日成交额 ≥ 5000万
      - 量比 ≥ 1.5（放量）
      - 涨幅 3%~9.5%（不追高、不做微涨）
    """
    signals: list = []
    try:
        rt = get_realtime_quotes()
        if rt.empty:
            return signals

        # 获取5日均线数据（批量，用于突破均线信号）
        # 此处采用轻量策略：用近期实时量能对比估算，不逐股拉历史K线
        zt_price_map: dict = {}

        for _, row in rt.iterrows():
            try:
                code    = str(row.get("code", "")).zfill(6)
                name    = str(row.get("name", ""))
                price   = float(row.get("price",      0) or 0)
                prev_c  = float(row.get("prev_close", 0) or 0)
                high    = float(row.get("high",        0) or 0)
                low     = float(row.get("low",         0) or 0)
                vol_r   = float(row.get("vol_ratio",   0) or 0)
                amount  = float(row.get("amount",      0) or 0)
                circ_c  = float(row.get("circ_cap",    0) or 0)

                if price <= 0 or prev_c <= 0:
                    continue
                if is_st(name):
                    continue
                # 涨跌幅
                chg = (price - prev_c) / prev_c
                # 过滤：涨停 / 跌停
                zt_p = calc_zt_price(prev_c)
                if price >= zt_p * 0.999:
                    continue  # 已涨停，走打板逻辑
                if chg <= -0.095:
                    continue  # 跌停
                # 基础过滤
                if not (0.03 <= chg <= 0.095):
                    continue
                if vol_r < 1.5:
                    continue
                if amount < 50_000_000:
                    continue
                cap_yi = circ_c / 1e8 if circ_c > 0 else 0
                if not (30 <= cap_yi <= 500):
                    continue

                # ─── 信号识别 ────────────────────────────────────
                sig_type = ""
                score    = 0.0
                reason   = ""

                # 信号1：量价共振（最强，当天量比≥3.0且涨幅≥3%）
                if vol_r >= 3.0 and chg >= 0.03:
                    sig_type = "量价共振"
                    score    = 50 + min(vol_r * 4, 20) + min(chg * 200, 15)
                    reason   = (f"量比{vol_r:.1f}x放量+涨幅{chg:+.1%}，"
                                f"量价共振强势，日内冲板概率高")

                # 信号2：突破前高（价格突破昨高，量比≥2.0）
                elif vol_r >= 2.0 and price > prev_c * 1.0 and high >= prev_c * 1.005:
                    # 用 high 估算昨日高点突破（实时行情只有 prev_close，无昨日高）
                    # 改为判断：今日高点 > 昨收 * 1.005（突破昨收上方0.5%，近似前高压力位）
                    sig_type = "突破前高"
                    score    = 45 + min(vol_r * 3, 15) + min(chg * 150, 10)
                    reason   = (f"价格突破前高压力，量比{vol_r:.1f}x，"
                                f"涨幅{chg:+.1%}，追强不追高")

                # 信号3：放量上涨但未达强标准（量比1.5~3.0，涨幅3%~6%）
                elif 1.5 <= vol_r < 3.0 and 0.03 <= chg <= 0.06:
                    sig_type = "量价共振"
                    score    = 35 + vol_r * 3 + chg * 100
                    reason   = (f"量比{vol_r:.1f}x温和放量，涨幅{chg:+.1%}，"
                                f"半路点火，注意量能持续性")

                if not sig_type or score < 40:
                    continue

                # ─── 入场/止损/目标价 ────────────────────────────
                entry  = round(price * 1.002, 2)   # 略高于现价买入（防滑点）
                sl     = round(price * 0.97, 2)    # 3% 止损
                target = round(price * (1 + min(chg * 2, 0.09)), 2)  # 目标 = 日内延伸

                signals.append(MidwaySignal(
                    code=code, name=name, price=price, chg_pct=chg,
                    vol_ratio=vol_r, signal_type=sig_type, score=score,
                    reason=reason, entry_price=entry, stop_loss=sl,
                    target=target, circ_cap=cap_yi,
                ))

            except Exception:
                continue

        # 按评分排序，取前10
        signals.sort(key=lambda x: x.score, reverse=True)
        return signals[:10]

    except Exception as e:
        log.warning(f"半路扫描异常: {e}")
        return signals


def format_midway_signal(sig: MidwaySignal, rank: int) -> str:
    type_icon = {"量价共振": "⚡", "突破前高": "🔝", "突破均线": "📶"}.get(sig.signal_type, "🚦")
    return (
        f"### {rank}. {type_icon}{sig.name}（{sig.code}）\n"
        f"**类型**：{sig.signal_type} | **评分**：{sig.score:.0f}分\n"
        f"**现价**：{sig.price:.2f}（{sig.chg_pct:+.1%}）| 量比：{sig.vol_ratio:.1f}x\n"
        f"**入场**：{sig.entry_price:.2f} | **止损**：{sig.stop_loss:.2f} | **目标**：{sig.target:.2f}\n"
        f"**流通市值**：{sig.circ_cap:.0f}亿\n"
        f"**理由**：{sig.reason}\n"
        f"> ⚠️ 半路追强，仓位控制5~10%，止损3%严格执行\n"
    )


def push_midway_signals(signals: list, market: dict) -> None:
    if not signals:
        return
    market_line = ""
    ms = market.get("market_state", "")
    if ms:
        market_line = f"> 大盘：{ms}\n\n"

    header = (
        f"# 🚦半路追强信号\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
        f"共 **{len(signals)}只**\n\n"
        f"{market_line}"
        f"> 半路点火 = 量价共振+放量突破，追强不追高，严格止损\n\n"
    )
    body = ""
    for i, sig in enumerate(signals[:8], 1):
        body += format_midway_signal(sig, i) + "\n---\n"

    footer = (
        "\n> 📌操作建议：现价附近分批入场，1%滑点内可追；"
        "止损价跌破后无条件出，不猜底\n"
        "> ⚡量价共振信号有效窗口约30~60分钟，超时降低参与意愿"
    )

    top = signals[0]
    title = (
        f"🚦半路:{top.name}{top.chg_pct:+.1%} 量{top.vol_ratio:.1f}x"
        f" [{beijing_now().strftime('%H:%M')}]"
    )
    send_wx(title, header + body + footer)


# ================================================================
# ★ 临近涨停预判模块 — 水下7%~9.4%时提前布局
# ================================================================

@dataclass
class PreZtSignal:
    code:         str
    name:         str
    price:        float
    chg_pct:      float   # 当前涨幅%（如 5.8 表示涨5.8%）
    open_chg_pct: float   # 开盘涨幅%（今日开盘相对前收的涨幅）
    zt_price:     float   # 涨停价
    gap_pct:      float   # 距涨停还差多少%（利润空间上界）
    vol_ratio:    float   # 量比
    amount:       float   # 成交额（元）
    circ_cap:     float   # 流通市值（亿）
    surge_tag:    str     # 拉升特征描述（"开盘持续拉升" / "盘中突然拉升" 等）
    score:        float
    reason:       str


def scan_pre_zt() -> list:
    """
    直线拉升预警扫描（v3.9 重构）。

    核心目标：在量价齐升的拉升途中尽早预警，为用户保留最大利润空间。

    触发条件（全部满足）：
      1. 涨幅在 PRE_ZT_MIN_CHG~PRE_ZT_MAX_CHG（4%~9.2%），距涨停有操作空间
      2. 量比 ≥ PRE_ZT_VOL_RATIO（3x），主力明确买入，非散户小打小闹
      3. 价格 ≈ 日内最高价（距高点 ≤1.2%），价格无回落，持续攻击形态
      4. 开盘价 ≥ 前收价（不接受低开，低开说明主力不拉或竞价出货）
      5. 成交额 ≥ PRE_ZT_MIN_AMOUNT（5000万），资金规模有效

    评分维度（满分100）：
      - 量比强度（主要指标，权重最高）
      - 涨幅位置（4%触发=利润空间大但确定性低；9%触发=确定性高但空间小）
      - 开盘即拉（早盘就拉=主力开盘就打，加大分）
      - 价格加速（涨幅远超开盘涨幅=盘中加速，大单持续打入）
      - 成交额规模
      - 市值弹性
    """
    signals: list = []
    try:
        rt = get_realtime_quotes()
        if rt.empty:
            return signals

        for _, row in rt.iterrows():
            try:
                code      = str(row.get("code", "")).zfill(6)
                name      = str(row.get("name", ""))
                price     = float(row.get("price",      0) or 0)
                prev_c    = float(row.get("prev_close", 0) or 0)
                open_p    = float(row.get("open",       0) or 0)
                high      = float(row.get("high",       0) or 0)
                vol_ratio = float(row.get("vol_ratio",  0) or 0)
                amount    = float(row.get("amount",     0) or 0)
                circ_c    = float(row.get("circ_mkt_cap", 0) or 0)
                turnover  = float(row.get("turnover",   0) or 0)

                if is_st(name):
                    continue
                # 科创板(688)、北交所(8xx/43x/92x) 排除：涨跌幅限制不同，涨停价计算不适用
                if code.startswith("688") or code.startswith("8") \
                        or code.startswith("43") or code.startswith("92"):
                    continue
                if code in PRE_ZT_PUSHED_TODAY:
                    continue
                if prev_c <= 0 or price <= 0:
                    continue

                # ── 基础过滤 ──────────────────────────────────────────────
                if amount < PRE_ZT_MIN_AMOUNT:
                    continue
                if circ_c > 0 and (circ_c < MIN_MKT_CAP or circ_c > MAX_MKT_CAP):
                    continue

                chg_pct    = (price  - prev_c) / prev_c   # 当前涨幅（小数，如0.058）
                open_chg   = (open_p - prev_c) / prev_c if open_p > 0 else 0.0

                # ── 核心条件1：涨幅区间（4%~9.2%，有操作空间）────────────
                if chg_pct < PRE_ZT_MIN_CHG or chg_pct >= PRE_ZT_MAX_CHG:
                    continue

                # ── 核心条件2：量比门槛（主力持续打入）──────────────────
                if vol_ratio < PRE_ZT_VOL_RATIO:
                    continue

                # ── 核心条件3：价格不回落（当前价接近日内最高）──────────
                # 直线拉升特征：价格始终贴近最高点，没有大幅回落
                if high > 0 and price < high * (1 - PRE_ZT_PRICE_NEAR_HIGH):
                    continue

                # ── 核心条件4：不接受低开（主力开盘就在拉，不是摊牌后拉）
                if open_p > 0 and open_chg < PRE_ZT_OPEN_CHG_MIN:
                    continue

                # ── 理论涨停价 ────────────────────────────────────────────
                # 创业板300xxx/301xxx 涨停20%
                if code.startswith("300") or code.startswith("301"):
                    zt_price = round(prev_c * 1.20, 2)
                else:
                    zt_price = round(prev_c * 1.10, 2)
                gap_pct = (zt_price - price) / price * 100   # 距涨停还差X%（利润空间上界）

                # ── 拉升特征标签 ──────────────────────────────────────────
                # 开盘就拉：open_chg >= 3% 且全程没回落
                if open_chg >= 0.03 and price >= open_p * 0.99:
                    surge_tag = "开盘持续拉升"
                # 盘中加速：开盘涨幅小，但当前涨幅远超开盘（盘中突然打入）
                elif open_p > 0 and chg_pct - open_chg >= 0.03:
                    surge_tag = "盘中突然加速"
                else:
                    surge_tag = "稳步拉升"

                # ── 评分（满分100）────────────────────────────────────────
                score = 0.0

                # ① 量比（核心指标，权重最高35分）——量是拉升的发动机
                # vol_ratio ≥8 = 爆量强攻，极少见，几乎锁定封板
                if vol_ratio >= 8.0:    score += 35
                elif vol_ratio >= 6.0:  score += 30
                elif vol_ratio >= 5.0:  score += 25
                elif vol_ratio >= 4.0:  score += 20
                else:                   score += 12   # 3x~4x，刚过门槛

                # ② 涨幅位置（25分）——早期触发=空间大但确定性低，晚期=确定性高但空间小
                # 平衡：7%~9.2% 给高分（确定性强），4%~5% 给低分（空间大但需更强量）
                if chg_pct >= 0.085:    score += 25   # 8.5%+ 接近封板，确定性最强
                elif chg_pct >= 0.070:  score += 22   # 7%~8.5%
                elif chg_pct >= 0.055:  score += 18   # 5.5%~7%，中段，空间和确定性均衡
                elif chg_pct >= 0.040:  score += 12   # 4%~5.5%，早期，空间最大但需更强量支撑

                # ③ 开盘即拉（15分）——主力开盘就打是最强信号，全天都在收筹码
                if open_chg >= 0.05:    score += 15   # 高开5%以上就开始拉，主力志在必得
                elif open_chg >= 0.03:  score += 12   # 高开3%以上，开盘就在拉
                elif open_chg >= 0.01:  score += 8    # 小幅高开，盘中发力
                elif open_chg >= 0.0:   score += 4    # 平开后拉升（可以接受，但不如高开）

                # ④ 盘中加速度（10分）——开盘后涨幅加速 = 有大单持续打入
                accel = chg_pct - open_chg if open_p > 0 else 0.0
                if accel >= 0.05:       score += 10   # 开盘后又涨了5%+ = 明显加速
                elif accel >= 0.03:     score += 7
                elif accel >= 0.01:     score += 4

                # ⑤ 成交额（10分）——资金体量决定持续性
                amt_yi = amount / 1e8
                if amt_yi >= 5.0:       score += 10
                elif amt_yi >= 2.0:     score += 8
                elif amt_yi >= 1.0:     score += 6
                elif amt_yi >= 0.5:     score += 3

                # ⑥ 市值弹性（10分）——中小市值弹性最大
                cap_yi = circ_c / 1e8
                if 20 <= cap_yi <= 100:  score += 10   # 中小市值，弹性最佳
                elif 10 <= cap_yi <= 200: score += 7
                elif 5 <= cap_yi <= 400:  score += 4

                # ⑦ 换手率修正（±5分）——换手过高=分歧大，封板概率下降
                if 3.0 <= turnover <= 12.0:
                    score += 5    # 适中换手，主力控盘
                elif turnover > 25.0:
                    score -= 5    # 换手太高，主力可能边拉边出

                score = max(0.0, min(score, 100.0))

                if score < PRE_ZT_MIN_SCORE:
                    continue

                # ── 组装信号说明 ──────────────────────────────────────────
                reason_parts = [
                    f"{surge_tag}",
                    f"涨{chg_pct:.1%}→涨停{zt_price:.2f}（还差{gap_pct:.1f}%）",
                    f"量比{vol_ratio:.1f}x",
                    f"成交{amt_yi:.1f}亿",
                ]
                if open_chg >= 0.01:
                    reason_parts.append(f"开盘+{open_chg:.1%}即拉")
                if accel >= 0.02:
                    reason_parts.append(f"盘中加速+{accel:.1%}")
                if turnover > 0:
                    reason_parts.append(f"换手{turnover:.1f}%")

                signals.append(PreZtSignal(
                    code=code, name=name, price=price,
                    chg_pct=round(chg_pct * 100, 2),
                    open_chg_pct=round(open_chg * 100, 2),
                    zt_price=zt_price,
                    gap_pct=round(gap_pct, 2),
                    vol_ratio=vol_ratio,
                    amount=amount,
                    circ_cap=round(cap_yi, 1),
                    surge_tag=surge_tag,
                    score=round(score, 1),
                    reason=" | ".join(reason_parts),
                ))

            except Exception as e:
                log.debug(f"直线拉升扫描异常 {row.get('code','?')}: {e}")

    except Exception as e:
        log.error(f"直线拉升预警扫描失败: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    return signals[:10]


def format_pre_zt_signal(sig: PreZtSignal, rank: int) -> str:
    """格式化单个直线拉升预警信号，突出利润空间和拉升特征。"""
    # 评级标签
    if sig.score >= 85:
        grade = "⭐⭐⭐ 极强"
    elif sig.score >= 70:
        grade = "⭐⭐ 强"
    else:
        grade = "⭐ 关注"

    # 利润空间文字（让用户一眼看到可操作收益）
    profit_desc = f"现价买入→涨停可赚约 **{sig.gap_pct:.1f}%**"

    # 开盘情况描述
    if sig.open_chg_pct >= 3.0:
        open_desc = f"高开{sig.open_chg_pct:.1f}%后持续拉升 🚀"
    elif sig.open_chg_pct >= 1.0:
        open_desc = f"小幅高开{sig.open_chg_pct:.1f}%后加速"
    elif sig.open_chg_pct >= 0.0:
        open_desc = "平开后盘中拉升"
    else:
        open_desc = f"低开{sig.open_chg_pct:.1f}%后强势反攻"

    stop_price = round(sig.price * 0.97, 2)   # 止损位：当前价-3%

    return (
        f"### {rank}. 🔥{sig.name}（{sig.code}）\n"
        f"**评分**：{sig.score:.0f}分 {grade} | **{sig.surge_tag}**\n"
        f"**现价**：{sig.price:.2f}　涨幅：**+{sig.chg_pct:.1f}%** → 涨停价：{sig.zt_price:.2f}\n"
        f"**{profit_desc}**\n"
        f"**量比**：{sig.vol_ratio:.1f}x 放量　成交额：{sig.amount/1e8:.1f}亿　流通：{sig.circ_cap:.0f}亿\n"
        f"**开盘**：{open_desc}\n"
        f"**信号**：{sig.reason}\n"
        f"> 💡操作：现价或微溢价挂单，止损参考 **{stop_price:.2f}**（-3%）\n"
    )


def push_pre_zt_signals(signals: list, market: dict) -> None:
    """推送直线拉升预警信号（v3.9 重构）"""
    if not signals:
        return
    now_str   = beijing_now().strftime("%H:%M")
    mkt_state = market.get("market_state", "")
    mkt_line  = f"> 大盘：{mkt_state}\n\n" if mkt_state else ""

    # 按涨幅分段统计，让用户了解分布
    early  = [s for s in signals if s.chg_pct < 5.5]    # 早期（4%~5.5%，空间最大）
    mid    = [s for s in signals if 5.5 <= s.chg_pct < 7.5]  # 中段（5.5%~7.5%）
    late   = [s for s in signals if s.chg_pct >= 7.5]   # 后期（7.5%~9.2%，确定性最强）

    dist_parts = []
    if early: dist_parts.append(f"早期({len(early)}只·空间大)")
    if mid:   dist_parts.append(f"中段({len(mid)}只·均衡)")
    if late:  dist_parts.append(f"后期({len(late)}只·确定性强)")
    dist_str = " / ".join(dist_parts) if dist_parts else f"共{len(signals)}只"

    top = signals[0]
    header = (
        f"# 🚀直线拉升预警\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
        f"共 **{len(signals)}只** 量价齐升中\n\n"
        f"{mkt_line}"
        f"> {dist_str}\n\n"
        f"> 以下股票正在直线拉升，量比≥3x，价格无回落，预判冲击涨停\n\n"
    )
    body = ""
    for i, sig in enumerate(signals[:6], 1):
        body += format_pre_zt_signal(sig, i) + "\n---\n"

    footer = (
        "\n> 📌**操作原则**：\n"
        "> 1. 量比持续放大（越来越大）= 主力在加仓，可跟\n"
        "> 2. 量比开始缩小（由大变小）= 主力在减仓，立即观望\n"
        "> 3. 价格回落超日内高点2%，信号失效，不追\n"
        "> ⚠️早期信号（<5.5%）利润空间大，但封板确定性相对低，适当控制仓位"
    )

    title = (
        f"🚀拉升预警:{top.name}+{top.chg_pct:.1f}% 量{top.vol_ratio:.1f}x"
        f" 还差{top.gap_pct:.1f}% [{now_str}]"
    )
    send_wx(title, header + body + footer)


# ================================================================
# ★ 尾盘套利模式 — 14:50 前强势横盘，搏隔日高开溢价
# ================================================================

# 尾盘套利推送标记（每天只推一次）
_TAIL_ARB_PUSHED: bool = False

@dataclass
class TailArbSignal:
    code:        str
    name:        str
    price:       float
    chg_pct:     float   # 当日涨幅
    vol_ratio:   float
    signal_type: str     # "尾盘强势" / "尾盘近涨停" / "弱转强"
    score:       float
    reason:      str
    entry_price: float
    stop_loss:   float   # 次日开盘跌破此价止损
    target:      float   # 隔日目标
    circ_cap:    float


def scan_tail_arb() -> list:
    """
    尾盘套利扫描（14:40~14:55 运行最有效）：
    捕捉收盘前有主力维护、强势横盘/弱转强的标的，搏隔日高开溢价。

    信号类型：
      1. 尾盘近涨停（最强）：涨幅≥8%且未封涨停，封板资金充足，尾盘强攻迹象
      2. 尾盘强势：涨幅5%~8%，量比≥2.0，全天走势平稳无大幅回落
      3. 弱转强：日内低点附近反弹，尾盘涨幅由负转正或由低位拉升≥3%

    过滤：
      - 成交额≥8000万（流动性足够）
      - 流通市值30亿~300亿（中小盘弹性好）
      - 剔除 ST / 跌停 / 已连续3日涨停（过热）
      - 全天波动（最高-最低）/昨收 ≤ 12%（排除超高振幅异常票）
    """
    signals: list = []
    try:
        rt = get_realtime_quotes()
        if rt.empty:
            return signals

        for _, row in rt.iterrows():
            try:
                code   = str(row.get("code", "")).zfill(6)
                name   = str(row.get("name", ""))
                price  = float(row.get("price",      0) or 0)
                prev_c = float(row.get("prev_close", 0) or 0)
                high   = float(row.get("high",       0) or 0)
                low    = float(row.get("low",        0) or 0)
                vol_r  = float(row.get("vol_ratio",  0) or 0)
                amount = float(row.get("amount",     0) or 0)
                circ_c = float(row.get("circ_cap",   0) or 0)

                if price <= 0 or prev_c <= 0:
                    continue
                if is_st(name):
                    continue

                chg    = (price - prev_c) / prev_c
                zt_p   = calc_zt_price(prev_c)
                amp    = (high - low) / prev_c if prev_c > 0 else 0

                # 基础过滤
                if chg <= -0.01:
                    continue  # 收盘仍是负收益，不做
                if chg >= 0.099:
                    continue  # 已涨停，走打板逻辑
                if amount < 80_000_000:
                    continue
                if amp > 0.12:
                    continue  # 超高振幅，主力不稳
                cap_yi = circ_c / 1e8 if circ_c > 0 else 0
                if not (30 <= cap_yi <= 300):
                    continue
                if vol_r < 1.2:
                    continue

                # ─── 信号识别 ────────────────────────────────────
                sig_type = ""
                score    = 0.0
                reason   = ""

                # 信号1：尾盘近涨停（涨幅≥8%，接近涨停价，未封死）
                if chg >= 0.08 and price >= zt_p * 0.97:
                    sig_type = "尾盘近涨停"
                    score    = 70 + min(chg * 200, 20)
                    reason   = (f"涨幅{chg:+.1%}，距涨停价{(zt_p-price)/zt_p:.1%}，"
                                f"量比{vol_r:.1f}x，尾盘强攻涨停，隔日溢价预期强")

                # 信号2：尾盘强势（涨幅5%~8%，全天稳定）
                elif 0.05 <= chg < 0.08 and vol_r >= 2.0:
                    # 强度判断：当前价距日内高点回落不超过1.5%（走势平稳）
                    dist_from_high = (high - price) / high if high > 0 else 0
                    if dist_from_high <= 0.015:
                        sig_type = "尾盘强势"
                        score    = 55 + min(vol_r * 3, 12) + min(chg * 200, 10)
                        reason   = (f"涨幅{chg:+.1%}，量比{vol_r:.1f}x，"
                                    f"尾盘距高点{dist_from_high:.1%}，主力维护迹象，"
                                    f"搏隔日高开溢价")

                # 信号3：弱转强（开盘弱势但尾盘拉升，量比放大）
                elif 0.02 <= chg < 0.05 and vol_r >= 2.5:
                    # 判断弱转强：当前价 > 今日开盘价 * 1.02（收盘明显强于开盘）
                    open_p = float(row.get("open", 0) or 0)
                    if open_p > 0 and price > open_p * 1.02:
                        sig_type = "弱转强"
                        score    = 45 + min(vol_r * 2, 10)
                        reason   = (f"开盘{open_p:.2f}→尾盘{price:.2f}"
                                    f"({(price/open_p-1):+.1%}弱转强)，"
                                    f"量比{vol_r:.1f}x，主力尾盘护盘/拉升")

                if not sig_type or score < 45:
                    continue

                # 入场/次日止损/目标
                entry  = round(price * 1.001, 2)          # 尾盘现价附近
                sl     = round(prev_c * 1.005, 2)         # 次日跌破涨幅0.5%即止损
                target = round(prev_c * (1 + min(chg + 0.03, 0.10)), 2)  # 隔日目标

                signals.append(TailArbSignal(
                    code=code, name=name, price=price, chg_pct=chg,
                    vol_ratio=vol_r, signal_type=sig_type, score=score,
                    reason=reason, entry_price=entry, stop_loss=sl,
                    target=target, circ_cap=cap_yi,
                ))

            except Exception:
                continue

        signals.sort(key=lambda x: x.score, reverse=True)
        return signals[:10]

    except Exception as e:
        log.warning(f"尾盘套利扫描异常: {e}")
        return signals


def format_tail_arb_signal(sig: TailArbSignal, rank: int) -> str:
    type_icon = {
        "尾盘近涨停": "🔥", "尾盘强势": "💪", "弱转强": "🔄"
    }.get(sig.signal_type, "🌙")
    return (
        f"### {rank}. {type_icon}{sig.name}（{sig.code}）\n"
        f"**类型**：{sig.signal_type} | **评分**：{sig.score:.0f}分\n"
        f"**现价**：{sig.price:.2f}（{sig.chg_pct:+.1%}）| 量比：{sig.vol_ratio:.1f}x\n"
        f"**入场**：{sig.entry_price:.2f}（尾盘现价） | 流通市值：{sig.circ_cap:.0f}亿\n"
        f"**次日止损**：{sig.stop_loss:.2f} | **隔日目标**：{sig.target:.2f}\n"
        f"**理由**：{sig.reason}\n"
        f"> 🌙 尾盘套利：仓位5~8%，次日开盘后跌破止损价立即出\n"
    )


def push_tail_arb_signals(signals: list, market: dict) -> None:
    if not signals:
        return
    market_line = ""
    ms = market.get("market_state", "")
    if ms:
        market_line = f"> 大盘：{ms}\n\n"

    type_count: dict = {}
    for s in signals:
        type_count[s.signal_type] = type_count.get(s.signal_type, 0) + 1
    type_summary = " | ".join([f"{k}{v}只" for k, v in type_count.items()])

    header = (
        f"# 🌙尾盘套利信号\n\n"
        f"时间：{beijing_now().strftime('%Y-%m-%d %H:%M')} | "
        f"共 **{len(signals)}只** | {type_summary}\n\n"
        f"{market_line}"
        f"> 策略逻辑：尾盘强势持股→搏隔日高开溢价，次日开盘止损价跌破即出\n\n"
    )
    body = ""
    for i, sig in enumerate(signals[:8], 1):
        body += format_tail_arb_signal(sig, i) + "\n---\n"

    footer = (
        "\n---\n"
        "> ⚠️ 注意事项：\n"
        "> 1. 尾盘套利仓位严格控制在5~8%，不超过3只同时持有\n"
        "> 2. 次日若高开≥2%可考虑减半兑利，完全兑利在≥5%\n"
        "> 3. 次日若跳空低开，开盘即止损，不等回调\n"
        "> 4. 大盘弱势日（大盘跌>1%）慎用此策略"
    )

    top = signals[0]
    title = (
        f"🌙尾盘:{top.name}{top.chg_pct:+.1%}"
        f" [{beijing_now().strftime('%H:%M')}]"
    )
    send_wx(title, header + body + footer)


# ================================================================
# ★ 出场系统 — 基于现价动态给出止盈/止损建议
# ================================================================

def exit_advice(entry_price: float, current_price: float,
                strategy: str, connect_days: int = 1) -> str:
    """
    根据入场价、现价、策略类型给出出场建议。

    出场逻辑：
      - 涨停持有：不急于出，次日竞价评估
      - 大涨5%+冲高：分批止盈（先减1/2）
      - 回到成本线：视情况减仓防守
      - 跌破止损线（-3%）：强制清仓

    返回格式化字符串。
    """
    if entry_price <= 0 or current_price <= 0:
        return ""

    ret = (current_price - entry_price) / entry_price

    # 根据策略设定止盈/止损标准
    if strategy in ("首板", "连板"):
        sl_pct   = -0.03  # 止损 3%
        tp1_pct  =  0.05  # 第一止盈档 5%
        tp2_pct  =  0.09  # 涨停附近
    elif strategy in ("洗盘", "日内"):
        sl_pct   = -0.04
        tp1_pct  =  0.05
        tp2_pct  =  0.09
    elif strategy == "反包":
        sl_pct   = -0.05
        tp1_pct  =  0.04
        tp2_pct  =  0.08
    else:  # 半路、尾盘
        sl_pct   = -0.03
        tp1_pct  =  0.04
        tp2_pct  =  0.08

    if ret >= 0.095:
        return "🏆 **出场建议**：已涨停！持仓过夜，次日竞价溢价评估后决定是否持有"
    elif ret >= tp2_pct:
        return f"📈 **出场建议**：浮盈{ret:+.1%}，接近涨停！建议减仓1/2锁利，剩余搏涨停"
    elif ret >= tp1_pct:
        return f"✅ **出场建议**：浮盈{ret:+.1%}，达第一止盈档，建议减仓1/3，移动止损至成本"
    elif ret >= 0.02:
        return f"📊 **出场建议**：浮盈{ret:+.1%}，持有观察，止损上移至成本线附近"
    elif ret >= 0:
        return f"➡️ **出场建议**：小幅盈利{ret:+.1%}，持有等待放量突破或尾盘评估"
    elif ret >= sl_pct:
        return f"⚠️ **出场建议**：浮亏{ret:+.1%}，接近止损线（{sl_pct:.0%}），减仓防守"
    else:
        return f"🛑 **出场建议**：浮亏{ret:+.1%}，**已触发止损线，立即清仓！**"


# ================================================================
# ★ CSV/Excel 导出 — 收盘后将所有信号保存为文件
# ================================================================

def export_signals_csv(all_signals: list, tail_arb: list = None,
                       midway: list = None) -> str:
    """
    将今日所有信号导出为 CSV 文件（GitHub Actions 环境保存到工作目录）。
    返回文件路径（供日志记录）。
    """
    import csv
    import os

    today = beijing_now().strftime("%Y%m%d")
    fname = f"signals_{today}.csv"

    rows = []

    # 打板/首板/连板/竞价/反包/洗盘/日内信号
    for s in (all_signals or []):
        rows.append({
            "日期":     today,
            "策略":     getattr(s, "strategy", ""),
            "代码":     getattr(s, "code", ""),
            "名称":     getattr(s, "name", ""),
            "评分":     round(getattr(s, "score", 0), 1),
            "现价":     getattr(s, "price", 0),
            "入场价":   getattr(s, "entry_price", 0),
            "止损价":   getattr(s, "stop_loss", 0),
            "封板强度": f"{getattr(s, 'seal_ratio', 0):.1%}" if hasattr(s, "seal_ratio") else "",
            "连板数":   getattr(s, "connect_days", 1),
            "封板时间": getattr(s, "seal_time_hm", 0),
            "风险提示": getattr(s, "fake_flags", ""),
            "理由":     getattr(s, "reason", ""),
        })

    # 半路信号
    for s in (midway or []):
        rows.append({
            "日期":   today,
            "策略":   f"半路-{s.signal_type}",
            "代码":   s.code,
            "名称":   s.name,
            "评分":   round(s.score, 1),
            "现价":   s.price,
            "入场价": s.entry_price,
            "止损价": s.stop_loss,
            "封板强度": "",
            "连板数":   0,
            "封板时间": 0,
            "风险提示": "",
            "理由":   s.reason,
        })

    # 尾盘套利信号
    for s in (tail_arb or []):
        rows.append({
            "日期":   today,
            "策略":   f"尾盘-{s.signal_type}",
            "代码":   s.code,
            "名称":   s.name,
            "评分":   round(s.score, 1),
            "现价":   s.price,
            "入场价": s.entry_price,
            "止损价": s.stop_loss,
            "封板强度": "",
            "连板数":   0,
            "封板时间": 0,
            "风险提示": "",
            "理由":   s.reason,
        })

    if not rows:
        return ""

    try:
        with open(fname, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"信号已导出至 {fname}，共 {len(rows)} 条")
        return fname
    except Exception as e:
        log.warning(f"CSV导出失败: {e}")
        return ""


# ================================================================
# 更新 push_startup 和 push_summary
# ================================================================

def push_startup() -> None:
    """启动推送 + 健康检查，让用户每天能确认系统是否真的跑起来了"""
    now_str  = beijing_now().strftime("%Y-%m-%d %H:%M:%S")
    phase    = current_phase()
    phase_cn = {
        "pre_open": "开市前等待", "pre_auction": "集合竞价",
        "opening": "开盘缓冲", "morning": "上午盘",
        "noon_break": "午休", "afternoon": "下午盘",
        "pre_close": "尾盘", "closed": "已收盘"
    }.get(phase, phase)
    send_wx(
        "🟢打板系统已启动 v9.5",
        f"**A股超短线量化交易系统 v9.5**\n\n"
        f"启动时间：{now_str}\n"
        f"当前阶段：**{phase_cn}**（{phase}）\n\n"
        f"**全覆盖六大策略**：\n"
        f"- ⚡ 竞价打板：开盘抢筹强度+高开幅度+竞价量能\n"
        f"- 🔥 首板/连板：质量过滤+假涨停识别+板块效应\n"
        f"- 🔄 跌停反包：次日竞价修复逻辑\n"
        f"- 🔍 洗盘低吸：均线多头+缩量+支撑企稳\n"
        f"- 📍 日内抄底：T字板/均价回踩/半T字三合一\n"
        f"- 🚦 半路追强：量价共振+突破前高（v4.0新增）\n"
        f"- 🌙 尾盘套利：强势横盘搏隔日溢价（v4.0新增）\n\n"
        f"**增强功能**：\n"
        f"- 🕯️ 长下影线参与信号\n"
        f"- 🚀 盘中量价异动实时推送\n"
        f"- 📊 大盘+外围+北向资金综合研判\n"
        f"- ⚠️ 冲高回落/炸板定时预警（9:25/14:30）\n"
        f"- 📰 消息与股价联动实时推送\n"
        f"- 🛡️ 出场建议系统：止盈/止损动态指导\n"
        f"- 📑 CSV信号导出：收盘后自动保存\n\n"
        f"**推送时段**：\n"
        f"- 08:50~09:25 竞价信号\n"
        f"- 09:25 开盘强制推送+炸板第一报\n"
        f"- 09:25~14:00 首板/连板/半路/日内抄底实时更新\n"
        f"- 14:30 炸板第二报\n"
        f"- 14:40~14:55 尾盘套利信号\n"
        f"- 收盘后 今日汇总+CSV导出"
    )

def push_summary(all_signals: list, tail_arb_signals: list = None,
                  midway_signals: list = None) -> None:
    """收盘后推送今日汇总，包含信号统计和全市场涨停情况，并导出 CSV"""
    now_str = beijing_now().strftime("%Y-%m-%d")
    # 获取全市场今日涨停数
    zt_total = 0
    try:
        zt_df_today = ak.stock_zt_pool_em(date=beijing_now().strftime("%Y%m%d"))
        zt_total = len(zt_df_today) if zt_df_today is not None else 0
    except:
        pass

    mkt_line = f"今日全市场涨停：**{zt_total}只**\n\n" if zt_total else ""

    # 导出 CSV（不管有没有信号都尝试导出）
    csv_path = export_signals_csv(all_signals, tail_arb_signals, midway_signals)
    csv_note = f"\n\n📑 信号已导出至 `{csv_path}`" if csv_path else ""

    total_extra = len(tail_arb_signals or []) + len(midway_signals or [])

    if not all_signals and not total_extra:
        send_wx(
            f"📊今日汇总({now_str})",
            f"**{now_str} 交易汇总**\n\n"
            f"{mkt_line}"
            f"今日系统无有效信号推送（过滤后）\n\n"
            f"> 如感觉漏信号，请检查 GitHub Actions 运行日志"
            f"{csv_note}"
        )
        return

    by_s: dict = {}
    top_sigs = sorted(all_signals, key=lambda x: x.score, reverse=True)[:5] if all_signals else []
    for s in (all_signals or []):
        by_s[s.strategy] = by_s.get(s.strategy, 0) + 1
    # 附加策略统计
    for s in (midway_signals or []):
        by_s["半路"] = by_s.get("半路", 0) + 1
    for s in (tail_arb_signals or []):
        by_s["尾盘"] = by_s.get("尾盘", 0) + 1
    summary_line = " | ".join([f"{k}:{v}只" for k, v in by_s.items()])
    total_count  = len(all_signals or []) + total_extra

    # ★ 收盘汇总加入今日表现对比（入场价 vs 收盘价）
    try:
        rt_close = get_realtime_quotes()
        rt_close_map = {}
        if not rt_close.empty:
            for _, _r in rt_close.iterrows():
                rt_close_map[str(_r.get("code","")).zfill(6)] = _r
    except Exception:
        rt_close_map = {}

    top_line_parts = []
    for i, s in enumerate(top_sigs):
        line = f"{i+1}. {s.name}({s.code}) {s.strategy} {s.score:.0f}分 | 入场价{s.entry_price:.2f}"
        _rt = rt_close_map.get(s.code)
        if _rt is not None:
            _close_p = float(_rt.get("price", 0) or 0)
            if _close_p > 0 and s.entry_price > 0:
                _ret = (_close_p - s.entry_price) / s.entry_price
                _icon = "📈" if _ret > 0.01 else ("📉" if _ret < -0.02 else "➡️")
                line += f" → 收盘{_close_p:.2f}({_icon}{_ret:+.1%})"
        top_line_parts.append(line)
    top_line = "\n".join(top_line_parts)

    send_wx(
        f"📊今日汇总({now_str})",
        f"**{now_str} 交易汇总**\n\n"
        f"{mkt_line}"
        f"**推送信号**：{summary_line}（共{total_count}只）\n\n"
        f"**TOP5信号（含当日表现）**：\n{top_line}"
        f"{csv_note}"
    )


# ================================================================
# 主扫描
# ================================================================
def run_scan(phase: str) -> tuple:
    """
    返回 (signals: list, emotion: dict)
    emotion 用于推送时展示市场情绪信息。
    """
    log.info(f"扫描中 [{phase}]...")
    signals = []
    emotion = {}
    try:
        yesterday_zt_df    = get_yesterday_zt()
        yesterday_zt_codes: set = set()
        if not yesterday_zt_df.empty:
            col = "代码" if "代码" in yesterday_zt_df.columns else yesterday_zt_df.columns[0]
            yesterday_zt_codes = set(yesterday_zt_df[col].astype(str).str.zfill(6))

        if phase == "pre_auction":
            signals.extend(scan_auction_board())
        else:
            # ★ 市场情绪过滤（非竞价阶段）
            # ★ v3.1修复：opening（9:25~9:35）阶段跳过情绪过滤
            #   原因：开盘几分钟内涨停池数据不完整，误判为"冷清"导致漏推
            emotion = get_market_emotion()
            zt_count = emotion.get("zt_count", 0)

            if phase not in ("opening",) and zt_count > 0 and zt_count < EMOTION_MIN_ZT:
                log.info(f"市场情绪过冷（涨停{zt_count}家），暂停推送")
                return signals, emotion   # 直接返回空信号

            zt_df        = get_zt_pool()
            sector_map   = get_sector_zt_map(zt_df)

            signals.extend(scan_connect_board(zt_df, yesterday_zt_codes, sector_map))
            signals.extend(scan_first_board(zt_df, yesterday_zt_codes, sector_map))
            dt_yesterday = get_yesterday_dt()
            signals.extend(scan_dt_recover(dt_yesterday))

    except Exception as e:
        log.error(f"扫描异常: {e}")

    log.info(f"本轮信号 {len(signals)} 只")
    return signals, emotion


# ================================================================
# 主循环
# ================================================================
def main():
    global OPENING_PUSHED, PULLBACK_PUSHED_925, PULLBACK_PUSHED_1430
    global _HEARTBEAT_PUSHED_HOURS, PRE_ZT_PUSHED_TODAY

    # ★ 启动前强检查：SENDKEY 必须通过环境变量传入
    if not SENDKEY:
        log.error("❌ SENDKEY 未配置！请在 GitHub Secrets / 环境变量中设置 SENDKEY。")
        log.error("   系统无法推送消息，退出。")
        raise SystemExit(1)

    manual = os.environ.get("MANUAL_TRIGGER", "0") == "1"
    test   = os.environ.get("TEST_MODE", "0") == "1"

    push_startup()

    if test:
        log.info("测试模式，退出")
        return

    if not is_trading_day() and not manual:
        send_wx("📅非交易日", f"今日（{beijing_now().strftime('%Y-%m-%d')}）非交易日，系统待机")
        return

    all_day_signals:     list = []
    all_midway_signals:  list = []   # 今日所有半路信号（用于收盘汇总/CSV）
    all_tail_arb_signals: list = []  # 今日所有尾盘套利信号
    log.info("进入主循环...")

    while True:
        try:
            phase = current_phase()
            now   = beijing_now()
            hm    = now.hour * 100 + now.minute

            # ── 收盘：汇总后退出 ────────────────────────────────────────
            if phase == "closed":
                push_summary(all_day_signals, all_tail_arb_signals, all_midway_signals)
                log.info("收盘，退出")
                break

            # ── 开市前：等待 ────────────────────────────────────────────
            if phase == "pre_open":
                log.info(f"等待开市 {now.strftime('%H:%M')}，60s后重试")
                time.sleep(60)
                continue

            # ── 午休：暂停 ──────────────────────────────────────────────
            if phase == "noon_break":
                log.info("午休，暂停扫描")
                time.sleep(60)
                continue

            # ── 获取大盘状态（所有阶段均需要）─────────────────────────
            market = get_market_index()

            # ★ v3.4 性能：每轮扫描开始时主动刷新全市场行情缓存一次
            # 后续所有子扫描函数（washout/shadow/intraday_dip等）复用此缓存，不再重复请求
            # ★ v3.5 修复：pre_auction（集合竞价 08:50~09:25）阶段 stock_zh_a_spot_em
            #   接口尚未就绪，东方财富会持续拒绝连接（RemoteDisconnected）。
            #   竞价阶段无盘中成交量，不需要实时行情，直接跳过，等 opening 阶段再拉。
            if phase in ("opening", "morning", "afternoon", "pre_close"):
                global _realtime_cache, _realtime_cache_time
                import time as _t
                _cache_age = _t.time() - _realtime_cache_time
                if _cache_age >= REALTIME_CACHE_TTL:
                    _realtime_cache_time = 0.0   # 强制过期，触发下次 get_realtime_quotes() 时刷新
                # 预拉取（提前缓存，让后续所有扫描直接命中缓存）
                get_realtime_quotes()

            # ────────────────────────────────────────────────────────────
            # ★ 9:25 强制推送：打板信号 + 第一次冲高回落/炸板提示
            # ────────────────────────────────────────────────────────────
            if hm >= 925 and not OPENING_PUSHED:
                log.info("9:25 强制扫描并推送开盘信号...")
                signals_925, emotion_925 = run_scan("opening")

                # ① 打板信号推送
                if signals_925:
                    def _sort(s: DaBanSignal) -> float:
                        return s.score + {"连板": s.connect_days * 8, "首板": 5,
                                          "竞价": 3, "反包": 2}.get(s.strategy, 0)
                    top = sorted(signals_925, key=_sort, reverse=True)[:10]
                    push_signals(top, "opening", emotion_925)
                    for s in top:
                        PUSHED_TODAY.add(s.code)
                    all_day_signals.extend([s for s in top if s.code not in {x.code for x in all_day_signals}])
                    log.info(f"9:25 强制推送 {len(top)} 只")
                else:
                    emo_info = ""
                    if emotion_925:
                        zt  = emotion_925.get("zt_count", 0)
                        emo = emotion_925.get("emotion", "")
                        emo_info = f"\n\n市场情绪：{emo}（涨停{zt}家）"
                    send_wx("📈打板系统 9:25", f"9:25 扫描暂无信号，持续监控中...{emo_info}")

                # ② 长下影线信号（开盘后即时扫描）
                ls_sigs = scan_lower_shadow()
                if ls_sigs:
                    push_lower_shadow_signals(ls_sigs, market)
                    log.info(f"9:25 长下影线信号 {len(ls_sigs)} 只")

                # ③ 冲高回落/炸板第一次推送（9:25）
                pb_sigs = scan_pullback_and_bomb()
                push_pullback_warning(pb_sigs, market, push_time="9:25")
                PULLBACK_PUSHED_925 = True
                log.info(f"9:25 冲高回落推送完成，发现 {len(pb_sigs)} 只")

                OPENING_PUSHED = True

            # ────────────────────────────────────────────────────────────
            # ★ 14:30 定时推送：冲高回落/炸板第二次提示
            # ────────────────────────────────────────────────────────────
            if hm >= 1430 and not PULLBACK_PUSHED_1430 and phase in ("afternoon", "pre_close"):
                log.info("14:30 冲高回落第二次推送...")
                pb_sigs_2 = scan_pullback_and_bomb()
                push_pullback_warning(pb_sigs_2, market, push_time="14:30")
                PULLBACK_PUSHED_1430 = True
                log.info(f"14:30 冲高回落推送完成，发现 {len(pb_sigs_2)} 只")

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：消息联动扫描（每轮必跑）
            # ────────────────────────────────────────────────────────────
            if phase in ("morning", "afternoon", "pre_close"):
                news_sigs = scan_news_price_linkage()
                if news_sigs:
                    push_news_signals(news_sigs)
                    log.info(f"消息联动推送 {len(news_sigs)} 条")

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：异动拉升扫描（量价齐升）
            # ────────────────────────────────────────────────────────────
            if phase in ("morning", "afternoon"):
                surge_sigs = scan_intraday_surge()
                new_surge  = [s for s in surge_sigs if s.code not in SURGE_PUSHED_TODAY]
                if new_surge:
                    push_surge_signals(new_surge, market)
                    log.info(f"盘中异动推送 {len(new_surge)} 只")

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：长下影线信号（盘中实时）
            # ────────────────────────────────────────────────────────────
            if phase in ("morning", "afternoon") and hm > 935:
                ls_new = scan_lower_shadow()
                # 仅推送今日未推过的
                ls_new_filtered = [
                    s for s in ls_new
                    if s.code not in PUSHED_TODAY
                    and s.signal_type == "日内"
                    and s.score >= 55
                ]
                if ls_new_filtered:
                    push_lower_shadow_signals(ls_new_filtered[:5], market)
                    for s in ls_new_filtered:
                        PUSHED_TODAY.add(s.code)
                    log.info(f"盘中长下影线推送 {len(ls_new_filtered)} 只")

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：涨停回调缩量再启动（近期涨停缩量洗盘企稳，等待再启）
            # 上午盘+下午盘扫描，每只每天只推送一次
            # ────────────────────────────────────────────────────────────
            if phase in ("morning", "afternoon") and hm > 935:
                zt_pb_sigs = scan_zt_pullback()
                new_zt_pb  = [s for s in zt_pb_sigs if s.code not in PUSHED_TODAY]
                if new_zt_pb:
                    push_zt_pullback_signals(new_zt_pb[:6], market)
                    for s in new_zt_pb:
                        PUSHED_TODAY.add(s.code)
                    log.info(f"涨停回调缩量推送 {len(new_zt_pb)} 只")

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：洗盘抄底信号（日线级别：均线多头+缩量回调支撑）
            # 只在上午盘和下午盘扫描，每个股票只推送一次
            # ────────────────────────────────────────────────────────────
            if phase in ("morning", "afternoon") and hm > 935:
                washout_sigs = scan_washout_dip()
                new_washout  = [s for s in washout_sigs if s.code not in PUSHED_TODAY]
                if new_washout:
                    push_washout_signals(new_washout[:8], market)
                    for s in new_washout:
                        PUSHED_TODAY.add(s.code)
                    log.info(f"洗盘抄底推送 {len(new_washout)} 只")

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：日内抄底信号（T字板低吸 / 均价回踩 / 半T字）
            # ★ 实时性原则：T字板立即单只推送（窗口极短）
            #   均价回踩/半T字每只当天同类型只推一次
            #   使用独立 INTRA_DIP_PUSHED_TODAY，不受打板/洗盘屏蔽影响
            # ────────────────────────────────────────────────────────────
            _has_tboard_this_round = False
            if phase in ("morning", "afternoon") and hm > 935:
                intra_dip_sigs = scan_intraday_dip()
                new_intra_dip  = []
                for s in intra_dip_sigs:
                    pushed_types = INTRA_DIP_PUSHED_TODAY.get(s.code, set())
                    if s.signal_type not in pushed_types:
                        new_intra_dip.append(s)
                if new_intra_dip:
                    # T字板立即全量推（不限数量，每只立刻推）
                    # 均价回踩/半T字取前3只推
                    t_boards  = [s for s in new_intra_dip if s.signal_type == "T字板"]
                    others    = [s for s in new_intra_dip if s.signal_type != "T字板"][:3]
                    to_push   = t_boards + others
                    push_intra_dip_signals(to_push, market)
                    for s in to_push:
                        INTRA_DIP_PUSHED_TODAY.setdefault(s.code, set()).add(s.signal_type)
                    _has_tboard_this_round = bool(t_boards)
                    log.info(
                        f"日内抄底推送 {len(to_push)} 只"
                        f"（T字板:{len(t_boards)}"
                        f" 均价回踩:{sum(1 for s in others if s.signal_type=='均价回踩')}"
                        f" 半T字:{sum(1 for s in others if s.signal_type=='半T字')}）"
                    )

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：临近涨停预判（水下7%~9.4%，提前预警）
            # 上午下午全程扫描，每只每天只推一次
            # ────────────────────────────────────────────────────────────
            if phase in ("morning", "afternoon") and hm > 935:
                pre_zt_sigs = scan_pre_zt()
                new_pre_zt  = [s for s in pre_zt_sigs if s.code not in PRE_ZT_PUSHED_TODAY]
                if new_pre_zt:
                    push_pre_zt_signals(new_pre_zt[:6], market)
                    for s in new_pre_zt:
                        PRE_ZT_PUSHED_TODAY.add(s.code)
                    log.info(f"临近涨停预警推送 {len(new_pre_zt)} 只")

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：半路追强信号（放量突破，追强不追高）
            # 上午盘 09:45~11:20，下午盘 13:10~14:20 运行（避开开收盘）
            # ────────────────────────────────────────────────────────────
            if phase == "morning" and 945 <= hm <= 1120:
                midway_sigs = scan_midway_surge()
                new_midway  = [s for s in midway_sigs
                               if s.code not in MIDWAY_PUSHED_TODAY
                               or "量价共振" not in MIDWAY_PUSHED_TODAY.get(s.code, set())]
                if new_midway:
                    push_midway_signals(new_midway[:6], market)
                    for s in new_midway:
                        MIDWAY_PUSHED_TODAY.setdefault(s.code, set()).add(s.signal_type)
                    all_midway_signals.extend(new_midway)
                    log.info(f"半路追强推送 {len(new_midway)} 只")
            elif phase == "afternoon" and 1310 <= hm <= 1420:
                midway_sigs = scan_midway_surge()
                new_midway  = [s for s in midway_sigs
                               if s.code not in MIDWAY_PUSHED_TODAY
                               or s.signal_type not in MIDWAY_PUSHED_TODAY.get(s.code, set())]
                if new_midway:
                    push_midway_signals(new_midway[:5], market)
                    for s in new_midway:
                        MIDWAY_PUSHED_TODAY.setdefault(s.code, set()).add(s.signal_type)
                    all_midway_signals.extend(new_midway)
                    log.info(f"下午半路追强推送 {len(new_midway)} 只")

            # ────────────────────────────────────────────────────────────
            # ★ 尾盘套利信号（14:40~14:55，每天只推一次）
            # ────────────────────────────────────────────────────────────
            global _TAIL_ARB_PUSHED
            if phase == "pre_close" and 1440 <= hm <= 1455 and not _TAIL_ARB_PUSHED:
                log.info("14:40 尾盘套利扫描...")
                tail_sigs = scan_tail_arb()
                if tail_sigs:
                    push_tail_arb_signals(tail_sigs, market)
                    all_tail_arb_signals.extend(tail_sigs)
                    log.info(f"尾盘套利推送 {len(tail_sigs)} 只")
                _TAIL_ARB_PUSHED = True

            # ────────────────────────────────────────────────────────────
            # ★ 盘中：打板信号出现新信号立即推送
            # ────────────────────────────────────────────────────────────
            signals, emotion = run_scan(phase)
            new_sigs = [s for s in signals if s.code not in PUSHED_TODAY]

            if new_sigs:
                push_signals(new_sigs, phase, emotion)
                for s in new_sigs:
                    PUSHED_TODAY.add(s.code)
                # ★ v3.4修复：all_day_signals去重改为O(1)（用PUSHED_TODAY集合判断）
                _pushed_in_all = {s.code for s in all_day_signals}
                all_day_signals.extend([s for s in new_sigs if s.code not in _pushed_in_all])
                log.info(f"推送 {len(new_sigs)} 只新信号，今日累计 {len(PUSHED_TODAY)} 只")
                # ★ v5.0：有信号后快速复扫间隔压缩（T字板10s，其他20s）
                time.sleep(10 if _has_tboard_this_round else 20)
            else:
                log.info(f"无新信号，已推送 {len(PUSHED_TODAY)} 只")
                # ★ v3.1：每整点心跳推送（确认系统在线 + 市场状态）
                # 推送时间：10:00 / 11:00 / 14:00（避免9:25和14:30附近重复）
                if phase in ("morning", "afternoon") and now.minute == 0:
                    hr = now.hour
                    if hr in (10, 11, 14) and hr not in _HEARTBEAT_PUSHED_HOURS:
                        _HEARTBEAT_PUSHED_HOURS.add(hr)
                        emo   = emotion if emotion else get_market_emotion()
                        zt_c  = emo.get("zt_count", 0)
                        dt_c  = emo.get("dt_count", 0)
                        emo_s = emo.get("emotion", "未知")
                        ms    = market.get("market_state", "")
                        il    = market.get("index_line", "")
                        # ★ v3.4：心跳加入今日已推信号的实时状态跟踪
                        top_track = ""
                        if all_day_signals:
                            rt_snap = get_realtime_quotes()
                            rt_map  = {}
                            if not rt_snap.empty:
                                for _, _r in rt_snap.iterrows():
                                    rt_map[str(_r.get("code","")).zfill(6)] = _r
                            track_lines = []
                            for _s in sorted(all_day_signals, key=lambda x: x.score, reverse=True)[:3]:
                                _rt = rt_map.get(_s.code)
                                if _rt is not None:
                                    _now_p  = float(_rt.get("price", _s.price) or _s.price)
                                    _chg    = (_now_p - _s.entry_price) / _s.entry_price if _s.entry_price > 0 else 0
                                    _icon   = "📈" if _chg > 0 else ("📉" if _chg < 0 else "➡️")
                                    track_lines.append(
                                        f"  {_icon}{_s.name}({_s.strategy}) 入场{_s.entry_price:.2f}→现价{_now_p:.2f}({_chg:+.1%})"
                                    )
                            if track_lines:
                                top_track = "\n\n**今日信号跟踪（TOP3）**：\n" + "\n".join(track_lines)
                        send_wx(
                            f"💓打板系统在线({hr}:00)",
                            f"**{hr}:00 系统心跳 | 正常运行中**\n\n"
                            f"大盘：{ms} | {il}\n"
                            f"市场情绪：{emo_s}（涨停{zt_c}家/跌停{dt_c}家）\n\n"
                            f"今日已推送信号：{len(PUSHED_TODAY)}只"
                            f"{top_track}\n\n"
                            f"持续监控中，有信号立即推送..."
                        )
                # ★ v5.0 sleep 自适应：T字板10s，竞价阶段45s，盘中60s（从90s压缩）
                if _has_tboard_this_round:
                    time.sleep(10)   # T字板分秒必争
                elif phase == "pre_auction":
                    time.sleep(45)
                else:
                    time.sleep(60)   # 普通盘中从90s压缩到60s

        except KeyboardInterrupt:
            log.info("手动中断，退出")
            break
        except Exception as e:
            log.error(f"主循环异常（已捕获，继续运行）: {e}", exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="打板策略选股系统")
    parser.add_argument("--test-push", action="store_true",
                        help="发送一条测试推送验证 SENDKEY 是否配置正确")
    args = parser.parse_args()

    if args.test_push:
        now_str = beijing_now().strftime("%Y-%m-%d %H:%M:%S")
        ok = send_wx(
            "📈打板系统 · 推送测试",
            f"## 推送测试\n\n"
            f"时间：{now_str}\n\n"
            f"如果你看到这条消息，说明 Server酱推送配置正确。\n\n"
            f"**监控策略**：首板 / 连板 / 竞价打板 / 跌停反包\n\n"
            f"> 今日无有效打板信号（测试消息）",
        )
        if ok:
            print("✅ 测试推送成功")
        else:
            print("❌ 测试推送失败，请检查 SENDKEY 配置")
    else:
        main()
