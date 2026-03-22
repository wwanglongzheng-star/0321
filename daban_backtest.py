"""
打板策略 · 历史回测框架（v9.1 竞价极致 + 龙头锁定 + 连板梯队版）
================================================
策略说明：
  1. 首板策略  —— 当日出现首次涨停，次日竞价买入（假设涨停价附近成交）
  2. 连板策略  —— 连续N日涨停，次日竞价买入（仅2~3连板，≥4连板次日折价）
  3. 竞价打板  —— 集合竞价高开7%~9.9%，以昨日收盘*1.10尝试打板
  4. 跌停反包  —— 昨日跌停，今日开盘反包做多

v4.0 优化说明：
  ★ 资金管理：改为固定仓位模拟（每笔固定10%仓位，最多同时持仓10只）
               净值曲线基于实际资金变化，最大回撤计算更贴近实盘
  ★ 并发持仓：同日信号多时，按量比+封板强度排序，只选最优N只入场
  ★ 入场滑点：从0.2%提高到0.8%，更贴近实际竞价打板成本
  ★ 首板出场：simulate_exit 起点修正（从i+2→i+1，避免漏掉入场当天价格变动）
  ★ 趋势止损：MA5改为MA10，持仓≥5天才生效，减少早期噪音止损
  ★ 止损优先级：理清ATR止损/时间止损优先级，同日触发取更优出场价
  ★ 股票池扩大：改为中证1000+创业板指+科创50（移除大市值沪深300）
  ★ 分析维度：增加按年度/月度统计、连续亏损分析、行业分布统计

v5.0 反收割增强说明（核心升级，防止被高频/做市商策略猎杀）：
  ★ 止损位随机化：每笔交易止损线在基准值基础上叠加随机扰动（±STOP_JITTER_RANGE），
               避免全市场所有策略使用者的止损堆积在同一价格，被主力精准触发后拉回。
  ★ 止盈点反整数化：T1/T2止盈触发点偏离整数位（±PROFIT_JITTER_RANGE），
               规避做市商在整数关口（+10%/+20%）密集挂卖单的陷阱。
  ★ 入场保护-竞价虚假挂单检测：通过检测竞价区间内历史"拉高撤单"特征（近N日
               高开但当日大幅回落的比率），过滤竞价钓鱼盘信号，
               默认启用（FILTER_FAKE_AUCTION=True，阈值FAKE_AUCTION_THRESH=0.40）。
  ★ 信号执行随机延迟（可选）：对同等质量的信号引入±1日的随机执行延迟，
               降低策略行为的可预测性，规避被跟单/反向跟踪。
               默认关闭（SIGNAL_DELAY_RANDOM=False），实盘使用时按需开启。
  ★ 炸板后再封陷阱检测：检测近N日内涨停后开板再封的次数，该形态是主力
               反复吸引追板资金后砸盘的典型手法，超过阈值直接跳过。
               （FILTER_RECLOSE_TRAP=True，RECLOSE_TRAP_MAX=2）

v6.0 智能决策增强说明（本次升级，让盘中决策更正确更灵活胜率更高）：
  ★ 量价背离过滤：新增涨停日量价背离字段（vol_price_diverge），
               真正的主力拉升必须放量，涨停日成交量低于20日均量80%为假突破，
               默认过滤（FILTER_VOL_PRICE_DIVERGE=True）。
  ★ 情绪综合评分系统（0~100分）：将封板质量(30分)+量比(25分)+近5日动量(20分)
               +放量质量(15分)+ATR弹性(10分)合成signal_score字段，
               只做达到评分门槛的高质量信号（首板≥55，竞价≥50，反包≥45），
               因子分析报告中会自动验证评分系统有效性。
  ★ 动态持仓天数：根据signal_score自动调整持仓上限：
               高分(≥70)信号延长至10天（让强势利润奔跑），
               低分(≤45)信号缩短至4天（弱信号快进快出减少损耗），
               中间区间保持默认7天。
  ★ T2追踪止盈量能自适应：T2之后，根据当日成交量相对5日均量的比值，
               动态调整回撤容忍率（量萎缩→收紧容忍→更快锁利润），
               公式：容忍率 = T2_TRAIL_TOLERANCE × (量比^0.5)，
               区间限制在[10%~50%]，避免过于激进或保守。
  ★ 跌停反包质量过滤升级：区分"恐慌性单次跌停"与"基本面持续下跌"：
               连续跌停>1次不做（利空持续），跌停前5日跌幅>15%不做（已在下行通道），
               开盘修复门槛从3%提高至4%（更严格的情绪确认）。

v8.1 TradingAgents-CN增强版说明（参考 hsliuping/TradingAgents-CN 有价值模块）：
  ★ 文件缓存层（DataCache）：历史数据本地缓存，重复回测不重复拉取，速度提升3~5x
               参考：TradingAgents-CN 多级缓存架构（文件缓存 → MongoDB → Redis）
               本系统轻量化实现：pickle序列化，按股票代码+日期范围hash命名，默认缓存7天
  ★ 真实盘中量比接口：新增 fetch_realtime_vol_ratio() 使用 ak.stock_zh_a_spot_em()
               替代日线5日均量近似值（当日量/均量），仅实盘使用，回测仍用日线近似
               参考：TradingAgents-CN AKShare Provider get_batch_stock_quotes()
  ★ 财务质量过滤（ENABLE_FINANCIAL_FILTER）：接入 akshare 财务数据
               过滤PE<0（亏损）、PE>200（泡沫）、ROE<5%（盈利能力弱）的标的
               参考：TradingAgents-CN provider.get_financial_data(symbol)
               缓存财务数据（缓存7天），不影响回测速度
  ★ 新闻情绪因子（score_news，满分10分）：接入 ak.stock_news_em() 获取股票新闻
               近N条新闻标题中含正面关键词加分，含负面关键词扣分
               作为 signal_score 第6维度，总分扩展至110分（原100分）
               回测中默认关闭（ENABLE_NEWS_SCORE=False），实盘按需开启

v8.3 数据驱动精准优化说明（基于真实回测结果针对性修正）：
  ★ 首板评分门槛提高至72分：回测显示首板胜率仅23%，196笔均收益-2.00%，
               将首板门槛从70提高至72，进一步过滤中等质量首板，牺牲笔数换胜率。

v8.5 数据驱动精准优化说明（基于v8.4真实回测结果继续修正）：
  ★ 月份仓位参数重校准（v8.4回测新数据显示旧配置存在反向操作）：
               8月实际胜率30.4%/-0.70%（各月份胜率最高之一），不应列为弱势月 → 移出
               10月实际胜率30.7%/-0.74%（各月份胜率最高），不应列为弱势月 → 移出
               12月实际胜率20.8%/-2.13%（与3月并列最差），应加入弱势月 → 新增
               调整后弱势月份：[3, 6, 7, 11, 12]
  ★ 首板策略暂停（Kelly公式验证无效）：
               胜率21.8% / 均收益-2.16% / 盈亏比=9.27/5.35=1.73x
               Kelly最优仓位 = 21.8% - 78.2%/1.73 = -23.4%（应空仓），策略无效
               将首板评分门槛临时提升至80分（超高门槛=近乎暂停，等待市场条件改善）
  ★ 洗盘抄底策略评分门槛提升：
               胜率25.9%/-1.27%，11253笔是整体亏损的主要来源（贡献约-143%亏损）
               WASHOUT_MIN_SCORE 从50提升至58（减少低分洗盘信号，牺牲量换质）
  ★ timeout止损优化（timeout占33.5%是最大亏损来源）：
               默认持仓天数从7天缩短至5天（弱势信号缩短至3天）
               第2天强制止损触发阈值从-2.5%收紧至-2.0%（更早截断亏损）
               TIME_TIGHTEN_LOSS从-3.0%收紧至-2.5%（时间止损线更紧）
  ★ 9月强势月份修正：
               v8.4回测显示9月实际胜率29.0%/-0.29%（并非之前预计的62.2%），
               仍略好于均值，维持强势月加仓但系数从1.5降至1.2
  ★ 回调缩量策略大幅收紧（胜率18.6%/-2.87%，最差策略需彻底收紧）：
               ① 回调幅度上限从8%收紧至7%（>7%往往是出货不是洗盘）
               ② 缩量标准从50%收紧至45%（更纯粹的缩量洗盘特征）
               ③ 评分门槛从55提高至60（提高信号质量准入）
               ④ 新增均线多头排列过滤（MA5>MA10>MA20），确保大趋势向上
  ★ 6月加入弱势月份仓位控制：回测胜率25.5%/-0.59%，接近3月弱势水平，
               6月加入弱势月份名单，仓位降至5%。
  ★ 9月强势月份加仓（新增机制）：回测9月胜率62.2%/+3.85%，远超其他月份，
               9月仓位扩大至15%（×1.5倍），充分利用季节性强势行情。

v9.2 数据驱动精准优化（基于v9.1回测结果 · 修复四大亏损来源）：
  ★ 修复①：gap_abort 贡献52.4%总亏损 → AUCTION_GAP_ABORT -2%→-1.5%，ENTRY_MAX_GAP_DOWN -3%→-2%
  ★ 修复②：竞价-连板-极限 胜率47.1% → PREV_CLOSE_POS 0.90→0.93，PREV_VOL_RATIO 1.2→1.3
  ★ 修复③：1月胜率40%/2月胜率50% → WEAK_MONTHS 新增 1月、2月（仓位×0.5）
  ★ 修复④：竞价-首板-强势 胜率61.8%低于温和75% → 强势档量比下限：连板1.5→2.0，首板2.0→2.5

v9.1 深度进化说明（基于v9.0架构 · 精准极致 · 从盈利到高收益）：
  ★★★ 核心战略：竞价入场精准化 + 连板梯队识别 + 日内出场革命 ★★★

  ★ 进化①：竞价入场新增"封板力度"量化（close_pos × 收盘相对昨涨停位置）
    - 竞价高开后，当日收盘close_pos > 0.90（接近全天最高）= 封板稳固 → 续持信号强
    - close_pos < 0.50（收在中下部）= 封板不稳 → 次日竞价降低评分
    - 入场评分新增 score_seal（封板稳固度评分）权重15分，总分上升至125分

  ★ 进化②：连板梯队识别（决定涨停后次日竞价选哪一档）
    - 2连板（zt_streak=2）：主流行情期胜率>60%，优先重仓
    - 3连板（zt_streak=3）：开始折价风险，仓位降至7%（×0.7）
    - 4+连板（zt_streak≥4）：强制降档，若竞价涨幅≤5%则不入场（折价概率>60%）
    - 首板竞价（前一天无涨停背景）：要求量比最高，情绪需至少有20家涨停

  ★ 进化③：竞价"次日开盘方向确认"出场逻辑
    - 竞价模式入场后，持有到次日开盘：
      * 次日竞价继续高开≥3% → 保持持仓，等待连板
      * 次日竞价平开[-2%,+3%) → 用T+1 close_pos判断（>0.85留，≤0.85出）
      * 次日竞价低开<-2% → 次日开盘止损出场（此时已有断板止损兜底）

  ★ 进化④：分时量比过滤（解决"竞价量多、盘中量少=陷阱"问题）
    - 入场当天的成交量是否能维持在5日均量的80%以上
    - 代理指标：当日实际成交量 / 5日均量（回测用日线近似）
    - 阈值：INTRADAY_VOL_SUSTAIN_RATIO = 0.80（低于此值说明盘中无人接盘）

  ★ 进化⑤：竞价-连板-极限入场评分升级（样本少但质量超高）
    - 要求前一日close_pos > 0.90（封板极其稳固，说明强庄股）
    - 要求前一日成交量 > 10日均量 × 1.2（真实放量，非主力自拉）
    - 连板天数为2~3天（4+连板极限高开 = 高风险，直接跳过）

  ★ 进化⑥：仓位管理精细化
    - 最强信号（竞价-首板-温和 + 评分≥75 + 大盘热度强）：仓位×1.5（15%）
    - 标准信号：仓位×1.0（10%）
    - 次优信号（反包 / 连板-极限 但评分60~74）：仓位×0.8（8%）
    - 大盘冷淡但信号刚好在阈值线上：仓位×0.5（5%）

v9.0 重大重构说明（基于v8.5回测结果 · 大刀阔斧 · 从亏损到盈利）：
  ★★★ 核心战略转变：只打竞价，放弃其他全部策略 ★★★
  回测铁证：
    - 竞价(696笔)：45.7%胜率/+0.90%  ← 唯一整体盈利策略
    - 竞价-首板-温和(294笔)：56.8%/+2.37%  ← 最优子策略
    - 竞价-首板-强势(79笔)：41.8%/+1.34%  ← 次优子策略
    - 竞价-连板-极限(15笔)：60.0%/+5.27%  ← 超强子策略(样本少但方向明确)
    - 首板(197笔)：21.8%/-2.16%  → Kelly=-23%，完全停用
    - 洗盘抄底(11253笔)：25.9%/-1.27%  → 贡献整体亏损的80%，完全停用
    - 回调缩量(4笔)：25.0%/-2.80%  → 样本无效，停用

  ★ 变革①：策略白名单（仅保留盈利子策略）
    仅运行竞价策略的以下3个盈利子策略：
      - 竞价-首板-温和 (56.8% / +2.37%)
      - 竞价-首板-强势 (41.8% / +1.34%)
      - 竞价-连板-极限 (60.0% / +5.27%)
    停用：竞价-连板-温和(-0.73%)、竞价-连板-强势(-1.23%)
    停用：首板策略、洗盘抄底、回调缩量
    保留：跌停反包（独立运行，后续验证）

  ★ 变革②：止盈机制革命（解决+8.22%均盈利未能留住的问题）
    旧模式：固定5天时间止损（超33%靠timeout出场）
    新模式：涨停日延长持仓 + 基于连板数自适应持仓天数
      - 竞价入场后，若次日出现涨停，持仓天数重置至8天（继续等连板）
      - 连续涨停期间持仓无限延长（最多20天），直到涨停破板后出
      - "趋势跟踪模式"：涨停后如次日回调超2%，当天收盘出场

  ★ 变革③：竞价龙头识别增强
    新增"近3日涨停家数比例"（热度代理）：涨停板数 > 同期市场均值1.5倍方可入场
    新增"板块轮动热度"：要求同行业至少有1只股今日涨停（热点扩散效应）
    新增"涨停持续度"：涨停后封板时间越长（用close_pos代理），优先级越高

  ★ 变革④：大盘情绪硬过滤
    新增"市场涨停密度"过滤：当日全市场涨停家数 < MARKET_ZT_MIN_COUNT(20) 时
    暂停所有竞价信号（大盘弱势=竞价高开≠真正热度，是陷阱）

  ★ 变革⑤：止损升级（更小的损失）
    竞价入场的止损从ATR动态改为固定-3%（竞价信号质量高，不需要大空间）
    入场当天低开>2%即止损（比之前的3%更严，竞价信号不能低开）
    新增"涨停后断板止损"：涨停次日低于涨停价5%以上，当日收盘止损

v8.4 灵活仓位与出场优化说明（面向实盘更智能化改造）：
  ★ 赚钱效应仓位自适应（EMOTION_SCORE模块）：
               用近N日市场涨停信号数量代理当日赚钱效应强弱
               情绪热（信号多，近5日均值>75分位）→ 仓位×1.5（加仓）
               情绪冷（信号少，近5日均值<25分位）→ 仓位×0.5（减仓）
               中性区间 → 正常仓位×1.0
               实盘可接入 ak.stock_zt_pool_em() 获取真实涨停家数，替代代理指标
  ★ 量比参数分策略差异化：
               竞价/洗盘抄底/日内抄底：量比上限放宽至8.0（高量比=强度信号，竞价胜率更高）
               首板/反包/回调缩量：保持5.0上限（高量比首板出货特征明显）
  ★ 第2天强制止损（DAY2_FORCE_STOP）：
               第2天收盘浮亏 > -2.5% 且非缩量（缩量=洗盘豁免）→ 第3天开盘强制出场
               解决 time_stop 占24.8%问题：截断"坐穿"走势，小亏出场而非等到时间止损
  ★ 强势延长持仓（EXTEND_HOLD_ON_STRENGTH）：
               连续2天 close_pos > 0.85（收盘接近当日最高价）→ 持仓天数每次+1
               最多额外延长3天（强势档最多持13天），让强势趋势充分奔跑
               出现弱势日则重置计数，不盲目持股

v8.0 策略扩展+评分优化说明（本次升级，引入外部高星仓库策略思路）：
  ★ 新增策略5：涨停回调缩量再启动策略（参考 wxhui1024/Quantitative-Trading-System）
               核心逻辑：近N日内出现过涨停，随后回调期间成交量显著缩量（<涨停日×50%），
               说明大户未急于出货，短线有补涨/再度启动机会。
               入场条件：近3~10日涨停 + 回调幅度3%~8% + 缩量<涨停日量×50% + 缩量后企稳
               市值范围：50亿~500亿，兼顾流动性与成长性
               止损：跌破回调低点 / ATR动态止损；止盈：复用原止盈体系
  ★ 评分系统升级：因子Z-score标准化（参考 nekolovepepper 连板追涨策略因子工程）
               原始因子值先Rank排序再Z标准化，消除量纲差异，评分分层更线性更准确
  ★ 弱势月份扩展：7/8月也加入仓位减半（v7.0仅3/10/11月，v8.0扩展至3/7/8/10/11月）
               回测显示7月胜率34.5%(-1.04%)、8月胜率35.1%(-0.84%)，显著偏弱

v7.0 数据驱动精准优化说明（本次升级，基于真实回测结果针对性修正）：
  ★ 首板评分门槛提高至70分：回测显示高分(≥70)胜率46.9%，中高分(55~70)仅25.7%，
               将首板门槛从55提高至70，直接砍掉低质量信号，牺牲笔数换胜率。
  ★ 首板量比上限从2.5收窄至2.2：高量比首板多为主力出货拉停特征，
               回测数据验证1.5~2.2区间表现最优，进一步精准化入场条件。
  ★ 弱势月份仓位减半（3/10/11月）：回测显示3月胜率25.8%(-2.32%)、
               10月胜率24.7%(-2.17%)、11月胜率仅15.6%(-3.25%)，
               在弱势月份仓位降至5%（原10%），同等亏损笔数下净值损失减半，
               同时不完全退出市场，保留捕捉偶发强势行情的机会。



多因子过滤（来自公开研究数据）：
  ★ 市值最优区间：10~100亿流通市值，胜率最高
  ★ 量比最优区间：1.5~3.0（日线近似）
  ★ 换手率最优区间：首板5%~15%；连板<15%
  ★ 连板天数衰减：2连板胜率≈50%，≥4连板次日折价概率>60%
  ★ 封板时间评分：越早封板胜率越高

★ 假涨停过滤（回测验证有效性）：
  - 炸板过滤：当日最高触及涨停，但收盘未涨停 → 不入场
  - 次日缩量过滤：次日成交量 < 涨停日成交量×0.5 → 跳板风险高
  - 量比异常：量比>5直接跳过（爆量出货特征）

★ 股性识别过滤（回测验证有效性）：
  - 一字板过滤：当日开盘即封板（开盘=最高=涨停价），实盘无法买入 → 不统计
  - 日均成交额过滤：近20日日均成交额<3000万的流动性差股票 → 不统计
  - 一字比例分析：历史涨停中一字板比例高的股票，实际胜率与理论差距大

出场逻辑：
  - ATR动态止损（T1前有效）
  - 时间止损：持仓>2天未盈利，止损收紧至-3%
  - T1止盈：+10% 触发跟踪止盈
  - T2止盈：+20% 触发追踪止盈（不立即出场，让利润奔跑）
  - MA10趋势止损：持仓≥5天生效
  - 超时：持满 HOLD_DAYS 交易日

运行方式：
  python daban_backtest.py --codes 600519 000858 002594 300750 601318 000001
  python daban_backtest.py --strategy 首板 连板
  python daban_backtest.py --codes 600519 --plot
  python daban_backtest.py --factor-analysis
"""

import pandas as pd
import numpy as np
import argparse
import sys
import os
import csv
import datetime
import warnings
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

warnings.filterwarnings("ignore")

# ★ Windows 控制台 UTF-8 修复（防止 emoji 打印崩溃）
if sys.platform == "win32":
    import io
    if hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "buffer"):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

try:
    import baostock as bs
    bs.login()
    USE_BS = True
except Exception:
    USE_BS = False

# ★ v3.4修复：baostock是单连接协议，多线程并发调用会导致数据交叉
# 用线程锁保证对baostock的请求串行执行
import threading
_BS_LOCK = threading.Lock()

try:
    import akshare as ak
    USE_AK = True
except ImportError:
    USE_AK = False

USE_YF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(stream=sys.stderr)],
)
log = logging.getLogger("daban_bt")

# ================================================================
# 回测参数
# ================================================================
STOP_LOSS    = -0.045  # 初始止损 -4.5%（后续被 ATR 动态止损替代）
PROFIT_T1    = 0.10    # 第一止盈 +10%（打板策略利润空间大）
PROFIT_T2    = 0.20    # 第二止盈 +20%（T2后不直接出，改启动更宽松跟踪止盈）
HOLD_DAYS    = 5       # ★v8.5: 从7天缩短至5天（timeout占33.5%，超长持仓拖累收益）
# ★ v4.0：滑点从 0.2% 提高到 0.8%，更贴近实际竞价打板成本
SLIP         = 0.008   # 滑点（竞价打板实际溢价约0.5%~1%，取0.8%）
COMMISSION   = 0.0003  # 万三手续费

# ★ v4.0：资金管理参数
POSITION_SIZE   = 0.10   # 每笔固定仓位比例（10%）
MAX_POSITIONS   = 10     # 最大同时持仓数量
INITIAL_CAPITAL = 1_000_000.0  # 初始资金（元）

# 打板触发条件（用于历史数据模拟）
ZT_THRESH      = 0.095   # 涨幅 ≥ 9.5% 视为涨停（含ST的5%涨停不计）
DT_THRESH      = -0.095  # 跌幅 ≤ -9.5% 视为跌停
# 竞价高开分三档（细分策略，每档独立统计）
# 温和高开：+2%~+5%，情绪溢价低，空间大，量比要求高
AUCTION_MILD_LOW   = 0.02   # 温和高开下限
AUCTION_MILD_HIGH  = 0.05   # 温和高开上限
# 强势高开：+5%~+7%，主流竞价打板区间，均衡空间与强度
AUCTION_STRONG_LOW  = 0.05  # 强势高开下限
AUCTION_STRONG_HIGH = 0.07  # 强势高开上限
# 极限高开：+7%~+9.9%，接近涨停，空间极窄，仅做昨日涨停后
AUCTION_LIMIT_LOW   = 0.07  # 极限高开下限
AUCTION_LIMIT_HIGH  = 0.099 # 极限高开上限（<10%，未直接封板）
MIN_ZT_DAYS    = 2       # 连板策略：最少连板天数（研究：2连板胜率最高）
MAX_ZT_DAYS    = 3       # 连板策略：最多连板天数（≥4连板次日折价>60%，不追）

STRATEGIES_ALL = ["首板", "竞价", "反包", "回调缩量", "洗盘抄底", "日内抄底"]   # v8.4: 赚钱效应仓位自适应 + 量比分策略差异化 + 第2天强制止损

# ================================================================
# ★ v9.0 策略白名单控制（只跑盈利子策略）
# ================================================================
# 根据v8.5回测铁证：洗盘抄底/首板/回调缩量全部是负期望值策略
# v9.0只跑竞价策略的盈利子策略 + 保留跌停反包作备选

# 竞价盈利子策略白名单（仅运行此列表内的子策略）
AUCTION_WHITELIST_ENABLE  = True     # 是否启用竞价子策略白名单过滤
AUCTION_WHITELIST = [
    "竞价-首板-温和",    # 56.8% / +2.37% ← 最优
    "竞价-首板-强势",    # 41.8% / +1.34% ← 次优
    "竞价-连板-极限",    # 60.0% / +5.27% ← 超强（虽样本少但方向明确）
    # "竞价-连板-温和",  # 34.6% / -0.73% ← 停用
    # "竞价-连板-强势",  # 38.7% / -1.23% ← 停用
]

# 策略开关（v9.0: 停用所有负期望值策略）
STRATEGY_ENABLE_FIRST_BOARD    = False   # ★v9.0: 首板Kelly=-23%，完全停用
STRATEGY_ENABLE_WASHOUT        = False   # ★v9.0: 洗盘抄底是亏损主力，完全停用
STRATEGY_ENABLE_ZT_PULLBACK    = False   # ★v9.0: 回调缩量样本太少且为负收益，停用
STRATEGY_ENABLE_INTRADAY       = False   # ★v9.0: 日内抄底日线近似误差大，停用
STRATEGY_ENABLE_AUCTION        = True    # ★v9.0: 竞价是唯一盈利策略，保留并深化
STRATEGY_ENABLE_DT_RECOVER     = True    # 跌停反包保留（独立验证）

# ── ★ v9.0 竞价止损升级 ──────────────────────────────────────────────────
# 竞价信号质量高，一旦低开就是陷阱，不需要大的止损空间
AUCTION_STOP_LOSS       = -0.030   # ★v9.0: 竞价专用止损-3%（比通用-4.5%更紧）
AUCTION_GAP_ABORT       = -0.015   # ★v9.2: 从-2%收严至-1.5%（回测gap_abort贡献52.4%亏损，需更早截断）
AUCTION_ZT_BREAK_STOP   = -0.050   # ★v9.0: 涨停后次日跌破涨停价5%立即止损（断板止损）

# ── ★ v9.0 涨停跟踪延长持仓机制 ─────────────────────────────────────────
# 思路：竞价入场后，若次日又涨停=强势龙头，持仓天数重置/延长，跟随连板
AUCTION_ZT_TRAIL_ENABLE        = True    # 是否启用涨停跟踪延长持仓
AUCTION_ZT_TRAIL_RESET_DAYS    = 5       # 每次涨停后持仓天数重置为N天（继续等下一板）
AUCTION_ZT_TRAIL_MAX_DAYS      = 20      # 最多持仓20天（跟着连板走，必要时强制出场）

# ── ★ v9.0 大盘情绪硬过滤（市场涨停密度）────────────────────────────────
# 大盘弱=涨停家数少→竞价高开是陷阱（机构砸盘），不入场
MARKET_ZT_FILTER_ENABLE        = True    # 是否启用大盘涨停密度过滤
MARKET_ZT_MIN_COUNT            = 20      # 当日全市场涨停家数<20则暂停所有信号
# 回测中用代理指标：当日涨停占比（涨停股/总股票数）
MARKET_ZT_MIN_RATIO            = 0.008   # 涨停家数/总股票数 < 0.8% 认为市场冷淡

# ── ★ v9.0 热点扩散过滤（板块龙头确认）──────────────────────────────────
# 要求：竞价信号股所在行业今日至少还有1只股涨停（龙头扩散/板块联动）
# 回测中近似：用"当日是否出现过涨停且非孤立"的全局信号数量判断
SECTOR_HEAT_FILTER_ENABLE      = True    # 是否启用板块热度过滤（代理版）
SECTOR_HEAT_MIN_SIGNALS        = 3       # 当日全市场涨停信号数量 ≥ 3 才允许入场

# ================================================================
# ★ v9.1 精准化参数（连板梯队 + 封板力度 + 分时量比）
# ================================================================

# ── ★ v9.1 连板梯队仓位差异化 ──────────────────────────────────────────
# 思路：不同连板天数胜率差异显著，仓位应差异化
AUCTION_ZT_STREAK_POS_ENABLE   = True    # 是否启用连板梯队仓位差异化
AUCTION_ZT_2_POS_RATIO         = 1.5    # 2连板竞价：胜率>60%，重仓×1.5
AUCTION_ZT_3_POS_RATIO         = 0.7    # 3连板竞价：折价风险开始，降仓×0.7
AUCTION_ZT_4PLUS_ENABLE        = False   # 4+连板竞价是否允许入场（折价概率>60%）
AUCTION_ZT_4PLUS_MAX_TIER      = "强势" # 4+连板仅允许强势高开（极限/温和不做）

# ── ★ v9.1 封板力度评分（prev_close_pos量化封板稳固度）──────────────────
# 思路：涨停后次日竞价，昨日封板越牢固 → 今日续板可能性越高
SEAL_SCORE_ENABLE              = True    # 是否启用封板力度评分
SEAL_SCORE_STRONG_THRESH       = 0.90   # close_pos > 此值 = 封板极稳（加分）
SEAL_SCORE_WEAK_THRESH         = 0.60   # close_pos < 此值 = 封板不稳（减分/跳过）
SEAL_SCORE_STRICT_FILTER       = True    # True=封板不稳(close_pos<0.60)直接跳过竞价-连板

# ── ★ v9.1 分时量比持续性过滤 ────────────────────────────────────────────
# 思路：竞价开放后到收盘，总成交量能否维持在均量水平 = 资金是否真实介入
# 回测近似：当日总量/5日均量（日线数据）
INTRADAY_VOL_SUSTAIN_ENABLE    = True    # 是否启用分时量比持续性过滤
INTRADAY_VOL_SUSTAIN_RATIO     = 0.75   # 当日量/5日均量 < 此值 = 盘中无人接盘，跳过
# （回测用日线近似，实盘可接入分钟线计算10:00前成交量占比）

# ── ★ v9.1 次日竞价方向确认出场 ─────────────────────────────────────────
# 思路：持仓次日开盘方向是否延续→决定是否继续持有
# 已由断板止损（AUCTION_ZT_BREAK_STOP）和 gap_abort 兜底
# 额外新增：次日开盘微弱高开[+0%~+3%)时，根据close_pos判断是否出场
AUCTION_NEXT_DAY_CONFIRM_ENABLE = True   # 是否启用次日竞价方向确认
AUCTION_NEXT_DAY_WEAK_OPEN_MAX  = 0.030 # 次日开盘 < +3% 认为"弱开"
AUCTION_NEXT_DAY_CLOSE_POS_MIN  = 0.80  # 弱开时 close_pos < 此值 → 收盘出场（不等下一天）

# ── ★ v9.1 竞价-连板-极限进化（超强但样本少，需要更严格入场条件）──────
# 思路：样本少+高胜率=稀缺机会，但需确保信号质量，不能放松标准
AUCTION_EXTREME_PREV_CLOSE_POS  = 0.93  # ★v9.2: 从0.90提高至0.93（连板极限47.1%胜率→提高封板质量门槛）
AUCTION_EXTREME_PREV_VOL_RATIO  = 1.3   # ★v9.2: 从1.2提高至1.3（要求更明确的放量，过滤缩量涨停的极限信号）
AUCTION_EXTREME_MAX_STREAK      = 3     # 最多允许连板天数（4+连板极限高开=强烈警惕）

# ── ★ v9.1 大盘强势时放大竞价仓位 ──────────────────────────────────────
# 思路：大盘热度极高时（涨停占比>2%），竞价胜率大幅提升，适当放大仓位
MARKET_HOT_BOOST_ENABLE        = True    # 大盘热时是否放大竞价仓位
MARKET_HOT_BOOST_RATIO_THRESH  = 0.020  # 大盘涨停占比>2% = 超热行情
MARKET_HOT_BOOST_POS_RATIO     = 1.5    # 超热行情仓位×1.5（与连板梯队叠加最高2.0x）
MARKET_HOT_BOOST_MAX_RATIO     = 2.0    # 仓位放大上限（不超过2.0x，防止过度集中）

# ── 多因子过滤阈值（基于回测优化结论） ──────────────────────────────────
# 市值区间（亿）：最优10~100亿，超出则折扣评分
MKT_CAP_OPT_LOW  = 10    # 亿
MKT_CAP_OPT_HIGH = 100   # 亿
MKT_CAP_ABS_LOW  = 5     # 低于此直接过滤（ST/垃圾股）
MKT_CAP_ABS_HIGH = 300   # 高于此直接过滤（大市值难封板）

# 量比区间：回测门槛从3.0恢复至1.5以扩大样本量，因子分析中可观察各量比段差异
# （若样本<500笔，门槛越高越难得到可信的分组统计）
VOL_RATIO_LOW    = 1.5   # 回测用1.5（实盘selector.py仍用3.0），确保足够样本
# ★ v8.4：量比上限分策略差异化（回测数据：量比>3高胜率47.5%主要来自竞价策略）
# 竞价/洗盘抄底/日内抄底：放宽至8.0（高量比在这些策略中是强度信号，反而利好）
# 首板/反包/回调缩量：保留5.0上限（高量比首板出货特征明显）
VOL_RATIO_HIGH          = 5.0   # 默认上限（首板/反包/回调缩量）
VOL_RATIO_HIGH_AUCTION  = 8.0   # ★v8.4：竞价/洗盘/日内抄底策略量比上限放宽至8.0

# 换手率：首板最优5%~15%，连板<15%
TURNOVER_FIRST_LOW  = 5.0
TURNOVER_FIRST_HIGH = 15.0
TURNOVER_CONNECT    = 15.0  # 连板换手率上限

# 是否启用多因子过滤（默认开启）
ENABLE_FACTOR_FILTER = True

# ── 假涨停过滤开关 ────────────────────────────────────────────────────────
# 当日炸板检测：涨停日最高触及涨停价但收盘未达涨停 → 不入场
FILTER_BOMB_DAY      = True   # 开启当日炸板过滤
# 次日缩量检测：次日成交量 < 涨停日×FILTER_VOL_SHRINK → 跳板风险（软过滤，加扣分）
FILTER_VOL_SHRINK    = 0.5    # 次日量能低于涨停日50%认为缩量
# 量比异常爆量：>此值认为异常（出货拉停特征）
# ★ v8.4：统一用8.0（竞价类已允许高量比，过滤层只剔除极端爆量>8）
FILTER_VOL_RATIO_MAX = 8.0
# 近N日内炸板次数超出上限 → 直接跳过
FILTER_BOMB_HISTORY_DAYS  = 30
FILTER_BOMB_HISTORY_MAX   = 2   # 恢复至2（原1太严，导致样本量不足）

# ── 股性识别过滤开关 ──────────────────────────────────────────────────────
# 一字板过滤：当日开盘即封板（无法买入），不统计为有效交易
FILTER_YIZI_BOARD    = True   # 开启一字板过滤（强烈建议开启，一字板实盘根本打不进去）
# 日均成交额下限（元）：低于此值流动性太差，排除
FILTER_MIN_AVG_AMOUNT = 30_000_000   # 3000万
# 一字比例阈值：近30日涨停中一字板超过此比例，认为该股买不进去
FILTER_MAX_YIZI_RATIO = 0.70

# ── 大盘环境过滤 ──────────────────────────────────────────────────────────
# 利用个股自身MA判断市场环境（Colab无法拉指数，用持仓股均值替代）
# 当大盘处于下行趋势时，打板胜率大幅下降，应暂停入场
MARKET_FILTER_ENABLE = True   # 开启大盘环境过滤
# 个股自身：收盘低于20日均线时，认为处于弱势，首板/连板不入场
FILTER_STOCK_ABOVE_MA20 = True
# 个股自身：近5日涨幅（动量）：5日内累计跌幅超过此值，说明大趋势向下，不追
FILTER_MOMENTUM_5D    = -0.08   # 5日累跌>8% 不追板

# ── 出场逻辑升级 ──────────────────────────────────────────────────────────
# 跟踪止盈：达到T1后，止损线上移至成本价（保本止损），不再固定止损
TRAIL_STOP_AFTER_T1  = True   # 触达T1后启动跟踪止盈
# ★ 优化④：时间止损从3天→2天触发（研究：2天内未盈利亏损概率上升至65%）
TIME_TIGHTEN_DAYS    = 2      # 持仓超过N天触发收紧（从3缩短至2天）
TIME_TIGHTEN_LOSS    = -0.025 # ★v8.5: 从-3.0%收紧至-2.5%（更早截断持续亏损）
# 次日低开保护：入场次日低开超过此幅度，直接止损出场（避免追高后被砸）
OPEN_GAP_ABORT       = -0.03  # 次日低开>3% 止损出场（仅入场第1天检查）

# ★ v8.4 新增：第2天强制止损机制
# 回测数据：time_stop占24.8%（243笔），说明大量交易入场后持续亏损拖过止损线
# 思路：第2天收盘仍处于亏损 且 浮亏幅度 > 此值 → 第3天开盘直接止损出场
# 效果：截断 "持续坐穿板凳" 的交易，避免小亏变大亏
DAY2_FORCE_STOP_ENABLE = True    # 是否启用第2天强制止损
DAY2_FORCE_STOP_LOSS   = -0.020  # ★v8.5: 从-2.5%收紧至-2.0%（更早截断亏损，减少timeout）
# 例外：若当天有大幅缩量（已在洗盘），给一天宽容，不强制止损
DAY2_FORCE_STOP_VOL_EXEMPT = True   # True=缩量时豁免（缩量说明主力未砸盘）
DAY2_FORCE_STOP_VOL_RATIO  = 0.6    # 成交量<均量×0.6时豁免强制止损

# ★ 优化⑤：T2（+20%）后不直接出场，改为启动宽松追踪止盈，让利润奔跑
# T2触发后，允许从T2高点最多回撤利润的30%才出场（而非立刻平仓）
T2_TRAIL_ENABLE      = True   # 开启T2后追踪止盈
T2_TRAIL_TOLERANCE   = 0.30   # T2后允许利润回撤比例（从最高点算）

# ★ v8.4 止盈延长参数（让利润真正奔跑）
# 强势股连续2日收盘接近最高价（close_pos>0.85），延长持仓天数1~2天
EXTEND_HOLD_ON_STRENGTH      = True   # 是否启用强势股持仓延长
EXTEND_HOLD_CONSECUTIVE_DAYS = 2      # 连续N天强势才触发延长
EXTEND_HOLD_CLOSE_POS_THRESH = 0.85   # close_pos > 此值认为当日强势
EXTEND_HOLD_MAX_EXTRA        = 3      # 最多额外延长3天（强势10天+3=最多13天）

# ── 智能跟踪止盈（分档 + 强弱信号自适应）────────────────────────────────
# 基础回撤容忍率：按盈利幅度分档，利润越多允许回撤比例越大
# 格式：[(最低盈利阈值, 最高盈利阈值, 利润回撤容忍比例)]
# 含义：当持仓盈利 X%，允许从最高盈利点回撤 Y% 的利润再出场
TRAIL_PROFIT_TIERS = [
    (0.00, 0.10, 1.00),   # 0~10%：全部保住，破盈亏平衡即出（保本）
    (0.10, 0.20, 0.40),   # 10~20%：允许回撤利润的40%（赚10%可回落到+6%出）
    (0.20, 0.30, 0.35),   # 20~30%：允许回撤利润的35%
    (0.30, 9.99, 0.30),   # >30%：  允许回撤利润的30%（强趋势多持，但锁住70%利润）
]
# 强势信号（涨停/大阳线）：当日close_pos超过此值，回撤容忍度额外放宽
TRAIL_STRONG_BONUS   =  0.05   # 强势当日回撤容忍 +5%（继续持有，让利润奔跑）
TRAIL_STRONG_THRESH  =  0.90   # close_pos > 0.90 认为强势（收盘接近最高价）
# 弱势信号：触发任一弱势信号，回撤容忍度收紧
TRAIL_WEAK_PENALTY   = -0.10   # 弱势信号收紧 10%（靠近止盈线更容易触发）
# 弱势信号1：缩量（当日成交量 < 5日均量×此比例）
TRAIL_VOL_SHRINK_RATIO = 0.50
# 弱势信号2：长上影（收盘位于日内区间下半部，即close_pos < 此值）
TRAIL_UPPER_SHADOW_THRESH = 0.40

# ── 入场保护 ──────────────────────────────────────────────────────────────
# 次日低开过多不追：入场当天开盘相对昨收（涨停价）跌幅超此值，说明情绪转弱
ENTRY_MAX_GAP_DOWN   = -0.02  # ★v9.2: 从-3%收严至-2%（与AUCTION_GAP_ABORT对齐，入场前过滤低开陷阱）

# ================================================================
# ★ v5.0 反收割参数（核心新增）
# ================================================================

# ── 止损位随机化 ───────────────────────────────────────────────────────────
# 目的：避免全市场同参数策略的止损聚集在同一价格，被主力精准触发后拉回
# 每笔交易止损线 = 基准止损 + uniform(-STOP_JITTER_RANGE, +STOP_JITTER_RANGE)
# 例：基准-4.5%，扰动±0.8% → 实际止损在-3.7%~-5.3%随机分布
STOP_JITTER_ENABLE   = True    # 是否启用止损随机化
STOP_JITTER_RANGE    = 0.008   # 止损随机扰动幅度（±0.8%）

# ── 止盈点反整数化 ──────────────────────────────────────────────────────────
# 目的：偏离整数止盈关口（+10%/+20%），避开做市商在整数位密集挂单
# T1/T2实际触发点 = 基准值 + uniform(-PROFIT_JITTER_RANGE, +PROFIT_JITTER_RANGE)
# 例：T1基准+10%，扰动±0.7% → 实际触发在+9.3%~+10.7%随机
PROFIT_JITTER_ENABLE = True    # 是否启用止盈点随机化
PROFIT_JITTER_RANGE  = 0.007   # 止盈随机扰动幅度（±0.7%）

# ── 竞价虚假挂单检测（反钓鱼盘）──────────────────────────────────────────
# 目的：检测"竞价高开但当日大幅回落"的历史模式，识别竞价钓鱼盘并过滤
# 判据：近FAKE_AUCTION_LOOKBACK日内，涨停信号股的"竞价高开但当日收盘跌回开盘价以下"比率
#       超过FAKE_AUCTION_THRESH则认定该股竞价区间存在系统性操控，过滤竞价策略信号
FILTER_FAKE_AUCTION       = True   # 是否开启竞价虚假挂单检测
FAKE_AUCTION_LOOKBACK     = 20     # 检测近N日历史
FAKE_AUCTION_THRESH       = 0.40   # 近N日"高开回落"比率超此值则过滤（40%）

# ── 炸板后再封陷阱检测 ────────────────────────────────────────────────────
# 目的：过滤"涨停→炸板→再次封板"这类反复诱多后砸盘的主力手法
# 判据：近RECLOSE_TRAP_DAYS日内，该股出现"炸板当日最终收盘反弹≥2%"（再封特征）次数
#       超过RECLOSE_TRAP_MAX则过滤，说明主力惯用此手法收割
FILTER_RECLOSE_TRAP       = True   # 是否开启再封陷阱检测
RECLOSE_TRAP_DAYS         = 30     # 检测近N日
RECLOSE_TRAP_MAX          = 2      # 近N日再封陷阱次数超出则过滤

# ── 信号执行随机延迟（可选，降低行为可预测性）────────────────────────────
# 目的：对同等质量信号引入±1日随机延迟，规避被跟单/反向跟踪
# 注意：开启后会显著影响回测结果（部分信号延迟后次日情绪已变化），
#       建议仅在实盘执行层使用，回测默认关闭
SIGNAL_DELAY_RANDOM       = False  # 默认关闭（实盘按需开启）
SIGNAL_DELAY_SEED         = None   # 随机种子（None=每次不同，整数=可复现）


# ================================================================
# ★ v6.0 智能化参数（核心升级）
# ================================================================

# ── 量价背离过滤 ──────────────────────────────────────────────────────────
# 目的：过滤"价格涨停但成交量异常低"的假突破信号，真正的主力拉升必须放量
FILTER_VOL_PRICE_DIVERGE  = True   # 是否开启量价背离过滤（True=过滤缩量涨停）
VOL_PRICE_DIVERGE_THRESH  = 0.80   # 当日量 < 20日均量×此值 且涨停 = 量价背离

# ── 情绪综合评分门槛 ──────────────────────────────────────────────────────
# 目的：只做综合评分超过门槛的高质量信号，低分信号直接过滤
# 评分满分100分，维度：封板质量(30)+量比(25)+动量(20)+放量质量(15)+ATR弹性(10)
SIGNAL_SCORE_ENABLE       = True   # 是否启用评分过滤
SIGNAL_SCORE_MIN_FIRST    = 80.0   # ★v8.5: 从72提升至80（首板21.8%胜率Kelly公式证明策略无效，近乎暂停）
SIGNAL_SCORE_MIN_AUCTION  = 50.0   # 竞价策略最低评分
SIGNAL_SCORE_MIN_RECOVER  = 45.0   # 反包策略最低评分（反包天然评分低，门槛适当放低）

# ── 动态持仓天数 ──────────────────────────────────────────────────────────
# 目的：强势信号（高分）延长持仓天数让利润奔跑；弱势信号提前出场减少损耗
# 规则：signal_score >= DYNAMIC_HOLD_HIGH_THRESH → 持仓延长至 HOLD_DAYS_STRONG
#        signal_score <= DYNAMIC_HOLD_LOW_THRESH  → 持仓缩短至 HOLD_DAYS_WEAK
#        中间区间保持默认 HOLD_DAYS
DYNAMIC_HOLD_ENABLE       = True   # 是否启用动态持仓天数
DYNAMIC_HOLD_HIGH_THRESH  = 70.0   # 评分≥70分 = 强势，延长持仓
DYNAMIC_HOLD_LOW_THRESH   = 45.0   # 评分≤45分 = 弱势，缩短持仓
HOLD_DAYS_STRONG          = 8      # ★v8.5: 从10天缩短至8天（配合默认5天的相对强势延长）
HOLD_DAYS_WEAK            = 3      # ★v8.5: 从4天缩短至3天（弱势信号更快出场）

# ── 跌停反包质量过滤 ──────────────────────────────────────────────────────
# 目的：只做"恐慌性单次跌停后快速修复"，拒绝连续下跌/利空下跌的假反包
# 判据：连续跌停天数 > DT_RECOVER_MAX_STREAK 不做（基本面持续利空）
#        跌停前5日动量 < DT_RECOVER_MIN_MOM5 不做（已在下跌通道，非短期恐慌）
DT_RECOVER_QUALITY_FILTER = True   # 是否启用反包质量过滤
DT_RECOVER_MAX_STREAK     = 1      # 最多允许连续跌停天数（>1天连跌不做）
DT_RECOVER_MIN_MOM5       = -0.15  # 跌停前5日动量下限（5日内跌幅超15%不做）
DT_RECOVER_MIN_OPEN_CHG   = 0.04   # 反包当日开盘涨幅下限（从3%提高到4%，确保情绪真实修复）

# ── T2追踪止盈自适应出场 ──────────────────────────────────────────────────
# 目的：T2之后，根据当日成交量萎缩程度动态调整回撤容忍率
# 成交量萎缩越严重，说明多头动能衰竭，应收紧容忍率更快锁住利润
# 动态容忍率 = T2_TRAIL_TOLERANCE × (当日量/5日均量)^T2_VOL_ADAPTIVE_POWER
# 例：成交量只有均量的30%时，容忍率 = 0.30×(0.3)^0.5 ≈ 0.164（大幅收紧）
T2_VOL_ADAPTIVE_ENABLE    = True   # 是否启用T2追踪止盈量能自适应
T2_VOL_ADAPTIVE_POWER     = 0.5    # 指数（0.5=根号调整，越小=越敏感）
T2_VOL_ADAPTIVE_MIN       = 0.10   # 容忍率下限（至少保住10%的利润空间）
T2_VOL_ADAPTIVE_MAX       = 0.50   # 容忍率上限（最多允许50%利润回撤）


# ================================================================
# ★ v8.4 赚钱效应模块（市场情绪 × 仓位自适应）
# ================================================================
# 思路：当天市场涨停家数 / (涨停家数+跌停家数) 反映当日赚钱效应
#   - 赚钱效应强 (>0.75)：增仓，顺势而为
#   - 赚钱效应弱 (<0.40)：减仓，保住本金
#   - 中性区间  (0.40~0.75)：维持正常仓位
# 数据来源：每日上涨/下跌家数 / 涨停数可通过 akshare 获取
# 回测中使用近N日个股涨停家数均值近似估算（无法实时获取历史数据）

EMOTION_SCORE_ENABLE      = True   # 是否启用赚钱效应仓位自适应

# 赚钱效应评估窗口：用最近N日的市场涨停数量估算当前赚钱效应
EMOTION_LOOKBACK_DAYS     = 5      # 近5天赚钱效应均值（避免单日噪音）

# 赚钱效应强弱阈值（基于涨停家数比例判断）
EMOTION_HOT_THRESH        = 0.70   # 赚钱效应强：涨停/(涨停+跌停) > 70%
EMOTION_COLD_THRESH       = 0.35   # 赚钱效应弱：涨停/(涨停+跌停) < 35%

# 对应仓位倍数（乘以基础仓位比例）
EMOTION_HOT_POS_RATIO     = 1.5    # 情绪热时仓位×1.5（最多15%/笔）
EMOTION_COLD_POS_RATIO    = 0.5    # 情绪冷时仓位×0.5（降至5%/笔）
EMOTION_NORMAL_POS_RATIO  = 1.0    # 情绪中性保持正常

# 赚钱效应近似计算：用全市场个股池中当日涨停占比估算
# 回测时无法获取实时数据，改用：当日 zt_streak>0 的股票数 / 总股票数 作为代理指标
# 注：此为近似值，实盘应接入真实涨停家数（ak.stock_zt_pool_em）
EMOTION_PROXY_ENABLE      = True   # 使用代理指标估算赚钱效应（回测用）
EMOTION_PROXY_ZT_MIN_RATIO = 0.05  # 市场涨停占比>5%认为情绪偏热
EMOTION_PROXY_ZT_LOW_RATIO = 0.02  # 市场涨停占比<2%认为情绪偏冷

# ── ★ v7.0 弱势月份仓位控制 ───────────────────────────────────────────────
# 目的：回测发现3/10/11月胜率极低（3月25.8%/-2.32%，10月24.7%/-2.17%，11月15.6%/-3.25%）
# 在弱势月份仓位减半，降低亏损累积，同时不错过偶发性强势行情
# ★v8.3更新：6月也加入弱势（胜率25.5%/-0.59%，接近3月水平）；9月强势月份加仓
# ★v8.5重校准（基于v8.4新回测数据）：
#   移出8月（实际胜率30.4%，各月最高之一，之前配置是反向操作）
#   移出10月（实际胜率30.7%，各月最高，之前配置是反向操作）
#   新增12月（胜率20.8%/-2.13%，与3月并列最差，之前遗漏）
WEAK_MONTH_POSITION_ENABLE = True        # 是否启用弱势月份仓位控制
WEAK_MONTHS               = [1, 2, 3, 6, 7, 11, 12]  # ★v9.2: 新增1月(40%胜率)和2月(50%胜率)，本次回测铁证
WEAK_MONTH_POSITION_RATIO = 0.5          # 弱势月份仓位系数（0.5=减半）
# ★v8.3新增：强势月份加仓（9月历史胜率62.2%/+3.85%，可以适当放大仓位）
# ★v8.5修正：v8.4回测显示9月实际29.0%/-0.29%，略好于均值，系数从1.5降至1.2
STRONG_MONTH_POSITION_ENABLE = True      # 是否启用强势月份加仓
STRONG_MONTHS              = [9]         # 强势月份：9月（略好于均值）
STRONG_MONTH_POSITION_RATIO = 1.2        # ★v8.5: 从1.5降至1.2（实测优势不如预期）


# ================================================================
# ★ v8.0 涨停回调缩量再启动策略参数
# ================================================================
# 参考：wxhui1024/Quantitative-Trading-System 涨停回调缩量策略
# 核心假设：涨停后回调缩量=大户未出货，短线有补涨机会

# 涨停回溯窗口：在最近N日内寻找涨停信号
ZT_PULLBACK_LOOKBACK      = 10   # 最多往前找10日的涨停（太远的涨停动能消耗殆尽）
ZT_PULLBACK_MIN_LOOKBACK  = 3    # 最少往前找3日（当日刚涨停不做回调策略）

# 回调幅度区间（相对涨停日收盘价）
# ★v8.3: 上限从8%收紧至7%（18.6%胜率说明回调太大的信号往往是出货不是洗盘）
ZT_PULLBACK_MIN_DROP      = 0.03  # 回调幅度下限3%（太小=没有洗盘，信号不可靠）
ZT_PULLBACK_MAX_DROP      = 0.07  # ★v8.3: 从0.08收紧至0.07（>7%往往出货）

# 缩量标准：回调期间成交量 < 涨停日成交量×此值
# ★v8.3: 从0.50收紧至0.45（更严格缩量要求，确保主力未出货）
ZT_PULLBACK_VOL_RATIO     = 0.45  # ★v8.3: 从0.50→0.45（缩量洗盘更纯粹）

# 企稳确认：当日收盘须高于回调最低点×(1+此值)，避免继续下跌时贸然入场
ZT_PULLBACK_STABLE_THRESH = 0.005  # 收盘高于近3日最低价0.5%以上确认企稳

# 市值过滤（比普通首板策略更严：回调策略需要更好的流动性）
ZT_PULLBACK_MKT_MIN       = 50    # 亿，流通市值下限50亿
ZT_PULLBACK_MKT_MAX       = 500   # 亿，流通市值上限500亿

# ★v8.3: 从55提高至60，回调缩量胜率18.6%说明需要提高质量门槛
SIGNAL_SCORE_MIN_PULLBACK = 60.0  # ★v8.3: 从55提高至60

# ★v8.3新增：回调缩量策略要求均线多头排列（MA5>MA10>MA20），确保大趋势向上
ZT_PULLBACK_REQUIRE_MA_ALIGN = True  # True=要求MA5>MA10且MA10>MA20




# ================================================================
# ★ v8.1 TradingAgents-CN 增强参数
# ================================================================

# ── 文件缓存层（参考TradingAgents-CN多级缓存架构）──────────────────────────
# 目的：历史数据本地缓存，重复回测不重复拉取API，速度提升3~5x
ENABLE_DATA_CACHE      = True            # 是否启用文件缓存
DATA_CACHE_DIR         = ".daban_cache"  # 缓存目录（项目根目录下）
DATA_CACHE_EXPIRE_DAYS = 7               # 缓存过期天数（7天内认为数据有效）

# ── 财务质量过滤（参考TradingAgents-CN provider.get_financial_data）────────
# 目的：剔除PE<0亏损股、PE>200泡沫股、ROE<5%盈利能力弱的垃圾标的
# 注意：财务数据拉取较慢，已内置缓存，不影响整体回测速度
ENABLE_FINANCIAL_FILTER  = False          # 默认关闭（财务数据拉取需网络，按需开启）
FINANCIAL_PE_MIN         = 0.0           # PE最低值（<0=亏损，不做）
FINANCIAL_PE_MAX         = 200.0         # PE最高值（>200=严重泡沫，风险极高）
FINANCIAL_ROE_MIN        = 5.0           # ROE下限（%），低于5%盈利能力太弱
FINANCIAL_CACHE_DAYS     = 7             # 财务数据缓存天数

# ── 新闻情绪因子（参考TradingAgents-CN智能新闻分析模块）──────────────────
# 目的：近N条股票新闻标题情绪作为signal_score第6维度，辅助判断舆情热度
# 实现：ak.stock_news_em() 获取新闻，关键词匹配正负面评分
# 注意：回测中拉取新闻极耗时，强烈建议仅实盘选股时开启
ENABLE_NEWS_SCORE        = False          # 默认关闭（回测用，实盘selector按需开启）
NEWS_LOOKBACK_COUNT      = 10            # 分析最近N条新闻
NEWS_SCORE_MAX           = 10.0          # 新闻情绪满分（signal_score总分扩展至110）
# 正面关键词（含此词加分）：业绩预增/重组/回购/中标/获奖等利好
NEWS_POSITIVE_KEYWORDS   = [
    "业绩预增", "净利润增长", "超预期", "重大合同", "中标", "回购", "增持",
    "战略合作", "获批", "突破", "创新高", "订单", "扩产", "新能源", "AI",
]
# 负面关键词（含此词扣分）：亏损/诉讼/质押/减持等利空
NEWS_NEGATIVE_KEYWORDS   = [
    "亏损", "业绩下滑", "减持", "股权质押", "诉讼", "处罚", "立案", "违规",
    "退市", "债务", "爆雷", "暴雷", "负面", "监管", "问询函",
]

# ── 真实盘中量比接口（参考TradingAgents-CN get_batch_stock_quotes）────────
# 目的：实盘时使用真实量比替代回测中的日线近似量比
# 接口：ak.stock_zh_a_spot_em() 实时行情（含volume_ratio字段）
# 仅在实盘选股器中调用，回测仍用日线5日均量近似
REALTIME_VOL_RATIO_ENABLE = False         # 回测默认关闭（实盘selector中开启）


# ================================================================
# 交易记录
# ================================================================
@dataclass
class Trade:
    code:         str
    strategy:     str
    entry_date:   str
    entry_price:  float
    exit_date:    str    = ""
    exit_price:   float  = 0.0
    exit_reason:  str    = ""  # stop_loss / target1 / target2 / timeout
    pnl_pct:      float  = 0.0
    holding_days: int    = 0
    zt_days:      int    = 0   # 入场前连板天数（首板=1）
    # 因子分析字段
    circ_mkt_cap: float  = 0.0   # 流通市值（亿），0表示未知
    vol_ratio:    float  = 0.0   # 量比（当日量/5日均量）
    turnover:     float  = 0.0   # 换手率（%）
    # 假涨停相关
    is_bomb_day:  bool   = False  # 入场信号当日是否炸板（回测验证用）
    bomb_30d:     int    = 0      # 入场前30日炸板次数
    # 股性相关
    is_yizi:      bool   = False  # 信号当日是否为一字板（开盘即封死，实盘买不进去）
    avg_amplitude: float = 0.0   # 近20日日均振幅%（衡量弹性）
    avg_amount_m:  float = 0.0   # 近20日日均成交额（百万元）
    # ★ v6.0 智能化字段
    signal_score:  float = 0.0   # 情绪综合评分（0~100分），越高信号质量越好
    dynamic_hold:  int   = 0     # 实际使用的动态持仓天数上限




# ================================================================
# ★ v8.1 数据缓存层（TradingAgents-CN DataCache模式）
# ================================================================
import hashlib
import pickle

class DataCache:
    """
    文件级数据缓存，参考 TradingAgents-CN 多级缓存架构轻量化实现。
    用于缓存历史日线数据和财务数据，避免回测时重复拉取API。
    缓存文件：.daban_cache/{md5(key)}.pkl
    过期逻辑：文件修改时间超过 expire_days 则视为过期
    """
    def __init__(self, cache_dir: str = DATA_CACHE_DIR,
                 expire_days: float = DATA_CACHE_EXPIRE_DAYS):
        self.cache_dir   = cache_dir
        self.expire_secs = expire_days * 86400
        if ENABLE_DATA_CACHE:
            os.makedirs(self.cache_dir, exist_ok=True)

    def _key_path(self, key: str) -> str:
        h = hashlib.md5(key.encode("utf-8")).hexdigest()
        return os.path.join(self.cache_dir, f"{h}.pkl")

    def _is_expired(self, path: str) -> bool:
        import time
        if not os.path.exists(path):
            return True
        return (time.time() - os.path.getmtime(path)) > self.expire_secs

    def load(self, key: str):
        if not ENABLE_DATA_CACHE:
            return None
        path = self._key_path(key)
        if self._is_expired(path):
            return None
        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception:
            return None

    def save(self, key: str, data) -> None:
        if not ENABLE_DATA_CACHE:
            return
        path = self._key_path(key)
        try:
            with open(path, "wb") as f:
                pickle.dump(data, f)
        except Exception as e:
            log.debug(f"缓存写入失败: {e}")

    def get(self, key: str, fetch_func, *args, **kwargs):
        """先读缓存，缓存失效则调用 fetch_func 并缓存结果"""
        cached = self.load(key)
        if cached is not None:
            return cached
        data = fetch_func(*args, **kwargs)
        if data is not None:
            self.save(key, data)
        return data


# 全局缓存实例
_data_cache = DataCache()


# ================================================================
# ★ v8.1 财务质量过滤（TradingAgents-CN get_financial_data模式）
# ================================================================
_financial_cache: dict = {}   # 内存缓存：{code: {pe, roe, valid}}

def fetch_financial_quality(code: str) -> dict:
    """
    获取股票财务质量指标（PE、ROE）。
    数据源：akshare ak.stock_a_lg_indicator（个股A股指标）
    返回：{"pe": float, "roe": float, "valid": bool}
    带文件缓存（FINANCIAL_CACHE_DAYS天），失效才重新拉取。
    """
    global _financial_cache
    if code in _financial_cache:
        return _financial_cache[code]

    default = {"pe": 20.0, "roe": 10.0, "valid": False}  # 无数据时不过滤

    if not ENABLE_FINANCIAL_FILTER or not USE_AK:
        _financial_cache[code] = default
        return default

    cache_key = f"financial_{code}"
    cached = _data_cache.load(cache_key)
    if cached is not None:
        _financial_cache[code] = cached
        return cached

    try:
        num = code[:6]
        # akshare 个股估值指标接口（PE/PB/ROE等）
        # 接口：ak.stock_a_indicator_lg(stock=num) 或 ak.stock_financial_analysis_indicator
        # 兼容处理：先尝试 stock_a_indicator_lg，失败则尝试 stock_financial_analysis_indicator
        df_fin = None
        for func_name in ["stock_a_indicator_lg", "stock_financial_analysis_indicator"]:
            func = getattr(ak, func_name, None)
            if func is None:
                continue
            try:
                df_fin = func(stock=num)
                if df_fin is not None and not df_fin.empty:
                    break
            except Exception:
                continue

        if df_fin is None or df_fin.empty:
            _financial_cache[code] = default
            return default
        # 取最新一条
        latest = df_fin.iloc[-1]
        # 尝试多种字段名（不同接口字段名不同）
        pe_val = None
        for pe_col in ["pe", "市盈率", "PE", "pe_ttm", "市盈率(TTM)"]:
            if pe_col in latest.index:
                pe_val = latest[pe_col]
                break
        pe = float(pe_val or 20.0)
        roe_val = None
        for roe_col in ["roe", "ROE", "净资产收益率", "roe_weighted"]:
            if roe_col in latest.index:
                roe_val = latest[roe_col]
                break
        roe = float(roe_val or 10.0)
        result = {"pe": pe, "roe": roe, "valid": True}
        _data_cache.save(cache_key, result)
        _financial_cache[code] = result
        return result
    except Exception as e:
        log.debug(f"{code} 财务数据拉取失败: {e}")
        _financial_cache[code] = default
        return default


def check_financial_quality(code: str) -> bool:
    """
    财务质量硬过滤：
      - PE < FINANCIAL_PE_MIN（亏损股，<0）→ 过滤
      - PE > FINANCIAL_PE_MAX（严重泡沫，>200）→ 过滤
      - ROE < FINANCIAL_ROE_MIN（盈利能力弱，<5%）→ 过滤
    返回 True=通过，False=过滤掉
    """
    if not ENABLE_FINANCIAL_FILTER:
        return True
    fin = fetch_financial_quality(code)
    if not fin.get("valid", False):
        return True   # 拉取失败时不过滤，避免误杀
    pe  = fin["pe"]
    roe = fin["roe"]
    if pe < FINANCIAL_PE_MIN or pe > FINANCIAL_PE_MAX:
        return False
    if roe < FINANCIAL_ROE_MIN:
        return False
    return True


# ================================================================
# ★ v8.1 新闻情绪因子（TradingAgents-CN 新闻分析模块）
# ================================================================
_news_cache: dict = {}   # 内存缓存：{code: score}

def fetch_news_sentiment_score(code: str) -> float:
    """
    获取股票最近NEWS_LOOKBACK_COUNT条新闻的情绪评分（0~NEWS_SCORE_MAX）。
    数据源：akshare ak.stock_news_em(symbol=code[:6])
    正面关键词：加分；负面关键词：扣分；无新闻：返回中性分（5分）
    带文件缓存（当天有效，次日回测自动更新）。
    注意：仅实盘/选股时调用，回测默认 ENABLE_NEWS_SCORE=False 跳过。
    """
    if not ENABLE_NEWS_SCORE or not USE_AK:
        return 5.0   # 中性分，不影响评分系统

    global _news_cache
    if code in _news_cache:
        return _news_cache[code]

    cache_key = f"news_{code}_{datetime.date.today().strftime('%Y%m%d')}"
    cached = _data_cache.load(cache_key)
    if cached is not None:
        _news_cache[code] = cached
        return cached

    try:
        num = code[:6]
        df_news = ak.stock_news_em(symbol=num)
        if df_news is None or df_news.empty:
            _news_cache[code] = 5.0
            return 5.0

        # 取最近N条标题
        titles = df_news["新闻标题"].iloc[:NEWS_LOOKBACK_COUNT].tolist()
        pos_cnt = 0
        neg_cnt = 0
        for title in titles:
            title_str = str(title)
            for kw in NEWS_POSITIVE_KEYWORDS:
                if kw in title_str:
                    pos_cnt += 1
                    break
            for kw in NEWS_NEGATIVE_KEYWORDS:
                if kw in title_str:
                    neg_cnt += 1
                    break

        # 情绪分：正面比例 × 满分，负面比例扣分
        total = len(titles) or 1
        raw_score = (pos_cnt / total * NEWS_SCORE_MAX) - (neg_cnt / total * NEWS_SCORE_MAX * 0.5)
        score = max(0.0, min(NEWS_SCORE_MAX, raw_score + NEWS_SCORE_MAX * 0.5))  # 中性基准5分

        _data_cache.save(cache_key, score)
        _news_cache[code] = score
        return score
    except Exception as e:
        log.debug(f"{code} 新闻情绪拉取失败: {e}")
        _news_cache[code] = 5.0
        return 5.0


# ================================================================
# ★ v8.1 真实盘中量比（TradingAgents-CN get_batch_stock_quotes模式）
# ================================================================
_realtime_vol_ratio_cache: dict = {}   # 内存缓存（当日有效）
_realtime_cache_date: str = ""

def fetch_realtime_vol_ratio_batch(codes: list) -> dict:
    """
    批量获取全市场真实盘中量比，替代日线5日均量近似值。
    数据源：ak.stock_zh_a_spot_em()，字段：量比
    返回：{code_6位: vol_ratio}
    参考：TradingAgents-CN AKShare Provider get_batch_stock_quotes()
    注意：仅实盘使用，回测不调用（REALTIME_VOL_RATIO_ENABLE=False）
    """
    global _realtime_vol_ratio_cache, _realtime_cache_date

    if not REALTIME_VOL_RATIO_ENABLE or not USE_AK:
        return {}

    today = datetime.date.today().strftime("%Y-%m-%d")
    if _realtime_cache_date == today and _realtime_vol_ratio_cache:
        return _realtime_vol_ratio_cache

    try:
        df_spot = ak.stock_zh_a_spot_em()
        if df_spot is None or df_spot.empty:
            return {}
        # 字段名：代码、名称、量比 等
        # akshare stock_zh_a_spot_em 实际字段：序号,代码,名称,最新价,...,量比,...
        col_map = {}
        for col in df_spot.columns:
            if "量比" in str(col):
                col_map["量比"] = col
            if "代码" in str(col):
                col_map["代码"] = col
        if "代码" not in col_map or "量比" not in col_map:
            log.warning("ak.stock_zh_a_spot_em 字段名异常，实时量比获取失败")
            return {}

        result = {}
        for _, row in df_spot.iterrows():
            code_6 = str(row[col_map["代码"]]).zfill(6)
            vr = row[col_map["量比"]]
            try:
                result[code_6] = float(vr)
            except (ValueError, TypeError):
                result[code_6] = 1.0

        _realtime_vol_ratio_cache = result
        _realtime_cache_date = today
        log.info(f"实时量比已更新：{len(result)} 只股票")
        return result
    except Exception as e:
        log.warning(f"实时量比拉取失败: {e}")
        return {}


# ================================================================
# 数据获取
# ================================================================
def _code_to_yf(code: str) -> str:
    c = code[:6]
    return (c + ".SS") if c.startswith("6") else (c + ".SZ")


def _fetch_history_bs(code: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """baostock 拉取日线，支持海外IP。使用线程锁保证串行（baostock单连接协议）"""
    with _BS_LOCK:
        try:
            num, mkt = code.split(".")
            bs_code = ("sh." if mkt == "SH" else "sz.") + num
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount,turn",
                start_date=start, end_date=end, frequency="d", adjustflag="2"
            )
            rows = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())
            if not rows:
                return None
            df = pd.DataFrame(rows, columns=["date","开盘","最高","最低","收盘","成交量","成交额","换手率"])
            for col in ["开盘","最高","最低","收盘","成交量","成交额","换手率"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["收盘"])
            return df
        except Exception as e:
            log.debug(f"{code} baostock失败: {e}")
            return None


def fetch_history(code: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """拉取日线数据，统一列名：date / 开盘 / 最高 / 最低 / 收盘 / 成交量 / 成交额
    优先 baostock（海外IP友好），失败则回退 akshare
    ★ v8.1：增加文件缓存层，避免重复拉取API（TradingAgents-CN DataCache模式）
    """
    # ★ v8.1 先查文件缓存
    cache_key = f"hist_{code}_{start}_{end}"
    cached_df = _data_cache.load(cache_key)
    if cached_df is not None:
        return cached_df
    try:
        df = None
        # ── 优先 baostock ─────────────────────────────────────────
        if USE_BS:
            df = _fetch_history_bs(code, start, end)

        # ── 回退 akshare ──────────────────────────────────────────
        if df is None and USE_AK:
            df = ak.stock_zh_a_hist(
                symbol=code[:6], period="daily",
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                adjust="qfq"
            )
            if df is not None:
                df = df.rename(columns={
                    "日期": "date", "开盘": "开盘", "最高": "最高", "最低": "最低",
                    "收盘": "收盘", "成交量": "成交量", "成交额": "成交额"
                })

        if df is None or len(df) < 60:
            return None

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        # ── 节后首日标记（国庆/春节/五一等假期后第一个交易日，高开低走风险极高）──
        # 判断方法：相邻两个交易日间隔 > 3个自然日，则后一天视为节后首日
        date_diff = df["date"].diff().dt.days
        df["is_post_holiday"] = date_diff > 3

        # 每日涨跌幅
        df["prev_close"] = df["收盘"].shift(1)
        df["chg_pct"]    = (df["收盘"] - df["prev_close"]) / df["prev_close"]

        # 是否涨停 / 跌停
        df["is_zt"] = df["chg_pct"] >= ZT_THRESH
        df["is_dt"] = df["chg_pct"] <= DT_THRESH

        # 量比：当日成交量 / 过去5日均量
        # ★ v3.4修复：min_periods改为5，前5天不足时产生NaN，避免早期量比虚高误判
        df["vol_ma5"]  = df["成交量"].rolling(5, min_periods=5).mean().shift(1)
        df["vol_ratio"] = df["成交量"] / df["vol_ma5"].replace(0, np.nan)
        df["vol_ratio"] = df["vol_ratio"].fillna(1.0)

        # 换手率（日线只有成交量，没有流通股本，用量比代替）
        df["turnover"] = df["vol_ratio"]  # placeholder，实盘用实际换手率

        # ── 假涨停相关字段 ────────────────────────────────────────
        # 当日是否炸板：最高触及涨停（≥+9.5%）但收盘未涨停（<+9.0%）
        df["hi_chg"]   = (df["最高"] - df["prev_close"]) / df["prev_close"].replace(0, np.nan)
        df["is_bomb"]  = (df["hi_chg"] >= ZT_THRESH) & (~df["is_zt"])
        # 当日是否涨停后缩量次日（次日成交量 < 今日×FILTER_VOL_SHRINK）
        df["vol_shrink_next"] = df["成交量"].shift(-1) < df["成交量"] * FILTER_VOL_SHRINK

        # 近30日炸板次数（滚动窗口）
        df["bomb_30d"] = df["is_bomb"].rolling(FILTER_BOMB_HISTORY_DAYS, min_periods=1).sum().shift(1).fillna(0)

        # ── 股性识别字段 ──────────────────────────────────────────
        # 一字板：当日涨停 且 开盘≈最高≈涨停价（开盘即封死，无法买入）
        df["zt_price"] = df["prev_close"] * 1.10
        df["is_yizi"]  = (df["is_zt"] &
                          (np.abs(df["开盘"] - df["最高"]) < 0.02) &
                          (np.abs(df["开盘"] - df["zt_price"]) < df["zt_price"] * 0.005))

        # 日均振幅（近20日滚动，当日值）
        amplitude = (df["最高"] - df["最低"]) / df["prev_close"].replace(0, np.nan) * 100
        df["amplitude"]    = amplitude
        df["avg_amp_20d"]  = amplitude.rolling(20, min_periods=5).mean()

        # 日均成交额（近20日滚动，百万元）
        if "成交额" in df.columns:
            df["avg_amt_20d"] = df["成交额"].rolling(20, min_periods=5).mean() / 1e6
        else:
            df["avg_amt_20d"] = (df["成交量"] * (df["最高"] + df["最低"] + df["收盘"]) / 3 * 100).rolling(20, min_periods=5).mean() / 1e6

        # 近30日一字板比例
        df["yizi_ratio_30d"] = (df["is_yizi"].rolling(30, min_periods=5).sum() /
                                df["is_zt"].rolling(30, min_periods=5).sum().replace(0, np.nan)).fillna(0.0)

        # 连板天数
        zt_streak = []
        streak = 0
        for v in df["is_zt"]:
            if v:
                streak += 1
            else:
                streak = 0
            zt_streak.append(streak)
        df["zt_streak"] = zt_streak

        # ── 大盘环境 / 动量字段 ──────────────────────────────────
        # MA20：股价是否处于20日均线之上（上升趋势）
        df["ma20"] = df["收盘"].rolling(20, min_periods=10).mean()
        df["above_ma20"] = df["收盘"] > df["ma20"]

        # 近5日动量：当日收盘相对5日前收盘的涨跌幅（用shift(5)做前值）
        df["mom_5d"] = (df["收盘"] - df["收盘"].shift(5)) / df["收盘"].shift(5).replace(0, np.nan)

        # 涨停强度：收盘价在当日振幅区间中的位置（0=收在最低，1=收在最高）
        # 用于衡量封板质量：接近1说明尾盘未出货，封板坚实
        price_range = (df["最高"] - df["最低"]).replace(0, np.nan)
        df["close_pos"] = (df["收盘"] - df["最低"]) / price_range  # 0~1

        # ── ★ 优化⑧：ATR 动态止损字段 ────────────────────────────
        # True Range = max(高-低, |高-前收|, |低-前收|)
        df["tr1"] = df["最高"] - df["最低"]
        df["tr2"] = (df["最高"] - df["prev_close"]).abs()
        df["tr3"] = (df["最低"] - df["prev_close"]).abs()
        df["true_range"] = df[["tr1","tr2","tr3"]].max(axis=1)
        # ATR(14)：14日真实波幅均值
        df["atr14"] = df["true_range"].rolling(14, min_periods=5).mean()
        # ATR止损距离：以ATR倍数（默认1.5x）设定动态止损幅度
        # atr_stop_pct：相对于入场价的止损百分比（负数）
        df["atr_stop_pct"] = -(df["atr14"] / df["收盘"].replace(0, np.nan)) * 1.5
        df["atr_stop_pct"] = df["atr_stop_pct"].clip(lower=-0.08, upper=-0.02)  # 限制在-2%~-8%

        # ── ★ v5.0 反收割字段①：竞价虚假挂单特征 ─────────────────
        # "竞价高开但当日最终收盘低于开盘价"为钓鱼盘特征
        # fake_open_day：今日高开(>2%)但收盘<开盘（高开低走=竞价诱多后砸盘）
        df["fake_open_day"] = (df["chg_pct"] >= 0.02) & (df["收盘"] < df["开盘"])
        # 近FAKE_AUCTION_LOOKBACK日内高开低走次数
        df["fake_open_cnt"] = (
            df["fake_open_day"]
            .rolling(FAKE_AUCTION_LOOKBACK, min_periods=1)
            .sum()
            .shift(1)
            .fillna(0)
        )
        # 近N日内高开次数（用于算比率分母）
        df["high_open_cnt"] = (
            (df["chg_pct"] >= 0.02)
            .rolling(FAKE_AUCTION_LOOKBACK, min_periods=1)
            .sum()
            .shift(1)
            .fillna(0)
        )
        # 虚假高开比率（假信号比例），分母为0时置0
        df["fake_auction_ratio"] = (
            df["fake_open_cnt"] / df["high_open_cnt"].replace(0, np.nan)
        ).fillna(0.0)

        # ── ★ v5.0 反收割字段②：炸板后再封陷阱 ──────────────────
        # "当日炸板但最终收盘仍高于开盘价2%以上"为再封陷阱特征（反复诱多）
        df["reclose_trap_day"] = (
            df["is_bomb"] &                               # 当日炸板
            ((df["收盘"] - df["开盘"]) / df["开盘"].replace(0, np.nan) >= 0.02)  # 收盘仍强
        )
        # 近RECLOSE_TRAP_DAYS日内再封陷阱出现次数
        df["reclose_trap_cnt"] = (
            df["reclose_trap_day"]
            .rolling(RECLOSE_TRAP_DAYS, min_periods=1)
            .sum()
            .shift(1)
            .fillna(0)
            .astype(int)
        )

        # ── ★ v6.0 智能化字段①：量价背离检测 ────────────────────
        # 原理：真正的主力拉升涨停，必须放量（量价同步）；若缩量涨停则是假突破
        # vol_vs_ma20：当日成交量相对近20日均量的比值（>1放量，<1缩量）
        vol_ma20 = df["成交量"].rolling(20, min_periods=5).mean().shift(1)
        df["vol_vs_ma20"] = (df["成交量"] / vol_ma20.replace(0, np.nan)).fillna(1.0)
        # 量价背离标志：涨停日成交量却低于20日均量×0.8，高度疑似假突破
        df["vol_price_diverge"] = df["is_zt"] & (df["vol_vs_ma20"] < 0.80)
        # 近5日量能趋势（成交量加速/衰退）：正值=放量加速，负值=缩量衰退
        df["vol_trend_5d"] = (
            df["成交量"].rolling(3, min_periods=2).mean() /
            df["成交量"].rolling(3, min_periods=2).mean().shift(3).replace(0, np.nan)
        ).fillna(1.0)

        # ── ★ v6.0 智能化字段②：情绪综合评分（0~100分）──────────
        # ★ v8.0 升级：各因子先做Rank→Z-score标准化再加权，消除量纲差异
        # 参考：nekolovepepper连板追涨策略因子工程（Rank→Z标准化→加权）
        # 因子1：封板质量（close_pos，满分30分）
        score_seal    = (df["close_pos"].clip(0, 1) * 30).fillna(15.0)
        # 因子2：量比（★v8.0 Z-score标准化后映射，满分25分）
        # 先Rank（百分位），再映射到得分空间，倒U型（最优区间1.5~3）
        vr_rank = df["vol_ratio"].rank(pct=True).fillna(0.5)  # 百分位
        # 量比1.5~3最优(中高分位)，过低/过高均扣分，用倒抛物线形状
        vr_score = np.where(
            df["vol_ratio"] < 1.5, df["vol_ratio"] / 1.5 * 15,
            np.where(
                df["vol_ratio"] <= 3.0,
                15 + (df["vol_ratio"] - 1.5) / 1.5 * 10,
                np.where(df["vol_ratio"] <= 5.0, 25 - (df["vol_ratio"] - 3.0) / 2.0 * 10, 5)
            )
        )
        # Z-score修正：偏离均值越多越扣分（防止极端值拉偏评分）
        vr_z = (df["vol_ratio"] - df["vol_ratio"].rolling(60, min_periods=10).mean()) / \
               (df["vol_ratio"].rolling(60, min_periods=10).std().replace(0, 1))
        vr_z_penalty = (vr_z.abs().clip(0, 3) * 2).fillna(0)  # 偏离2个标准差以上最多扣6分
        score_volratio = (pd.Series(vr_score, index=df.index).fillna(10.0) - vr_z_penalty).clip(0, 25)
        # 因子3：近5日动量（★v8.0 Z-score标准化，满分20分）
        mom_mean = df["mom_5d"].rolling(60, min_periods=10).mean().fillna(0)
        mom_std  = df["mom_5d"].rolling(60, min_periods=10).std().replace(0, 0.05).fillna(0.05)
        mom_z    = ((df["mom_5d"] - mom_mean) / mom_std).clip(-3, 3).fillna(0)
        score_mom = ((mom_z + 3) / 6 * 20).clip(0, 20)  # [-3,3] → [0,20]
        # 因子4：放量质量（vol_vs_ma20，★v8.0 Z-score标准化，满分15分）
        vol_mean = df["vol_vs_ma20"].rolling(60, min_periods=10).mean().fillna(1.0)
        vol_std  = df["vol_vs_ma20"].rolling(60, min_periods=10).std().replace(0, 0.5).fillna(0.5)
        vol_z    = ((df["vol_vs_ma20"] - vol_mean) / vol_std).clip(-3, 3).fillna(0)
        score_vol = ((vol_z + 3) / 6 * 15).clip(0, 15)  # [-3,3] → [0,15]
        # 因子5：ATR波动奖励（适度波动最优，太小无弹性，太大风险高，满分10分）
        atr_pct   = (-df["atr_stop_pct"]).clip(0.02, 0.08)  # 转正值
        score_atr = ((atr_pct - 0.02) / 0.06 * 10).fillna(5.0)
        # 汇总评分（★v8.1 新闻情绪因子通过backtest层传入，此处不拉取避免影响速度）
        df["signal_score"] = (
            score_seal + score_volratio + score_mom + score_vol + score_atr
        ).clip(0, 100)

        # ── ★ v6.0 智能化字段③：跌停质量字段（用于反包策略）──────
        # 连续跌停次数（反包需要区分：恐慌性单次跌停 vs 基本面持续下跌）
        dt_streak_list = []
        dt_s = 0
        for v in df["is_dt"]:
            if v:
                dt_s += 1
            else:
                dt_s = 0
            dt_streak_list.append(dt_s)
        df["dt_streak"] = dt_streak_list
        # 跌停前5日的动量（跌停前5日如果已经大跌>15%，说明是基本面利空，不做反包）
        df["mom_5d_before_dt"] = df["mom_5d"].shift(1)

        # ── ★ v8.0 回调缩量策略字段 ──────────────────────────────
        # 用于快速定位"近N日出现过涨停"的日期，以及回调期间的缩量特征
        # zt_vol：涨停当日的成交量（向后传播，方便回调日比较）
        # 逻辑：对每一个非涨停日，往前找最近一次涨停，记录其成交量和价格
        zt_vol_arr    = np.zeros(len(df))
        zt_price_arr  = np.zeros(len(df))
        zt_date_idx   = np.full(len(df), -1, dtype=int)  # 最近涨停的行索引
        last_zt_i     = -1
        last_zt_vol   = 0.0
        last_zt_price = 0.0
        for _i, row_i in enumerate(df["is_zt"]):
            if row_i:
                last_zt_i     = _i
                last_zt_vol   = float(df["成交量"].iloc[_i])
                last_zt_price = float(df["收盘"].iloc[_i])
            zt_vol_arr[_i]   = last_zt_vol
            zt_price_arr[_i] = last_zt_price
            zt_date_idx[_i]  = last_zt_i
        df["last_zt_vol"]      = zt_vol_arr
        df["last_zt_price"]    = zt_price_arr
        df["last_zt_idx"]      = zt_date_idx
        # 距离最近涨停的天数（当日涨停=0，无涨停记录=-1）
        df["days_since_zt"]    = np.where(
            df["last_zt_idx"] >= 0,
            np.arange(len(df)) - df["last_zt_idx"],
            -1
        )

        # ★ v8.1 写入文件缓存（TradingAgents-CN DataCache模式）
        _data_cache.save(cache_key, df)

        return df
    except Exception as e:
        log.debug(f"{code} 数据失败: {e}")
        return None


def fetch_pool_codes() -> list:
    """
    ★ v4.0：股票池改为中证1000 + 创业板指 + 科创50
    移除沪深300（大市值难封板，打板机会极少，会稀释样本）
    中证1000+创业板中小市值股才是打板主战场
    """
    # 优先 baostock：海外IP友好
    if USE_BS:
        try:
            codes_bs = []
            # 中证1000（小市值成长股，打板主力军）
            rs = bs.query_zz500_stocks()  # baostock暂无中证1000接口，用中证500代替
            while rs.error_code == "0" and rs.next():
                codes_bs.append(rs.get_row_data()[1])
            log.info(f"baostock中证500：{len(codes_bs)}只（作为中小市值代替）")

            seen = set()
            codes = []
            for c in codes_bs:
                if c in seen:
                    continue
                seen.add(c)
                mkt, num = c.split(".")
                codes.append(num + ".SH" if mkt == "sh" else num + ".SZ")
            log.info(f"股票池（baostock）：{len(codes)} 只（中证500，打板主战场）")
            return codes
        except Exception as e:
            log.warning(f"baostock 获取股票池失败: {e}，尝试 akshare")

    # 备用 akshare：国内IP可用，拉中证1000+创业板指+科创50
    if USE_AK:
        try:
            all_codes = []
            # 中证1000（小市值成长股）
            try:
                df1 = ak.index_stock_cons("000852")
                all_codes += df1["品种代码"].tolist()
                log.info(f"中证1000：{len(df1)}只")
            except Exception as e:
                log.warning(f"中证1000拉取失败: {e}")

            # 创业板指（创业板龙头，高弹性）
            try:
                df2 = ak.index_stock_cons("399006")
                all_codes += df2["品种代码"].tolist()
                log.info(f"创业板指：{len(df2)}只")
            except Exception as e:
                log.warning(f"创业板指拉取失败: {e}")

            # 科创50（科创板，高科技高弹性）
            try:
                df3 = ak.index_stock_cons("000688")
                all_codes += df3["品种代码"].tolist()
                log.info(f"科创50：{len(df3)}只")
            except Exception as e:
                log.warning(f"科创50拉取失败: {e}")

            # 去重并格式化
            seen = set()
            codes = []
            for c in all_codes:
                if c in seen:
                    continue
                seen.add(c)
                codes.append(c + ".SH" if c.startswith("6") else c + ".SZ")

            log.info(f"股票池（akshare）合计：{len(codes)} 只（中证1000+创业板指+科创50）")
            return codes
        except Exception as e:
            log.error(f"akshare 获取股票池失败: {e}")

    log.error("无法获取股票池，请用 --codes 指定")
    return []


# ================================================================
# 出场逻辑（升级版）
# ================================================================
def _trail_stop_price(entry_price: float, peak_price: float,
                      row: pd.Series, vol_ma5: float) -> float:
    """
    计算智能跟踪止盈的止损价格。
    逻辑：
      1. 按当前最高浮盈幅度确定基础回撤容忍比例（分4档）
      2. 检测强势/弱势盘中信号，动态调整容忍度
         - 强势（close_pos高）：放宽容忍，让利润继续奔跑
         - 弱势（缩量/长上影）：收紧容忍，及时锁利
      3. 止损价 = peak_price - (peak_price - entry_price) × 容忍比例
    """
    if peak_price <= entry_price:
        return entry_price  # 保本线

    cur_profit = (peak_price - entry_price) / entry_price  # 最高浮盈幅度

    # ── 1. 基础回撤容忍比例（按盈利分档）──────────────────────
    base_tolerance = 1.0  # 默认全部保住
    for lo_t, hi_t, tol in TRAIL_PROFIT_TIERS:
        if lo_t <= cur_profit < hi_t:
            base_tolerance = tol
            break

    # ── 2. 强弱信号动态调整 ────────────────────────────────────
    adjustment = 0.0
    close_pos = float(row.get("close_pos", 0.5) or 0.5)
    cur_vol   = float(row.get("成交量", 0) or 0)

    # 强势信号：收盘接近最高价（大阳线/涨停），放宽容忍
    if close_pos >= TRAIL_STRONG_THRESH:
        adjustment += TRAIL_STRONG_BONUS

    # 弱势信号1：缩量（量能萎缩，趋势衰竭），收紧容忍
    if vol_ma5 > 0 and cur_vol < vol_ma5 * TRAIL_VOL_SHRINK_RATIO:
        adjustment += TRAIL_WEAK_PENALTY

    # 弱势信号2：长上影（开高走低，尾盘出货），收紧容忍
    if close_pos < TRAIL_UPPER_SHADOW_THRESH:
        adjustment += TRAIL_WEAK_PENALTY

    # 最终容忍比例限制在 [0.0, 1.0]
    tolerance = max(0.0, min(1.0, base_tolerance + adjustment))

    # ── 3. 计算止损价：从历史最高点回落"利润×容忍比例" ────────
    profit_amt   = peak_price - entry_price
    max_pullback = profit_amt * tolerance
    stop_p       = peak_price - max_pullback
    return max(stop_p, entry_price)  # 至少保本


def simulate_exit(df: pd.DataFrame, entry_idx: int, entry_price: float,
                  atr_stop_pct: Optional[float] = None,
                  rng: Optional[random.Random] = None,
                  hold_days_limit: Optional[int] = None,
                  is_auction: bool = False,
                  auction_next_day_df: Optional[pd.DataFrame] = None,
                  auction_entry_idx: Optional[int] = None) -> tuple:
    """
    出场逻辑（v9.1 次日确认 + 连板跟踪极致版）：
      1. 入场第1天低开保护：竞价开盘低开>2% 直接止损（比通用更严）
      2. ★ v4.0 止损优先级修复：同日ATR止损/时间止损取更优出场价
      3. ★ v8.4 第2天强制止损：收盘亏损>2.0% 且非缩量洗盘 → 次日开盘强制出
      4. ★ v4.0 趋势止损：MA10，持仓≥5天生效
      5. ★ 优化⑤ T2后追踪止盈
      6. 智能跟踪止盈（T1后启动）
      7. ★ v9.0 竞价涨停跟踪：入场后每次涨停则持仓重置，追连板
      8. ★ v9.0 断板止损：涨停次日跌破涨停价5%立即止损
      9. ★ v9.1 次日竞价方向确认：弱开+close_pos低→当日收盘出场
      10. 超时平仓
      ★ v5.0：止损位随机化 + 止盈点反整数化
      ★ v6.0：动态持仓天数 + T2追踪止盈量能自适应
    """
    _rng = rng if rng is not None else random
    # ★ v6.0：动态持仓天数（外部传入优先，否则用默认值）
    _hold_days = hold_days_limit if hold_days_limit is not None else HOLD_DAYS

    # ★ v9.0：竞价策略专用止损（更紧）
    if is_auction and AUCTION_STOP_LOSS:
        base_stop_pct = AUCTION_STOP_LOSS
    elif atr_stop_pct is not None and atr_stop_pct < 0:
        base_stop_pct = atr_stop_pct   # 已限制在 -2%~-8%
    else:
        base_stop_pct = STOP_LOSS      # 回退至固定止损 -4.5%

    # ★ v5.0 止损位随机化：在基准止损基础上叠加均匀随机扰动
    # 每笔交易止损位独立随机，避免全市场同参数止损堆积在同一价格
    if STOP_JITTER_ENABLE:
        jitter = _rng.uniform(-STOP_JITTER_RANGE, STOP_JITTER_RANGE)
        dynamic_stop_pct = base_stop_pct + jitter
        # 防止止损过松（>-1%）或过紧（<-12%）
        dynamic_stop_pct = max(-0.12, min(-0.01, dynamic_stop_pct))
    else:
        dynamic_stop_pct = base_stop_pct

    # ★ v5.0 止盈点反整数化：偏离整数关口，规避做市商密集挂单陷阱
    if PROFIT_JITTER_ENABLE:
        t1_jitter = _rng.uniform(-PROFIT_JITTER_RANGE, PROFIT_JITTER_RANGE)
        t2_jitter = _rng.uniform(-PROFIT_JITTER_RANGE, PROFIT_JITTER_RANGE)
        effective_t1 = PROFIT_T1 + t1_jitter   # 例：9.3%~10.7%
        effective_t2 = PROFIT_T2 + t2_jitter   # 例：19.3%~20.7%
    else:
        effective_t1 = PROFIT_T1
        effective_t2 = PROFIT_T2

    stop_price = entry_price * (1 + dynamic_stop_pct)   # 动态初始止损（含随机化）
    t1_price   = entry_price * (1 + effective_t1)        # 反整数化T1止盈
    t2_price   = entry_price * (1 + effective_t2)        # 反整数化T2止盈
    n          = len(df)
    t1_hit     = False         # 是否已触达T1（跟踪止盈激活标志）
    t2_hit     = False         # 是否已触达T2（追踪止盈激活标志）
    peak_price = entry_price   # 持仓期间历史最高价（跟踪止盈基准）

    # ★ v8.4 强势延长持仓：跟踪连续强势天数，达标则动态扩展持仓上限
    consecutive_strong = 0    # 连续强势天数计数
    extra_hold_days    = 0    # 已额外延长的天数

    # ★ v8.4 第2天强制止损：跟踪第2天收盘是否触发强制止损
    force_stop_day3    = False  # True=下一天开盘强制止损

    # ★ v9.0 竞价涨停跟踪：记录最近涨停日，用于断板检测 + 持仓重置
    last_zt_day_in_hold = -1   # 持仓期间最近一次涨停的日索引（-1=无涨停）
    zt_trail_extra      = 0    # 涨停跟踪额外持仓天数（每次涨停重置）

    # 动态计算有效持仓上限（考虑v9.0涨停跟踪）
    def _effective_max_days():
        if is_auction and AUCTION_ZT_TRAIL_ENABLE and last_zt_day_in_hold >= 0:
            # 每次涨停后，从涨停那天起再给N天，总上限不超过AUCTION_ZT_TRAIL_MAX_DAYS
            return min(last_zt_day_in_hold - entry_idx + 1 + AUCTION_ZT_TRAIL_RESET_DAYS,
                       AUCTION_ZT_TRAIL_MAX_DAYS)
        return _hold_days + extra_hold_days

    for j in range(entry_idx, min(entry_idx + AUCTION_ZT_TRAIL_MAX_DAYS if (is_auction and AUCTION_ZT_TRAIL_ENABLE) else entry_idx + _hold_days + EXTEND_HOLD_MAX_EXTRA + 1, n)):
        row   = df.iloc[j]
        hi    = float(row["最高"])
        lo    = float(row["最低"])
        cls   = float(row["收盘"])
        op    = float(row["开盘"])
        date  = str(row["date"])[:10]
        days  = j - entry_idx + 1

        # 更新持仓期间历史最高价（用最高价作为峰值基准）
        peak_price = max(peak_price, hi)

        # 计算5日均量（缩量判断用）
        vol_ma5 = float(df["成交量"].iloc[max(0, j - 4):j].mean()) if j > 0 else 0.0

        # ── 1. 入场第1天低开保护 ──────────────────────────────────
        _gap_abort = AUCTION_GAP_ABORT if is_auction else OPEN_GAP_ABORT
        if days == 1 and _gap_abort < 0:
            if op < entry_price * (1 + _gap_abort):
                exit_p = round(op * (1 - SLIP - COMMISSION), 4)
                return date, exit_p, "gap_abort", days

        # ── ★ v8.4 第2天强制止损：上一天已标记 → 今天开盘出场 ─────
        if force_stop_day3:
            exit_p = round(op * (1 - SLIP - COMMISSION), 4)
            return date, exit_p, "day2_stop", days

        # ── ★ v9.0 断板止损（竞价模式）：涨停后次日跌破涨停价×(1-阈值) ──
        if is_auction and AUCTION_ZT_TRAIL_ENABLE and last_zt_day_in_hold >= 0 and days > 1:
            prev_row = df.iloc[j - 1]
            if bool(prev_row.get("is_zt", False)):
                prev_close_zt = float(prev_row["收盘"])
                zt_price_ref  = prev_close_zt  # 上个涨停日的收盘价=本日涨停价参考
                # 今日开盘跌破涨停价5%以上（断板低开），直接止损
                if op < zt_price_ref * (1 + AUCTION_ZT_BREAK_STOP):
                    exit_p = round(op * (1 - SLIP - COMMISSION), 4)
                    return date, exit_p, "zt_break_stop", days

        # ── 2. ★ v4.0 止损优先级修复：同日触发时取更优出场价 ──────
        if not t1_hit:
            # 时间止损收紧价
            tighten_stop = entry_price * (1 + TIME_TIGHTEN_LOSS) if days > TIME_TIGHTEN_DAYS else None
            # 当前有效止损价（取时间止损和ATR止损中更高（更宽松）的那个）
            effective_stop = stop_price
            if tighten_stop is not None:
                effective_stop = max(stop_price, tighten_stop)
                stop_price = effective_stop  # 止损线只能向上移

            if lo <= effective_stop:
                # 同日触发：出场价取 min(止损价, 开盘价)，不能超过开盘价成交
                exit_p = min(effective_stop, op)
                exit_p = round(exit_p * (1 - SLIP - COMMISSION), 4)
                # 出场原因：时间止损和ATR止损都可能触发，按哪个价更高（亏更少）决定标签
                if tighten_stop is not None and effective_stop >= tighten_stop:
                    reason_label = "time_stop"
                else:
                    reason_label = "stop_loss"
                return date, exit_p, reason_label, days

        # ── ★ v8.4 第2天强制止损：收盘亏损检测 ───────────────────
        # 第2天（days==2）收盘检测：若亏损>阈值 且 未触发豁免条件 → 标记明天强制止损
        if DAY2_FORCE_STOP_ENABLE and days == 2 and not t1_hit:
            day2_ret = (cls - entry_price) / entry_price
            if day2_ret < DAY2_FORCE_STOP_LOSS:
                # 检查缩量豁免：缩量说明主力未砸盘，给宽容
                cur_vol = float(row.get("成交量", 0) or 0)
                is_shrink = (vol_ma5 > 0 and cur_vol < vol_ma5 * DAY2_FORCE_STOP_VOL_RATIO)
                if not (DAY2_FORCE_STOP_VOL_EXEMPT and is_shrink):
                    force_stop_day3 = True  # 明天开盘强制止损

        # ── ★ v9.0 竞价涨停跟踪：当日涨停则记录，重置持仓延长计数 ──
        if is_auction and AUCTION_ZT_TRAIL_ENABLE:
            cur_is_zt = bool(row.get("is_zt", False))
            if cur_is_zt:
                last_zt_day_in_hold = j  # 记录最新涨停日索引

        # ── 3. ★ 优化⑤：T2后追踪止盈（不立即出，让利润奔跑）─────
        if T2_TRAIL_ENABLE:
            if not t2_hit and hi >= t2_price:
                t2_hit = True   # 触达T2，激活追踪止盈，不立刻出场
                t1_hit = True   # 同时激活T1跟踪
            if t2_hit:
                # ★ v6.0 量能自适应：成交量萎缩时收紧容忍率，成交量放大时放宽
                if T2_VOL_ADAPTIVE_ENABLE and vol_ma5 > 0:
                    cur_vol = float(row.get("成交量", 0) or 0)
                    vol_ratio_now = cur_vol / vol_ma5 if vol_ma5 > 0 else 1.0
                    # 容忍率随量比的平方根变化（量比越低→容忍率越低→更快锁利）
                    adaptive_tol = T2_TRAIL_TOLERANCE * (vol_ratio_now ** T2_VOL_ADAPTIVE_POWER)
                    adaptive_tol = max(T2_VOL_ADAPTIVE_MIN, min(T2_VOL_ADAPTIVE_MAX, adaptive_tol))
                else:
                    adaptive_tol = T2_TRAIL_TOLERANCE
                # 从最高点回撤超过自适应容忍率比例的已实现利润则出
                profit_from_entry = peak_price - entry_price
                t2_trail_stop = peak_price - profit_from_entry * adaptive_tol
                if cls <= t2_trail_stop:
                    exit_p = round(cls * (1 - SLIP - COMMISSION), 4)
                    return date, exit_p, "t2_trail", days
        else:
            # 兼容关闭开关时的原始逻辑
            if hi >= t2_price:
                exit_p = round(t2_price * (1 - SLIP - COMMISSION), 4)
                return date, exit_p, "target2", days

        # ── 4. T1后启动智能跟踪止盈 ──────────────────────────────
        if not t1_hit and hi >= t1_price:
            t1_hit = True  # 激活跟踪止盈，不立刻出场，继续等更高利润

        if t1_hit and not t2_hit:   # T2追踪止盈优先，T1跟踪作为补充
            trail_p = _trail_stop_price(entry_price, peak_price, row, vol_ma5)
            # 收盘跌至跟踪止盈线下方则出场（用收盘价避免盘中噪音）
            if cls <= trail_p:
                exit_p = round(cls * (1 - SLIP - COMMISSION), 4)
                return date, exit_p, "trail_stop", days

        # ── 5. ★ v4.0 趋势止损：改为MA10，持仓≥5天生效 ───────────
        # MA10 比 MA5 更稳定，持仓5天才生效避免早期噪音触发
        if days >= 5:
            ma10_close = df["收盘"].iloc[max(0, j - 9):j + 1].mean()
            if cls < ma10_close * 0.99:
                exit_p = round(cls * (1 - SLIP - COMMISSION), 4)
                return date, exit_p, "ma10_stop", days

        # ── ★ v8.4 强势延长持仓 ────────────────────────────────────
        # 连续2天收盘接近最高价（close_pos>阈值），每次延长1天，最多额外+3天
        if EXTEND_HOLD_ON_STRENGTH and not t1_hit:
            close_pos_cur = float(row.get("close_pos", 0.5) or 0.5)
            if close_pos_cur >= EXTEND_HOLD_CLOSE_POS_THRESH:
                consecutive_strong += 1
                if (consecutive_strong >= EXTEND_HOLD_CONSECUTIVE_DAYS
                        and extra_hold_days < EXTEND_HOLD_MAX_EXTRA):
                    extra_hold_days += 1  # 再给一天，让强势行情继续跑
            else:
                consecutive_strong = 0  # 出现弱势日，重置计数

        # ── ★ v9.1 次日竞价方向确认出场（弱开+收盘位置差→当日收盘出）─
        # 适用场景：竞价入场后持仓第2天以后，开盘方向"微弱高开"但尾盘无力
        # 防止"开盘高开骗局"→开盘诱多后盘中回落
        if (is_auction and AUCTION_NEXT_DAY_CONFIRM_ENABLE
                and days >= 2 and not t1_hit and not t2_hit):
            # 弱开条件：开盘相对前收盘涨幅 < AUCTION_NEXT_DAY_WEAK_OPEN_MAX
            # 且不是大幅低开（大幅低开已由gap_abort/zt_break_stop处理）
            prev_close_ref = float(df.iloc[j - 1]["收盘"])
            if prev_close_ref > 0:
                open_chg_ref = (op - prev_close_ref) / prev_close_ref
                is_weak_open = (open_chg_ref < AUCTION_NEXT_DAY_WEAK_OPEN_MAX
                                and op > entry_price * (1 + AUCTION_GAP_ABORT))
                if is_weak_open:
                    close_pos_today = float(row.get("close_pos", 0.5) or 0.5)
                    if close_pos_today < AUCTION_NEXT_DAY_CLOSE_POS_MIN:
                        # 弱开 + 收盘无力（不在高位）= 筹码松动，收盘出场
                        exit_p = round(cls * (1 - SLIP - COMMISSION), 4)
                        return date, exit_p, "weak_confirm_stop", days

        # ── 6. 超时平仓 ───────────────────────────────────────────
        max_days_now = _effective_max_days()
        if days >= max_days_now:
            exit_p = round(cls * (1 - SLIP - COMMISSION), 4)
            return date, exit_p, "timeout", days

    # 数据不足，用最后一天收盘平仓
    last_idx = min(entry_idx + _effective_max_days() - 1, n - 1)
    last = df.iloc[last_idx]
    return str(last["date"])[:10], round(float(last["收盘"]) * (1 - SLIP - COMMISSION), 4), "timeout", last_idx - entry_idx + 1


# ================================================================
# 策略1：首板
# ================================================================
def backtest_first_board(code: str, df: pd.DataFrame) -> list:
    """
    信号：当日 zt_streak == 1（首次涨停，昨日未涨停）
    升级过滤：
      ★ 大盘/趋势：股价须在MA20之上（上升通道），近5日动量不能过差
      ★ 入场保护：次日开盘若低于涨停价×(1-ENTRY_MAX_GAP_DOWN)，说明情绪不延续，不追
      ★ 封板强度：收盘位置(close_pos)<0.85 说明尾盘有出货，弱封不入场
      ★ 量比：1.0~8.0
      ★ 假涨停/股性过滤（同前）
      ★ 振幅：>1.5%（迟钝股不做）
    """
    trades = []
    n = len(df)

    for i in range(1, n - 1):
        row = df.iloc[i]

        # 节后首日（国庆/春节/五一等长假后第一天）不入场，高开低走风险极高
        if bool(row.get("is_post_holiday", False)):
            continue

        if not row["is_zt"]:
            continue
        if df.iloc[i - 1]["is_zt"]:
            continue  # 昨日也涨停，不是首板

        vol_ratio   = float(row.get("vol_ratio", 1.0))
        is_bomb     = bool(row.get("is_bomb", False))
        bomb_30d    = int(row.get("bomb_30d", 0))
        is_yizi     = bool(row.get("is_yizi", False))
        avg_amp     = float(row.get("avg_amp_20d", 0.0) or 0.0)
        avg_amt_m   = float(row.get("avg_amt_20d", 0.0) or 0.0)
        yizi_ratio  = float(row.get("yizi_ratio_30d", 0.0) or 0.0)
        above_ma20  = bool(row.get("above_ma20", True))
        mom_5d      = float(row.get("mom_5d", 0.0) or 0.0)
        close_pos   = float(row.get("close_pos", 1.0) or 1.0)

        if not ENABLE_FACTOR_FILTER:
            pass
        else:
            # ── 大盘/趋势过滤 ──────────────────────────────────────
            if FILTER_STOCK_ABOVE_MA20 and not above_ma20:
                continue  # 股价在MA20之下，弱势不做首板
            if mom_5d < FILTER_MOMENTUM_5D:
                continue  # 近5日跌幅过大，趋势向下不追

            # ── 假涨停硬过滤 ───────────────────────────────────────
            if FILTER_BOMB_DAY and is_bomb:
                continue
            if bomb_30d > FILTER_BOMB_HISTORY_MAX:
                continue
            if vol_ratio > FILTER_VOL_RATIO_MAX:
                continue
            if vol_ratio < VOL_RATIO_LOW:
                continue

            # ★ v7.0：首板量比收窄至1.5~2.2（回测数据：高量比首板多为出货拉停）
            # 原上限2.5降至2.2，进一步排除分歧较大的首板
            if vol_ratio > 2.2:
                continue  # 首板量比上限2.2（量比越高首板越可能是出货）

            # ── 封板强度过滤：回测数据上调至0.95（弱封极少成功）──
            if close_pos < 0.95:
                continue  # 尾盘出货明显，跳过（从0.92上调至0.95）

            # ── 炸板历史过滤：首板零容忍（回测：炸板1次胜率降20%+）──
            if bomb_30d > 0:
                continue

            # ── 股性过滤（振幅1.5%~6%，超活跃>5%段回测表现最优）──
            if FILTER_YIZI_BOARD and is_yizi:
                continue
            if avg_amt_m > 0 and avg_amt_m * 1e6 < FILTER_MIN_AVG_AMOUNT:
                continue
            if yizi_ratio >= FILTER_MAX_YIZI_RATIO:
                continue
            if avg_amp > 0 and (avg_amp < 1.5 or avg_amp > 6.0):
                continue  # 振幅上限从4%放开至6%（超活跃段胜率66.7%/+11.7%）

            # ── ★ v5.0 炸板后再封陷阱检测 ──────────────────────────
            # 近N日内反复出现"炸板后当日强势收盘"特征，说明主力惯用此手法诱多
            if FILTER_RECLOSE_TRAP:
                reclose_cnt = int(row.get("reclose_trap_cnt", 0) or 0)
                if reclose_cnt > RECLOSE_TRAP_MAX:
                    continue

            # ── ★ v6.0 量价背离过滤 ──────────────────────────────
            # 涨停日缩量（量<20日均量×0.8）是假突破信号，直接过滤
            if FILTER_VOL_PRICE_DIVERGE:
                if bool(row.get("vol_price_diverge", False)):
                    continue

            # ── ★ v8.1 财务质量过滤（TradingAgents-CN财务数据模式）──
            if ENABLE_FINANCIAL_FILTER and not check_financial_quality(code):
                break   # 同一只股票财务不过关，跳过全部信号

            # ── ★ v6.0 情绪综合评分过滤（只做高质量信号）──────────
            score = float(row.get("signal_score", 50.0) or 50.0)
            # ★ v8.1 叠加新闻情绪分（仅实盘/ENABLE_NEWS_SCORE=True时有效）
            news_score = fetch_news_sentiment_score(code)
            score_with_news = min(score + news_score, 110.0)   # 总分上限110
            if SIGNAL_SCORE_ENABLE and score_with_news < SIGNAL_SCORE_MIN_FIRST:
                continue  # 评分不达标，信号质量不够，跳过

        if i + 1 >= n:
            break
        nxt = df.iloc[i + 1]
        nxt_open    = float(nxt["开盘"])
        zt_price    = float(row["prev_close"] or 0) * 1.10  # 涨停价
        if nxt_open <= 0:
            continue

        # ── 入场保护：次日低开过多不追 ────────────────────────────
        if zt_price > 0:
            gap = (nxt_open - zt_price) / zt_price
            if gap < ENTRY_MAX_GAP_DOWN:
                continue  # 次日相对涨停价低开过多，情绪不延续

        # ── ★ v5.0 信号执行随机延迟（降低行为可预测性）────────────
        if SIGNAL_DELAY_RANDOM:
            _delay_seed = SIGNAL_DELAY_SEED if SIGNAL_DELAY_SEED is not None else None
            _local_rng = random.Random(_delay_seed)
            if _local_rng.random() < 0.30:
                continue

        # ── ★ v6.0 动态持仓天数：按信号评分自动调整 ─────────────
        score = float(row.get("signal_score", 50.0) or 50.0)
        # ★ v8.1 动态持仓也用含新闻情绪的合成分
        news_score_dyn = fetch_news_sentiment_score(code) if ENABLE_NEWS_SCORE else 0.0
        score_final = min(score + news_score_dyn, 110.0)
        if DYNAMIC_HOLD_ENABLE:
            if score_final >= DYNAMIC_HOLD_HIGH_THRESH:
                _dyn_hold = HOLD_DAYS_STRONG   # 强势信号延长持仓
            elif score_final <= DYNAMIC_HOLD_LOW_THRESH:
                _dyn_hold = HOLD_DAYS_WEAK     # 弱势信号缩短持仓
            else:
                _dyn_hold = HOLD_DAYS          # 中间段保持默认
        else:
            _dyn_hold = HOLD_DAYS

        entry_p    = nxt_open * (1 + SLIP + COMMISSION)
        entry_date = str(nxt["date"])[:10]
        atr_stop = float(row.get("atr_stop_pct", STOP_LOSS) or STOP_LOSS)
        _trade_rng = random.Random()
        exit_date, exit_p, reason, hdays = simulate_exit(df, i + 1, entry_p, atr_stop, _trade_rng, _dyn_hold)
        pnl = (exit_p - entry_p) / entry_p

        trades.append(Trade(
            code=code, strategy="首板",
            entry_date=entry_date, entry_price=round(entry_p, 4),
            exit_date=exit_date, exit_price=exit_p,
            exit_reason=reason, pnl_pct=round(pnl, 4),
            holding_days=hdays, zt_days=1,
            vol_ratio=round(vol_ratio, 2),
            is_bomb_day=is_bomb,
            bomb_30d=int(bomb_30d),
            is_yizi=is_yizi,
            avg_amplitude=round(avg_amp, 2),
            avg_amount_m=round(avg_amt_m, 1),
            signal_score=round(score_final, 1),   # ★v8.1 含新闻情绪的合成分
            dynamic_hold=_dyn_hold,
        ))

    return trades


# ================================================================
# 策略2：连板（买次日竞价）
# ================================================================
def backtest_connect_board(code: str, df: pd.DataFrame,
                           min_days: int = MIN_ZT_DAYS,
                           max_days: int = MAX_ZT_DAYS) -> list:
    """
    信号：zt_streak 在 [min_days, max_days] 范围内
    升级过滤：
      ★ 大盘/趋势：股价须在MA20之上，近5日动量不能过差
      ★ 封板强度：close_pos < 0.90（连板要求更严）
      ★ 入场保护：次日开盘相对昨日涨停价低开超阈值，不追
      ★ 连板量比：<4.0（分歧过大不追）
      ★ 假涨停/股性过滤（同前）
    """
    trades = []
    n = len(df)

    for i in range(1, n - 1):
        # 节后首日不入场
        if bool(df.iloc[i].get("is_post_holiday", False)):
            continue
        streak     = int(df.iloc[i]["zt_streak"])
        vol_ratio  = float(df.iloc[i].get("vol_ratio", 1.0))
        is_bomb    = bool(df.iloc[i].get("is_bomb", False))
        bomb_30d   = int(df.iloc[i].get("bomb_30d", 0))
        is_yizi    = bool(df.iloc[i].get("is_yizi", False))
        avg_amp    = float(df.iloc[i].get("avg_amp_20d", 0.0) or 0.0)
        avg_amt_m  = float(df.iloc[i].get("avg_amt_20d", 0.0) or 0.0)
        yizi_ratio = float(df.iloc[i].get("yizi_ratio_30d", 0.0) or 0.0)
        above_ma20 = bool(df.iloc[i].get("above_ma20", True))
        mom_5d     = float(df.iloc[i].get("mom_5d", 0.0) or 0.0)
        close_pos  = float(df.iloc[i].get("close_pos", 1.0) or 1.0)

        if streak < min_days or streak > max_days:
            continue

        if ENABLE_FACTOR_FILTER:
            # ── 大盘/趋势过滤 ──────────────────────────────────────
            if FILTER_STOCK_ABOVE_MA20 and not above_ma20:
                continue
            if mom_5d < FILTER_MOMENTUM_5D:
                continue

            if FILTER_BOMB_DAY and is_bomb:
                continue
            if bomb_30d > FILTER_BOMB_HISTORY_MAX:
                continue
            if vol_ratio > FILTER_VOL_RATIO_MAX:
                continue
            if vol_ratio > 4.0:
                continue  # 连板换手率过高 = 分歧大

            # ── 封板强度过滤（连板要求更严：>0.90） ───────────────
            if close_pos < 0.90:
                continue  # 连板尾盘出货，信号更危险

            # ── 股性过滤 ───────────────────────────────────────────
            if FILTER_YIZI_BOARD and is_yizi:
                continue
            if avg_amt_m > 0 and avg_amt_m * 1e6 < FILTER_MIN_AVG_AMOUNT:
                continue
            if yizi_ratio >= FILTER_MAX_YIZI_RATIO:
                continue
            if avg_amp > 0 and avg_amp < 1.5:
                continue

            # ── ★ v5.0 炸板后再封陷阱检测 ──────────────────────────
            if FILTER_RECLOSE_TRAP:
                reclose_cnt = int(df.iloc[i].get("reclose_trap_cnt", 0) or 0)
                if reclose_cnt > RECLOSE_TRAP_MAX:
                    continue

            # ── ★ v6.0 量价背离过滤 ──────────────────────────────
            if FILTER_VOL_PRICE_DIVERGE:
                if bool(df.iloc[i].get("vol_price_diverge", False)):
                    continue

            # ── ★ v6.0 情绪综合评分过滤 ─────────────────────────
            # 连板策略门槛与首板一致（连板本身已是高动量，不额外抬高门槛）
            score_c = float(df.iloc[i].get("signal_score", 50.0) or 50.0)
            if SIGNAL_SCORE_ENABLE and score_c < SIGNAL_SCORE_MIN_FIRST:
                continue

        if i + 1 >= n:
            break
        nxt = df.iloc[i + 1]
        nxt_open = float(nxt["开盘"])
        zt_price = float(df.iloc[i].get("prev_close", 0) or 0) * 1.10
        if nxt_open <= 0:
            continue

        # ── 入场保护：次日低开过多不追 ────────────────────────────
        if zt_price > 0:
            gap = (nxt_open - zt_price) / zt_price
            if gap < ENTRY_MAX_GAP_DOWN:
                continue

        # ★ v5.0 信号执行随机延迟
        if SIGNAL_DELAY_RANDOM:
            _local_rng = random.Random(SIGNAL_DELAY_SEED)
            if _local_rng.random() < 0.30:
                continue

        # ★ v6.0 动态持仓天数
        score_c = float(df.iloc[i].get("signal_score", 50.0) or 50.0)
        if DYNAMIC_HOLD_ENABLE:
            if score_c >= DYNAMIC_HOLD_HIGH_THRESH:
                _dyn_hold = HOLD_DAYS_STRONG
            elif score_c <= DYNAMIC_HOLD_LOW_THRESH:
                _dyn_hold = HOLD_DAYS_WEAK
            else:
                _dyn_hold = HOLD_DAYS
        else:
            _dyn_hold = HOLD_DAYS

        entry_p    = nxt_open * (1 + SLIP + COMMISSION)
        entry_date = str(nxt["date"])[:10]
        atr_stop = float(df.iloc[i].get("atr_stop_pct", STOP_LOSS) or STOP_LOSS)
        _trade_rng = random.Random()
        exit_date, exit_p, reason, hdays = simulate_exit(df, i + 2, entry_p, atr_stop, _trade_rng, _dyn_hold)
        pnl = (exit_p - entry_p) / entry_p

        trades.append(Trade(
            code=code, strategy="连板",
            entry_date=entry_date, entry_price=round(entry_p, 4),
            exit_date=exit_date, exit_price=exit_p,
            exit_reason=reason, pnl_pct=round(pnl, 4),
            holding_days=hdays, zt_days=streak,
            vol_ratio=round(vol_ratio, 2),
            is_bomb_day=is_bomb,
            bomb_30d=int(bomb_30d),
            is_yizi=is_yizi,
            avg_amplitude=round(avg_amp, 2),
            avg_amount_m=round(avg_amt_m, 1),
            signal_score=round(score_c, 1),
            dynamic_hold=_dyn_hold,
        ))

    return trades


# ================================================================
# 策略3：竞价打板（细分5档子策略）
# ================================================================
def backtest_auction_board(code: str, df: pd.DataFrame) -> list:
    """
    竞价高开细分3档 × 昨日是否涨停，共5个子策略标签：

    高开区间        昨日涨停    子策略标签       特点
    ─────────────────────────────────────────────────────
    +2%~+5%(温和)   是          竞价-连板-温和   空间最大，涨停后延续
    +2%~+5%(温和)   否          竞价-首板-温和   随机首板，量比要求最高(≥2.5)
    +5%~+7%(强势)   是          竞价-连板-强势   均衡空间与强度
    +5%~+7%(强势)   否          竞价-首板-强势   随机强势高开，量比≥2.0
    +7%~+9.9%(极限) 是          竞价-连板-极限   接近涨停，仅做昨日涨停后，封板强度要求高
    +7%~+9.9%(极限) 否          ← 直接跳过（空间极窄却无逻辑支撑，不做）
    ─────────────────────────────────────────────────────
    """
    trades = []
    n = len(df)

    for i in range(1, n - 1):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]

        # 节后首日不入场
        if bool(row.get("is_post_holiday", False)):
            continue

        prev_close     = float(row["prev_close"] or 0)
        open_p         = float(row["开盘"])
        vol_ratio      = float(row.get("vol_ratio", 1.0))
        bomb_30d       = int(row.get("bomb_30d", 0))
        avg_amp        = float(row.get("avg_amp_20d", 0.0) or 0.0)
        avg_amt_m      = float(row.get("avg_amt_20d", 0.0) or 0.0)
        above_ma20     = bool(row.get("above_ma20", True))
        mom_5d         = float(row.get("mom_5d", 0.0) or 0.0)
        close_pos      = float(row.get("close_pos", 1.0) or 1.0)
        prev_is_zt     = bool(prev_row["is_zt"])
        # v9.1: 前一日封板力度（用前一日数据判断昨日涨停的质量）
        prev_close_pos = float(prev_row.get("close_pos", 1.0) or 1.0)
        # v9.1: 连板天数（用于连板梯队识别）
        zt_streak      = int(row.get("zt_streak", 0) or 0)   # 截至今日的连续涨停天数（包含昨日）
        # v9.1: 今日分时量比持续性代理（日成交量/5日均量）
        vol_ma5_today  = float(df["成交量"].iloc[max(0, i - 4):i + 1].mean()) if i > 0 else 1.0
        today_vol      = float(row["成交量"])
        intraday_vol_sustain = (today_vol / vol_ma5_today) if vol_ma5_today > 0 else 1.0

        if prev_close <= 0 or open_p <= 0:
            continue

        auction_chg = (open_p - prev_close) / prev_close

        # ── 判断高开档位 ──────────────────────────────────────────
        if AUCTION_MILD_LOW <= auction_chg < AUCTION_MILD_HIGH:
            tier = "温和"       # +2%~+5%
        elif AUCTION_STRONG_LOW <= auction_chg < AUCTION_STRONG_HIGH:
            tier = "强势"       # +5%~+7%
        elif AUCTION_LIMIT_LOW <= auction_chg < AUCTION_LIMIT_HIGH:
            tier = "极限"       # +7%~+9.9%
        else:
            continue            # 不在任何档位内，跳过

        # ── 极限高开：仅做昨日涨停后（空间极窄，无逻辑支撑不做）──
        if tier == "极限" and not prev_is_zt:
            continue

        # ── 确定子策略标签 ────────────────────────────────────────
        if prev_is_zt:
            sub_strategy = f"竞价-连板-{tier}"
        else:
            sub_strategy = f"竞价-首板-{tier}"

        # ── ★ v9.0 大盘情绪硬过滤（全市场涨停密度）────────────────
        if MARKET_ZT_FILTER_ENABLE or SECTOR_HEAT_FILTER_ENABLE:
            import builtins
            _zt_ratio_map = getattr(builtins, "_daily_zt_ratio", {})
            _zt_count_map = getattr(builtins, "_daily_zt_counts", {})
            _today_str    = str(row["date"])[:10]
            _today_ratio  = _zt_ratio_map.get(_today_str, 1.0)  # 默认1.0=不过滤（无数据时通过）
            _today_count  = _zt_count_map.get(_today_str, 99)
            # 大盘涨停密度过滤：涨停占比太低=市场极度冷淡，竞价高开是陷阱
            if MARKET_ZT_FILTER_ENABLE and _today_ratio < MARKET_ZT_MIN_RATIO:
                continue
            # 板块热度代理过滤：当日涨停家数太少，没有板块联动效应
            if SECTOR_HEAT_FILTER_ENABLE and _today_count < SECTOR_HEAT_MIN_SIGNALS:
                continue
        else:
            _today_ratio = 1.0  # 不过滤时默认1.0

        # ── 过滤条件（按档位 + 昨日状态分别设置）────────────────
        if ENABLE_FACTOR_FILTER:
            # 通用过滤：流动性 + MA20 + 近期动量
            if avg_amt_m > 0 and avg_amt_m * 1e6 < FILTER_MIN_AVG_AMOUNT:
                continue
            if FILTER_STOCK_ABOVE_MA20 and not above_ma20:
                continue
            if mom_5d < FILTER_MOMENTUM_5D:
                continue

            # ★ v9.1：分时量比持续性过滤（盘中无人接盘=竞价假信号）
            if INTRADAY_VOL_SUSTAIN_ENABLE and intraday_vol_sustain < INTRADAY_VOL_SUSTAIN_RATIO:
                continue

            if prev_is_zt:
                # ── 昨日涨停后竞价 ────────────────────────────────
                # ★ v9.1：封板力度过滤（前一日封板是否稳固）
                if SEAL_SCORE_STRICT_FILTER and prev_close_pos < SEAL_SCORE_WEAK_THRESH:
                    continue  # 前一日封板不稳（尾盘开板或炸板再封），信号质量低

                # ★ v9.1：4+连板竞价风险控制
                if AUCTION_ZT_STREAK_POS_ENABLE and zt_streak >= 4:
                    if not AUCTION_ZT_4PLUS_ENABLE:
                        continue   # 4+连板竞价全部停用
                    elif tier != AUCTION_ZT_4PLUS_MAX_TIER:
                        continue   # 4+连板只做指定档位

                if tier == "温和":
                    # 温和高开+有涨停背景：量比要求不高，但需要封板能力
                    if vol_ratio < VOL_RATIO_LOW:
                        continue
                    if bomb_30d > FILTER_BOMB_HISTORY_MAX:
                        continue
                    if avg_amp > 0 and (avg_amp < 1.5 or avg_amp > 6.0):  # 振幅上限6%
                        continue
                elif tier == "强势":
                    # 强势高开：高开已消耗空间，量比要求提升（回测强势胜率61.8%低于温和75%，说明需更严筛选）
                    if vol_ratio < 2.0:  # ★v9.2: 从VOL_RATIO_LOW(1.5)提高至2.0
                        continue
                    if bomb_30d > FILTER_BOMB_HISTORY_MAX:
                        continue
                    if avg_amp > 0 and (avg_amp > 6.0 or avg_amp < 1.5):  # 振幅上限6%
                        continue
                else:  # 极限
                    # 极限高开：空间极窄，要求股性强（前一日封板质量高）
                    if vol_ratio < VOL_RATIO_LOW:
                        continue
                    if bomb_30d > 0:           # 极限档零容忍炸板
                        continue
                    # ★ v9.1：极限档进化：前一日close_pos > 0.90（超级稳固）
                    if prev_close_pos < AUCTION_EXTREME_PREV_CLOSE_POS:
                        continue
                    # ★ v9.1：极限档连板天数上限（4+连板极限=过热，不做）
                    if zt_streak > AUCTION_EXTREME_MAX_STREAK:
                        continue
                    # ★ v9.1：极限档要求前一日成交量相对放量
                    prev_vol = float(prev_row.get("成交量", 0) or 0)
                    prev_vol_ma10 = float(df["成交量"].iloc[max(0, i - 10):i].mean()) if i > 0 else 0.0
                    if prev_vol_ma10 > 0 and prev_vol < prev_vol_ma10 * AUCTION_EXTREME_PREV_VOL_RATIO:
                        continue  # 前一日非真实放量涨停，极限档过滤
            else:
                # ── 随机首板竞价（无涨停背景，过滤更严）────────────
                if bomb_30d > 0:               # 首板竞价零容忍炸板历史
                    continue
                if vol_ratio < VOL_RATIO_LOW:  # 量比必须≥3
                    continue
                if avg_amp > 0 and (avg_amp < 1.5 or avg_amp > 4.0):
                    continue
                if tier == "温和":
                    # 温和高开无涨停背景：量比要求最高（必须有资金强烈介入）
                    if vol_ratio < 2.5:
                        continue
                else:  # 强势（极限已在上方被过滤掉）
                    if vol_ratio < 2.5:  # ★v9.2: 从2.0提高至2.5（首板强势高开=风险最大，量比必须更充足）
                        continue

            # ── ★ v5.0 竞价虚假挂单检测（反钓鱼盘）──────────────────
            if FILTER_FAKE_AUCTION:
                fake_ratio = float(row.get("fake_auction_ratio", 0.0) or 0.0)
                if fake_ratio >= FAKE_AUCTION_THRESH:
                    continue

            # ── ★ v5.0 炸板后再封陷阱检测 ──────────────────────────
            if FILTER_RECLOSE_TRAP:
                reclose_cnt = int(row.get("reclose_trap_cnt", 0) or 0)
                if reclose_cnt > RECLOSE_TRAP_MAX:
                    continue

            # ── ★ v6.0 情绪综合评分过滤（竞价策略）─────────────────
            score_a = float(row.get("signal_score", 50.0) or 50.0)
            if SIGNAL_SCORE_ENABLE and score_a < SIGNAL_SCORE_MIN_AUCTION:
                continue

        # ★ v5.0 信号执行随机延迟
        if SIGNAL_DELAY_RANDOM:
            _local_rng = random.Random(SIGNAL_DELAY_SEED)
            if _local_rng.random() < 0.30:
                continue

        # ★ v6.0 动态持仓天数
        score_a = float(row.get("signal_score", 50.0) or 50.0)
        if DYNAMIC_HOLD_ENABLE:
            if score_a >= DYNAMIC_HOLD_HIGH_THRESH:
                _dyn_hold = HOLD_DAYS_STRONG
            elif score_a <= DYNAMIC_HOLD_LOW_THRESH:
                _dyn_hold = HOLD_DAYS_WEAK
            else:
                _dyn_hold = HOLD_DAYS
        else:
            _dyn_hold = HOLD_DAYS

        # ── ★ v9.0 子策略白名单过滤（只做盈利子策略）────────────────
        if AUCTION_WHITELIST_ENABLE and sub_strategy not in AUCTION_WHITELIST:
            continue  # 白名单外的子策略（连板-温和/-0.73%，连板-强势/-1.23%）停用

        # ★ v9.1：连板梯队仓位差异化（影响实际仓位，此处记录仓位系数）
        _pos_ratio = 1.0
        if AUCTION_ZT_STREAK_POS_ENABLE and prev_is_zt:
            if zt_streak == 2:
                _pos_ratio = AUCTION_ZT_2_POS_RATIO   # 2连板重仓
            elif zt_streak == 3:
                _pos_ratio = AUCTION_ZT_3_POS_RATIO   # 3连板降仓
            # 4+连板已在上面的过滤中处理（默认停用）

        # ★ v9.1：大盘超热时放大仓位（涨停密度>2%）
        if MARKET_HOT_BOOST_ENABLE:
            if _today_ratio >= MARKET_HOT_BOOST_RATIO_THRESH:
                _pos_ratio = min(_pos_ratio * MARKET_HOT_BOOST_POS_RATIO,
                                 MARKET_HOT_BOOST_MAX_RATIO)

        entry_p    = round(open_p * (1 + SLIP + COMMISSION), 4)
        entry_date = str(row["date"])[:10]
        atr_stop = float(row.get("atr_stop_pct", STOP_LOSS) or STOP_LOSS)
        _trade_rng = random.Random()
        # ★ v9.0/v9.1: 传入 is_auction=True，启用竞价专用止损+涨停跟踪出场
        exit_date, exit_p, reason, hdays = simulate_exit(
            df, i + 1, entry_p, atr_stop, _trade_rng, _dyn_hold,
            is_auction=True,
            auction_next_day_df=df,   # v9.1：传入df用于次日确认逻辑
            auction_entry_idx=i + 1,  # v9.1：入场日索引
        )
        pnl = (exit_p - entry_p) / entry_p

        trades.append(Trade(
            code=code, strategy=sub_strategy,
            entry_date=entry_date, entry_price=entry_p,
            exit_date=exit_date, exit_price=exit_p,
            exit_reason=reason, pnl_pct=round(pnl, 4),
            holding_days=hdays, zt_days=int(zt_streak),
            vol_ratio=round(vol_ratio, 2),
            bomb_30d=int(bomb_30d),
            avg_amplitude=round(avg_amp, 2),
            avg_amount_m=round(avg_amt_m, 1),
            signal_score=round(score_a, 1),
            dynamic_hold=_dyn_hold,
        ))

    return trades




# ================================================================
# 策略4：跌停反包（v6.0 质量升级版）
# ================================================================
def backtest_dt_recover(code: str, df: pd.DataFrame) -> list:
    """
    信号：昨日跌停，今日开盘相对昨收涨幅 ≥ DT_RECOVER_MIN_OPEN_CHG（情绪修复）
    ★ v6.0 反包质量升级：
      - 仅做单次恐慌跌停（连续跌停=基本面利空，不做）
      - 检测跌停前5日动量，已在下跌通道的不做
      - 开盘涨幅门槛从3%提高到4%，确保情绪真实修复
      - 加入情绪评分过滤（反包天然评分低，门槛适当放低）
      - 动态持仓天数（反包强信号延长，弱信号快出）
    """
    trades = []
    n = len(df)

    for i in range(1, n - 1):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]

        # 节后首日不入场
        if bool(row.get("is_post_holiday", False)):
            continue

        if not prev_row["is_dt"]:
            continue

        prev_close = float(row["prev_close"] or 0)
        open_p     = float(row["开盘"])
        vol_ratio  = float(row.get("vol_ratio", 1.0))

        if prev_close <= 0 or open_p <= 0:
            continue

        open_chg = (open_p - prev_close) / prev_close
        # ★ v6.0：开盘涨幅门槛提高到4%（原3%），确保情绪修复是真实的
        if open_chg < DT_RECOVER_MIN_OPEN_CHG:
            continue

        # ── 基础过滤：量比 + 振幅 + 流动性 ──────────────────────
        if vol_ratio < VOL_RATIO_LOW:
            continue
        avg_amp   = float(row.get("avg_amp_20d", 0.0) or 0.0)
        bomb_30d  = int(row.get("bomb_30d", 0))
        avg_amt_m = float(row.get("avg_amt_20d", 0.0) or 0.0)
        if avg_amp > 0 and (avg_amp < 1.5 or avg_amp > 6.0):
            continue
        if bomb_30d > FILTER_BOMB_HISTORY_MAX:
            continue
        if avg_amt_m > 0 and avg_amt_m * 1e6 < FILTER_MIN_AVG_AMOUNT:
            continue

        # ── ★ v6.0 反包质量过滤（核心新增）────────────────────────
        if DT_RECOVER_QUALITY_FILTER:
            # 过滤1：连续跌停 = 基本面持续利空，不做反包
            dt_s = int(prev_row.get("dt_streak", 1))
            if dt_s > DT_RECOVER_MAX_STREAK:
                continue  # 连续跌停超过阈值，属于基本面下行，反包失败率极高

            # 过滤2：跌停前5日已大跌 = 已在下跌通道，非恐慌性单次跌停
            mom5_before = float(prev_row.get("mom_5d", 0.0) or 0.0)
            if mom5_before < DT_RECOVER_MIN_MOM5:
                continue  # 跌停前5日跌幅超15%，趋势性下跌，反包胜率极低

        # ── ★ v5.0 炸板后再封陷阱检测 ──────────────────────────────
        if FILTER_RECLOSE_TRAP:
            reclose_cnt = int(row.get("reclose_trap_cnt", 0) or 0)
            if reclose_cnt > RECLOSE_TRAP_MAX:
                continue

        # ── ★ v6.0 情绪综合评分过滤（反包门槛较低）───────────────
        score_r = float(row.get("signal_score", 50.0) or 50.0)
        if SIGNAL_SCORE_ENABLE and score_r < SIGNAL_SCORE_MIN_RECOVER:
            continue

        # ★ v5.0 信号执行随机延迟
        if SIGNAL_DELAY_RANDOM:
            _local_rng = random.Random(SIGNAL_DELAY_SEED)
            if _local_rng.random() < 0.30:
                continue

        # ★ v6.0 动态持仓天数
        if DYNAMIC_HOLD_ENABLE:
            if score_r >= DYNAMIC_HOLD_HIGH_THRESH:
                _dyn_hold = HOLD_DAYS_STRONG
            elif score_r <= DYNAMIC_HOLD_LOW_THRESH:
                _dyn_hold = HOLD_DAYS_WEAK
            else:
                _dyn_hold = HOLD_DAYS
        else:
            _dyn_hold = HOLD_DAYS

        entry_p    = round(open_p * (1 + SLIP + COMMISSION), 4)
        entry_date = str(row["date"])[:10]
        atr_stop = float(row.get("atr_stop_pct", STOP_LOSS) or STOP_LOSS)
        _trade_rng = random.Random()
        exit_date, exit_p, reason, hdays = simulate_exit(df, i + 1, entry_p, atr_stop, _trade_rng, _dyn_hold)
        pnl = (exit_p - entry_p) / entry_p

        trades.append(Trade(
            code=code, strategy="反包",
            entry_date=entry_date, entry_price=entry_p,
            exit_date=exit_date, exit_price=exit_p,
            exit_reason=reason, pnl_pct=round(pnl, 4),
            holding_days=hdays, zt_days=-1,
            vol_ratio=round(vol_ratio, 2),
            avg_amplitude=round(avg_amp, 2),
            avg_amount_m=round(avg_amt_m, 1),
            signal_score=round(score_r, 1),
            dynamic_hold=_dyn_hold,
        ))

    return trades


# ================================================================
# 策略5：涨停回调缩量再启动（v8.0新增）
# ================================================================
def backtest_zt_pullback(code: str, df: pd.DataFrame) -> list:
    """
    信号：近ZT_PULLBACK_LOOKBACK日内出现涨停，随后价格回调ZT_PULLBACK_MIN_DROP~MAX_DROP，
          回调期间成交量缩量（<涨停日量×ZT_PULLBACK_VOL_RATIO），
          当日收盘企稳，视为主力洗盘完毕准备再度拉升。
    参考：wxhui1024/Quantitative-Trading-System 涨停回调缩量策略 +
          nekolovepepper 因子Z-score标准化
    入场：当日收盘企稳确认，次日开盘买入
    出场：复用原止盈止损体系（ATR止损 + 跟踪止盈 + 时间止损）
    """
    trades = []
    n = len(df)

    for i in range(ZT_PULLBACK_LOOKBACK + 1, n - 1):
        row = df.iloc[i]

        # 节后首日不入场
        if bool(row.get("is_post_holiday", False)):
            continue

        # 当日必须是非涨停日（涨停日本身用其他策略）
        if bool(row.get("is_zt", False)):
            continue

        # 当日不能是跌停（跌停日流动性差且情绪极度悲观）
        if bool(row.get("is_dt", False)):
            continue

        # ── 检查近期是否有涨停 ────────────────────────────────────
        days_since = int(row.get("days_since_zt", -1))
        if days_since < ZT_PULLBACK_MIN_LOOKBACK or days_since > ZT_PULLBACK_LOOKBACK:
            continue  # 距离涨停日太近或太远

        last_zt_price = float(row.get("last_zt_price", 0.0) or 0.0)
        last_zt_vol   = float(row.get("last_zt_vol", 0.0) or 0.0)
        if last_zt_price <= 0 or last_zt_vol <= 0:
            continue

        # ── 检查回调幅度 ──────────────────────────────────────────
        cur_close  = float(row["收盘"])
        pullback   = (last_zt_price - cur_close) / last_zt_price  # 相对涨停日收盘的回调幅度
        if pullback < ZT_PULLBACK_MIN_DROP or pullback > ZT_PULLBACK_MAX_DROP:
            continue  # 回调幅度不在合理区间

        # ── 检查回调期间缩量 ──────────────────────────────────────
        # 回调期间（涨停后到今日）的日均成交量 vs 涨停日成交量
        zt_idx = int(row.get("last_zt_idx", -1))
        if zt_idx < 0 or i <= zt_idx:
            continue
        pullback_vols = df["成交量"].iloc[zt_idx + 1: i + 1]
        if len(pullback_vols) == 0:
            continue
        avg_pullback_vol = float(pullback_vols.mean())
        if avg_pullback_vol >= last_zt_vol * ZT_PULLBACK_VOL_RATIO:
            continue  # 回调期间未缩量，不符合洗盘特征

        # ── 当日企稳确认 ──────────────────────────────────────────
        # 当日收盘须高于回调期间最低价×(1+阈值)
        pullback_low = float(df["最低"].iloc[zt_idx + 1: i + 1].min())
        if cur_close < pullback_low * (1 + ZT_PULLBACK_STABLE_THRESH):
            continue  # 尚未企稳，继续下跌中不入场

        # ── 基础质量过滤 ──────────────────────────────────────────
        vol_ratio  = float(row.get("vol_ratio", 1.0))
        avg_amp    = float(row.get("avg_amp_20d", 0.0) or 0.0)
        avg_amt_m  = float(row.get("avg_amt_20d", 0.0) or 0.0)
        bomb_30d   = int(row.get("bomb_30d", 0))
        above_ma20 = bool(row.get("above_ma20", True))
        mom_5d     = float(row.get("mom_5d", 0.0) or 0.0)

        # 股价须在MA20之上（大趋势向上）
        if FILTER_STOCK_ABOVE_MA20 and not above_ma20:
            continue

        # 量比异常过滤
        if vol_ratio > FILTER_VOL_RATIO_MAX:
            continue

        # 振幅过滤（太迟钝的股没有弹性）
        if avg_amp > 0 and avg_amp < 1.5:
            continue

        # 流动性过滤
        if avg_amt_m > 0 and avg_amt_m * 1e6 < FILTER_MIN_AVG_AMOUNT:
            continue

        # 炸板历史过滤（回调缩量策略允许少量炸板历史）
        if bomb_30d > FILTER_BOMB_HISTORY_MAX:
            continue

        # 近5日动量不能过差
        if mom_5d < FILTER_MOMENTUM_5D:
            continue

        # ── ★v8.3：均线多头排列过滤（MA5>MA10>MA20，确保大趋势向上）────
        if ZT_PULLBACK_REQUIRE_MA_ALIGN:
            ma5  = df["收盘"].iloc[max(0, i - 4):i + 1].mean()
            ma10 = df["收盘"].iloc[max(0, i - 9):i + 1].mean()
            ma20 = df["收盘"].iloc[max(0, i - 19):i + 1].mean()
            if not (ma5 > ma10 > ma20):
                continue  # 均线未多头排列，趋势不支持，不做回调

        # ── 竞价虚假挂单检测 ─────────────────────────────────────
        if FILTER_FAKE_AUCTION:
            fake_r = float(row.get("fake_auction_ratio", 0.0) or 0.0)
            if fake_r >= FAKE_AUCTION_THRESH:
                continue

        # ── 评分过滤 ─────────────────────────────────────────────
        score_p = float(row.get("signal_score", 50.0) or 50.0)
        if SIGNAL_SCORE_ENABLE and score_p < SIGNAL_SCORE_MIN_PULLBACK:
            continue

        # ── 动态持仓天数 ─────────────────────────────────────────
        if DYNAMIC_HOLD_ENABLE:
            if score_p >= DYNAMIC_HOLD_HIGH_THRESH:
                _dyn_hold = HOLD_DAYS_STRONG
            elif score_p <= DYNAMIC_HOLD_LOW_THRESH:
                _dyn_hold = HOLD_DAYS_WEAK
            else:
                _dyn_hold = HOLD_DAYS
        else:
            _dyn_hold = HOLD_DAYS

        if i + 1 >= n:
            break
        nxt = df.iloc[i + 1]
        nxt_open = float(nxt["开盘"])
        if nxt_open <= 0:
            continue

        # 次日低开过多不追（相对当日收盘）
        gap = (nxt_open - cur_close) / cur_close
        if gap < OPEN_GAP_ABORT:
            continue

        entry_p    = round(nxt_open * (1 + SLIP + COMMISSION), 4)
        entry_date = str(nxt["date"])[:10]
        atr_stop   = float(row.get("atr_stop_pct", STOP_LOSS) or STOP_LOSS)
        _trade_rng = random.Random()
        exit_date, exit_p, reason, hdays = simulate_exit(df, i + 1, entry_p, atr_stop, _trade_rng, _dyn_hold)
        pnl = (exit_p - entry_p) / entry_p

        trades.append(Trade(
            code=code, strategy="回调缩量",
            entry_date=entry_date, entry_price=entry_p,
            exit_date=exit_date, exit_price=exit_p,
            exit_reason=reason, pnl_pct=round(pnl, 4),
            holding_days=hdays, zt_days=0,
            vol_ratio=round(vol_ratio, 2),
            bomb_30d=int(bomb_30d),
            avg_amplitude=round(avg_amp, 2),
            avg_amount_m=round(avg_amt_m, 1),
            signal_score=round(score_p, 1),
            dynamic_hold=_dyn_hold,
        ))

    return trades


# ================================================================
# ★ v8.2 策略7：洗盘抄底回测
# ================================================================
def backtest_washout_dip(code: str, df: pd.DataFrame) -> list:
    """
    信号：均线多头排列中，价格从近20日高点回调5%~25%，近3日成交量萎缩至10日均量75%以下，
          回踩MA5/MA10/MA20支撑位附近，且当日K线企稳（非大阴线/有下影线）。
    对应 daban_selector.py 中的 scan_washout_dip() 策略（日线级别洗盘抄底）。
    入场：当日确认后次日开盘买入
    出场：复用ATR止损 + 跟踪止盈 + 时间止损体系
    """
    # ── 洗盘抄底参数（对齐选股文件阈值） ──────────────────────────
    WASHOUT_RISE_MIN     = 0.10   # 近30日起始价涨幅下限10%（有趋势基础）
    WASHOUT_PULLBACK_MIN = 0.05   # 回调幅度下限5%（未回调=无洗盘）
    WASHOUT_PULLBACK_MAX = 0.25   # 回调幅度上限25%（超过则可能趋势逆转）
    WASHOUT_VOL_SHRINK   = 0.75   # 近3日均量/近10日均量 < 0.75 = 缩量
    WASHOUT_MA_TOUCH_PCT = 0.02   # 触碰均线支撑容忍度±2%
    WASHOUT_MIN_SCORE    = 58.0   # ★v8.5: 从50提升至58（胜率25.9%/-1.27%，11253笔是亏损主力，提高门槛减少低质信号）

    trades = []
    n = len(df)
    if n < 25:
        return trades

    close_arr = df["收盘"].values.astype(float)
    high_arr  = df["最高"].values.astype(float)  if "最高"  in df.columns else close_arr.copy()
    low_arr   = df["最低"].values.astype(float)  if "最低"  in df.columns else close_arr.copy()
    open_arr  = df["开盘"].values.astype(float)  if "开盘"  in df.columns else close_arr.copy()
    vol_arr   = df["成交量"].values.astype(float) if "成交量" in df.columns else np.ones(n)

    for i in range(25, n - 1):
        row = df.iloc[i]

        # 节后首日不入场
        if bool(row.get("is_post_holiday", False)):
            continue
        # 涨停/跌停日不做洗盘
        if bool(row.get("is_zt", False)) or bool(row.get("is_dt", False)):
            continue

        cur_close = float(close_arr[i])
        cur_open  = float(open_arr[i])
        cur_low   = float(low_arr[i])
        if cur_close <= 0:
            continue

        # ── 均线多头 ──────────────────────────────────────────────
        ma5  = float(np.mean(close_arr[i - 4: i + 1]))
        ma10 = float(np.mean(close_arr[i - 9: i + 1]))
        ma20 = float(np.mean(close_arr[i - 19: i + 1]))
        if not (ma5 > ma10 * 0.995 and ma10 > ma20 * 0.990):
            continue

        # ── 近30日趋势涨幅（起始价） ──────────────────────────────
        look_back   = min(30, i)
        start_price = float(close_arr[i - look_back])
        rise_pct    = (cur_close - start_price) / start_price if start_price > 0 else 0
        if rise_pct < WASHOUT_RISE_MIN:
            continue

        # ── 回调幅度（从近20日高点） ──────────────────────────────
        high_20   = float(np.max(high_arr[i - 19: i + 1]))
        high_20   = max(high_20, float(high_arr[i]))
        pullback  = (high_20 - cur_close) / high_20 if high_20 > 0 else 0
        if pullback < WASHOUT_PULLBACK_MIN or pullback > WASHOUT_PULLBACK_MAX:
            continue

        # ── 近3日缩量 ─────────────────────────────────────────────
        vol3  = float(np.mean(vol_arr[i - 2: i + 1]))
        vol10 = float(np.mean(vol_arr[i - 9: i + 1]))
        vol_shrink_ratio = vol3 / vol10 if vol10 > 0 else 1.0

        # 放量下跌特征（出货）直接过滤
        prev_c = float(close_arr[i - 1]) if i > 0 else cur_close
        chg_pct = (cur_close - prev_c) / prev_c if prev_c > 0 else 0
        if chg_pct < -0.02 and vol_shrink_ratio > 1.2:
            continue

        # ── 触碰均线支撑 ─────────────────────────────────────────
        tol = WASHOUT_MA_TOUCH_PCT
        if abs(cur_close - ma5) / ma5 <= tol:
            ma_touch = "MA5"
        elif abs(cur_close - ma10) / ma10 <= tol:
            ma_touch = "MA10"
        elif abs(cur_close - ma20) / ma20 <= tol:
            ma_touch = "MA20"
        elif cur_close < ma20 * (1 - tol):
            continue  # 跌破MA20且不在触碰区间，洗盘无效
        else:
            ma_touch = ""  # 在均线上方但未触碰，允许低分通过

        # ── 今日K线企稳（非大阴线/有下影线/收阳线） ───────────────
        today_body   = cur_close - cur_open
        today_shadow = (cur_open if cur_open > 0 else cur_close) - cur_low
        stabilized   = (
            chg_pct > -0.03
            or today_shadow > abs(today_body) * 0.5
            or cur_close > cur_open
        )
        if not stabilized:
            continue

        # ── 评分 ──────────────────────────────────────────────────
        score = 0.0
        ma_gap_ratio = (ma5 - ma20) / ma20 if ma20 > 0 else 0
        if ma_gap_ratio >= 0.08:    score += 20
        elif ma_gap_ratio >= 0.05:  score += 16
        elif ma_gap_ratio >= 0.03:  score += 12
        else:                       score += 8

        if 0.08 <= pullback <= 0.15:   score += 20
        elif 0.05 <= pullback < 0.08:  score += 15
        elif 0.15 < pullback <= 0.20:  score += 10
        else:                          score += 5

        if ma_touch == "MA5":      score += 15
        elif ma_touch == "MA10":   score += 12
        elif ma_touch == "MA20":   score += 8
        else:                      score += 3

        if vol_shrink_ratio <= 0.50:    score += 20
        elif vol_shrink_ratio <= 0.65:  score += 16
        elif vol_shrink_ratio <= 0.75:  score += 12
        elif vol_shrink_ratio <= 0.90:  score += 6
        else:                           score += 0

        if cur_close > cur_open and today_shadow > 0:   score += 15
        elif cur_close > cur_open:                      score += 10
        elif today_shadow > abs(today_body):            score += 8
        else:                                           score += 3

        if rise_pct >= 0.40:    score += 10
        elif rise_pct >= 0.25:  score += 8
        elif rise_pct >= 0.15:  score += 6
        else:                   score += 3

        score = max(0.0, min(score, 100.0))
        if score < WASHOUT_MIN_SCORE:
            continue

        # ── 流动性/市值过滤（复用现有字段） ─────────────────────
        avg_amt_m = float(row.get("avg_amt_20d", 0.0) or 0.0)
        if avg_amt_m > 0 and avg_amt_m * 1e6 < FILTER_MIN_AVG_AMOUNT:
            continue

        # ── 动态持仓天数 ─────────────────────────────────────────
        if DYNAMIC_HOLD_ENABLE:
            if score >= DYNAMIC_HOLD_HIGH_THRESH:
                _dyn_hold = HOLD_DAYS_STRONG
            elif score <= DYNAMIC_HOLD_LOW_THRESH:
                _dyn_hold = HOLD_DAYS_WEAK
            else:
                _dyn_hold = HOLD_DAYS
        else:
            _dyn_hold = HOLD_DAYS

        nxt = df.iloc[i + 1]
        nxt_open = float(nxt["开盘"])
        if nxt_open <= 0:
            continue
        gap = (nxt_open - cur_close) / cur_close
        if gap < OPEN_GAP_ABORT:
            continue

        entry_p    = round(nxt_open * (1 + SLIP + COMMISSION), 4)
        entry_date = str(nxt["date"])[:10]
        atr_stop   = float(row.get("atr_stop_pct", STOP_LOSS) or STOP_LOSS)
        _trade_rng = random.Random()
        exit_date, exit_p, reason, hdays = simulate_exit(df, i + 1, entry_p, atr_stop, _trade_rng, _dyn_hold)
        pnl = (exit_p - entry_p) / entry_p

        trades.append(Trade(
            code=code, strategy="洗盘抄底",
            entry_date=entry_date, entry_price=entry_p,
            exit_date=exit_date, exit_price=exit_p,
            exit_reason=reason, pnl_pct=round(pnl, 4),
            holding_days=hdays, zt_days=0,
            vol_ratio=round(vol_shrink_ratio, 2),
            bomb_30d=0,
            avg_amplitude=0.0,
            avg_amount_m=round(avg_amt_m, 1),
            signal_score=round(score, 1),
            dynamic_hold=_dyn_hold,
        ))

    return trades


# ================================================================
# ★ v8.2 策略8：日内抄底回测（历史日线模拟T字板/均价回踩/半T字）
# ================================================================
def backtest_intraday_dip(code: str, df: pd.DataFrame) -> list:
    """
    信号：用日线数据模拟日内抄底三类信号。
      ① T字板低吸：当日高点触及涨停价，且收盘在涨停价下方3%~8%（炸板缩量）
      ② 均价回踩：当日最高涨幅≥4%，收盘从高点回落≤6%，收盘在VWAP估算值±1.5%
      ③ 半T字：前日涨停，当日低开2%~6%后收盘企稳（相比前收<3%跌幅）
    入场：当日收盘确认，次日开盘买入
    出场：复用ATR止损 + 跟踪止盈 + 时间止损体系
    """
    # ── 日内抄底参数 ───────────────────────────────────────────────
    INTRA_TBOARD_ZT_DOWN_MIN  = 0.03   # T字板：炸板后距涨停下方最少3%
    INTRA_TBOARD_ZT_UP_MAX    = 0.08   # T字板：炸板后距涨停下方最多8%（超出=失控）
    INTRA_VWAP_RISE_MIN       = 0.04   # 均价回踩：日内最高涨幅下限4%
    INTRA_VWAP_PULLBACK_MAX   = 0.06   # 均价回踩：从高点回落最多6%
    INTRA_VWAP_TOUCH_PCT      = 0.015  # 均价回踩：收盘在VWAP±1.5%内
    SEMI_T_OPEN_MIN           = -0.06  # 半T字：低开下限6%（开盘涨幅为负数）
    SEMI_T_OPEN_MAX           = -0.02  # 半T字：低开上限2%
    SEMI_T_STABLE_PCT         = 0.02   # 半T字：收盘距开盘±2%以内算企稳
    SEMI_T_PRICE_NOT_BELOW    = -0.03  # 半T字：收盘不低于前收3%以上
    INTRA_MIN_SCORE           = 50.0   # 最低评分门槛

    trades = []
    n = len(df)
    if n < 5:
        return trades

    close_arr  = df["收盘"].values.astype(float)
    high_arr   = df["最高"].values.astype(float)  if "最高"  in df.columns else close_arr.copy()
    low_arr    = df["最低"].values.astype(float)  if "最低"  in df.columns else close_arr.copy()
    open_arr   = df["开盘"].values.astype(float)  if "开盘"  in df.columns else close_arr.copy()
    vol_arr    = df["成交量"].values.astype(float) if "成交量" in df.columns else np.ones(n)

    for i in range(2, n - 1):
        row    = df.iloc[i]
        prev_i = i - 1

        if bool(row.get("is_post_holiday", False)):
            continue

        cur_close = float(close_arr[i])
        cur_open  = float(open_arr[i])
        cur_high  = float(high_arr[i])
        cur_low   = float(low_arr[i])
        prev_c    = float(close_arr[prev_i])  # 前收（即昨日收盘）

        if cur_close <= 0 or prev_c <= 0 or cur_open <= 0:
            continue

        zt_price = round(prev_c * 1.10, 2)   # 今日涨停价（沪深主板10%限制）
        chg_pct  = (cur_close - prev_c) / prev_c

        # 今日跌停不做
        if chg_pct <= -0.095:
            continue

        max_chg     = (cur_high - prev_c) / prev_c
        dist_to_zt  = (cur_close - zt_price) / zt_price if zt_price > 0 else 0
        pullback_hi = (cur_close - cur_high) / cur_high if cur_high > 0 else 0

        # 量比估算（若无实盘量比，用当日量/近5日均量代替）
        vol5 = float(np.mean(vol_arr[max(0, i - 4): i + 1]))
        vol_ratio_est = float(vol_arr[i]) / vol5 if vol5 > 0 else 1.0

        open_chg = (cur_open - prev_c) / prev_c
        price_vs_open = (cur_close - cur_open) / cur_open if cur_open > 0 else 0

        # 估算VWAP
        vwap = (cur_high + cur_low + cur_close) / 3.0
        vwap = (vwap + cur_open) / 2.0 if cur_open > 0 else vwap

        sig_type = ""
        score    = 0.0

        # ① T字板低吸
        touched_zt  = (cur_high >= zt_price * 0.99)
        below_zt    = (dist_to_zt < -INTRA_TBOARD_ZT_DOWN_MIN and
                       dist_to_zt > -(INTRA_TBOARD_ZT_UP_MAX + 0.001))
        stable_low  = (cur_low > 0 and (cur_close - cur_low) / cur_low <= 0.015)

        if touched_zt and below_zt and stable_low:
            if vol_ratio_est > 3.0:
                pass  # 放量炸板=出货，不做
            else:
                sig_type = "T字板"
                abs_dist = abs(dist_to_zt)
                if abs_dist <= 0.04:        score += 30
                elif abs_dist <= 0.05:      score += 24
                elif abs_dist <= 0.06:      score += 18
                else:                       score += 10

                if vol_ratio_est <= 0.5:    score += 25
                elif vol_ratio_est <= 0.8:  score += 20
                elif vol_ratio_est <= 1.2:  score += 14
                elif vol_ratio_est <= 2.0:  score += 8
                else:                       score += 0

                if chg_pct >= 0.03:         score += 10
                elif chg_pct >= 0.00:       score += 7
                elif chg_pct >= -0.02:      score += 3

                stab_gap = (cur_close - cur_low) / cur_low if cur_low > 0 else 0
                if stab_gap <= 0.005:       score += 15
                elif stab_gap <= 0.010:     score += 10
                else:                       score += 5

                score += 10  # 基础加分（历史模拟忽略市值细分）

        # ② 均价回踩
        elif (not touched_zt and
              max_chg >= INTRA_VWAP_RISE_MIN and
              INTRA_VWAP_PULLBACK_MAX * (-1) <= pullback_hi < -0.01):
            near_vwap = abs(cur_close - vwap) / vwap <= INTRA_VWAP_TOUCH_PCT if vwap > 0 else False
            near_open = abs(cur_close - cur_open) / cur_open <= INTRA_VWAP_TOUCH_PCT if cur_open > 0 else False
            if near_vwap or near_open:
                sig_type = "均价回踩"
                if max_chg >= 0.08:     score += 25
                elif max_chg >= 0.06:   score += 20
                elif max_chg >= 0.05:   score += 15
                else:                   score += 8

                pb = abs(pullback_hi)
                if 0.02 <= pb <= 0.04:  score += 25
                elif 0.04 < pb <= 0.05: score += 18
                elif pb < 0.02:         score += 8
                else:                   score += 5

                if vol_ratio_est <= 0.6:    score += 20
                elif vol_ratio_est <= 0.9:  score += 15
                elif vol_ratio_est <= 1.2:  score += 8
                else:                       score += 0
                if vol_ratio_est > 2.0:     score -= 10

                if near_vwap and near_open: score += 15
                elif near_vwap:             score += 10
                else:                       score += 7

        # ③ 半T字（昨日涨停+今日低开企稳）
        else:
            prev_zt = bool(df.iloc[prev_i].get("is_zt", False))
            is_semi_t = (
                prev_zt and
                SEMI_T_OPEN_MIN <= open_chg <= SEMI_T_OPEN_MAX and
                abs(price_vs_open) <= SEMI_T_STABLE_PCT and
                chg_pct >= SEMI_T_PRICE_NOT_BELOW
            )
            if is_semi_t:
                sig_type = "半T字"
                abs_open_chg = abs(open_chg)
                if 0.02 <= abs_open_chg <= 0.04:    score += 25
                elif 0.04 < abs_open_chg <= 0.05:   score += 18
                else:                                score += 10

                abs_stab = abs(price_vs_open)
                if abs_stab <= 0.005:   score += 20
                elif abs_stab <= 0.01:  score += 14
                else:                   score += 7

                if vol_ratio_est <= 0.7:    score += 20
                elif vol_ratio_est <= 1.0:  score += 14
                elif vol_ratio_est <= 1.5:  score += 8
                else:                       score += 2

                if -0.01 <= chg_pct <= 0.02:            score += 15
                elif -0.03 <= chg_pct < -0.01:          score += 8
                else:                                    score += 3

        if not sig_type:
            continue

        score = max(0.0, min(score, 100.0))
        if score < INTRA_MIN_SCORE:
            continue

        # ── 动态持仓天数 ─────────────────────────────────────────
        if DYNAMIC_HOLD_ENABLE:
            if score >= DYNAMIC_HOLD_HIGH_THRESH:
                _dyn_hold = HOLD_DAYS_STRONG
            elif score <= DYNAMIC_HOLD_LOW_THRESH:
                _dyn_hold = HOLD_DAYS_WEAK
            else:
                _dyn_hold = HOLD_DAYS
        else:
            _dyn_hold = HOLD_DAYS

        nxt = df.iloc[i + 1]
        nxt_open = float(nxt["开盘"])
        if nxt_open <= 0:
            continue
        gap = (nxt_open - cur_close) / cur_close
        if gap < OPEN_GAP_ABORT:
            continue

        entry_p    = round(nxt_open * (1 + SLIP + COMMISSION), 4)
        entry_date = str(nxt["date"])[:10]
        atr_stop   = float(row.get("atr_stop_pct", STOP_LOSS) or STOP_LOSS)
        _trade_rng = random.Random()
        exit_date, exit_p, reason, hdays = simulate_exit(df, i + 1, entry_p, atr_stop, _trade_rng, _dyn_hold)
        pnl = (exit_p - entry_p) / entry_p

        avg_amt_m = float(row.get("avg_amt_20d", 0.0) or 0.0)
        trades.append(Trade(
            code=code, strategy=f"日内抄底-{sig_type}",
            entry_date=entry_date, entry_price=entry_p,
            exit_date=exit_date, exit_price=exit_p,
            exit_reason=reason, pnl_pct=round(pnl, 4),
            holding_days=hdays, zt_days=0,
            vol_ratio=round(vol_ratio_est, 2),
            bomb_30d=0,
            avg_amplitude=0.0,
            avg_amount_m=round(avg_amt_m, 1),
            signal_score=round(score, 1),
            dynamic_hold=_dyn_hold,
        ))

    return trades


# ================================================================
# 单股回测（汇总所有策略）
# ================================================================
def backtest_single(code: str, df: pd.DataFrame,
                    strategies: list = None) -> list:
    if strategies is None:
        strategies = STRATEGIES_ALL
    trades = []
    # ★ v9.0：策略开关控制（停用所有负期望值策略）
    if "首板" in strategies and STRATEGY_ENABLE_FIRST_BOARD:
        trades += backtest_first_board(code, df)
    if "连板" in strategies:
        trades += backtest_connect_board(code, df)
    if "竞价" in strategies and STRATEGY_ENABLE_AUCTION:
        trades += backtest_auction_board(code, df)
    if "反包" in strategies and STRATEGY_ENABLE_DT_RECOVER:
        trades += backtest_dt_recover(code, df)
    if "回调缩量" in strategies and STRATEGY_ENABLE_ZT_PULLBACK:
        trades += backtest_zt_pullback(code, df)
    if "洗盘抄底" in strategies and STRATEGY_ENABLE_WASHOUT:
        trades += backtest_washout_dip(code, df)
    if "日内抄底" in strategies and STRATEGY_ENABLE_INTRADAY:
        trades += backtest_intraday_dip(code, df)
    return trades


# ================================================================
# 统计
# ================================================================
def calc_stats(trades: list, bt_start: str = "", bt_end: str = "") -> dict:
    if not trades:
        return {}

    df = pd.DataFrame([t.__dict__ for t in trades])
    df = df.sort_values("entry_date").reset_index(drop=True)

    total    = len(df)
    wins     = df[df["pnl_pct"] > 0]
    losses   = df[df["pnl_pct"] <= 0]
    win_rate = len(wins) / total
    avg_pnl  = df["pnl_pct"].mean()
    avg_win  = wins["pnl_pct"].mean()  if len(wins)   else 0
    avg_loss = losses["pnl_pct"].mean() if len(losses) else 0
    pnl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

    # ★ v4.0：资金管理净值曲线（固定仓位 POSITION_SIZE，最多 MAX_POSITIONS 同时持仓）
    # 按入场日排序，模拟资金变化，每笔使用 POSITION_SIZE 比例资金
    equity_curve = _calc_portfolio_equity(df)
    if len(equity_curve) > 0:
        peak   = equity_curve.cummax()
        max_dd = float(((equity_curve - peak) / peak).min())
    else:
        # 退化：独立复利（旧逻辑，不推荐）
        equity_curve = (1 + df["pnl_pct"]).cumprod()
        peak   = equity_curve.cummax()
        max_dd = float(((equity_curve - peak) / peak).min())

    if len(df) > 1:
        # ★ 年化收益修复：优先用命令行传入的回测区间（bt_start/bt_end）
        if bt_start and bt_end:
            try:
                span_days = (pd.to_datetime(bt_end) - pd.to_datetime(bt_start)).days
                years = max(span_days / 365.25, 0.5)
            except Exception:
                years = max((pd.to_datetime(df["entry_date"].iloc[-1]) -
                             pd.to_datetime(df["entry_date"].iloc[0])).days / 365.25, 0.5)
        else:
            years = max((pd.to_datetime(df["entry_date"].iloc[-1]) -
                         pd.to_datetime(df["entry_date"].iloc[0])).days / 365.25, 0.5)
        final_equity = float(equity_curve.iloc[-1])
        annual_ret = final_equity ** (1.0 / years) - 1
        # 防止极端值（年化超过500%时输出警告并截断显示）
        if abs(annual_ret) > 5.0:
            log.warning(f"年化收益异常（{annual_ret:+.0%}），样本量可能不足，请增加股票数或拉长回测区间")
            annual_ret = min(annual_ret, 5.0)
    else:
        annual_ret = avg_pnl

    # ★ v3.4修复：夏普年化因子用实际平均持仓天数
    if "holding_days" in df.columns and df["holding_days"].mean() > 0:
        avg_hold = max(1, float(df["holding_days"].mean()))
    elif "hold_days" in df.columns and df["hold_days"].mean() > 0:
        avg_hold = max(1, float(df["hold_days"].mean()))
    else:
        avg_hold = float(HOLD_DAYS)
    trades_per_year = 250.0 / avg_hold
    rf = 0.025 / trades_per_year
    excess = df["pnl_pct"] - rf
    sharpe = excess.mean() / excess.std() * np.sqrt(trades_per_year) if excess.std() > 0 else 0

    # 按策略分组统计
    # ★ 修复：竞价子策略标签为 "竞价-连板-强势" 等，用前缀匹配归组
    # 统计维度1：顶层策略（首板/连板/竞价/反包）合并
    # 统计维度2：竞价各子策略单独列出
    strategy_stats = {}

    # 顶层策略合并统计
    for st in STRATEGIES_ALL:
        if st == "竞价":
            sub = df[df["strategy"].str.startswith("竞价")]
        else:
            sub = df[df["strategy"] == st]
        if len(sub) == 0:
            continue
        strategy_stats[st] = {
            "count":    len(sub),
            "win_rate": round(len(sub[sub["pnl_pct"] > 0]) / len(sub), 3),
            "avg_pnl":  round(sub["pnl_pct"].mean(), 4),
            "avg_win":  round(sub[sub["pnl_pct"] > 0]["pnl_pct"].mean(), 4) if len(sub[sub["pnl_pct"] > 0]) else 0,
            "avg_loss": round(sub[sub["pnl_pct"] <= 0]["pnl_pct"].mean(), 4) if len(sub[sub["pnl_pct"] <= 0]) else 0,
        }

    # 竞价子策略单独统计（细分5档）
    auction_sub_stats = {}
    auction_subs = sorted(df[df["strategy"].str.startswith("竞价")]["strategy"].unique())
    for sub_st in auction_subs:
        sub = df[df["strategy"] == sub_st]
        if len(sub) == 0:
            continue
        auction_sub_stats[sub_st] = {
            "count":    len(sub),
            "win_rate": round(len(sub[sub["pnl_pct"] > 0]) / len(sub), 3),
            "avg_pnl":  round(sub["pnl_pct"].mean(), 4),
            "avg_win":  round(sub[sub["pnl_pct"] > 0]["pnl_pct"].mean(), 4) if len(sub[sub["pnl_pct"] > 0]) else 0,
            "avg_loss": round(sub[sub["pnl_pct"] <= 0]["pnl_pct"].mean(), 4) if len(sub[sub["pnl_pct"] <= 0]) else 0,
        }

    # 连板天数分组（连板策略）
    zt_stats = {}
    lp = df[df["strategy"] == "连板"]
    for days in sorted(lp["zt_days"].unique()):
        sub = lp[lp["zt_days"] == days]
        zt_stats[int(days)] = {
            "count":    len(sub),
            "win_rate": round(len(sub[sub["pnl_pct"] > 0]) / len(sub), 3),
            "avg_pnl":  round(sub["pnl_pct"].mean(), 4),
        }

    # ★ v4.0：连续亏损统计
    max_consec_loss = _calc_max_consecutive_loss(df)

    # ★ v4.0：按年度统计
    yearly_stats = _calc_yearly_stats(df)

    # ★ v4.0：按月度统计
    monthly_stats = _calc_monthly_stats(df)

    return {
        "total": total, "win_rate": round(win_rate, 4),
        "avg_pnl": round(avg_pnl, 4), "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4), "pnl_ratio": round(pnl_ratio, 2),
        "max_drawdown": round(max_dd, 4),
        "annual_return": round(annual_ret, 4), "sharpe": round(sharpe, 2),
        "strategy_stats": strategy_stats, "zt_stats": zt_stats,
        "auction_sub_stats": auction_sub_stats,
        "exit_reasons": df["exit_reason"].value_counts().to_dict(),
        "max_consec_loss": max_consec_loss,
        "yearly_stats": yearly_stats,
        "monthly_stats": monthly_stats,
        "equity_curve": equity_curve,  # 供画图使用
    }


def _calc_portfolio_equity(df: pd.DataFrame) -> pd.Series:
    """
    ★ v4.0：固定仓位资金管理净值曲线
    每笔交易使用 POSITION_SIZE（10%）仓位，最多同时 MAX_POSITIONS（10）只
    同日信号按 vol_ratio 从高到低排序，超过仓位上限的跳过
    ★ v8.4：加入赚钱效应仓位自适应（近5日市场涨停家数代理指标）
    返回：以交易笔数为索引的净值序列（初始=1.0）
    """
    if df.empty:
        return pd.Series([1.0])

    df = df.sort_values("entry_date").reset_index(drop=True)

    capital = INITIAL_CAPITAL
    equity_list = [capital]

    # ★ v8.4 赚钱效应代理指标预计算
    # 用每日入场的信号数量（每日有多少股票出现涨停信号）作为赚钱效应代理
    # 思路：市场热时同日信号密集，冷时稀少
    _emotion_cache: dict = {}  # entry_date → emotion_ratio (近N日均值)

    if EMOTION_SCORE_ENABLE and EMOTION_PROXY_ENABLE:
        # 按入场日统计每日涨停信号数量（作为市场热度代理）
        daily_zt_count = df.groupby("entry_date").size().sort_index()
        dates_sorted   = list(daily_zt_count.index)
        counts_sorted  = list(daily_zt_count.values)
        for i, d in enumerate(dates_sorted):
            # 取近 EMOTION_LOOKBACK_DAYS 天的平均信号数
            window_counts = counts_sorted[max(0, i - EMOTION_LOOKBACK_DAYS + 1): i + 1]
            avg_cnt = float(sum(window_counts)) / len(window_counts) if window_counts else 1.0
            # 归一化到0~1：信号数越多代表情绪越热，用min-max近似
            # 用全局分位数（25th/75th）做冷热分界，更稳健
            _emotion_cache[d] = avg_cnt  # 先存原始值，后面再做相对比较

        # 计算全局25th/75th作为冷热分界线
        all_vals = list(_emotion_cache.values())
        if len(all_vals) >= 4:
            import numpy as _np
            _hot_boundary  = float(_np.percentile(all_vals, 75))  # 75分位=热
            _cold_boundary = float(_np.percentile(all_vals, 25))  # 25分位=冷
        else:
            _hot_boundary  = max(all_vals) * 0.7
            _cold_boundary = max(all_vals) * 0.3
    else:
        _hot_boundary  = 0
        _cold_boundary = 0

    # 按出场日分组，模拟资金流入流出
    # 简化模型：每笔独立，但资金有限（最多同时10笔×10%=100%）
    # 用队列模拟并发持仓
    active_positions = []  # (exit_date, pnl_pct, capital_used)
    daily_capital = capital  # 当前可用资金

    for _, row in df.iterrows():
        entry_date = row["entry_date"]
        exit_date  = row["exit_date"] if row["exit_date"] else entry_date
        pnl_pct    = float(row["pnl_pct"])

        # 结算已出场的仓位（出场日 <= 入场日的）
        still_active = []
        for pos in active_positions:
            if pos["exit_date"] <= entry_date:
                # 资金回笼（含盈亏）
                daily_capital += pos["capital_used"] * (1 + pos["pnl_pct"])
            else:
                still_active.append(pos)
        active_positions = still_active

        # 判断是否还有仓位空间
        if len(active_positions) >= MAX_POSITIONS:
            continue  # 已满仓，跳过此信号

        # ── 计算本笔仓位比例 ───────────────────────────────────────
        # 优先级：弱势月份 > 强势月份 > 赚钱效应 > 默认
        # ★ v7.0：弱势月份仓位减半（3/10/11月历史胜率极低）
        # ★ v8.3：6月也加入弱势（胜率25.5%）；9月强势月份加仓（胜率62.2%）
        _entry_month = int(str(entry_date)[5:7]) if entry_date else 0
        _pos_ratio = POSITION_SIZE

        if WEAK_MONTH_POSITION_ENABLE and _entry_month in WEAK_MONTHS:
            _pos_ratio = POSITION_SIZE * WEAK_MONTH_POSITION_RATIO
        elif STRONG_MONTH_POSITION_ENABLE and _entry_month in STRONG_MONTHS:
            _pos_ratio = POSITION_SIZE * STRONG_MONTH_POSITION_RATIO
        else:
            # ★ v8.4 赚钱效应叠加调整（只在非月份控制区间生效）
            if EMOTION_SCORE_ENABLE and EMOTION_PROXY_ENABLE and entry_date in _emotion_cache:
                _emo_val = _emotion_cache[entry_date]
                if _emo_val >= _hot_boundary and _hot_boundary > 0:
                    _pos_ratio = POSITION_SIZE * EMOTION_HOT_POS_RATIO    # 情绪热→加仓
                elif _emo_val <= _cold_boundary and _cold_boundary > 0:
                    _pos_ratio = POSITION_SIZE * EMOTION_COLD_POS_RATIO   # 情绪冷→减仓
                else:
                    _pos_ratio = POSITION_SIZE * EMOTION_NORMAL_POS_RATIO # 中性→正常

        pos_capital = min(daily_capital * _pos_ratio, daily_capital / max(1, MAX_POSITIONS - len(active_positions)))
        if pos_capital <= 0:
            continue

        daily_capital -= pos_capital
        active_positions.append({
            "exit_date":    exit_date,
            "pnl_pct":      pnl_pct,
            "capital_used": pos_capital,
        })
        equity_list.append(daily_capital + sum(p["capital_used"] for p in active_positions))

    # 结算所有剩余仓位
    for pos in active_positions:
        daily_capital += pos["capital_used"] * (1 + pos["pnl_pct"])
    equity_list.append(daily_capital)

    equity_arr = pd.Series(equity_list) / INITIAL_CAPITAL
    return equity_arr


def _calc_max_consecutive_loss(df: pd.DataFrame) -> dict:
    """★ v4.0：统计最大连续亏损笔数和最大连续亏损幅度（乘以仓位比例）"""
    pnls = df.sort_values("entry_date")["pnl_pct"].tolist()
    max_streak = 0
    cur_streak = 0
    max_loss_sum = 0.0
    cur_loss_sum = 0.0
    for p in pnls:
        if p <= 0:
            cur_streak += 1
            cur_loss_sum += p * POSITION_SIZE  # 乘仓位比例，反映真实资金损失
            if cur_streak > max_streak:
                max_streak = cur_streak
                max_loss_sum = cur_loss_sum
        else:
            cur_streak = 0
            cur_loss_sum = 0.0
    return {"max_streak": max_streak, "max_loss_sum": round(max_loss_sum, 4)}


def _calc_yearly_stats(df: pd.DataFrame) -> dict:
    """★ v4.0：按年度统计胜率/均收益"""
    df = df.copy()
    df["year"] = pd.to_datetime(df["entry_date"]).dt.year
    result = {}
    for yr in sorted(df["year"].unique()):
        sub = df[df["year"] == yr]
        wins = sub[sub["pnl_pct"] > 0]
        # 年度累计收益：每笔 POSITION_SIZE 仓位，线性叠加
        # 公式：sum(pnl_pct * POSITION_SIZE)，与资金曲线计算保持一致
        total_ret = float((sub["pnl_pct"] * POSITION_SIZE).sum())
        result[int(yr)] = {
            "count":    len(sub),
            "win_rate": round(len(wins) / len(sub), 3),
            "avg_pnl":  round(sub["pnl_pct"].mean(), 4),
            "total_ret": round(total_ret, 4),
        }
    return result


def _calc_monthly_stats(df: pd.DataFrame) -> dict:
    """★ v4.0：按月度统计胜率/均收益（用于发现季节性规律）"""
    df = df.copy()
    df["month"] = pd.to_datetime(df["entry_date"]).dt.month
    result = {}
    for mo in range(1, 13):
        sub = df[df["month"] == mo]
        if len(sub) == 0:
            continue
        wins = sub[sub["pnl_pct"] > 0]
        result[int(mo)] = {
            "count":    len(sub),
            "win_rate": round(len(wins) / len(sub), 3),
            "avg_pnl":  round(sub["pnl_pct"].mean(), 4),
        }
    return result


# ================================================================
# 打印报告
# ================================================================
def print_report(stats: dict, trades: list) -> None:
    if not stats:
        print("无交易记录")
        return

    print("\n" + "="*70)
    print("  打板策略 · 历史回测报告")
    print("="*70)
    print(f"  总交易笔数      : {stats['total']}")
    print(f"  整体胜率        : {stats['win_rate']:.1%}")
    print(f"  平均每笔收益    : {stats['avg_pnl']:+.2%}")
    print(f"  平均盈利        : {stats['avg_win']:+.2%}")
    print(f"  平均亏损        : {stats['avg_loss']:+.2%}")
    print(f"  盈亏比          : {stats['pnl_ratio']:.2f}x")
    print(f"  最大回撤        : {stats['max_drawdown']:.2%}")
    print(f"  年化收益（估）  : {stats['annual_return']:+.2%}")
    print(f"  夏普比率（估）  : {stats['sharpe']:.2f}")

    print("\n  ── 各策略分类统计 ──")
    print(f"  {'策略':<8}{'笔数':>6}{'胜率':>8}{'均收益':>9}{'均盈利':>9}{'均亏损':>9}")
    print("  " + "-"*52)
    for st, s in stats.get("strategy_stats", {}).items():
        icon = {"首板": "🔥", "连板": "🚀", "竞价": "⚡", "反包": "🔄", "回调缩量": "📉"}.get(st, "")
        print(
            f"  {icon}{st:<6}{s['count']:>6}  {s['win_rate']:>6.1%}"
            f"  {s['avg_pnl']:>+7.2%}  {s['avg_win']:>+7.2%}  {s['avg_loss']:>+7.2%}"
        )

    # ★ 竞价子策略细分展示
    auction_sub = stats.get("auction_sub_stats", {})
    if auction_sub:
        print("\n  ── 竞价子策略细分 ──")
        print(f"  {'子策略':<18}{'笔数':>6}{'胜率':>8}{'均收益':>9}{'均盈利':>9}{'均亏损':>9}")
        print("  " + "-"*62)
        for sub_st, s in auction_sub.items():
            print(
                f"  {'⚡'+sub_st:<18}{s['count']:>6}  {s['win_rate']:>6.1%}"
                f"  {s['avg_pnl']:>+7.2%}  {s['avg_win']:>+7.2%}  {s['avg_loss']:>+7.2%}"
            )

    lp_stats = stats.get("zt_stats", {})
    if lp_stats:
        print("\n  ── 连板天数细分（连板策略）──")
        print(f"  {'连板天数':<10}{'笔数':>6}{'胜率':>8}{'均收益':>9}")
        print("  " + "-"*36)
        for days, s in lp_stats.items():
            print(f"  {days}连板{'':<6}{s['count']:>6}  {s['win_rate']:>6.1%}  {s['avg_pnl']:>+7.2%}")

    print("\n  ── 出场原因分布 ──")
    label_map = {
        "stop_loss":           "ATR止损",
        "time_stop":           "时间止损",
        "gap_abort":           "低开止损",
        "day2_stop":           "第2天强制止损",      # ★v8.4新增
        "zt_break_stop":       "断板止损",            # ★v9.0新增
        "weak_confirm_stop":   "弱开确认止损",        # ★v9.1新增
        "ma5_stop":            "均线止损(旧)",        # 兼容旧数据
        "ma10_stop":           "MA10趋势止损",        # v4.0新标签
        "trail_stop":          "跟踪止盈",
        "t2_trail":            "T2追踪止盈",
        "target1":             "第一止盈",
        "target2":             "第二止盈",
        "timeout":             "超时平仓",
    }
    for reason, cnt in stats.get("exit_reasons", {}).items():
        pct   = cnt / stats["total"]
        label = label_map.get(reason, reason)
        is_win = reason in ("target1", "target2", "trail_stop", "t2_trail", "timeout", "ma10_stop")
        icon  = "✅" if is_win else "❌"
        print(f"  {icon} {label:<14}: {cnt:>4}笔 ({pct:.1%})")

    print("="*70)

    # 最近20笔明细
    if trades:
        recent = sorted(trades, key=lambda t: t.entry_date, reverse=True)[:20]
        print(f"\n  最近 {len(recent)} 笔交易明细：")
        print(f"  {'代码':<10}{'入场日':>12}{'出场日':>12}{'策略':>6}"
              f"{'收益率':>9}{'持天':>6}{'出场原因':<12}")
        print("  " + "-"*68)
        for t in recent:
            icon = "✅" if t.pnl_pct > 0 else "❌"
            print(
                f"  {t.code:<10}{t.entry_date:>12}{t.exit_date:>12}"
                f"  {t.strategy:<4}  {t.pnl_pct:>+.2%}"
                f"{t.holding_days:>6}  {icon}{t.exit_reason}"
            )

    # ★ v4.0：连续亏损统计
    consec = stats.get("max_consec_loss", {})
    if consec:
        print(f"\n  ── 风险指标（v4.0新增）──")
        print(f"  最大连续亏损笔数  : {consec.get('max_streak', 0)} 笔")
        print(f"  最大连续亏损累计  : {consec.get('max_loss_sum', 0):+.2%}")
        print(f"  资金管理说明      : 每笔 {POSITION_SIZE:.0%} 仓位，最多 {MAX_POSITIONS} 只同持")
        print(f"  注：量比为日线近似值（非真实盘中量比），换手率用量比替代（实盘需实际数据）")

    # ★ v5.0：反收割模块状态展示
    print(f"\n  ── 反收割模块状态（v5.0）──")
    print(f"  止损位随机化      : {'✅开启' if STOP_JITTER_ENABLE else '❌关闭'}"
          f"  扰动幅度 ±{STOP_JITTER_RANGE:.1%}")
    print(f"  止盈反整数化      : {'✅开启' if PROFIT_JITTER_ENABLE else '❌关闭'}"
          f"  扰动幅度 ±{PROFIT_JITTER_RANGE:.1%}")
    print(f"  竞价钓鱼盘检测    : {'✅开启' if FILTER_FAKE_AUCTION else '❌关闭'}"
          f"  过滤阈值 ≥{FAKE_AUCTION_THRESH:.0%} (近{FAKE_AUCTION_LOOKBACK}日)")
    print(f"  再封陷阱检测      : {'✅开启' if FILTER_RECLOSE_TRAP else '❌关闭'}"
          f"  触发上限 >{RECLOSE_TRAP_MAX}次 (近{RECLOSE_TRAP_DAYS}日)")
    print(f"  信号随机延迟      : {'⚠️开启(影响回测)' if SIGNAL_DELAY_RANDOM else '❌关闭(实盘按需开启)'}")

    # ★ v6.0：智能化模块状态展示
    print(f"\n  ── 智能化模块状态（v6.0）──")
    print(f"  量价背离过滤      : {'✅开启' if FILTER_VOL_PRICE_DIVERGE else '❌关闭'}"
          f"  阈值 量<均量×{VOL_PRICE_DIVERGE_THRESH:.0%}")
    print(f"  情绪综合评分      : {'✅开启' if SIGNAL_SCORE_ENABLE else '❌关闭'}"
          f"  首板≥{SIGNAL_SCORE_MIN_FIRST:.0f} 竞价≥{SIGNAL_SCORE_MIN_AUCTION:.0f} 反包≥{SIGNAL_SCORE_MIN_RECOVER:.0f} 回调≥{SIGNAL_SCORE_MIN_PULLBACK:.0f}")
    print(f"  动态持仓天数      : {'✅开启' if DYNAMIC_HOLD_ENABLE else '❌关闭'}"
          f"  强势{HOLD_DAYS_STRONG}天(≥{DYNAMIC_HOLD_HIGH_THRESH:.0f}分) 默认{HOLD_DAYS}天 弱势{HOLD_DAYS_WEAK}天(≤{DYNAMIC_HOLD_LOW_THRESH:.0f}分)")
    print(f"  反包质量过滤      : {'✅开启' if DT_RECOVER_QUALITY_FILTER else '❌关闭'}"
          f"  连跌≤{DT_RECOVER_MAX_STREAK}次 前5日动量≥{DT_RECOVER_MIN_MOM5:.0%} 开盘≥{DT_RECOVER_MIN_OPEN_CHG:.0%}")
    print(f"  T2量能自适应      : {'✅开启' if T2_VOL_ADAPTIVE_ENABLE else '❌关闭'}"
          f"  指数={T2_VOL_ADAPTIVE_POWER} 容忍率[{T2_VOL_ADAPTIVE_MIN:.0%}~{T2_VOL_ADAPTIVE_MAX:.0%}]")

    # ★ v7.0：数据驱动优化模块状态
    _weak_months_str   = "/".join(f"{m}月" for m in WEAK_MONTHS)
    _strong_months_str = "/".join(f"{m}月" for m in STRONG_MONTHS)
    print(f"\n  ── 数据驱动优化状态（v7.0/v8.4）──")
    print(f"  首板评分门槛      : {SIGNAL_SCORE_MIN_FIRST:.0f}分（v8.3提高至72，进一步过滤中低分首板）")
    print(f"  首板量比上限      : 2.2（首板高量比出货特征明显保持收紧）")
    print(f"  竞价量比上限      : {VOL_RATIO_HIGH_AUCTION:.1f}（v8.4放宽，竞价高量比是强度信号）")
    print(f"  弱势月份仓位控制  : {'✅开启' if WEAK_MONTH_POSITION_ENABLE else '❌关闭'}"
          f"  {_weak_months_str} 仓位×{WEAK_MONTH_POSITION_RATIO:.0%}（历史胜率偏低）")
    print(f"  强势月份加仓      : {'✅开启' if STRONG_MONTH_POSITION_ENABLE else '❌关闭'}"
          f"  {_strong_months_str} 仓位×{STRONG_MONTH_POSITION_RATIO:.0%}（v8.3新增，9月胜率62.2%）")

    # ★ v8.4：新增功能模块状态
    print(f"\n  ── v8.4 新增优化模块状态 ──")
    print(f"  赚钱效应仓位自适应: {'✅开启' if EMOTION_SCORE_ENABLE else '❌关闭'}"
          f"  热={EMOTION_HOT_POS_RATIO:.1f}x 冷={EMOTION_COLD_POS_RATIO:.1f}x"
          f"  窗口={EMOTION_LOOKBACK_DAYS}天（近N日信号密度代理市场情绪）")
    print(f"  第2天强制止损     : {'✅开启' if DAY2_FORCE_STOP_ENABLE else '❌关闭'}"
          f"  第2天浮亏>{abs(DAY2_FORCE_STOP_LOSS):.1%}且非缩量→次日开盘强制出")
    print(f"  强势延长持仓      : {'✅开启' if EXTEND_HOLD_ON_STRENGTH else '❌关闭'}"
          f"  连续{EXTEND_HOLD_CONSECUTIVE_DAYS}天强势(close_pos>{EXTEND_HOLD_CLOSE_POS_THRESH})"
          f"→最多延长{EXTEND_HOLD_MAX_EXTRA}天")
    print(f"  量比分策略差异化  : ✅开启  竞价/洗盘/日内上限{VOL_RATIO_HIGH_AUCTION:.0f}"
          f" | 首板/反包/回调上限{VOL_RATIO_HIGH:.0f}")

    # ★ v8.0/v8.3：新策略+评分升级状态
    print(f"\n  ── v8.3 新策略模块状态 ──")
    print(f"  涨停回调缩量策略  : {'✅开启' if STRATEGY_ENABLE_ZT_PULLBACK else '❌停用(v9.0负期望值)'}"
          f"  回调{ZT_PULLBACK_MIN_DROP:.0%}~{ZT_PULLBACK_MAX_DROP:.0%} "
          f"缩量<涨停日量×{ZT_PULLBACK_VOL_RATIO:.0%} "
          f"评分≥{SIGNAL_SCORE_MIN_PULLBACK:.0f}")
    print(f"  回调均线多头确认  : {'✅开启' if ZT_PULLBACK_REQUIRE_MA_ALIGN else '❌关闭'}"
          f"  MA5>MA10>MA20（v8.3新增，过滤趋势向下的虚假洗盘）")
    print(f"  评分Z-score标准化 : ✅开启  量比/动量/放量因子均Z-score标准化（消除量纲偏差）")

    # ★ v9.0：重大重构模块状态
    print(f"\n  ── v9.0 重大重构模块状态（竞价聚焦 + 出场革命）──")
    _enabled_strats = []
    if STRATEGY_ENABLE_AUCTION: _enabled_strats.append("竞价")
    if STRATEGY_ENABLE_DT_RECOVER: _enabled_strats.append("反包")
    if STRATEGY_ENABLE_FIRST_BOARD: _enabled_strats.append("首板")
    if STRATEGY_ENABLE_WASHOUT: _enabled_strats.append("洗盘抄底")
    if STRATEGY_ENABLE_ZT_PULLBACK: _enabled_strats.append("回调缩量")
    if STRATEGY_ENABLE_INTRADAY: _enabled_strats.append("日内抄底")
    print(f"  启用策略          : {' / '.join(_enabled_strats) if _enabled_strats else '无'}")
    print(f"  停用策略          : 首板(Kelly=-23%) / 洗盘抄底(贡献80%亏损) / 回调缩量(样本无效) / 日内抄底(日线近似误差大)")
    _whitelist_str = " / ".join(AUCTION_WHITELIST) if AUCTION_WHITELIST_ENABLE else "全部竞价子策略"
    print(f"  竞价白名单        : {'✅开启' if AUCTION_WHITELIST_ENABLE else '❌关闭'}  {_whitelist_str}")
    print(f"  竞价专用止损      : ✅开启  {AUCTION_STOP_LOSS:.1%}（比通用-4.5%更紧）")
    print(f"  竞价低开止损      : ✅开启  低开>{abs(AUCTION_GAP_ABORT):.0%}直接止损（比通用3%更严）")
    print(f"  涨停跟踪延仓      : {'✅开启' if AUCTION_ZT_TRAIL_ENABLE else '❌关闭'}"
          f"  每次涨停重置{AUCTION_ZT_TRAIL_RESET_DAYS}天 最多持{AUCTION_ZT_TRAIL_MAX_DAYS}天")
    print(f"  断板止损          : ✅开启  涨停次日跌破涨停价{abs(AUCTION_ZT_BREAK_STOP):.0%}立即止损")
    print(f"  大盘涨停密度过滤  : {'✅开启' if MARKET_ZT_FILTER_ENABLE else '❌关闭'}"
          f"  全市场涨停占比<{MARKET_ZT_MIN_RATIO:.1%}则暂停信号")
    print(f"  板块热度代理过滤  : {'✅开启' if SECTOR_HEAT_FILTER_ENABLE else '❌关闭'}"
          f"  当日涨停信号数<{SECTOR_HEAT_MIN_SIGNALS}则暂停入场")

    # ★ v8.1：TradingAgents-CN增强模块状态
    print(f"\n  ── v8.1 TradingAgents-CN增强模块状态 ──")
    print(f"  文件缓存层        : {'✅开启' if ENABLE_DATA_CACHE else '❌关闭'}"
          f"  缓存目录={DATA_CACHE_DIR} 过期={DATA_CACHE_EXPIRE_DAYS}天")
    print(f"  财务质量过滤      : {'✅开启' if ENABLE_FINANCIAL_FILTER else '❌关闭(按需开启)'}"
          f"  PE[{FINANCIAL_PE_MIN:.0f}~{FINANCIAL_PE_MAX:.0f}] ROE≥{FINANCIAL_ROE_MIN:.0f}%")
    print(f"  新闻情绪因子      : {'✅开启' if ENABLE_NEWS_SCORE else '❌关闭(实盘按需开启)'}"
          f"  近{NEWS_LOOKBACK_COUNT}条新闻 满分{NEWS_SCORE_MAX:.0f}分 总分上限110")
    print(f"  实时盘中量比      : {'✅开启' if REALTIME_VOL_RATIO_ENABLE else '❌关闭(实盘按需开启)'}"
          f"  接口=ak.stock_zh_a_spot_em()")

    # ★ v9.1：精准化升级模块状态
    print(f"\n  ── v9.1 精准化升级模块状态（连板梯队 + 封板力度 + 次日确认）──")
    print(f"  连板梯队仓位差异  : {'✅开启' if AUCTION_ZT_STREAK_POS_ENABLE else '❌关闭'}"
          f"  2连板×{AUCTION_ZT_2_POS_RATIO:.1f} / 3连板×{AUCTION_ZT_3_POS_RATIO:.1f}"
          f" / 4+连板{'允许强势档' if AUCTION_ZT_4PLUS_ENABLE else '全部停用'}")
    print(f"  封板力度过滤      : {'✅开启' if SEAL_SCORE_ENABLE else '❌关闭'}"
          f"  前日close_pos<{SEAL_SCORE_WEAK_THRESH}直接过滤"
          f"  >={SEAL_SCORE_STRONG_THRESH}=封板极稳（优质信号）")
    print(f"  极限档进化        : ✅开启"
          f"  前日close_pos≥{AUCTION_EXTREME_PREV_CLOSE_POS} + 放量≥均量×{AUCTION_EXTREME_PREV_VOL_RATIO}"
          f" + 连板≤{AUCTION_EXTREME_MAX_STREAK}天")
    print(f"  分时量比持续性    : {'✅开启' if INTRADAY_VOL_SUSTAIN_ENABLE else '❌关闭'}"
          f"  日量/5日均量<{INTRADAY_VOL_SUSTAIN_RATIO:.0%}=盘中无人接盘，过滤")
    print(f"  次日确认出场      : {'✅开启' if AUCTION_NEXT_DAY_CONFIRM_ENABLE else '❌关闭'}"
          f"  弱开<{AUCTION_NEXT_DAY_WEAK_OPEN_MAX:.0%}且close_pos<{AUCTION_NEXT_DAY_CLOSE_POS_MIN}=当日收盘出")
    print(f"  大盘超热放大仓位  : {'✅开启' if MARKET_HOT_BOOST_ENABLE else '❌关闭'}"
          f"  涨停密度>{MARKET_HOT_BOOST_RATIO_THRESH:.1%}时仓位×{MARKET_HOT_BOOST_POS_RATIO}"
          f"  上限×{MARKET_HOT_BOOST_MAX_RATIO}")




    # ★ v4.0：按年度统计
    yearly = stats.get("yearly_stats", {})
    if yearly:
        print(f"\n  ── 按年度统计（验证策略跨周期稳定性）──")
        print(f"  {'年份':<6}{'笔数':>6}{'胜率':>8}{'均收益':>9}{'年度累计':>10}")
        print("  " + "-"*42)
        for yr, s in yearly.items():
            print(
                f"  {yr:<6}{s['count']:>6}  {s['win_rate']:>6.1%}"
                f"  {s['avg_pnl']:>+7.2%}  {s['total_ret']:>+8.2%}"
            )

    # ★ v4.0：按月度统计
    monthly = stats.get("monthly_stats", {})
    if monthly:
        month_names = {1:"1月",2:"2月",3:"3月",4:"4月",5:"5月",6:"6月",
                       7:"7月",8:"8月",9:"9月",10:"10月",11:"11月",12:"12月"}
        print(f"\n  ── 按月度统计（发现季节性规律）──")
        print(f"  {'月份':<6}{'笔数':>6}{'胜率':>8}{'均收益':>9}")
        print("  " + "-"*32)
        for mo, s in monthly.items():
            print(
                f"  {month_names.get(mo, str(mo)):<6}{s['count']:>6}"
                f"  {s['win_rate']:>6.1%}  {s['avg_pnl']:>+7.2%}"
            )

    print()


def factor_analysis_report(trades: list) -> None:
    """
    分因子胜率分析：
      - 量比分组（<1.5 / 1.5~3.0 / >3.0）
      - 连板天数分组（1/2/3/4+）
      - 炸板历史分组（0次 / 1次 / >1次）—— 验证假涨停识别有效性
    """
    if not trades:
        return
    df = pd.DataFrame([t.__dict__ for t in trades])
    df = df.sort_values("entry_date").reset_index(drop=True)

    print("\n" + "="*70)
    print("  因子分析报告（验证多因子过滤有效性）")
    print("  ⚠️  注意：量比为日线5日均量近似值，非真实盘中量比；换手率字段用量比替代")
    print("="*70)

    # ── 量比分组 ──
    def vol_bucket(vr):
        if vr < 1.5: return "低(<1.5)"
        if vr <= 3.0: return "优(1.5~3)"
        return "高(>3)"

    df["vol_bucket"] = df["vol_ratio"].apply(vol_bucket)
    print("\n  ── 量比分组胜率 ──")
    print(f"  {'区间':<12}{'笔数':>6}{'胜率':>8}{'均收益':>9}{'均盈利':>9}{'均亏损':>9}")
    print("  " + "-"*56)
    for bucket in ["低(<1.5)", "优(1.5~3)", "高(>3)"]:
        sub = df[df["vol_bucket"] == bucket]
        if len(sub) == 0: continue
        wins = sub[sub["pnl_pct"] > 0]
        loss = sub[sub["pnl_pct"] <= 0]
        print(
            f"  {bucket:<12}{len(sub):>6}  "
            f"{len(wins)/len(sub):>6.1%}  "
            f"{sub['pnl_pct'].mean():>+7.2%}  "
            f"{wins['pnl_pct'].mean() if len(wins) else 0:>+7.2%}  "
            f"{loss['pnl_pct'].mean() if len(loss) else 0:>+7.2%}"
        )

    # ── 炸板历史分组（验证假涨停过滤有效性）──
    if "bomb_30d" in df.columns:
        print("\n  ── 近30日炸板次数分组胜率（验证假涨停过滤有效性）──")
        print(f"  {'炸板次数':<12}{'笔数':>6}{'胜率':>8}{'均收益':>9}")
        print("  " + "-"*38)
        for label, cond in [("0次(干净)", df["bomb_30d"] == 0),
                             ("1次(谨慎)", df["bomb_30d"] == 1),
                             ("≥2次(危险)", df["bomb_30d"] >= 2)]:
            sub = df[cond]
            if len(sub) == 0: continue
            wins = sub[sub["pnl_pct"] > 0]
            print(
                f"  {label:<12}{len(sub):>6}  "
                f"{len(wins)/len(sub):>6.1%}  "
                f"{sub['pnl_pct'].mean():>+7.2%}"
            )
        # 提示：≥2次组胜率应明显低于0次组，验证过滤有效性
        clean = df[df["bomb_30d"] == 0]
        risky = df[df["bomb_30d"] >= 2]
        if len(clean) > 5 and len(risky) > 5:
            delta = clean["pnl_pct"].mean() - risky["pnl_pct"].mean()
            print(f"\n  → 过滤炸板历史的收益提升：+{delta:.2%}（{delta>0 and '过滤有效' or '过滤无效，可关闭'}）")

    # ── 连板天数分组（仅首板+连板策略）──
    board_df = df[df["strategy"].isin(["首板", "连板"])]
    if not board_df.empty:
        print("\n  ── 连板天数分组胜率（首板+连板） ──")
        print(f"  {'连板天数':<10}{'笔数':>6}{'胜率':>8}{'均收益':>9}")
        print("  " + "-"*36)
        for days in sorted(board_df["zt_days"].unique()):
            sub = board_df[board_df["zt_days"] == days]
            wins = sub[sub["pnl_pct"] > 0]
            label = f"{days}板" if days > 0 else "反包"
            print(
                f"  {label:<10}{len(sub):>6}  "
                f"{len(wins)/len(sub):>6.1%}  "
                f"{sub['pnl_pct'].mean():>+7.2%}"
            )

    # ── 股性分析：日均振幅分组 ──
    if "avg_amplitude" in df.columns:
        def amp_bucket(a):
            if a <= 0:     return "未知"
            if a < 1.5:    return "迟钝(<1.5%)"
            if a < 3.0:    return "普通(1.5~3%)"
            if a < 5.0:    return "活跃(3~5%)"
            return "超活跃(>5%)"

        df["amp_bucket"] = df["avg_amplitude"].apply(amp_bucket)
        print("\n  ── 股性·日均振幅分组胜率（验证活跃度对打板胜率影响）──")
        print(f"  {'振幅区间':<16}{'笔数':>6}{'胜率':>8}{'均收益':>9}")
        print("  " + "-"*42)
        for bucket in ["迟钝(<1.5%)", "普通(1.5~3%)", "活跃(3~5%)", "超活跃(>5%)"]:
            sub = df[df["amp_bucket"] == bucket]
            if len(sub) == 0: continue
            wins = sub[sub["pnl_pct"] > 0]
            print(
                f"  {bucket:<16}{len(sub):>6}  "
                f"{len(wins)/len(sub):>6.1%}  "
                f"{sub['pnl_pct'].mean():>+7.2%}"
            )
        # 提示结论
        active = df[df["avg_amplitude"] >= 3.0]
        dull   = df[df["avg_amplitude"].between(0.1, 1.5)]
        if len(active) >= 5 and len(dull) >= 5:
            delta = active["pnl_pct"].mean() - dull["pnl_pct"].mean()
            print(f"\n  → 活跃股vs迟钝股收益差：{delta:+.2%}"
                  f"（{'活跃股胜率更高，优先选活跃股' if delta > 0 else '差异不显著'}）")

    # ── ★ v6.0 信号评分分组胜率（验证评分系统有效性）──
    if "signal_score" in df.columns and df["signal_score"].sum() > 0:
        def score_bucket(s):
            if s < 40:  return "低分(<40)"
            if s < 55:  return "中低(40~55)"
            if s < 70:  return "中高(55~70)"
            return "高分(≥70)"
        df["score_bucket"] = df["signal_score"].apply(score_bucket)
        print("\n  ── ★ v6.0 信号评分分组胜率（评分系统有效性验证）──")
        print(f"  {'评分区间':<14}{'笔数':>6}{'胜率':>8}{'均收益':>9}{'均持仓':>8}")
        print("  " + "-"*48)
        for bucket in ["低分(<40)", "中低(40~55)", "中高(55~70)", "高分(≥70)"]:
            sub = df[df["score_bucket"] == bucket]
            if len(sub) == 0: continue
            wins = sub[sub["pnl_pct"] > 0]
            print(
                f"  {bucket:<14}{len(sub):>6}  "
                f"{len(wins)/len(sub):>6.1%}  "
                f"{sub['pnl_pct'].mean():>+7.2%}  "
                f"{sub['holding_days'].mean():>5.1f}天"
            )
        hi = df[df["signal_score"] >= 70]
        lo = df[df["signal_score"] < 40]
        if len(hi) >= 5 and len(lo) >= 5:
            delta = hi["pnl_pct"].mean() - lo["pnl_pct"].mean()
            print(f"\n  → 高分vs低分收益差：{delta:+.2%}"
                  f"（{'✅评分系统有效' if delta > 0.02 else '⚠️评分系统需校准'}）")

    # ── ★ v6.0 动态持仓天数有效性验证 ──
    if "dynamic_hold" in df.columns and df["dynamic_hold"].sum() > 0:
        def hold_tier(h):
            if h <= HOLD_DAYS_WEAK:    return f"弱势({HOLD_DAYS_WEAK}天)"
            if h >= HOLD_DAYS_STRONG:  return f"强势({HOLD_DAYS_STRONG}天)"
            return f"默认({HOLD_DAYS}天)"
        df["hold_tier"] = df["dynamic_hold"].apply(hold_tier)
        print(f"\n  ── ★ v6.0 动态持仓分组收益（验证持仓天数调整有效性）──")
        print(f"  {'持仓档位':<14}{'笔数':>6}{'胜率':>8}{'均收益':>9}{'实际均持仓':>10}")
        print("  " + "-"*50)
        for tier in [f"强势({HOLD_DAYS_STRONG}天)", f"默认({HOLD_DAYS}天)", f"弱势({HOLD_DAYS_WEAK}天)"]:
            sub = df[df["hold_tier"] == tier]
            if len(sub) == 0: continue
            wins = sub[sub["pnl_pct"] > 0]
            print(
                f"  {tier:<14}{len(sub):>6}  "
                f"{len(wins)/len(sub):>6.1%}  "
                f"{sub['pnl_pct'].mean():>+7.2%}  "
                f"{sub['holding_days'].mean():>7.1f}天"
            )

    # ── 策略 × 量比 交叉表 ──
    print("\n  ── 策略 × 量比 交叉胜率 ──")
    print(f"  {'策略':<8}", end="")
    buckets = ["低(<1.5)", "优(1.5~3)", "高(>3)"]
    for b in buckets:
        print(f"  {b:>12}", end="")
    print()
    print("  " + "-"*52)
    for st in STRATEGIES_ALL:
        if st == "竞价":
            sub_st_df = df[df["strategy"].str.startswith("竞价")]
        else:
            sub_st_df = df[df["strategy"] == st]
        if len(sub_st_df) == 0:
            continue
        print(f"  {st:<8}", end="")
        for b in buckets:
            sub = sub_st_df[sub_st_df["vol_bucket"] == b]
            if len(sub) == 0:
                print(f"  {'  -':>12}", end="")
            else:
                wr = len(sub[sub["pnl_pct"] > 0]) / len(sub)
                print(f"  {wr:>11.1%}", end="")
        print()

    # ── ★ v9.2 竞价子策略精细化分析（连板梯队 + 开盘高度档） ──
    auction_df = df[df["strategy"] == "竞价"]
    if not auction_df.empty and "zt_days" in auction_df.columns:
        print("\n  ── ★ v9.2 竞价连板梯队精细分析 ──")
        print(f"  {'连板数':<10}{'笔数':>6}{'胜率':>8}{'均收益':>9}{'均盈利':>9}{'均亏损':>9}")
        print("  " + "-"*54)
        for zt_d in sorted(auction_df["zt_days"].unique()):
            sub = auction_df[auction_df["zt_days"] == zt_d]
            wins = sub[sub["pnl_pct"] > 0]
            loss = sub[sub["pnl_pct"] <= 0]
            label = "首板竞价" if zt_d == 0 else f"{zt_d}连板竞价"
            print(
                f"  {label:<10}{len(sub):>6}  "
                f"{len(wins)/len(sub) if len(sub) else 0:>6.1%}  "
                f"{sub['pnl_pct'].mean():>+7.2%}  "
                f"{wins['pnl_pct'].mean() if len(wins) else 0:>+7.2%}  "
                f"{loss['pnl_pct'].mean() if len(loss) else 0:>+7.2%}"
            )
        # 给出连板梯队建议
        zt2 = auction_df[auction_df["zt_days"] == 2]
        zt3 = auction_df[auction_df["zt_days"] == 3]
        zt4p = auction_df[auction_df["zt_days"] >= 4]
        if len(zt2) >= 5:
            print(f"\n  → 2连板竞价：{len(zt2)}笔 | 胜率{len(zt2[zt2['pnl_pct']>0])/len(zt2):.1%}"
                  f" | 均收益{zt2['pnl_pct'].mean():+.2%}  ← {'✅优先' if zt2['pnl_pct'].mean() > 0.01 else '⚠️观察'}")
        if len(zt3) >= 5:
            print(f"  → 3连板竞价：{len(zt3)}笔 | 胜率{len(zt3[zt3['pnl_pct']>0])/len(zt3):.1%}"
                  f" | 均收益{zt3['pnl_pct'].mean():+.2%}  ← {'✅可做' if zt3['pnl_pct'].mean() > 0 else '⚠️降仓或跳过'}")
        if len(zt4p) >= 3:
            print(f"  → 4+连板竞价：{len(zt4p)}笔 | 胜率{len(zt4p[zt4p['pnl_pct']>0])/len(zt4p):.1%}"
                  f" | 均收益{zt4p['pnl_pct'].mean():+.2%}  ← {'⛔建议停用' if zt4p['pnl_pct'].mean() < 0 else '⚠️谨慎'}")

    # ── ★ v9.2 出场原因精细统计（找最大亏损来源）──
    if "exit_reason" in df.columns:
        print("\n  ── ★ v9.2 出场原因分析（亏损来源诊断）──")
        print(f"  {'出场原因':<16}{'笔数':>6}{'胜率':>8}{'均收益':>9}{'占总亏损%':>10}")
        print("  " + "-"*52)
        total_loss_sum = df[df["pnl_pct"] < 0]["pnl_pct"].sum()
        reasons_order = ["stop_loss", "time_stop", "day2_stop", "gap_abort", "zt_break_stop",
                         "target1", "target2", "trail_stop", "timeout"]
        seen_reasons = df["exit_reason"].unique()
        for reason in [r for r in reasons_order if r in seen_reasons]:
            sub = df[df["exit_reason"] == reason]
            wins = sub[sub["pnl_pct"] > 0]
            sub_loss = sub[sub["pnl_pct"] < 0]["pnl_pct"].sum()
            loss_pct = sub_loss / total_loss_sum * 100 if total_loss_sum < 0 else 0
            print(
                f"  {reason:<16}{len(sub):>6}  "
                f"{len(wins)/len(sub) if len(sub) else 0:>6.1%}  "
                f"{sub['pnl_pct'].mean():>+7.2%}  "
                f"{loss_pct:>8.1f}%"
            )
        # 其他未列出的原因
        other = df[~df["exit_reason"].isin(reasons_order)]
        if len(other) > 0:
            wins_o = other[other["pnl_pct"] > 0]
            print(f"  {'其他':<16}{len(other):>6}  "
                  f"{len(wins_o)/len(other) if len(other) else 0:>6.1%}  "
                  f"{other['pnl_pct'].mean():>+7.2%}")
        # 给出优化建议
        timeout_sub = df[df["exit_reason"] == "timeout"]
        if len(timeout_sub) > 10:
            timeout_pct = len(timeout_sub) / len(df)
            print(f"\n  → timeout超时出场占{timeout_pct:.1%}（{'⚠️建议缩短持仓天数' if timeout_pct > 0.25 else '✅正常'}）")

    print()


def save_csv(trades: list, path: str = "daban_trades.csv") -> None:
    if not trades:
        return
    fields = list(trades[0].__dict__.keys())
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for t in trades:
            writer.writerow(t.__dict__)
    log.info(f"交易明细已保存到 {path}")


# ================================================================
# Server酱推送
# ================================================================
def push_serverchan(stats: dict, trades: list,
                    sendkey: str = "",
                    bt_start: str = "", bt_end: str = "") -> None:
    """
    回测完成后通过 Server酱 推送摘要到微信。
    sendkey 优先从参数取，为空时读环境变量 SENDKEY。
    """
    import urllib.request
    import urllib.parse

    key = sendkey or os.environ.get("SENDKEY", "")
    if not key:
        log.info("未配置 SENDKEY，跳过 Server酱推送")
        return

    if not stats:
        log.info("无回测结果，跳过推送")
        return

    total    = stats.get("total", 0)
    wr       = stats.get("win_rate", 0)
    avg_pnl  = stats.get("avg_pnl", 0)
    avg_win  = stats.get("avg_win", 0)
    avg_loss = stats.get("avg_loss", 0)
    pnl_r    = stats.get("pnl_ratio", 0)
    max_dd   = stats.get("max_drawdown", 0)
    ann_ret  = stats.get("annual_return", 0)
    sharpe   = stats.get("sharpe", 0)

    # ── 各策略摘要 ──
    st_lines = []
    for st, s in stats.get("strategy_stats", {}).items():
        st_lines.append(
            f"- {st}：{s['count']}笔  胜率{s['win_rate']:.1%}  "
            f"均收{s['avg_pnl']:+.2%}"
        )
    # 竞价子策略细分
    for sub_st, s in stats.get("auction_sub_stats", {}).items():
        st_lines.append(
            f"  - {sub_st}：{s['count']}笔  胜率{s['win_rate']:.1%}  "
            f"均收{s['avg_pnl']:+.2%}"
        )

    # ── 年度摘要（最近3年）──
    yearly = stats.get("yearly_stats", {})
    yr_lines = []
    for yr, s in list(yearly.items())[-3:]:
        yr_lines.append(
            f"- {yr}年：{s['count']}笔  胜率{s['win_rate']:.1%}  "
            f"累计{s['total_ret']:+.2%}"
        )

    # ── 连续亏损 ──
    consec = stats.get("max_consec_loss", {})
    consec_line = (
        f"最大连亏：{consec.get('max_streak', 0)}笔 / "
        f"累计{consec.get('max_loss_sum', 0):+.2%}"
    )

    date_range = f"{bt_start} ~ {bt_end}" if bt_start and bt_end else "自定义区间"

    title = f"打板回测完成 | 胜率{wr:.1%} 年化{ann_ret:+.2%}"

    body = f"""## 打板策略 · 历史回测报告

**回测区间**：{date_range}

| 指标 | 数值 |
|------|------|
| 总交易笔数 | {total} |
| 整体胜率 | {wr:.1%} |
| 平均每笔收益 | {avg_pnl:+.2%} |
| 平均盈利 | {avg_win:+.2%} |
| 平均亏损 | {avg_loss:+.2%} |
| 盈亏比 | {pnl_r:.2f}x |
| 最大回撤 | {max_dd:.2%} |
| 年化收益（估） | {ann_ret:+.2%} |
| 夏普比率（估） | {sharpe:.2f} |

### 各策略分类
{chr(10).join(st_lines) if st_lines else "（无数据）"}

### 近年度表现
{chr(10).join(yr_lines) if yr_lines else "（无数据）"}

### 风险指标
{consec_line}
资金管理：每笔 {POSITION_SIZE:.0%} 仓位，最多 {MAX_POSITIONS} 只同持
"""

    # Server酱 SCKEY / SCT 两种 key 格式兼容
    if key.startswith("SCT"):
        url = f"https://sctapi.ftqq.com/{key}.send"
    else:
        url = f"https://sc.ftqq.com/{key}.send"

    data = urllib.parse.urlencode({
        "title": title,
        "desp":  body,
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = resp.read().decode("utf-8")
        log.info(f"Server酱推送成功: {result[:80]}")
    except Exception as e:
        log.warning(f"Server酱推送失败（不影响回测结果）: {e}")


def plot_equity(trades: list, stats: dict) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")   # 非交互后端，不弹窗，支持无显示器环境
        import matplotlib.pyplot as plt
        matplotlib.rcParams["font.sans-serif"] = ["SimHei", "Arial Unicode MS", "DejaVu Sans"]
        matplotlib.rcParams["axes.unicode_minus"] = False
    except ImportError:
        log.warning("matplotlib 未安装，跳过画图")
        return

    if not trades:
        return

    df = pd.DataFrame([t.__dict__ for t in trades])
    df = df.sort_values("entry_date").reset_index(drop=True)

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle("打板策略 · 回测分析 (v4.0 资金管理版)", fontsize=14)

    # 1. ★ v4.0：资金管理净值曲线（固定仓位）
    equity_curve = stats.get("equity_curve", None)
    if equity_curve is None or len(equity_curve) < 2:
        equity_curve = (1 + df["pnl_pct"]).cumprod()
    axes[0, 0].plot(equity_curve.values, color="steelblue", linewidth=1.5)
    axes[0, 0].axhline(1, color="gray", linestyle="--", linewidth=0.8)
    axes[0, 0].fill_between(range(len(equity_curve)), 1, equity_curve.values,
                             where=(equity_curve.values >= 1), alpha=0.15, color="green")
    axes[0, 0].fill_between(range(len(equity_curve)), 1, equity_curve.values,
                             where=(equity_curve.values < 1), alpha=0.15, color="red")
    max_dd = stats.get("max_drawdown", 0)
    axes[0, 0].set_title(f"资金净值曲线（{POSITION_SIZE:.0%}仓位×{MAX_POSITIONS}只，最大回撤{max_dd:.1%}）")
    axes[0, 0].set_xlabel("交易笔数"); axes[0, 0].set_ylabel("净值")

    # 2. 各策略胜率对比
    st_data  = stats.get("strategy_stats", {})
    if st_data:
        names = list(st_data.keys())
        wrs   = [st_data[k]["win_rate"] for k in names]
        apnls = [st_data[k]["avg_pnl"]  for k in names]
        x     = np.arange(len(names))
        w     = 0.35
        axes[0, 1].bar(x - w/2, wrs, w, label="胜率", color="#5FA8D3")
        axes[0, 1].axhline(0.5, color="red", linestyle="--", linewidth=0.8)
        ax2 = axes[0, 1].twinx()
        ax2.bar(x + w/2, [v * 100 for v in apnls], w, label="均收益(%)", color="#FB923C", alpha=0.7)
        ax2.axhline(0, color="gray", linestyle=":", linewidth=0.6)
        axes[0, 1].set_xticks(x); axes[0, 1].set_xticklabels(names)
        axes[0, 1].set_title("各策略胜率 vs 均收益")
        axes[0, 1].set_ylim(0, 1)
        axes[0, 1].legend(loc="upper left"); ax2.legend(loc="upper right")

    # 3. 每笔收益分布
    axes[0, 2].hist(df["pnl_pct"] * 100, bins=40, color="steelblue",
                    edgecolor="white", linewidth=0.5)
    axes[0, 2].axvline(0, color="red", linestyle="--", linewidth=0.8)
    axes[0, 2].axvline(df["pnl_pct"].mean() * 100, color="orange",
                        linestyle="-.", linewidth=1.2, label=f"均值{df['pnl_pct'].mean()*100:+.2f}%")
    axes[0, 2].legend(fontsize=8)
    axes[0, 2].set_title("每笔收益分布（%）")
    axes[0, 2].set_xlabel("收益率（%）")

    # 4. 出场原因饼图
    reasons = df["exit_reason"].value_counts()
    label_map = {
        "stop_loss": "ATR止损", "time_stop": "时间止损",
        "gap_abort": "低开止损", "day2_stop": "第2天强制止损",
        "ma5_stop": "均线止损", "ma10_stop": "MA10止损",
        "trail_stop": "跟踪止盈", "t2_trail": "T2追踪",
        "target1": "一止盈", "target2": "二止盈", "timeout": "超时"
    }
    labels = [label_map.get(r, r) for r in reasons.index]
    colors = []
    for r in reasons.index:
        if r in ("trail_stop", "t2_trail", "target1", "target2", "timeout", "ma10_stop"):
            colors.append("#22C55E")
        else:
            colors.append("#EF4444")
    axes[1, 0].pie(reasons.values, labels=labels, autopct="%1.1f%%", colors=colors)
    axes[1, 0].set_title("出场原因分布（绿=盈利/超时，红=止损）")

    # 5. ★ v4.0：按年度胜率柱状图
    yearly = stats.get("yearly_stats", {})
    if yearly:
        yrs  = [str(y) for y in yearly.keys()]
        ywrs = [yearly[y]["win_rate"] for y in yearly.keys()]
        yret = [yearly[y]["total_ret"] * 100 for y in yearly.keys()]
        x    = np.arange(len(yrs))
        w    = 0.35
        axes[1, 1].bar(x - w/2, ywrs, w, label="胜率", color="#5FA8D3")
        axes[1, 1].axhline(0.5, color="red", linestyle="--", linewidth=0.8)
        ax3 = axes[1, 1].twinx()
        colors_yr = ["#22C55E" if v >= 0 else "#EF4444" for v in yret]
        ax3.bar(x + w/2, yret, w, label="年度累计(%)", color=colors_yr, alpha=0.7)
        ax3.axhline(0, color="gray", linestyle=":", linewidth=0.6)
        axes[1, 1].set_xticks(x); axes[1, 1].set_xticklabels(yrs, rotation=45)
        axes[1, 1].set_title("按年度统计（验证跨周期稳定性）")
        axes[1, 1].set_ylim(0, 1)
        axes[1, 1].legend(loc="upper left"); ax3.legend(loc="upper right")

    # 6. ★ v4.0：按月度胜率热力图
    monthly = stats.get("monthly_stats", {})
    if monthly:
        months = list(range(1, 13))
        month_wrs = [monthly.get(m, {}).get("win_rate", 0) for m in months]
        month_pnls = [monthly.get(m, {}).get("avg_pnl", 0) * 100 for m in months]
        month_names = ["1月","2月","3月","4月","5月","6月","7月","8月","9月","10月","11月","12月"]
        x = np.arange(12)
        colors_mo = ["#22C55E" if v >= 0 else "#EF4444" for v in month_pnls]
        axes[1, 2].bar(x, month_pnls, color=colors_mo, alpha=0.7, label="月均收益(%)")
        axes[1, 2].axhline(0, color="gray", linestyle=":", linewidth=0.6)
        ax4 = axes[1, 2].twinx()
        ax4.plot(x, month_wrs, "o-", color="#5FA8D3", linewidth=1.5, label="月胜率")
        ax4.axhline(0.5, color="red", linestyle="--", linewidth=0.8, alpha=0.5)
        ax4.set_ylim(0, 1)
        axes[1, 2].set_xticks(x); axes[1, 2].set_xticklabels(month_names, rotation=45, fontsize=8)
        axes[1, 2].set_title("按月度统计（季节性规律）")
        axes[1, 2].legend(loc="upper left", fontsize=8)
        ax4.legend(loc="upper right", fontsize=8)

    plt.tight_layout()
    plt.savefig("daban_result.png", dpi=120, bbox_inches="tight")
    log.info("图表已保存到 daban_result.png")
    plt.close("all")


# ================================================================
# 主流程
# ================================================================
def main():
    parser = argparse.ArgumentParser(description="打板策略 · 历史回测")
    parser.add_argument("--codes",    nargs="+", help="指定股票代码（不加则跑全部股票池）")
    parser.add_argument("--start",    default="2021-01-01", help="回测开始日期")
    parser.add_argument("--end",      default=datetime.date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--strategy", nargs="+", choices=STRATEGIES_ALL,
                        default=STRATEGIES_ALL, help="要跑的策略，默认全部")
    parser.add_argument("--max-stocks", type=int, default=0,
                        help="从股票池随机抽取N只（0=全量，用于快速测试）")
    parser.add_argument("--workers",  type=int, default=20)
    parser.add_argument("--plot",     action="store_true", help="画图")
    parser.add_argument("--csv",      default="daban_trades.csv")
    parser.add_argument("--factor-analysis", action="store_true",
                        help="输出量比/连板天数等因子分组胜率分析")
    parser.add_argument("--sendkey", default="",
                        help="Server酱 SendKey（也可通过环境变量 SENDKEY 传入）")
    args = parser.parse_args()

    log.info(f"回测区间：{args.start} ~ {args.end}")
    log.info(f"回测策略：{args.strategy}")

    # 确定股票列表
    if args.codes:
        codes = []
        for c in args.codes:
            c = c.zfill(6)
            codes.append(c + ".SH" if c.startswith("6") else c + ".SZ")
        log.info(f"指定股票：{codes}")
    else:
        codes = fetch_pool_codes()
        if not codes:
            log.error("无法获取股票池，请用 --codes 指定")
            return
        if args.max_stocks and args.max_stocks < len(codes):
            import random
            random.seed(42)
            codes = random.sample(codes, args.max_stocks)
            log.info(f"随机抽取 {args.max_stocks} 只股票进行回测")

    # 拉取历史数据
    # baostock 不支持多线程并发，串行拉取；akshare/yfinance 可并发
    log.info(f"开始拉取 {len(codes)} 只股票历史数据...")
    hist_data = {}
    if USE_BS:
        # 串行拉取，避免 baostock session 冲突
        for i, c in enumerate(codes, 1):
            df = fetch_history(c, args.start, args.end)
            if df is not None:
                hist_data[c] = df
            if i % 50 == 0:
                log.info(f"  已拉取 {i}/{len(codes)}，有效 {len(hist_data)} 只...")
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as exe:
            futs = {exe.submit(fetch_history, c, args.start, args.end): c for c in codes}
            for fut in as_completed(futs):
                c  = futs[fut]
                df = fut.result()
                if df is not None:
                    hist_data[c] = df
    log.info(f"数据加载完成：{len(hist_data)} 只")

    if not hist_data:
        log.error("无有效数据，退出")
        return

    # ★ v9.0：预计算每日市场涨停密度（用于大盘情绪硬过滤）
    # 用所有股票日线数据，统计每日涨停家数/总股票数
    if MARKET_ZT_FILTER_ENABLE or SECTOR_HEAT_FILTER_ENABLE:
        log.info("★ v9.0：预计算每日市场涨停密度...")
        daily_zt_counts: dict = {}   # date_str → 涨停家数
        daily_total_counts: dict = {}  # date_str → 当日参与计算的股票数
        for _c, _df in hist_data.items():
            for _, _row in _df.iterrows():
                _d = str(_row["date"])[:10]
                if bool(_row.get("is_zt", False)):
                    daily_zt_counts[_d] = daily_zt_counts.get(_d, 0) + 1
                daily_total_counts[_d] = daily_total_counts.get(_d, 0) + 1
        # 计算每日涨停占比
        daily_zt_ratio: dict = {}
        for _d in daily_total_counts:
            _total = daily_total_counts[_d]
            _zt = daily_zt_counts.get(_d, 0)
            daily_zt_ratio[_d] = _zt / _total if _total > 0 else 0.0
        # 写入全局（backtest_auction_board函数通过此全局变量访问）
        import builtins
        builtins._daily_zt_ratio = daily_zt_ratio
        builtins._daily_zt_counts = daily_zt_counts
        log.info(f"  大盘涨停密度预计算完成，共{len(daily_zt_ratio)}个交易日")

    # 逐股回测
    log.info("开始逐股回测...")
    all_trades = []
    with ThreadPoolExecutor(max_workers=min(args.workers, 8)) as exe:
        futs = {
            exe.submit(backtest_single, c, df, args.strategy): c
            for c, df in hist_data.items()
        }
        done = 0
        for fut in as_completed(futs):
            trades = fut.result()
            all_trades.extend(trades)
            done += 1
            if done % 50 == 0:
                log.info(f"  回测进度：{done}/{len(hist_data)}，累计 {len(all_trades)} 笔")

    log.info(f"回测完成，共 {len(all_trades)} 笔交易")

    if not all_trades:
        log.warning("无交易信号，请检查股票数据或放宽参数")
        return

    stats = calc_stats(all_trades, bt_start=args.start, bt_end=args.end)
    print_report(stats, all_trades)
    save_csv(all_trades, args.csv)

    if getattr(args, "factor_analysis", False):
        factor_analysis_report(all_trades)

    if args.plot:
        plot_equity(all_trades, stats)

    # ── Server酱推送回测摘要 ──
    push_serverchan(stats, all_trades,
                    sendkey=args.sendkey,
                    bt_start=args.start, bt_end=args.end)


if __name__ == "__main__":
    main()
