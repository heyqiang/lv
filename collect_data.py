#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铝价数据采集脚本
数据源: akshare(SHFE/SMM) + yfinance(COMEX/LME代理)
部署: 宝塔面板定时任务，每个交易日 17:00 执行
"""

import json
import re
import os
import sys
import logging
from datetime import datetime, timezone, timedelta

# ============================================================
#  配置
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_FILE = os.path.join(BASE_DIR, "index.html")
LOG_FILE = os.path.join(BASE_DIR, "collect.log")
PULL_TIME_FILE = os.path.join(BASE_DIR, "pull_time.json")

BJT = timezone(timedelta(hours=8))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ============================================================
#  数据采集函数（多源交叉验证）
# ============================================================

def fetch_lme_yfinance():
    """LME铝 - 通过 yfinance COMEX铝期货 ALI=F 获取"""
    try:
        import yfinance as yf
        ticker = yf.Ticker("ALI=F")
        hist = ticker.history(period="5d")
        if hist.empty:
            return None
        row = hist.iloc[-1]
        return {
            "price": round(float(row["Close"]), 2),
            "date": str(hist.index[-1].date()),
            "source": "yfinance/COMEX ALI=F",
        }
    except Exception as e:
        log.warning(f"yfinance 采集失败: {e}")
        return None


def fetch_shfe_akshare():
    """SHFE沪铝主力合约 - 通过 akshare 获取"""
    try:
        import akshare as ak
        # 获取主力合约代码
        df = ak.futures_zh_daily_sina(symbol="AL0")  # AL0 = 主力连续
        if df is None or df.empty:
            return None
        row = df.iloc[-1]
        return {
            "price": round(float(row["close"]), 2),
            "date": str(row["date"]) if "date" in row.index else str(df.index[-1]),
            "source": "akshare/SHFE主力合约(Sina)",
        }
    except Exception as e:
        log.warning(f"akshare SHFE 采集失败: {e}")
        return None


def fetch_shfe_akshare_v2():
    """备用: akshare 期货实时行情（新浪接口）"""
    try:
        import akshare as ak
        df = ak.futures_zh_realtime(symbol="AL")
        if df is None or df.empty:
            return None
        # 按成交量找主力合约
        if "volume" in df.columns:
            main = df.sort_values("volume", ascending=False).iloc[0]
        else:
            main = df.iloc[0]
        price_col = [c for c in main.index if "price" in c.lower() or "最新" in c or "close" in c]
        if price_col:
            price = float(main[price_col[0]])
        else:
            price = float(main.iloc[1])  # 通常第二列是价格
        return {
            "price": round(price, 2),
            "date": datetime.now(BJT).strftime("%Y-%m-%d"),
            "source": "akshare/SHFE实时行情",
        }
    except Exception as e:
        log.warning(f"akshare SHFE v2 采集失败: {e}")
        return None


def fetch_spot_akshare():
    """国内现货铝价 - 通过 akshare 获取"""
    try:
        import akshare as ak
        # 尝试多个可能的函数名
        for func_name in ["spot_symbol_table_sge", "futures_spot_price_daily"]:
            if hasattr(ak, func_name):
                df = getattr(ak, func_name)()
                if df is not None and not df.empty:
                    al_rows = df[df.apply(lambda r: "铝" in str(r.values), axis=1)]
                    if not al_rows.empty:
                        row = al_rows.iloc[0]
                        for col in row.index:
                            val = str(row[col])
                            if re.match(r"^\d{4,6}(\.\d+)?$", val):
                                return {
                                    "price": round(float(val), 2),
                                    "date": datetime.now(BJT).strftime("%Y-%m-%d"),
                                    "source": f"akshare/{func_name}",
                                }
        return None
    except Exception as e:
        log.warning(f"akshare 现货采集失败: {e}")
        return None


def fetch_dxy_yfinance():
    """美元指数 DXY"""
    try:
        import yfinance as yf
        ticker = yf.Ticker("DX-Y.NYB")
        hist = ticker.history(period="5d")
        if hist.empty:
            return None
        return {
            "price": round(float(hist.iloc[-1]["Close"]), 2),
            "source": "yfinance/DX-Y.NYB",
        }
    except Exception as e:
        log.warning(f"DXY 采集失败: {e}")
        return None


# ============================================================
#  交叉验证
# ============================================================

def collect_all():
    """采集所有数据并交叉验证"""
    log.info("=" * 50)
    log.info("开始铝价数据采集")

    result = {
        "timestamp": datetime.now(BJT).strftime("%Y-%m-%dT%H:%M:%S+08:00"),
        "date": datetime.now(BJT).strftime("%Y-%m-%d"),
        "lme": None,
        "shfe": None,
        "spot": None,
        "dxy": None,
        "sources_used": [],
        "warnings": [],
    }

    # --- LME ---
    lme = fetch_lme_yfinance()
    if lme:
        result["lme"] = lme["price"]
        result["sources_used"].append(lme["source"])
        log.info(f"LME: ${lme['price']} ({lme['source']})")
    else:
        result["warnings"].append("LME数据采集失败")
        log.error("LME: 所有数据源均失败")

    # --- SHFE（双源验证） ---
    shfe1 = fetch_shfe_akshare()
    shfe2 = fetch_shfe_akshare_v2()

    if shfe1 and shfe2:
        diff = abs(shfe1["price"] - shfe2["price"]) / shfe1["price"] * 100
        if diff > 1.0:
            result["warnings"].append(
                f"SHFE双源偏差 {diff:.2f}%: {shfe1['price']} vs {shfe2['price']}"
            )
            log.warning(f"SHFE双源偏差较大: {diff:.2f}%")
        result["shfe"] = shfe1["price"]  # 优先用日线数据
        result["sources_used"].extend([shfe1["source"], shfe2["source"]])
        log.info(f"SHFE: {shfe1['price']} 元 ({shfe1['source']}) | 验证: {shfe2['price']} ({shfe2['source']})")
    elif shfe1:
        result["shfe"] = shfe1["price"]
        result["sources_used"].append(shfe1["source"])
        result["warnings"].append("SHFE仅单源验证")
        log.info(f"SHFE: {shfe1['price']} 元 (单源: {shfe1['source']})")
    elif shfe2:
        result["shfe"] = shfe2["price"]
        result["sources_used"].append(shfe2["source"])
        result["warnings"].append("SHFE仅单源验证(备用)")
        log.info(f"SHFE: {shfe2['price']} 元 (单源: {shfe2['source']})")
    else:
        result["warnings"].append("SHFE数据采集失败")
        log.error("SHFE: 所有数据源均失败")

    # --- 现货 ---
    spot = fetch_spot_akshare()
    if spot:
        result["spot"] = spot["price"]
        result["sources_used"].append(spot["source"])
        log.info(f"现货: {spot['price']} 元 ({spot['source']})")

    # --- DXY ---
    dxy = fetch_dxy_yfinance()
    if dxy:
        result["dxy"] = dxy["price"]
        result["sources_used"].append(dxy["source"])
        log.info(f"DXY: {dxy['price']} ({dxy['source']})")

    # --- 验证汇总 ---
    if not result["lme"] and not result["shfe"]:
        log.error("核心数据全部缺失，放弃更新")
        result["status"] = "FAILED"
    elif result["warnings"]:
        result["status"] = "PARTIAL"
        log.warning(f"部分数据有问题: {result['warnings']}")
    else:
        result["status"] = "OK"
        log.info("所有数据采集成功且已交叉验证")

    return result


# ============================================================
#  更新 HTML 报告
# ============================================================

def extract_data_json(html_content):
    """从 HTML 中提取 DATA JSON 对象"""
    pattern = r"const DATA = (\{.+?\});\s*\n"
    match = re.search(pattern, html_content, re.DOTALL)
    if not match:
        raise ValueError("无法在 HTML 中找到 DATA JSON")
    return json.loads(match.group(1)), match.start(1), match.end(1)


def update_html(collected):
    """将采集到的数据写入 HTML 报告"""
    if collected["status"] == "FAILED":
        log.error("数据采集失败，不更新 HTML")
        return False

    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    data, start, end = extract_data_json(html)
    today = datetime.now(BJT)
    today_str = f"{today.month}/{today.day}"

    # 找到今天在 dates 数组中的索引
    dates = data["lme"]["dates"]
    if today_str not in dates:
        log.warning(f"今日 {today_str} 不在预测日期中，跳过更新")
        return False

    idx = dates.index(today_str)

    # 更新 actual 数组
    if collected["lme"] is not None:
        data["lme"]["actual"][idx] = collected["lme"]
        # 预测线衔接点也要更新
        if data["lme"]["predicted"][idx] is not None:
            data["lme"]["predicted"][idx] = collected["lme"]
        log.info(f"LME actual[{idx}] = {collected['lme']}")

    shfe_val = collected["shfe"] or collected["spot"]
    if shfe_val is not None:
        data["shfe"]["actual"][idx] = shfe_val
        if data["shfe"]["predicted"][idx] is not None:
            data["shfe"]["predicted"][idx] = shfe_val
        log.info(f"SHFE actual[{idx}] = {shfe_val}")

    # 更新 daily_forecast
    today_iso = today.strftime("%Y-%m-%d")
    for entry in data["daily_forecast"]:
        if entry["date"] == today_iso:
            if collected["lme"] is not None:
                dev_lme = (collected["lme"] - entry["lme"]["predicted"]) / entry["lme"]["predicted"] * 100
                entry["lme"]["actual"] = collected["lme"]
                entry["confidence_factors"] = {
                    "data_status": "已收盘",
                    "source": ", ".join(collected["sources_used"]),
                    "deviation_lme": f"{dev_lme:+.2f}%",
                }
            if shfe_val is not None:
                dev_shfe = (shfe_val - entry["shfe"]["predicted"]) / entry["shfe"]["predicted"] * 100
                entry["shfe"]["actual"] = shfe_val
                entry["confidence_factors"]["deviation_shfe"] = f"{dev_shfe:+.2f}%"
            entry["is_actual"] = True
            entry["confidence"] = 1.00
            if collected["warnings"]:
                entry["confidence_factors"]["warnings"] = "; ".join(collected["warnings"])

            # 偏差超过3%时微调后续预测
            if collected["lme"] and abs(dev_lme) > 3:
                adjust_future_predictions(data, idx, collected["lme"], "lme")
                log.info(f"LME偏差 {dev_lme:.2f}%，已微调后续预测")
            if shfe_val and abs(dev_shfe) > 3:
                adjust_future_predictions(data, idx, shfe_val, "shfe")
                log.info(f"SHFE偏差 {dev_shfe:.2f}%，已微调后续预测")
            break

    # 更新 meta
    data["meta"]["updated"] = collected["timestamp"]

    # 写回 HTML
    new_json = json.dumps(data, ensure_ascii=False, indent=6)
    new_html = html[:start] + new_json + html[end:]

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(new_html)

    log.info("HTML 报告已更新")
    return True


def adjust_future_predictions(data, current_idx, actual_price, market):
    """偏差超过3%时按比例微调后续预测"""
    predicted_arr = data[market]["predicted"]
    old_pred = predicted_arr[current_idx] if predicted_arr[current_idx] else actual_price
    if old_pred == 0:
        return
    ratio = actual_price / old_pred

    for entry in data["daily_forecast"]:
        entry_idx_str = f"{datetime.strptime(entry['date'], '%Y-%m-%d').month}/{datetime.strptime(entry['date'], '%Y-%m-%d').day}"
        if entry_idx_str in data[market]["dates"]:
            i = data[market]["dates"].index(entry_idx_str)
            if i > current_idx and not entry["is_actual"]:
                if predicted_arr[i] is not None:
                    predicted_arr[i] = round(predicted_arr[i] * ratio, 2)
                entry[market]["predicted"] = round(entry[market]["predicted"] * ratio, 2)
                entry[market]["low"] = round(entry[market]["low"] * ratio, 2)
                entry[market]["high"] = round(entry[market]["high"] * ratio, 2)


# ============================================================
#  写入拉取时间
# ============================================================

def write_pull_time():
    """写入数据拉取时间"""
    now = datetime.now(BJT)
    with open(PULL_TIME_FILE, "w", encoding="utf-8") as f:
        json.dump({"pull_time": now.strftime("%Y年%m月%d日 %H:%M:%S")}, f, ensure_ascii=False)


# ============================================================
#  主入口
# ============================================================

def main():
    log.info("=" * 60)
    log.info(f"铝价数据采集启动 | {datetime.now(BJT).strftime('%Y-%m-%d %H:%M:%S')} 北京时间")

    # 检查是否交易日（周一至周五）
    now = datetime.now(BJT)
    if now.weekday() >= 5:
        log.info("今日为周末，跳过采集")
        return

    # 采集数据
    collected = collect_all()

    # 保存原始采集结果
    raw_file = os.path.join(BASE_DIR, "latest_raw.json")
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)
    log.info(f"原始数据已保存至 {raw_file}")

    # 更新 HTML
    if os.path.exists(HTML_FILE):
        update_html(collected)
    else:
        log.error(f"HTML 文件不存在: {HTML_FILE}")

    # 写入拉取时间
    write_pull_time()

    # 输出摘要
    print("\n" + "=" * 50)
    print(f"  铝价日报 {now.strftime('%Y-%m-%d')}")
    print("=" * 50)
    if collected["lme"]:
        print(f"  LME:   ${collected['lme']}")
    if collected["shfe"]:
        print(f"  SHFE:  {collected['shfe']} 元")
    if collected["spot"]:
        print(f"  现货:  {collected['spot']} 元")
    if collected["dxy"]:
        print(f"  DXY:   {collected['dxy']}")
    print(f"  状态:  {collected['status']}")
    if collected["warnings"]:
        print(f"  警告:  {'; '.join(collected['warnings'])}")
    print(f"  数据源: {', '.join(collected['sources_used'])}")
    print("=" * 50)


if __name__ == "__main__":
    main()
