from __future__ import annotations

import math
import re
from functools import lru_cache
from typing import Any

import akshare as ak
import pandas as pd

from src.data.cache import get_cache
from src.data.models import CompanyNews, FinancialMetrics, InsiderTrade, LineItem, Price
from src.tools.eastmoney_excel_bridge import (
    collect_field_values,
    filter_values_for_agent,
    get_allowed_fields_for_agent,
)
from src.tools.scrapling_framework import DataQualityGuard, StabilityConfig, StableFetcher

_cache = get_cache()
_dq = DataQualityGuard()
_stable = StableFetcher(
    StabilityConfig(
        timeout_sec=15.0,
        max_retries=4,
        backoff_base_sec=0.7,
        backoff_cap_sec=8.0,
        backoff_jitter_ratio=0.25,
        rate_limit_qps=2.0,
        breaker_fail_threshold=4,
        breaker_open_sec=30.0,
    )
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _code(ticker: str) -> str:
    x = "".join(re.findall(r"\d", str(ticker or "")))
    if len(x) != 6:
        raise ValueError(f"Invalid A-share code: {ticker}")
    return x


def _secid(code: str) -> str:
    # Eastmoney secid: 1=SH, 0=SZ/BJ.
    return f"{1 if code.startswith('6') else 0}.{code}"


def _em_symbol(ticker: str) -> str:
    c = _code(ticker)
    if c.startswith(("4", "8")):
        return f"BJ{c}"
    if c.startswith("6"):
        return f"SH{c}"
    return f"SZ{c}"


def _headers_for_eastmoney(code: str) -> dict[str, str]:
    return {
        "Referer": f"https://data.eastmoney.com/bbsj/{code}.html",
        "User-Agent": _UA,
        "Accept": "application/json,text/plain,*/*",
    }


def _f(v: Any) -> float | None:
    return _dq.normalize_number(v)


def _ratio(v: Any) -> float | None:
    return _dq.percent_to_ratio(v)


def _i(v: Any) -> int | None:
    x = _f(v)
    if x is None:
        return None
    return int(round(x))


def _safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or abs(b) < 1e-9:
        return None
    x = a / b
    if math.isinf(x) or math.isnan(x):
        return None
    return x


def _sum(*vals: float | None) -> float | None:
    xs = [x for x in vals if x is not None]
    return None if not xs else float(sum(xs))


def _coalesce(*vals: float | None) -> float | None:
    for v in vals:
        if v is not None:
            return v
    return None


def _date(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v)
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        if re.fullmatch(r"\d{8}", s):
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return None


def _in_window(ds: str | None, start: str | None, end: str | None) -> bool:
    if not ds:
        return False
    if start and ds < start:
        return False
    if end and ds > end:
        return False
    return True


def _is_valid_name(name: str | None) -> bool:
    if not name:
        return False
    if re.search(r"[\uac00-\ud7a3]", name):
        return False
    return re.search(r"[\u4e00-\u9fffA-Za-z]", name) is not None


def _sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dq.sanitize_payload(payload)


def _snapshot(code: str, agent_name: str | None = None) -> dict[str, Any]:
    try:
        values = collect_field_values(code)
    except Exception:
        return {}
    return filter_values_for_agent(values, agent_name)


def _scope_line_items(items: list[str], agent_name: str | None) -> list[str]:
    allowed = get_allowed_fields_for_agent(agent_name)
    if not allowed:
        return items
    return [item for item in items if item in allowed]


def _growth_to_ratio(value: Any) -> float | None:
    x = _f(value)
    if x is None:
        return None
    return x / 100.0 if abs(x) > 2 else x


_AGENT_METRIC_ALLOWLIST: dict[str, set[str]] = {
    "warren_buffett_agent": {
        "market_cap",
        "enterprise_value",
        "price_to_earnings_ratio",
        "price_to_book_ratio",
        "price_to_sales_ratio",
        "enterprise_value_to_ebitda_ratio",
        "enterprise_value_to_revenue_ratio",
        "free_cash_flow_yield",
        "peg_ratio",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "return_on_equity",
        "return_on_assets",
        "return_on_invested_capital",
        "asset_turnover",
        "inventory_turnover",
        "receivables_turnover",
        "days_sales_outstanding",
        "operating_cycle",
        "working_capital_turnover",
        "current_ratio",
        "quick_ratio",
        "cash_ratio",
        "operating_cash_flow_ratio",
        "debt_to_equity",
        "debt_to_assets",
        "interest_coverage",
        "revenue_growth",
        "earnings_growth",
        "book_value_growth",
        "earnings_per_share_growth",
        "free_cash_flow_growth",
        "operating_income_growth",
        "ebitda_growth",
        "payout_ratio",
        "earnings_per_share",
        "book_value_per_share",
        "free_cash_flow_per_share",
        "current_price",
        "dividend_yield",
        "interest_bearing_debt",
        "operating_liabilities",
        "intrinsic_value_estimate",
    },
    "stanley_druckenmiller_agent": {
        "market_cap",
        "enterprise_value",
        "price_to_earnings_ratio",
        "price_to_book_ratio",
        "peg_ratio",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "asset_turnover",
        "debt_to_equity",
        "revenue_growth",
        "earnings_growth",
        "earnings_per_share",
        "current_price",
        "interest_bearing_debt",
        "intrinsic_value_estimate",
    },
    "fundamentals_analyst_agent": {
        "price_to_earnings_ratio",
        "price_to_book_ratio",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "return_on_equity",
        "current_ratio",
        "debt_to_equity",
        "revenue_growth",
        "earnings_growth",
        "current_price",
        "intrinsic_value_estimate",
    },
    "growth_analyst_agent": {
        "price_to_earnings_ratio",
        "price_to_book_ratio",
        "peg_ratio",
        "gross_margin",
        "operating_margin",
        "revenue_growth",
        "earnings_growth",
        "current_ratio",
        "debt_to_equity",
        "current_price",
        "intrinsic_value_estimate",
    },
    "peter_lynch_agent": {
        "market_cap",
        "price_to_earnings_ratio",
        "price_to_book_ratio",
        "peg_ratio",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "debt_to_equity",
        "revenue_growth",
        "earnings_growth",
        "earnings_per_share",
        "current_price",
        "intrinsic_value_estimate",
    },
    "charlie_munger_agent": {
        "market_cap",
        "price_to_earnings_ratio",
        "price_to_book_ratio",
        "gross_margin",
        "operating_margin",
        "return_on_equity",
        "return_on_invested_capital",
        "debt_to_equity",
        "revenue_growth",
        "earnings_growth",
        "earnings_per_share",
        "current_ratio",
        "current_price",
        "interest_bearing_debt",
        "intrinsic_value_estimate",
    },
    "soros_agent": {
        "price_to_earnings_ratio",
        "price_to_book_ratio",
        "revenue_growth",
        "earnings_growth",
        "current_price",
    },
}


def _apply_metric_scope(payload: dict[str, Any], agent_name: str | None) -> dict[str, Any]:
    if not agent_name:
        return payload
    allow = _AGENT_METRIC_ALLOWLIST.get(agent_name)
    if not allow:
        return payload
    keep_always = {"ticker", "report_period", "period", "currency"}
    filtered = dict(payload)
    for key in list(filtered.keys()):
        if key in keep_always:
            continue
        if key not in allow:
            filtered[key] = None
    return filtered


@lru_cache(maxsize=1024)
def _push2_quote(code: str) -> dict[str, float | str | None]:
    params = {
        "fltt": "2",
        "invt": "2",
        "secid": _secid(code),
        "fields": "f57,f58,f43,f59,f60,f116,f117,f162,f167,f168,f169,f170,f171,f184,f127,f292",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
    }
    payload = _stable.get_json(
        provider="eastmoney.push2.quote",
        url="https://push2.eastmoney.com/api/qt/stock/get",
        params=params,
        headers=_headers_for_eastmoney(code),
    )
    data = payload.get("data") or {}

    price = _f(data.get("f43"))
    if price is not None and price > 100000:
        price = price / 100.0

    pe = _coalesce(_f(data.get("f162")), _f(data.get("f163")), _f(data.get("f164")))
    pb = _f(data.get("f167"))
    mcap = _coalesce(_f(data.get("f116")), _f(data.get("f117")))
    dividend_yield = _ratio(data.get("f184"))

    return {
        "stock_name": str(data.get("f58") or code),
        "price": price,
        "mcap": mcap,
        "pe": pe,
        "pb": pb,
        "dividend_yield": dividend_yield,
    }


@lru_cache(maxsize=1024)
def _individual_info(code: str) -> dict[str, Any]:
    def _fetch() -> pd.DataFrame:
        return ak.stock_individual_info_em(symbol=code)

    try:
        df = _stable.execute(provider="ak.stock_individual_info_em", fn=_fetch)
    except Exception:
        return {}
    if df is None or df.empty:
        return {}

    out: dict[str, Any] = {}
    for _, row in df.iterrows():
        try:
            out[str(row["item"]).strip()] = row["value"]
        except Exception:
            continue
    return out


def _quote(code: str) -> dict[str, float | str | None]:
    try:
        push = _push2_quote(code)
    except Exception:
        push = {}

    info = _individual_info(code)
    ak_price = _coalesce(_f(info.get("最新")), _f(info.get("现价")), _f(info.get("最新价")))
    ak_mcap = _coalesce(_f(info.get("总市值")), _f(info.get("总市值(元)")))
    ak_pe = _coalesce(_f(info.get("市盈率(动态)")), _f(info.get("市盈率")), _f(info.get("动态市盈率")))
    ak_pb = _coalesce(_f(info.get("市净率")), _f(info.get("市净率MRQ")))
    ak_div_yield = _coalesce(_ratio(info.get("股息率")), _ratio(info.get("股息率TTM")))
    ak_name = _coalesce(str(info.get("股票简称") or ""), str(info.get("简称") or ""), str(info.get("名称") or ""))
    dynamic_name = ""
    try:
        snap = _bbsj_snapshot(code)
        name_from_page = str((snap.get("stock_info") or {}).get("name") or "")
        name_from_dc = str((snap.get("latest") or {}).get("SECURITY_NAME_ABBR") or "")
        dynamic_name = name_from_page if _is_valid_name(name_from_page) else name_from_dc
    except Exception:
        dynamic_name = ""

    push_name = str(push.get("stock_name") or "")
    if not _is_valid_name(push_name):
        push_name = ""
    if not _is_valid_name(str(ak_name or "")):
        ak_name = ""
    if not _is_valid_name(dynamic_name):
        dynamic_name = ""

    return {
        "stock_name": str(push_name or ak_name or dynamic_name or code),
        "price": _coalesce(push.get("price"), ak_price),  # type: ignore[arg-type]
        "mcap": _coalesce(push.get("mcap"), ak_mcap),  # type: ignore[arg-type]
        "pe": _coalesce(push.get("pe"), ak_pe),  # type: ignore[arg-type]
        "pb": _coalesce(push.get("pb"), ak_pb),  # type: ignore[arg-type]
        "dividend_yield": _coalesce(push.get("dividend_yield"), ak_div_yield),  # type: ignore[arg-type]
    }


def _rows_to_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "REPORTDATE" in df.columns and "REPORT_DATE" not in df.columns:
        df = df.rename(columns={"REPORTDATE": "REPORT_DATE"})
    if "REPORT_DATE" in df.columns:
        df["REPORT_DATE"] = pd.to_datetime(df["REPORT_DATE"], errors="coerce")
        df = df.dropna(subset=["REPORT_DATE"]).sort_values("REPORT_DATE", ascending=False)
    return df


@lru_cache(maxsize=512)
def _datacenter_report(
    report_name: str,
    code: str,
    sort_column: str = "REPORT_DATE",
    page_size: int = 20,
    max_pages: int = 3,
) -> pd.DataFrame:
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    rows: list[dict[str, Any]] = []
    pages = 1
    for page in range(1, max_pages + 1):
        params = {
            "reportName": report_name,
            "columns": "ALL",
            "filter": f'(SECURITY_CODE="{code}")',
            "sortColumns": sort_column,
            "sortTypes": "-1",
            "pageSize": str(page_size),
            "pageNumber": str(page),
            "source": "WEB",
            "client": "WEB",
        }
        try:
            payload = _stable.get_json(
                provider=f"eastmoney.datacenter.{report_name}",
                url=url,
                params=params,
                headers=_headers_for_eastmoney(code),
            )
        except Exception:
            break
        result = payload.get("result") or {}
        data = result.get("data") or []
        if not data:
            break
        rows.extend(data)
        pages = int(result.get("pages") or 1)
        if page >= pages:
            break
    return _rows_to_df(rows)


@lru_cache(maxsize=512)
def _bbsj_snapshot(code: str) -> dict[str, Any]:
    """
    Dynamic crawl chain based on Scrapling:
    1) try dynamic/stealth fetch of bbsj page
    2) parse stockInfo
    3) enrich with datacenter latest financial snapshot
    """
    html = ""
    try:
        html = _stable.fetch_html_with_scrapling(
            url=f"https://data.eastmoney.com/bbsj/{code}.html",
            provider_prefix="eastmoney.bbsj",
            timeout_sec=20.0,
        )
    except Exception:
        html = ""

    stock_info: dict[str, Any] = {}
    if html:
        match = re.search(r"var\s+stockInfo\s*=\s*(\{.*?\});", html, flags=re.S)
        if match:
            blob = match.group(1)
            try:
                import json

                stock_info = json.loads(blob)
            except Exception:
                stock_info = {}

    snapshot_df = _datacenter_report("RPT_LICO_FN_CPD", code=code, sort_column="REPORTDATE", page_size=5, max_pages=1)
    latest = snapshot_df.iloc[0].to_dict() if not snapshot_df.empty else {}
    return {"stock_info": stock_info, "latest": latest}


@lru_cache(maxsize=512)
def _stmts(code: str) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {
        "p": pd.DataFrame(),
        "b": pd.DataFrame(),
        "c": pd.DataFrame(),
        "summary": pd.DataFrame(),
    }
    em_symbol = _em_symbol(code)

    ak_calls = {
        "p": (ak.stock_profit_sheet_by_report_em, {"symbol": em_symbol}),
        "b": (ak.stock_balance_sheet_by_report_em, {"symbol": em_symbol}),
        "c": (ak.stock_cash_flow_sheet_by_report_em, {"symbol": em_symbol}),
    }

    for key, (fn, kwargs) in ak_calls.items():
        try:
            df = _stable.execute(provider=f"ak.{fn.__name__}", fn=lambda fn=fn, kwargs=kwargs: fn(**kwargs))
        except Exception:
            df = pd.DataFrame()
        if isinstance(df, pd.DataFrame) and not df.empty:
            out[key] = _rows_to_df(df.to_dict(orient="records"))

    # Datacenter fallback for sparse symbols / STAR board edge-cases.
    if out["p"].empty:
        out["p"] = _datacenter_report("RPT_DMSK_FN_INCOME", code=code, sort_column="REPORT_DATE", page_size=20, max_pages=4)
    if out["b"].empty:
        out["b"] = _datacenter_report("RPT_DMSK_FN_BALANCE", code=code, sort_column="REPORT_DATE", page_size=20, max_pages=4)
    if out["c"].empty:
        out["c"] = _datacenter_report("RPT_DMSK_FN_CASHFLOW", code=code, sort_column="REPORT_DATE", page_size=20, max_pages=4)

    summary_df = _datacenter_report("RPT_LICO_FN_CPD", code=code, sort_column="REPORTDATE", page_size=20, max_pages=3)
    out["summary"] = summary_df

    # Merge summary columns into income sheet for ROE/EPS/YoY fallback.
    if not out["p"].empty and not summary_df.empty:
        cols = [
            "REPORT_DATE",
            "BASIC_EPS",
            "WEIGHTAVG_ROE",
            "BPS",
            "XSMLL",
            "YSTZ",
            "SJLTZ",
            "PARENT_NETPROFIT",
            "TOTAL_OPERATE_INCOME",
        ]
        summary_use = summary_df[[c for c in cols if c in summary_df.columns]].copy()
        if "REPORT_DATE" in summary_use.columns:
            merged = out["p"].merge(summary_use, on="REPORT_DATE", how="left", suffixes=("", "_SUMMARY"))
            for c in ["BASIC_EPS", "WEIGHTAVG_ROE", "BPS", "XSMLL", "YSTZ", "SJLTZ", "PARENT_NETPROFIT", "TOTAL_OPERATE_INCOME"]:
                sc = f"{c}_SUMMARY"
                if sc in merged.columns:
                    if c in merged.columns:
                        merged[c] = merged[c].combine_first(merged[sc])
                    else:
                        merged[c] = merged[sc]
                    merged = merged.drop(columns=[sc])
            out["p"] = merged

    return out


def _rv(row: pd.Series | None, *cols: str) -> float | None:
    if row is None:
        return None
    for c in cols:
        if c in row.index:
            v = _f(row[c])
            if v is not None:
                return v
    return None


def _row(df: pd.DataFrame, report_date: pd.Timestamp | None) -> pd.Series | None:
    if df.empty:
        return None
    if report_date is None or "REPORT_DATE" not in df.columns:
        return df.iloc[0]
    exact = df[df["REPORT_DATE"] == report_date]
    if not exact.empty:
        return exact.iloc[0]
    prev = df[df["REPORT_DATE"] <= report_date]
    return prev.iloc[0] if not prev.empty else df.iloc[0]


def _interest_bearing_debt(b: pd.Series | None) -> float | None:
    return _sum(
        _rv(b, "SHORT_LOAN"),
        _rv(b, "LONG_LOAN"),
        _rv(b, "BOND_PAYABLE"),
        _rv(b, "NONCURRENT_LIAB_1YEAR"),
        _rv(b, "LONG_PAYABLE"),
        _rv(b, "LEASE_LIAB"),
    )


def _estimate_intrinsic_value_per_share(
    free_cash_flow: float | None,
    shares: float | None,
    growth_hint: float | None,
) -> float | None:
    if free_cash_flow is None or shares is None or shares <= 0:
        return None
    if free_cash_flow <= 0:
        return None

    g = growth_hint if growth_hint is not None else 0.03
    g = max(0.0, min(0.12, g))
    discount = 0.10
    terminal = 0.03
    if discount <= terminal:
        return None

    equity_value = free_cash_flow * (1 + g) / (discount - terminal)
    per_share = _safe_div(equity_value, shares)
    if per_share is None:
        return None
    if per_share <= 0 or per_share > 1_000_000:
        return None
    return per_share


def _core_values(
    code: str,
    p: pd.Series | None,
    b: pd.Series | None,
    c: pd.Series | None,
    q: dict[str, float | str | None],
) -> dict[str, float | None]:
    revenue = _coalesce(_rv(p, "TOTAL_OPERATE_INCOME"), _rv(p, "OPERATE_INCOME"))
    operating_income = _coalesce(_rv(p, "OPERATE_PROFIT"), _rv(p, "OPERATE_PROFIT_BALANCE"))
    net_income = _coalesce(_rv(p, "PARENT_NETPROFIT"), _rv(p, "NETPROFIT"))
    gross_profit = _rv(p, "GROSS_PROFIT")
    if gross_profit is None:
        total_cost = _coalesce(_rv(p, "TOTAL_OPERATE_COST"), _rv(p, "OPERATE_COST"))
        if revenue is not None and total_cost is not None:
            gross_profit = revenue - total_cost

    total_assets = _coalesce(_rv(b, "TOTAL_ASSETS"), _rv(b, "ASSET_BALANCE"))
    total_liabilities = _coalesce(_rv(b, "TOTAL_LIABILITIES"), _rv(b, "LIAB_BALANCE"))
    shareholders_equity = _coalesce(_rv(b, "TOTAL_EQUITY"), _rv(b, "PARENT_EQUITY_BALANCE"), _rv(b, "EQUITY_BALANCE"))
    current_assets = _coalesce(_rv(b, "TOTAL_CURRENT_ASSETS"), _rv(b, "CURRENT_ASSET_BALANCE"))
    current_liabilities = _coalesce(_rv(b, "TOTAL_CURRENT_LIAB"), _rv(b, "CURRENT_LIAB_BALANCE"))
    cash = _coalesce(_rv(b, "MONETARYFUNDS"), _rv(c, "END_CASH_EQUIVALENTS"), _rv(c, "END_CCE"))

    debt = _interest_bearing_debt(b)
    operating_liabilities = None if total_liabilities is None or debt is None else total_liabilities - debt

    depreciation = _coalesce(
        _sum(
            _rv(c, "FA_IR_DEPR"),
            _rv(c, "IA_AMORTIZE"),
            _rv(c, "LPE_AMORTIZE"),
            _rv(c, "USERIGHT_ASSET_AMORTIZE"),
        ),
        _rv(p, "DEPRECIATION_AND_AMORTIZATION"),
    )

    capex = _coalesce(_rv(c, "CONSTRUCT_LONG_ASSET"), _rv(c, "INVEST_PAY_CASH"))
    if capex is not None:
        capex = abs(capex)
    operating_cash_flow = _coalesce(_rv(c, "NETCASH_OPERATE"), _rv(c, "OPERATE_NETCASH_BALANCE"), _rv(c, "OPERATE_NETCASH_OTHER"))
    free_cash_flow = None if operating_cash_flow is None or capex is None else operating_cash_flow - capex

    shares = _coalesce(_rv(b, "SHARE_CAPITAL"), _f(_individual_info(code).get("总股本")))
    price = _f(q.get("price"))
    mcap = _f(q.get("mcap"))
    pe = _f(q.get("pe"))
    pb = _f(q.get("pb"))
    dividend_yield = _f(q.get("dividend_yield"))

    r = {
        "revenue": revenue,
        "net_income": net_income,
        "operating_income": operating_income,
        "gross_profit": gross_profit,
        "gross_margin": _coalesce(_safe_div(gross_profit, revenue), _ratio(_rv(p, "XSMLL"))),
        "operating_margin": _safe_div(operating_income, revenue),
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
        "shareholders_equity": shareholders_equity,
        "current_assets": current_assets,
        "current_liabilities": current_liabilities,
        "cash_and_equivalents": cash,
        "total_debt": debt,
        "interest_bearing_debt": debt,
        "operating_liabilities": operating_liabilities,
        "working_capital": None if current_assets is None or current_liabilities is None else current_assets - current_liabilities,
        "free_cash_flow": free_cash_flow,
        "capital_expenditure": capex,
        "depreciation_and_amortization": depreciation,
        "interest_expense": _coalesce(_rv(p, "INTEREST_EXPENSE"), _rv(p, "FE_INTEREST_EXPENSE")),
        "ebit": _coalesce(_rv(p, "EBIT"), operating_income),
        "earnings_per_share": _coalesce(_rv(p, "BASIC_EPS"), _rv(p, "DILUTED_EPS")),
        "outstanding_shares": shares,
        "book_value_per_share": _safe_div(shareholders_equity, shares),
        "dividends_and_other_cash_distributions": _coalesce(_rv(c, "ASSIGN_DIVIDEND_PORFIT"), _rv(c, "SUBSIDY_RECE")),
        "issuance_or_purchase_of_equity_shares": _coalesce(_rv(c, "ACCEPT_INVEST_CASH"), _rv(c, "BUY_SUBSIDIARY_EQUITY")),
        "debt_to_equity": _safe_div(debt, shareholders_equity),
        "research_and_development": _coalesce(_rv(p, "RESEARCH_EXPENSE"), _rv(p, "ME_RESEARCH_EXPENSE")),
        "operating_expense": _sum(_rv(p, "SALE_EXPENSE"), _rv(p, "MANAGE_EXPENSE"), _rv(p, "FINANCE_EXPENSE")),
        "goodwill_and_intangible_assets": _sum(_rv(b, "GOODWILL"), _rv(b, "INTANGIBLE_ASSET")),
        "return_on_invested_capital": _safe_div(operating_income, _sum(shareholders_equity, debt)),
        "price_to_earnings_ratio": pe,
        "price_to_book_ratio": pb,
        "market_cap": mcap,
        "current_price": price,
        "dividend_yield": dividend_yield,
    }
    r["ebitda"] = _sum(r["ebit"], depreciation)
    return r


def _push2_daily_prices(code: str, start_date: str, end_date: str) -> list[Price]:
    params = {
        "secid": _secid(code),
        "klt": "101",
        "fqt": "1",
        "beg": start_date.replace("-", ""),
        "end": end_date.replace("-", ""),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }
    payload = _stable.get_json(
        provider="eastmoney.push2his.kline",
        url="https://push2his.eastmoney.com/api/qt/stock/kline/get",
        params=params,
        headers=_headers_for_eastmoney(code),
    )
    data = payload.get("data") or {}
    klines = data.get("klines") or []
    out: list[Price] = []
    for line in klines:
        try:
            ds, op, cl, hi, lo, vol, *_ = str(line).split(",")
            o = _f(op)
            c = _f(cl)
            h = _f(hi)
            l = _f(lo)
            v = _i(vol) or 0
            if not ds or None in {o, c, h, l}:
                continue
            out.append(Price(open=o, close=c, high=h, low=l, volume=v, time=f"{ds}T00:00:00"))
        except Exception:
            continue
    return out


def get_prices(
    ticker: str,
    start_date: str,
    end_date: str,
    api_key: str = None,
    agent_name: str | None = None,
) -> list[Price]:
    code = _code(ticker)
    scope = agent_name or "global"
    key = f"{scope}_{code}_{start_date}_{end_date}"
    if cached := _cache.get_prices(key):
        return [Price(**x) for x in cached]

    df = pd.DataFrame()
    try:
        df = _stable.execute(
            provider="ak.stock_zh_a_hist",
            fn=lambda: ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust="qfq",
            ),
        )
    except Exception:
        df = pd.DataFrame()

    out: list[Price] = []

    if isinstance(df, pd.DataFrame) and not df.empty:
        d = df.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
            }
        )
        for _, r in d.iterrows():
            ds = _date(r.get("date"))
            o, c, h, l = _f(r.get("open")), _f(r.get("close")), _f(r.get("high")), _f(r.get("low"))
            v = _i(r.get("volume")) or 0
            if not ds or None in {o, c, h, l}:
                continue
            out.append(Price(open=o, close=c, high=h, low=l, volume=v, time=f"{ds}T00:00:00"))

    if not out:
        try:
            out = _push2_daily_prices(code=code, start_date=start_date, end_date=end_date)
        except Exception:
            out = []

    if not out:
        try:
            min_df = _stable.execute(
                provider="ak.stock_zh_a_hist_min_em",
                fn=lambda: ak.stock_zh_a_hist_min_em(
                    symbol=code,
                    start_date=f"{start_date} 09:30:00",
                    end_date=f"{end_date} 15:00:00",
                    period="5",
                    adjust="",
                ),
            )
        except Exception:
            min_df = pd.DataFrame()

        if isinstance(min_df, pd.DataFrame) and not min_df.empty:
            minute = min_df.rename(
                columns={
                    "时间": "dt",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                }
            )
            if "dt" in minute.columns:
                minute["date"] = pd.to_datetime(minute["dt"], errors="coerce").dt.date
                minute = minute.dropna(subset=["date"])
                if not minute.empty:
                    daily = (
                        minute.groupby("date", as_index=False)
                        .agg({"open": "first", "close": "last", "high": "max", "low": "min", "volume": "sum"})
                    )
                    for _, r in daily.iterrows():
                        ds = _date(r.get("date"))
                        o, c, h, l = _f(r.get("open")), _f(r.get("close")), _f(r.get("high")), _f(r.get("low"))
                        v = _i(r.get("volume")) or 0
                        if not ds or None in {o, c, h, l}:
                            continue
                        out.append(Price(open=o, close=c, high=h, low=l, volume=v, time=f"{ds}T00:00:00"))

    if out:
        _cache.set_prices(key, [x.model_dump() for x in out])
    return out


def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
    api_key: str = None,
    agent_name: str | None = None,
) -> list[FinancialMetrics]:
    code = _code(ticker)
    scope = agent_name or "global"
    key = f"{scope}_{code}_{period}_{end_date}_{limit}"
    if cached := _cache.get_financial_metrics(key):
        return [FinancialMetrics(**x) for x in cached]

    scoped_snapshot = _snapshot(code, agent_name)
    q = _quote(code)
    statements = _stmts(code)
    p_df, b_df, c_df, summary_df = statements["p"], statements["b"], statements["c"], statements["summary"]
    dynamic_latest = {}
    try:
        dynamic_latest = _bbsj_snapshot(code).get("latest") or {}
    except Exception:
        dynamic_latest = {}

    if p_df.empty:
        if not summary_df.empty:
            latest_summary = summary_df.iloc[0]
        elif dynamic_latest:
            latest_summary = pd.Series(dynamic_latest)
        else:
            latest_summary = None
        roe = _ratio(_rv(latest_summary, "WEIGHTAVG_ROE")) if latest_summary is not None else None
        eps = _rv(latest_summary, "BASIC_EPS") if latest_summary is not None else None
        payload = _sanitize_payload(
            {
                "ticker": code,
                "report_period": end_date,
                "period": period,
                "currency": "CNY",
                "market_cap": _coalesce(_f(scoped_snapshot.get("market_cap_total")), _f(q.get("mcap"))),
                "enterprise_value": _coalesce(_f(scoped_snapshot.get("market_cap_total")), _f(q.get("mcap"))),
                "price_to_earnings_ratio": _coalesce(_f(scoped_snapshot.get("pe_ttm")), _f(q.get("pe"))),
                "price_to_book_ratio": _coalesce(_f(scoped_snapshot.get("pb")), _f(q.get("pb"))),
                "price_to_sales_ratio": None,
                "enterprise_value_to_ebitda_ratio": None,
                "enterprise_value_to_revenue_ratio": None,
                "free_cash_flow_yield": None,
                "peg_ratio": _coalesce(_f(scoped_snapshot.get("peg_ratio")), None),
                "gross_margin": _coalesce(
                    _growth_to_ratio(scoped_snapshot.get("gross_margin")),
                    _ratio(_rv(latest_summary, "XSMLL")) if latest_summary is not None else None,
                ),
                "operating_margin": _growth_to_ratio(scoped_snapshot.get("operating_margin")),
                "net_margin": _growth_to_ratio(scoped_snapshot.get("net_margin")),
                "return_on_equity": _coalesce(_growth_to_ratio(scoped_snapshot.get("return_on_equity")), roe),
                "return_on_assets": None,
                "return_on_invested_capital": _growth_to_ratio(scoped_snapshot.get("return_on_invested_capital")),
                "asset_turnover": _f(scoped_snapshot.get("asset_turnover")),
                "inventory_turnover": None,
                "receivables_turnover": None,
                "days_sales_outstanding": None,
                "operating_cycle": None,
                "working_capital_turnover": None,
                "current_ratio": _f(scoped_snapshot.get("current_ratio")),
                "quick_ratio": None,
                "cash_ratio": None,
                "operating_cash_flow_ratio": None,
                "debt_to_equity": _f(scoped_snapshot.get("debt_to_equity")),
                "debt_to_assets": None,
                "interest_coverage": None,
                "revenue_growth": _coalesce(
                    _growth_to_ratio(scoped_snapshot.get("revenue_growth")),
                    _ratio(_rv(latest_summary, "YSTZ")) if latest_summary is not None else None,
                ),
                "earnings_growth": _coalesce(
                    _growth_to_ratio(scoped_snapshot.get("earnings_growth")),
                    _ratio(_rv(latest_summary, "SJLTZ")) if latest_summary is not None else None,
                ),
                "book_value_growth": None,
                "earnings_per_share_growth": None,
                "free_cash_flow_growth": None,
                "operating_income_growth": None,
                "ebitda_growth": None,
                "payout_ratio": None,
                "earnings_per_share": _coalesce(_f(scoped_snapshot.get("earnings_per_share")), eps),
                "book_value_per_share": _rv(latest_summary, "BPS") if latest_summary is not None else None,
                "free_cash_flow_per_share": None,
                "current_price": _coalesce(_f(scoped_snapshot.get("current_price")), _f(q.get("price"))),
                "dividend_yield": _coalesce(_growth_to_ratio(scoped_snapshot.get("dividend_yield")), _f(q.get("dividend_yield"))),
                "interest_bearing_debt": _f(scoped_snapshot.get("total_debt")),
                "operating_liabilities": (
                    _f(scoped_snapshot.get("total_liabilities")) - _f(scoped_snapshot.get("total_debt"))
                    if _f(scoped_snapshot.get("total_liabilities")) is not None and _f(scoped_snapshot.get("total_debt")) is not None
                    else None
                ),
                "intrinsic_value_estimate": _f(scoped_snapshot.get("intrinsic_value_estimate")),
            }
        )
        payload = _apply_metric_scope(payload, agent_name)
        one = FinancialMetrics(**payload)
        _cache.set_financial_metrics(key, [one.model_dump()])
        return [one]

    end_ts = pd.to_datetime(end_date)
    p_rows = p_df[p_df["REPORT_DATE"] <= end_ts] if "REPORT_DATE" in p_df.columns else p_df
    p_rows = p_rows.head(limit)
    out: list[FinancialMetrics] = []

    for idx, (_, p_row) in enumerate(p_rows.iterrows()):
        report_date = p_row.get("REPORT_DATE")
        prev_p_row = p_rows.iloc[idx + 1] if idx + 1 < len(p_rows) else None
        b_row = _row(b_df, report_date)
        prev_b_row = _row(b_df, p_rows.iloc[idx + 1].get("REPORT_DATE") if idx + 1 < len(p_rows) else None)
        c_row = _row(c_df, report_date)
        prev_c_row = _row(c_df, p_rows.iloc[idx + 1].get("REPORT_DATE") if idx + 1 < len(p_rows) else None)

        vals = _core_values(code, p_row, b_row, c_row, q)
        prev_vals = _core_values(code, prev_p_row, prev_b_row, prev_c_row, q) if prev_p_row is not None else {}

        revenue = vals.get("revenue")
        net_income = vals.get("net_income")
        operating_income = vals.get("operating_income")
        total_assets = vals.get("total_assets")
        total_liabilities = vals.get("total_liabilities")
        equity = vals.get("shareholders_equity")
        debt = vals.get("total_debt")
        cash = vals.get("cash_and_equivalents")
        fcf = vals.get("free_cash_flow")
        ebitda = vals.get("ebitda")

        prev_revenue = prev_vals.get("revenue")
        prev_net_income = prev_vals.get("net_income")
        prev_equity = prev_vals.get("shareholders_equity")
        prev_eps = prev_vals.get("earnings_per_share")
        prev_fcf = prev_vals.get("free_cash_flow")
        prev_operating_income = prev_vals.get("operating_income")
        prev_ebitda = prev_vals.get("ebitda")

        revenue_growth = _coalesce(
            _safe_div((revenue - prev_revenue) if revenue is not None and prev_revenue is not None else None, abs(prev_revenue) if prev_revenue not in (None, 0) else None),
            _ratio(_rv(p_row, "YSTZ")),
            _ratio(_rv(p_row, "TOTAL_OPERATE_INCOME_YOY")),
        )
        earnings_growth = _coalesce(
            _safe_div((net_income - prev_net_income) if net_income is not None and prev_net_income is not None else None, abs(prev_net_income) if prev_net_income not in (None, 0) else None),
            _ratio(_rv(p_row, "SJLTZ")),
            _ratio(_rv(p_row, "PARENT_NETPROFIT_YOY")),
        )
        book_value_growth = _safe_div((equity - prev_equity) if equity is not None and prev_equity is not None else None, abs(prev_equity) if prev_equity not in (None, 0) else None)

        market_cap = vals.get("market_cap")
        enterprise_value = None if market_cap is None else market_cap + (debt or 0.0) - (cash or 0.0)
        pe = vals.get("price_to_earnings_ratio") if vals.get("price_to_earnings_ratio") is not None else _safe_div(market_cap, net_income) if net_income not in (None, 0) else None
        pb = vals.get("price_to_book_ratio") if vals.get("price_to_book_ratio") is not None else _safe_div(market_cap, equity) if equity not in (None, 0) else None

        operating_cash_flow = _coalesce(_rv(c_row, "NETCASH_OPERATE"), _rv(c_row, "OPERATE_NETCASH_BALANCE"), _rv(c_row, "OPERATE_NETCASH_OTHER"))
        inventory = _rv(b_row, "INVENTORY")
        receivables = _coalesce(_rv(b_row, "ACCOUNTS_RECE"), _rv(b_row, "NOTE_ACCOUNTS_RECE"))
        inventory_turnover = _safe_div(_coalesce(_rv(p_row, "TOTAL_OPERATE_COST"), _rv(p_row, "OPERATE_COST")), inventory)
        receivables_turnover = _safe_div(revenue, receivables)
        dso = _safe_div(365.0, receivables_turnover)
        inventory_days = _safe_div(365.0, inventory_turnover)
        operating_cycle = None if dso is None or inventory_days is None else dso + inventory_days

        roe = _coalesce(_safe_div(net_income, equity), _ratio(_rv(p_row, "WEIGHTAVG_ROE")))
        roic = vals.get("return_on_invested_capital")
        current_ratio = _coalesce(_safe_div(vals.get("current_assets"), vals.get("current_liabilities")), _ratio(_rv(b_row, "CURRENT_RATIO")))
        debt_to_assets = _coalesce(_safe_div(total_liabilities, total_assets), _ratio(_rv(b_row, "DEBT_ASSET_RATIO")))
        gross_margin = _coalesce(vals.get("gross_margin"), _ratio(_rv(p_row, "XSMLL")))

        eps = vals.get("earnings_per_share")
        eps_growth = _safe_div((eps - prev_eps) if eps is not None and prev_eps is not None else None, abs(prev_eps) if prev_eps not in (None, 0) else None)
        fcf_growth = _safe_div((fcf - prev_fcf) if fcf is not None and prev_fcf is not None else None, abs(prev_fcf) if prev_fcf not in (None, 0) else None)
        operating_income_growth = _safe_div((operating_income - prev_operating_income) if operating_income is not None and prev_operating_income is not None else None, abs(prev_operating_income) if prev_operating_income not in (None, 0) else None)
        ebitda_growth = _safe_div((ebitda - prev_ebitda) if ebitda is not None and prev_ebitda is not None else None, abs(prev_ebitda) if prev_ebitda not in (None, 0) else None)

        intrinsic_value_estimate = _estimate_intrinsic_value_per_share(
            free_cash_flow=fcf,
            shares=vals.get("outstanding_shares"),
            growth_hint=revenue_growth,
        )
        snap = scoped_snapshot if idx == 0 else {}
        snap_market_cap = _f(snap.get("market_cap_total"))
        snap_pe = _f(snap.get("pe_ttm"))
        snap_pb = _f(snap.get("pb"))
        snap_current_price = _f(snap.get("current_price"))
        snap_dividend = _growth_to_ratio(snap.get("dividend_yield"))
        snap_rev_growth = _growth_to_ratio(snap.get("revenue_growth"))
        snap_earn_growth = _growth_to_ratio(snap.get("earnings_growth"))
        snap_roe = _growth_to_ratio(snap.get("return_on_equity"))
        snap_net_margin = _growth_to_ratio(snap.get("net_margin"))
        snap_op_margin = _growth_to_ratio(snap.get("operating_margin"))
        snap_intrinsic = _f(snap.get("intrinsic_value_estimate"))
        snap_interest_debt = _f(snap.get("total_debt"))
        snap_total_liab = _f(snap.get("total_liabilities"))

        payload = _sanitize_payload(
            {
                "ticker": code,
                "report_period": report_date.strftime("%Y-%m-%d") if isinstance(report_date, pd.Timestamp) else end_date,
                "period": period,
                "currency": "CNY",
                "market_cap": _coalesce(snap_market_cap, market_cap),
                "enterprise_value": enterprise_value,
                "price_to_earnings_ratio": _coalesce(snap_pe, pe),
                "price_to_book_ratio": _coalesce(snap_pb, pb),
                "price_to_sales_ratio": _safe_div(market_cap, revenue),
                "enterprise_value_to_ebitda_ratio": _safe_div(enterprise_value, ebitda),
                "enterprise_value_to_revenue_ratio": _safe_div(enterprise_value, revenue),
                "free_cash_flow_yield": _safe_div(fcf, market_cap),
                "peg_ratio": _safe_div(pe, revenue_growth * 100.0) if pe is not None and revenue_growth not in (None, 0) else None,
                "gross_margin": gross_margin,
                "operating_margin": _coalesce(snap_op_margin, vals.get("operating_margin")),
                "net_margin": _coalesce(snap_net_margin, _safe_div(net_income, revenue)),
                "return_on_equity": _coalesce(snap_roe, roe),
                "return_on_assets": _safe_div(net_income, total_assets),
                "return_on_invested_capital": roic,
                "asset_turnover": _safe_div(revenue, total_assets),
                "inventory_turnover": inventory_turnover,
                "receivables_turnover": receivables_turnover,
                "days_sales_outstanding": dso,
                "operating_cycle": operating_cycle,
                "working_capital_turnover": _safe_div(revenue, vals.get("working_capital")),
                "current_ratio": current_ratio,
                "quick_ratio": _safe_div(
                    None if vals.get("current_assets") is None else vals.get("current_assets") - (inventory or 0),
                    vals.get("current_liabilities"),
                ),
                "cash_ratio": _safe_div(cash, vals.get("current_liabilities")),
                "operating_cash_flow_ratio": _safe_div(operating_cash_flow, vals.get("current_liabilities")),
                "debt_to_equity": _safe_div(debt, equity),
                "debt_to_assets": debt_to_assets,
                "interest_coverage": _safe_div(operating_income, vals.get("interest_expense")),
                "revenue_growth": _coalesce(snap_rev_growth, revenue_growth),
                "earnings_growth": _coalesce(snap_earn_growth, earnings_growth),
                "book_value_growth": book_value_growth,
                "earnings_per_share_growth": eps_growth,
                "free_cash_flow_growth": fcf_growth,
                "operating_income_growth": operating_income_growth,
                "ebitda_growth": ebitda_growth,
                "payout_ratio": _safe_div(vals.get("dividends_and_other_cash_distributions"), net_income),
                "earnings_per_share": eps,
                "book_value_per_share": vals.get("book_value_per_share"),
                "free_cash_flow_per_share": _safe_div(fcf, vals.get("outstanding_shares")),
                "current_price": _coalesce(snap_current_price, vals.get("current_price")),
                "dividend_yield": _coalesce(snap_dividend, vals.get("dividend_yield")),
                "interest_bearing_debt": _coalesce(snap_interest_debt, vals.get("interest_bearing_debt")),
                "operating_liabilities": _coalesce(
                    (snap_total_liab - snap_interest_debt) if snap_total_liab is not None and snap_interest_debt is not None else None,
                    vals.get("operating_liabilities"),
                ),
                "intrinsic_value_estimate": _coalesce(snap_intrinsic, intrinsic_value_estimate),
            }
        )
        payload = _apply_metric_scope(payload, agent_name)
        out.append(FinancialMetrics(**payload))

    if out:
        _cache.set_financial_metrics(key, [x.model_dump() for x in out])
    return out


def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
    api_key: str = None,
    agent_name: str | None = None,
) -> list[LineItem]:
    code = _code(ticker)
    scoped_line_items = _scope_line_items(line_items, agent_name)
    if not scoped_line_items and not agent_name:
        scoped_line_items = list(line_items)
    scope = agent_name or "global"
    key = f"{scope}_{code}_{period}_{end_date}_{limit}_{'_'.join(sorted(scoped_line_items))}"
    if cached := _cache.get_line_items(key):
        return [LineItem(**x) for x in cached]

    scoped_snapshot = _snapshot(code, agent_name)
    q = _quote(code)
    statements = _stmts(code)
    p_df, b_df, c_df = statements["p"], statements["b"], statements["c"]
    if p_df.empty:
        payload: dict[str, Any] = {
            "ticker": code,
            "report_period": str(scoped_snapshot.get("report_period") or end_date),
            "period": period,
            "currency": "CNY",
        }
        for item in line_items:
            payload[item] = scoped_snapshot.get(item) if item in scoped_line_items else None
        one = LineItem(**payload)
        _cache.set_line_items(key, [one.model_dump()])
        return [one]

    end_ts = pd.to_datetime(end_date)
    p_rows = p_df[p_df["REPORT_DATE"] <= end_ts] if "REPORT_DATE" in p_df.columns else p_df
    out: list[LineItem] = []
    for _, p_row in p_rows.head(limit).iterrows():
        report_date = p_row.get("REPORT_DATE")
        b_row = _row(b_df, report_date)
        c_row = _row(c_df, report_date)
        vals = _core_values(code, p_row, b_row, c_row, q)

        payload: dict[str, Any] = {
            "ticker": code,
            "report_period": report_date.strftime("%Y-%m-%d") if isinstance(report_date, pd.Timestamp) else end_date,
            "period": period,
            "currency": "CNY",
        }
        for item in line_items:
            if item not in scoped_line_items:
                payload[item] = None
            else:
                payload[item] = _coalesce(vals.get(item), scoped_snapshot.get(item))
        out.append(LineItem(**payload))

    if out:
        _cache.set_line_items(key, [x.model_dump() for x in out])
    return out


def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
    api_key: str = None,
    agent_name: str | None = None,
) -> list[InsiderTrade]:
    code = _code(ticker)
    scope = agent_name or "global"
    key = f"{scope}_{code}_{start_date or 'none'}_{end_date}_{limit}"
    if cached := _cache.get_insider_trades(key):
        return [InsiderTrade(**x) for x in cached]

    scoped_snapshot = _snapshot(code, agent_name)
    out: list[InsiderTrade] = []
    page_number = 1
    while len(out) < limit:
        params = {
            "reportName": "RPT_SHARE_HOLDER_INCREASE",
            "columns": "ALL",
            "filter": f'(SECURITY_CODE="{code}")',
            "sortColumns": "END_DATE",
            "sortTypes": "-1",
            "pageSize": "100",
            "pageNumber": str(page_number),
            "source": "WEB",
            "client": "WEB",
        }
        try:
            payload = _stable.get_json(
                provider="eastmoney.datacenter.insider",
                url="https://datacenter-web.eastmoney.com/api/data/v1/get",
                params=params,
                headers=_headers_for_eastmoney(code),
            )
        except Exception:
            break

        result = payload.get("result") or {}
        rows = result.get("data") or []
        if not rows:
            break
        for row in rows:
            ds = _date(_coalesce(row.get("END_DATE"), row.get("TRADE_DATE"), row.get("NOTICE_DATE")))
            if not _in_window(ds, start_date, end_date):
                continue
            trans_shares = _coalesce(_f(row.get("CHANGE_NUM_SYMBOL")), _f(row.get("CHANGE_NUM")))
            trans_type = str(row.get("DIRECTION") or row.get("CHANGE_REASON") or "")
            trans_shares_signed = trans_shares
            if trans_shares_signed is not None:
                trans_type_l = trans_type.lower()
                if any(x in trans_type_l for x in ("sell", "reduce", "decrease")) or ("\u51cf" in trans_type) or ("\u5356" in trans_type):
                    trans_shares_signed = -abs(trans_shares_signed)
                elif any(x in trans_type_l for x in ("buy", "increase")) or ("\u589e" in trans_type) or ("\u4e70" in trans_type):
                    trans_shares_signed = abs(trans_shares_signed)
            out.append(
                InsiderTrade(
                    ticker=code,
                    issuer=row.get("SECURITY_NAME"),
                    name=row.get("HOLDER_NAME"),
                    title=row.get("CHANGE_REASON"),
                    is_board_director=None,
                    transaction_date=ds,
                    transaction_shares=trans_shares_signed,
                    transaction_price_per_share=_f(_coalesce(row.get("AVERAGE_PRICE"), row.get("AVG_PRICE"))),
                    transaction_value=_f(_coalesce(row.get("CHANGE_AMOUNT"), row.get("CHANGE_MARKET_CAP"))),
                    shares_owned_before_transaction=None,
                    shares_owned_after_transaction=_f(_coalesce(row.get("END_HOLD_NUM"), row.get("HOLD_NUM"))),
                    security_title=trans_type,
                    filing_date=ds or end_date,
                )
            )
            if len(out) >= limit:
                break
        pages = int(result.get("pages") or 1)
        if page_number >= pages:
            break
        page_number += 1

    if not out and scoped_snapshot:
        ds = _date(scoped_snapshot.get("insider_transaction_time")) or end_date
        raw_shares = _f(scoped_snapshot.get("insider_transaction_shares"))
        direction = str(scoped_snapshot.get("insider_transaction_type") or "")
        signed = raw_shares
        if signed is not None:
            if ("\u51cf" in direction) or ("\u5356" in direction):
                signed = -abs(signed)
            elif ("\u589e" in direction) or ("\u4e70" in direction):
                signed = abs(signed)
        out.append(
            InsiderTrade(
                ticker=code,
                issuer=str(scoped_snapshot.get("stock_name") or code),
                name=None,
                title=direction or None,
                is_board_director=None,
                transaction_date=ds,
                transaction_shares=signed,
                transaction_price_per_share=None,
                transaction_value=None,
                shares_owned_before_transaction=None,
                shares_owned_after_transaction=None,
                security_title=direction or None,
                filing_date=ds,
            )
        )

    if out:
        _cache.set_insider_trades(key, [x.model_dump() for x in out])
    return out


def get_company_news(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
    api_key: str = None,
    agent_name: str | None = None,
) -> list[CompanyNews]:
    code = _code(ticker)
    scope = agent_name or "global"
    key = f"{scope}_{code}_{start_date or 'none'}_{end_date}_{limit}"
    if cached := _cache.get_company_news(key):
        return [CompanyNews(**x) for x in cached]

    out: list[CompanyNews] = []
    page_index = 1
    while len(out) < limit:
        params = {
            "ann_type": "A",
            "client_source": "web",
            "stock_list": code,
            "page_size": min(100, max(10, limit)),
            "page_index": page_index,
        }
        try:
            payload = _stable.get_json(
                provider="eastmoney.notice.list",
                url="https://np-anotice-stock.eastmoney.com/api/security/ann",
                params=params,
                headers=_headers_for_eastmoney(code),
            )
        except Exception:
            break

        data = payload.get("data") or {}
        rows = data.get("list") or []
        if not rows:
            break

        for row in rows:
            ds = _date(_coalesce(row.get("display_time"), row.get("notice_date")))
            if not _in_window(ds, start_date, end_date):
                continue

            columns = row.get("columns") if isinstance(row, dict) else None
            source = None
            if isinstance(columns, list) and columns:
                source = (columns[0] or {}).get("column_name")
            source = str(source or row.get("source_type") or "东方财富")
            title = str(row.get("title") or "")
            art_code = str(row.get("art_code") or row.get("artCode") or "")
            detail_url = f"https://data.eastmoney.com/notices/detail/{code}/{art_code}.html" if art_code else ""

            out.append(
                CompanyNews(
                    ticker=code,
                    title=title,
                    author=source,
                    source=source,
                    date=f"{ds}T00:00:00" if ds else f"{end_date}T00:00:00",
                    url=detail_url,
                    sentiment=None,
                )
            )
            if len(out) >= limit:
                break

        total_hits = int(data.get("total_hits") or 0)
        page_size = int(data.get("page_size") or params["page_size"])
        if page_size <= 0:
            break
        if page_index * page_size >= total_hits:
            break
        page_index += 1

    if not out:
        for symbol in [code, _em_symbol(code)]:
            try:
                df = _stable.execute(provider="ak.stock_news_em", fn=lambda symbol=symbol: ak.stock_news_em(symbol=symbol))
            except Exception:
                df = pd.DataFrame()
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            for _, row in df.iterrows():
                ds = _date(row.get("发布时间"))
                if not _in_window(ds, start_date, end_date):
                    continue
                out.append(
                    CompanyNews(
                        ticker=code,
                        title=str(row.get("新闻标题") or ""),
                        author=str(row.get("文章来源") or "东方财富"),
                        source=str(row.get("文章来源") or "东方财富"),
                        date=f"{ds}T00:00:00" if ds else f"{end_date}T00:00:00",
                        url=str(row.get("新闻链接") or ""),
                        sentiment=None,
                    )
                )
                if len(out) >= limit:
                    break
            if out:
                break

    if out:
        _cache.set_company_news(key, [x.model_dump() for x in out])
    return out[:limit]


def get_market_cap(
    ticker: str,
    end_date: str,
    api_key: str = None,
    agent_name: str | None = None,
) -> float | None:
    code = _code(ticker)
    scoped_snapshot = _snapshot(code, agent_name)
    snap_mcap = _f(scoped_snapshot.get("market_cap_total"))
    if snap_mcap is not None:
        return snap_mcap

    q = _quote(code)
    mcap = _f(q.get("mcap"))
    if mcap is not None:
        return mcap

    info = _individual_info(code)
    price = _coalesce(_f(q.get("price")), _f(info.get("最新价")), _f(info.get("现价")))
    shares = _coalesce(_f(info.get("总股本")), _f(info.get("总股本(股)")))
    if price is not None and shares is not None:
        return price * shares

    metrics = get_financial_metrics(code, end_date=end_date, period="ttm", limit=1, agent_name=agent_name)
    return metrics[0].market_cap if metrics else None


def prices_to_df(prices: list[Price]) -> pd.DataFrame:
    if not prices:
        return pd.DataFrame(columns=["open", "close", "high", "low", "volume"])
    df = pd.DataFrame([p.model_dump() for p in prices])
    df["Date"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["Date"]).set_index("Date")
    for col in ["open", "close", "high", "low", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_index()


def get_price_data(
    ticker: str,
    start_date: str,
    end_date: str,
    api_key: str = None,
    agent_name: str | None = None,
) -> pd.DataFrame:
    return prices_to_df(get_prices(ticker, start_date, end_date, api_key=api_key, agent_name=agent_name))
