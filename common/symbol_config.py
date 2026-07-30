"""Shared symbol universe loader.

This is intentionally small and dependency-light. It uses PyYAML when present,
but also supports the simple YAML subset used by config/symbols.yaml so the
project does not need a new package just to centralize symbols.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

DEFAULT_SYMBOL_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "symbols.yaml"


class SymbolConfigError(ValueError):
    """Raised when symbol config is malformed or inconsistent."""


def _strip_comment(line: str) -> str:
    if "#" not in line:
        return line.rstrip()
    in_quote = False
    quote_char = ""
    result: list[str] = []
    for char in line:
        if char in {"'", '"'}:
            if in_quote and char == quote_char:
                in_quote = False
                quote_char = ""
            elif not in_quote:
                in_quote = True
                quote_char = char
        if char == "#" and not in_quote:
            break
        result.append(char)
    return "".join(result).rstrip()


def _parse_scalar(raw_value: str) -> Any:
    value = raw_value.strip()
    if not value:
        return ""
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part) for part in inner.split(",")]
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    items: list[tuple[int, str]] = []
    for line in text.splitlines():
        line = _strip_comment(line)
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        items.append((indent, line.strip()))

    root: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, root)]

    for index, (indent, content) in enumerate(items):
        while stack and indent <= stack[-1][0]:
            stack.pop()
        if not stack:
            raise SymbolConfigError("invalid symbols yaml indentation")
        parent = stack[-1][1]

        if content.startswith("- "):
            if not isinstance(parent, list):
                raise SymbolConfigError("invalid symbols yaml list item")
            parent.append(_parse_scalar(content[2:]))
            continue

        key, separator, value = content.partition(":")
        if not separator:
            raise SymbolConfigError(f"invalid symbols yaml line: {content}")
        key = key.strip()
        value = value.strip()
        if not isinstance(parent, dict):
            raise SymbolConfigError("invalid symbols yaml mapping")

        if value:
            parent[key] = _parse_scalar(value)
            continue

        child: Any = {}
        for next_indent, next_content in items[index + 1 :]:
            if next_indent <= indent:
                break
            child = [] if next_content.startswith("- ") else {}
            break
        parent[key] = child
        stack.append((indent, child))

    return root


def _load_yaml(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore[import-untyped]
    except ModuleNotFoundError:
        parsed = _parse_simple_yaml(raw)
    else:
        parsed = yaml.safe_load(raw)
    if not isinstance(parsed, dict):
        raise SymbolConfigError(f"symbol config must be a mapping: {path}")
    return parsed


def _normalize_symbols(raw_symbols: Any, *, label: str) -> list[str]:
    if not isinstance(raw_symbols, list):
        raise SymbolConfigError(f"{label} must be a list")
    symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in raw_symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            raise SymbolConfigError(f"{label} contains an empty symbol")
        if symbol in seen:
            raise SymbolConfigError(f"{label} contains duplicate symbol: {symbol}")
        seen.add(symbol)
        symbols.append(symbol)
    if not symbols:
        raise SymbolConfigError(f"{label} must not be empty")
    return symbols


def _market_config(config: dict[str, Any], exchange: str, market: str) -> dict[str, Any]:
    exchange_key = exchange.strip().lower()
    market_key = market.strip().lower()
    symbols = config.get("symbols")
    if not isinstance(symbols, dict) or exchange_key not in symbols:
        raise SymbolConfigError(f"unknown exchange in symbol config: {exchange}")
    exchange_cfg = symbols[exchange_key]
    if not isinstance(exchange_cfg, dict) or market_key not in exchange_cfg:
        raise SymbolConfigError(f"unknown market in symbol config: {exchange}.{market}")
    market_cfg = exchange_cfg[market_key]
    if not isinstance(market_cfg, dict):
        raise SymbolConfigError(f"{exchange}.{market} config must be a mapping")
    return market_cfg


def validate_symbol_universe(config: dict[str, Any]) -> None:
    symbols_cfg = config.get("symbols")
    if not isinstance(symbols_cfg, dict) or not symbols_cfg:
        raise SymbolConfigError("symbols config must define at least one exchange")

    all_collect_symbols: set[str] = set()
    core_symbols = set(_normalize_symbols(config.get("core_symbols"), label="core_symbols"))
    display_symbols = set(_normalize_symbols(config.get("display_symbols"), label="display_symbols"))

    for exchange, exchange_cfg in symbols_cfg.items():
        if not isinstance(exchange_cfg, dict) or not exchange_cfg:
            raise SymbolConfigError(f"symbols.{exchange} must define at least one market")
        for market, market_cfg in exchange_cfg.items():
            if not isinstance(market_cfg, dict):
                raise SymbolConfigError(f"symbols.{exchange}.{market} must be a mapping")
            collect = set(_normalize_symbols(market_cfg.get("collect"), label=f"{exchange}.{market}.collect"))
            all_collect_symbols.update(collect)

            if "trade" in market_cfg:
                trade = set(_normalize_symbols(market_cfg.get("trade"), label=f"{exchange}.{market}.trade"))
                if not trade <= collect:
                    missing = sorted(trade - collect)
                    raise SymbolConfigError(f"{exchange}.{market}.trade must be a subset of collect: {missing}")

            if "backtest" in market_cfg:
                backtest = set(
                    _normalize_symbols(
                        market_cfg.get("backtest"),
                        label=f"{exchange}.{market}.backtest",
                    )
                )
                allowed = collect | core_symbols
                if not backtest <= allowed:
                    missing = sorted(backtest - allowed)
                    raise SymbolConfigError(
                        f"{exchange}.{market}.backtest must be within collect or core_symbols: {missing}"
                    )

    if not core_symbols <= all_collect_symbols:
        missing = sorted(core_symbols - all_collect_symbols)
        raise SymbolConfigError(f"core_symbols must be collected somewhere: {missing}")

    if not display_symbols <= (all_collect_symbols | core_symbols):
        missing = sorted(display_symbols - all_collect_symbols - core_symbols)
        raise SymbolConfigError(f"display_symbols must be within collect or core_symbols: {missing}")


def load_symbol_config(path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    config_path = Path(path) if path is not None else DEFAULT_SYMBOL_CONFIG_PATH
    config = _load_yaml(config_path)
    validate_symbol_universe(config)
    return config


def get_collect_symbols(exchange: str = "binance", market: str = "futures") -> list[str]:
    config = load_symbol_config()
    market_cfg = _market_config(config, exchange, market)
    return _normalize_symbols(market_cfg.get("collect"), label=f"{exchange}.{market}.collect")


def get_trade_symbols(exchange: str = "binance", market: str = "futures") -> list[str]:
    config = load_symbol_config()
    market_cfg = _market_config(config, exchange, market)
    return _normalize_symbols(market_cfg.get("trade"), label=f"{exchange}.{market}.trade")


def get_backtest_symbols(exchange: str = "binance", market: str = "futures") -> list[str]:
    config = load_symbol_config()
    market_cfg = _market_config(config, exchange, market)
    return _normalize_symbols(market_cfg.get("backtest"), label=f"{exchange}.{market}.backtest")


def get_display_symbols() -> list[str]:
    config = load_symbol_config()
    return _normalize_symbols(config.get("display_symbols"), label="display_symbols")


def get_core_symbols() -> list[str]:
    config = load_symbol_config()
    return _normalize_symbols(config.get("core_symbols"), label="core_symbols")


def validate_known_symbol(symbol: str, allowed_symbols: list[str] | None = None) -> str:
    normalized = str(symbol).strip().upper()
    if not normalized:
        raise SymbolConfigError("symbol must not be empty")

    if allowed_symbols is None:
        config = load_symbol_config()
        allowed: set[str] = set(get_core_symbols()) | set(get_display_symbols())
        symbols_cfg = config.get("symbols", {})
        if isinstance(symbols_cfg, dict):
            for exchange_cfg in symbols_cfg.values():
                if not isinstance(exchange_cfg, dict):
                    continue
                for market_cfg in exchange_cfg.values():
                    if not isinstance(market_cfg, dict):
                        continue
                    for key in ("collect", "trade", "backtest"):
                        raw_symbols = market_cfg.get(key)
                        if raw_symbols is not None:
                            allowed.update(_normalize_symbols(raw_symbols, label=f"symbols.{key}"))
    else:
        allowed = set(_normalize_symbols(allowed_symbols, label="allowed_symbols"))

    if normalized not in allowed:
        raise SymbolConfigError(f"unknown symbol: {normalized}")
    return normalized


def parse_symbol_list(raw_symbols: str | None, default_symbols: list[str]) -> list[str]:
    if raw_symbols is None or not raw_symbols.strip():
        return list(default_symbols)
    return _normalize_symbols(
        [symbol for symbol in raw_symbols.split(",")],
        label="env symbols",
    )
