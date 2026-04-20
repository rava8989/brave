// Pre-earnings UOA scoring engine. Single source of truth for:
//   - backtester UI (earnings_uoa.html)
//   - live dashboard (earnings_live.html)
//   - worker cron (schwab-proxy.js)
//
// Loaded as a browser <script> (exposes window.EarningsEngine) and as a
// module via `import { scoreEarningsEvent, evaluateBacktest } from './earnings-engine.js'`.
// Must not reference any file-specific DOM or globals.

(function () {
  'use strict';

  // ── Signal definitions ─────────────────────────────────────────────
  // Each signal has its own threshold and its own independent evaluation.
  // Following repo Strategy Independence rule — no cross-signal fallbacks.
  const UOA_SIGNALS = {
    vor: {
      label: 'Volume / 20d avg',
      field: 'vol_ratio_calls',
      thresholdDefault: 1.5,  // today's volume ≥ 1.5× trailing 20d avg
      description: "Volume relative to this ticker's trailing 20-day options flow. Substitutes for volume/OI ratio (OI unavailable on Polygon plan).",
    },
    premium: {
      label: 'Call premium $',
      field: 'call_premium_usd',
      thresholdDefault: 5_000_000,  // $5M on call side
      description: 'Total call-side premium spent on that lookback day. Large $ = large institutional bet.',
    },
    ivrank: {
      label: 'IV Rank (252d)',
      field: 'iv_rank_252d',  // derived client-side from iv_atm distribution per ticker
      thresholdDefault: 80,   // IV at 80th percentile or higher vs trailing year
      description: "ATM IV ranked against ticker's own trailing 252 calendar-day iv_atm samples.",
    },
    blocks: {
      label: 'Large blocks',
      field: 'large_block_count_calls',
      thresholdDefault: 10,
      description: 'Large block trades (size ≥ 100 AND premium ≥ $25k). Not computed in PR1 — S3 trades_v1 permissions needed.',
      unavailable: true,
    },
  };

  const DEFAULT_CONFIG = Object.fromEntries(
    Object.entries(UOA_SIGNALS).map(([k, s]) => [k, {
      enabled: !s.unavailable,
      threshold: s.thresholdDefault,
      side: 'both',  // 'call' | 'put' | 'both'
    }])
  );

  // ── IV rank computation (client-side, once per ticker) ─────────────
  // Annotates each event with iv_rank_252d based on ticker's IV history.
  function annotateIvRank(events) {
    const byTicker = new Map();
    for (const ev of events) {
      const iv = ev.lookback?.['-1']?.iv_atm;
      if (iv == null) continue;
      if (!byTicker.has(ev.ticker)) byTicker.set(ev.ticker, []);
      byTicker.get(ev.ticker).push({ date: ev.report_date, iv });
    }
    for (const arr of byTicker.values()) arr.sort((a, b) => a.date.localeCompare(b.date));

    for (const ev of events) {
      const lb = ev.lookback?.['-1'];
      if (!lb || lb.iv_atm == null) continue;
      const hist = byTicker.get(ev.ticker) || [];
      // Use last 252 calendar days of this ticker's iv_atm samples (earnings-only
      // samples, so sparse; treat as best-available baseline).
      const cutoff = new Date(ev.report_date);
      cutoff.setDate(cutoff.getDate() - 252);
      const cutoffIso = cutoff.toISOString().slice(0, 10);
      const window = hist.filter(x => x.date < ev.report_date && x.date >= cutoffIso).map(x => x.iv);
      if (window.length < 3) continue;
      const below = window.filter(iv => iv < lb.iv_atm).length;
      lb.iv_rank_252d = Math.round(100 * below / window.length);
    }
  }

  // ── Single-event scoring ───────────────────────────────────────────
  // Returns { vor: {...}, premium: {...}, ivrank: {...}, blocks: {...}, direction, firesAny, firesCount }
  function scoreEarningsEvent(event, config) {
    const cfg = { ...DEFAULT_CONFIG, ...(config || {}) };
    const lb = event.lookback?.['-1'] || {};
    const out = {
      vor: evalSide(cfg.vor, lb.vol_ratio_calls, lb.vol_ratio_puts),
      premium: evalSide(cfg.premium, lb.call_premium_usd, lb.put_premium_usd),
      ivrank: evalRank(cfg.ivrank, lb.iv_rank_252d),
      blocks: evalSide(cfg.blocks, lb.large_block_count_calls, lb.large_block_count_puts),
      direction: decideDirection(event),
    };
    out.firesCount = Object.keys(UOA_SIGNALS).filter(k => out[k]?.fires).length;
    out.firesAny = out.firesCount > 0;
    return out;
  }

  function evalSide(sigCfg, callVal, putVal) {
    if (!sigCfg?.enabled) return { fires: false, side: null, value: null };
    const thr = sigCfg.threshold;
    const callFires = (callVal != null) && callVal >= thr;
    const putFires = (putVal != null) && putVal >= thr;
    if (sigCfg.side === 'call') {
      return { fires: callFires, side: callFires ? 'call' : null, value: callVal };
    }
    if (sigCfg.side === 'put') {
      return { fires: putFires, side: putFires ? 'put' : null, value: putVal };
    }
    // both
    if (callFires && putFires) {
      return callVal >= putVal
        ? { fires: true, side: 'call', value: callVal }
        : { fires: true, side: 'put', value: putVal };
    }
    if (callFires) return { fires: true, side: 'call', value: callVal };
    if (putFires) return { fires: true, side: 'put', value: putVal };
    return { fires: false, side: null, value: Math.max(callVal ?? 0, putVal ?? 0) };
  }

  function evalRank(sigCfg, rank) {
    if (!sigCfg?.enabled || rank == null) return { fires: false, side: null, value: rank };
    return { fires: rank >= sigCfg.threshold, side: null, value: rank };
  }

  function decideDirection(event) {
    const lb = event.lookback?.['-1'];
    if (!lb) return null;
    const callVol = lb.total_call_volume || 0;
    const putVol = lb.total_put_volume || 0;
    if (callVol + putVol === 0) return null;
    const callShare = callVol / (callVol + putVol);
    if (callShare >= 0.6) return 'long';
    if (callShare <= 0.4) return 'short';
    return null;  // mixed → skip per notification rules
  }

  // ── Backtest ───────────────────────────────────────────────────────
  // params: { enabledSignals: ['vor','premium',...], combine: 'any'|'all',
  //          holdDays: 1|3|5|10, longOnCalls: bool, shortOnPuts: bool,
  //          dateFrom, dateTo, universe (filter fn ticker->bool) }
  // Returns: { trades: [...], perSignal: {sig: {trades, winRate, avgMove, sharpe}},
  //            combined: {trades, winRate, avgMove, sharpe, maxDD} }
  function evaluateBacktest(events, config, params) {
    const {
      holdDays = 1,
      longOnCalls = true,
      shortOnPuts = true,
      combine = 'any',
      dateFrom,
      dateTo,
      universe,
    } = params || {};
    const signalKeys = Object.keys(UOA_SIGNALS);
    const moveField = `move_${holdDays}d`;

    const filtered = events.filter(e => {
      if (dateFrom && e.report_date < dateFrom) return false;
      if (dateTo && e.report_date > dateTo) return false;
      if (universe && !universe(e.ticker)) return false;
      const lb = e.lookback?.['-1'];
      return lb && e.labels && e.labels[moveField] != null;
    });

    const perSignalTrades = Object.fromEntries(signalKeys.map(k => [k, []]));
    const combinedTrades = [];

    for (const ev of filtered) {
      const scored = scoreEarningsEvent(ev, config);
      const direction = scored.direction;
      if (!direction) continue;
      if (direction === 'long' && !longOnCalls) continue;
      if (direction === 'short' && !shortOnPuts) continue;

      const move = ev.labels[moveField];
      const pnlPct = direction === 'long' ? move : -move;
      const tradeBase = {
        ticker: ev.ticker,
        report_date: ev.report_date,
        direction,
        entry: ev.labels.pre_close,
        exit_close: ev.labels.pre_close ? ev.labels.pre_close * (1 + move) : null,
        move,
        pnl_pct: pnlPct,
      };

      // Per-signal breakdown (each signal's card decides its own fate)
      for (const sig of signalKeys) {
        if (scored[sig]?.fires) {
          perSignalTrades[sig].push({ ...tradeBase, signal: sig });
        }
      }

      // Combined (respects user's combine mode)
      const firedSet = signalKeys.filter(k => scored[k]?.fires);
      const combinedFires = combine === 'all'
        ? firedSet.length === signalKeys.filter(k => config?.[k]?.enabled ?? DEFAULT_CONFIG[k].enabled).length
        : firedSet.length > 0;
      if (combinedFires) {
        combinedTrades.push({ ...tradeBase, signals_fired: firedSet });
      }
    }

    return {
      perSignal: Object.fromEntries(signalKeys.map(k => [k, computeKpis(perSignalTrades[k])])),
      combined: computeKpis(combinedTrades),
    };
  }

  function computeKpis(trades) {
    const n = trades.length;
    if (!n) return { trades: [], count: 0, winRate: 0, avgMove: 0, sharpe: 0, maxDD: 0, profitFactor: 0 };
    const pnls = trades.map(t => t.pnl_pct);
    const wins = pnls.filter(p => p > 0).length;
    const avgMove = pnls.reduce((a, b) => a + b, 0) / n;
    const variance = pnls.reduce((a, b) => a + (b - avgMove) ** 2, 0) / n;
    const std = Math.sqrt(variance);
    const sharpe = std > 0 ? (avgMove / std) * Math.sqrt(252) : 0;
    // Profit factor
    const grossWin = pnls.filter(p => p > 0).reduce((a, b) => a + b, 0);
    const grossLoss = Math.abs(pnls.filter(p => p < 0).reduce((a, b) => a + b, 0));
    const profitFactor = grossLoss > 0 ? grossWin / grossLoss : (grossWin > 0 ? Infinity : 0);
    // Sorted-by-date equity curve + max drawdown
    const sorted = [...trades].sort((a, b) => a.report_date.localeCompare(b.report_date));
    let equity = 1, peak = 1, maxDD = 0;
    const curve = [];
    for (const t of sorted) {
      equity *= (1 + t.pnl_pct);
      peak = Math.max(peak, equity);
      const dd = (equity - peak) / peak;
      if (dd < maxDD) maxDD = dd;
      curve.push({ date: t.report_date, equity });
    }
    return {
      trades: sorted,
      count: n,
      winRate: wins / n,
      avgMove,
      sharpe,
      maxDD,
      profitFactor,
      curve,
    };
  }

  // Win-rate gate for worker cron notifications
  function winRateGate(backtestStats, signalName, minWinRate) {
    const s = backtestStats?.[signalName];
    if (!s) return false;
    return s.winRate >= minWinRate && s.count >= 20;  // min sample size guard
  }

  const api = {
    UOA_SIGNALS,
    DEFAULT_CONFIG,
    annotateIvRank,
    scoreEarningsEvent,
    decideDirection,
    evaluateBacktest,
    computeKpis,
    winRateGate,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  }
  if (typeof window !== 'undefined') {
    window.EarningsEngine = api;
  }
})();
