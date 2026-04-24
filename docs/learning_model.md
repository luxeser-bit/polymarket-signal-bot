# Learning Model And Data Strategy

This project should be treated as a paper-only research system that must prove
an edge before any live execution work is considered. The long-term target is
86,000,000 analyzed trades and 14,000 tracked wallets, but volume only matters if
the data is clean enough to improve decisions.

## Three-Level Learning Model

### Level 1: Rule Auto-Calibration

The system should automatically compare rule settings on historical replays and
paper outcomes, then save the best paper-mode settings.

Settings to calibrate:

- policy mode: baseline, strict cohort, balanced cohort, stable-only, liquidity-watch;
- minimum wallet score;
- minimum trade notional;
- copy delay assumptions;
- max hold time;
- risk limits: exposure, market exposure, wallet exposure, open-position cap;
- exit behavior: stop loss, take profit, max hold, stale price, risk trim.

This level is not a neural model. It is still learning because the bot changes
its operating rules from measured results instead of fixed assumptions.

### Level 2: Adaptive Wallet Scoring

Wallet scoring should become outcome-aware. A wallet is not simply good or bad;
it can be useful only in specific market conditions.

The system should learn:

- which wallets produce profitable paper entries;
- which wallets often lead to stop loss, stale exits, or risk trim;
- which wallets work by market category: sports, politics, crypto, business, other;
- which wallets work only with enough liquidity and narrow spread;
- which wallets are early movers versus late/noisy movers;
- which wallets copy other wallets rather than initiate useful movement.

The scoring target is contextual:

> This wallet is useful in this category, with this liquidity, at this delay, and
> at this position size.

### Level 3: ML Signal Classifier

Only after enough paper decisions exist, train a model that predicts whether a
new signal should be copied.

Candidate features:

- wallet score, repeatability, drawdown, cohort status, cohort stability;
- wallet category-specific performance;
- trade notional, observed price, suggested price, side, outcome;
- market category, event slug, market age;
- order-book spread, depth, liquidity score, price deviation;
- cluster wallet count, cluster notional, similar-wallet behavior;
- copy delay, signal age, recent volatility;
- current portfolio exposure and risk state;
- historical close reasons for similar signals.

Candidate labels:

- profitable/unprofitable after fees and slippage assumptions;
- realized paper PnL;
- max adverse excursion;
- close reason: take_profit, stop_loss, max_hold, stale_price, risk_trim;
- blocked reason: duplicate_asset, total_exposure_cap, market_exposure_cap,
  wallet_exposure_cap, open_position_cap, cohort_auto_policy.

The first ML model should be a simple classifier or ranker. It should not replace
the risk guard. Risk limits remain hard constraints.

## Data Required For The 86M/14K Target

Every future feature should either improve decisions or improve the data needed
to train decisions. Data collection should prioritize these entities.

### Trades

Store enough trade history to reconstruct what the bot would have seen:

- trade id, wallet, side, asset, condition id, timestamp;
- size, price, notional;
- title, slug, event slug, outcome;
- source endpoint and inserted timestamp;
- whether the trade came from wallet sync or market-flow sync.

### Wallets

Track both discovered wallets and watched wallets:

- wallet source: leaderboard, market-flow, trade-flow, file, manual;
- first seen, last seen, sync checkpoint;
- trade count, active days, market count, category mix;
- notional volume, buy/sell balance, average trade size;
- repeatability, drawdown, stability, cohort status;
- paper outcome stats by wallet and category.

### Markets And Liquidity

A signal is only useful if it can be entered at a reasonable price:

- category, market title, slug, event slug;
- latest order-book best bid/ask, mid, spread;
- bid/ask depth in USDC;
- liquidity score;
- last trade price;
- stale-price age.

### Paper Decisions

The system must log its own behavior, not only public market behavior:

- signal created;
- signal opened;
- signal blocked;
- position closed;
- reason for every block and close;
- policy mode, cohort status, risk state at decision time;
- price, size, PnL, hold time.

This decision journal is the bridge from rule-based paper trading to adaptive
learning.

## Noise Filtering Priorities

More data is not automatically better. The pipeline should filter or label:

- tiny trades below notional thresholds;
- duplicate assets already open in paper;
- wallets with extreme trade frequency and low repeatability;
- wallets active only in one burst/day;
- markets with stale prices;
- wide-spread or low-depth markets;
- late entries where book price already moved too far;
- unknown or low-stability cohorts;
- signals blocked by risk limits;
- wallets whose paper outcomes are consistently negative in a category.

Do not delete all noisy observations blindly. Prefer storing enough context to
learn why they were filtered, while keeping them out of automatic paper entries.

## Operating Rule

For every new feature, ask:

> Does this help us collect cleaner data, reduce noise, improve wallet scoring,
> calibrate rules, or train a better signal classifier?

If the answer is no, it is secondary to the 86M trades / 14K wallets learning
goal.
