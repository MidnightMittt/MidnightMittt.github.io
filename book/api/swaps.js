// SWAP-SOURCED CANDLES AND REAL VOLUME, SERVED FROM THE EDGE.
//
// ── WHY THIS ENDPOINT EXISTS ──────────────────────────────────────────────
// The chart's history was SAMPLED: a cron read slot0 every 2 minutes, so a
// candle's high was the highest SAMPLE, not the highest trade, and there was
// no volume at all. This reads the actual Swap logs — the same primary source
// GeckoTerminal aggregates — so candles are true OHLC at any resolution and
// every bar carries the dollars that actually traded.
//
// It also removes two browser-side problems:
//   1. GeckoTerminal rate-limits a browser hard; a cached server call does not.
//   2. eth_getLogs over a wide span is many round trips. Doing it here once,
//      behind a CDN cache, costs every viewer one request.
//
// ── ZERO DEPENDENCIES ON PURPOSE ──────────────────────────────────────────
// No ethers. A serverless function that needs an install step is a function
// that can fail to deploy for reasons unrelated to its logic. Everything here
// is raw JSON-RPC and hex slicing.
//
// ── THE TIMESTAMP PROBLEM, AND WHAT MEASUREMENT SETTLED ───────────────────
// eth_getLogs does NOT return block times, and this chain mines ~9.5 blocks a
// second. Fetching every swap's block individually cost ~400 extra RPC calls
// and the whole request collapsed: 72 chunks failed and zero candles came
// back despite 447 swaps being found.
// So timestamps are ANCHORED AND INTERPOLATED — but the anchor spacing was
// measured, not guessed. Interpolating across a 40,000-block gap misplaced a
// block by up to 15.5s, which would throw a swap a quarter of the way into
// the wrong 1-minute bar. At 5,000 blocks the same test lands inside ~2s.
// That is the spacing used.
//
// ── ONE QUERY FOR ALL V4 POOLS ────────────────────────────────────────────
// topics[1] accepts an ARRAY, which is an OR filter, so every V4 pool is read
// in a single getLogs per block range instead of one per pool. That alone cut
// the request count by eight.

/* Several independent public endpoints serve chain 4663. Qualification at
   build time found that only the official one answers eth_getLogs, while
   three others serve batched block reads fine — so calls are routed by the
   capability they need and a throttled node is skipped rather than retried.
   A 429 here is why the first version of this endpoint returned zero candles
   while reporting 447 swaps found. */
const RPCS = {
  logs:   ['https://rpc.mainnet.chain.robinhood.com'],
  blocks: ['https://rpc.mainnet.chain.robinhood.com',
           'https://robinhood-rpc.publicnode.com',
           'https://robinhood.drpc.org',
           'https://robinhood.daybreak-rpc.network'],
};
let rr = 0;
const PM  = '0x8366a39cc670b4001a1121b8f6a443a643e40951';
// keccak("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)")
const V4_SWAP = '0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f';
// keccak("Swap(address,address,int256,int256,uint160,uint128,int24)")
const V3_SWAP = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67';

const hex = n => '0x' + n.toString(16);
const W   = (d, i) => d.slice(2 + i * 64, 2 + (i + 1) * 64);
// Accepts both a bare 64-char ABI word and an already-prefixed RPC quantity.
// The first version assumed bare words and produced "0x0x6ad9ac2c" the moment
// it met a block number.
const u   = h => BigInt(String(h).startsWith('0x') ? h : '0x' + h);
const sN  = (h, bits) => BigInt.asIntN(bits, u(h));

async function rpc(body, need = 'blocks') {
  const urls = RPCS[need] || RPCS.blocks;
  let last = null;
  for (let k = 0; k < urls.length * 2; k++) {
    const url = urls[(rr++) % urls.length];
    try {
      const r = await fetch(url, {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body)
      });
      if (r.status === 429 || r.status >= 500) { last = 'http ' + r.status;
        await new Promise(s => setTimeout(s, 120 * (k + 1))); continue; }
      if (!r.ok) throw new Error('rpc http ' + r.status);
      const j = await r.json();
      const bad = Array.isArray(j) ? (j.length > 0 && j.every(x => x && x.error)) : !!(j && j.error);
      if (bad) { last = 'rpc error'; continue; }
      return j;
    } catch (e) { last = String(e.message || e); }
  }
  throw new Error('all endpoints failed: ' + last);
}
async function one(method, params, need) {
  const j = await rpc({ jsonrpc: '2.0', id: 1, method, params },
                      need || (method === 'eth_getLogs' ? 'logs' : 'blocks'));
  if (j.error) throw new Error(j.error.message);
  return j.result;
}
// Batched, chunked, and tolerant: a chunk that fails yields nulls rather than
// taking the whole response down. A missing swap understates volume, which is
// the safe direction; a 500 gives the viewer nothing.
async function many(calls, size = 20, need) {
  const out = [];
  for (let i = 0; i < calls.length; i += size) {
    const chunk = calls.slice(i, i + size);
    try {
      const j = await rpc(chunk.map((c, k) => ({ jsonrpc: '2.0', id: i + k, ...c })), need);
      if (!Array.isArray(j)) throw new Error('not a batch');
      j.sort((a, b) => a.id - b.id);
      j.forEach(e => out.push(e.error ? null : e.result));
    } catch (e) {
      // One retry at half width before giving up on the chunk. The first
      // version nulled immediately and lost half the window to transient 429s.
      try {
        const half = Math.max(1, Math.ceil(chunk.length / 2)), got = [];
        for (let k = 0; k < chunk.length; k += half) {
          const sub = chunk.slice(k, k + half);
          await new Promise(r => setTimeout(r, 120));
          const j2 = await rpc(sub.map((c, m) => ({ jsonrpc: '2.0', id: m, ...c })), need);
          if (!Array.isArray(j2)) throw new Error('not a batch');
          j2.sort((a, b) => a.id - b.id);
          j2.forEach(e => got.push(e.error ? null : e.result));
        }
        got.forEach(v => out.push(v));
      } catch (e2) { chunk.forEach(() => out.push(null)); }
    }
  }
  return out;
}

// A swap that leaves a pool at MIN_TICK or MAX_TICK has exhausted one side of
// that pool. The log is real; the price is the edge of the tick range, not a
// tradeable number. One of these reported $3.4e50 and flattened the daily
// chart, so they are rejected by TICK — the thing that actually identifies the
// condition — rather than by guessing a magnitude cutoff.
const MIN_TICK = -887272, MAX_TICK = 887272, TICK_EDGE = 272;

// Decode one Swap log into a priced, dollar-denominated event.
function pushEvent(out, p, lg, ethUsd, isV4) {
  const d = lg.data;
  const tk = Number(sN(W(d, 4), 24));
  if (tk <= MIN_TICK + TICK_EDGE || tk >= MAX_TICK - TICK_EDGE) return 'edge';
  const a0 = sN(W(d, 0), isV4 ? 128 : 256);
  const a1 = sN(W(d, 1), isV4 ? 128 : 256);
  const sp = Number(u(W(d, 2))) / 2 ** 96;
  const rawPx = sp * sp * Math.pow(10, p.d0 - p.d1);
  const qUsd = p.quote === 'WETH' ? ethUsd : 1;
  const px = (p.tokIs0 ? rawPx : 1 / rawPx) * qUsd;
  if (!isFinite(px) || px <= 0) return;
  // Volume is the QUOTE leg — unambiguous, and independent of the price we
  // just derived from the same log.
  const qAmt = p.tokIs0 ? a1 : a0;
  const qDec = p.tokIs0 ? p.d1 : p.d0;
  out.push({ b: Number(u(lg.blockNumber)), px,
             vol: Math.abs(Number(qAmt)) / Math.pow(10, qDec) * qUsd, pool: p.name });
}

export default async function handler(req, res) {
  const q = (req && req.query) || {};
  const minutes = Math.max(5, Math.min(720, Number(q.minutes) || 180));
  const tf      = Math.max(1, Math.min(240, Number(q.tf) || 1));
  let pools;
  try { pools = JSON.parse(q.pools || '[]'); } catch (e) { pools = []; }
  if (!Array.isArray(pools) || !pools.length) {
    res.status(400).json({ error: 'pools[] required: [{key,kind,tokIs0,d0,d1,quote,name}]' });
    return;
  }

  try {
    const head = Number(await one('eth_blockNumber', []));
    // Block time is MEASURED, not assumed. A hardcoded 0.1s would silently
    // fetch the wrong window the day the chain's cadence changes.
    const hb = await one('eth_getBlockByNumber', [hex(head), false]);
    const pb = await one('eth_getBlockByNumber', [hex(head - 20000), false]);
    const secPerBlock = (Number(u(hb.timestamp)) - Number(u(pb.timestamp))) / 20000 || 0.1;
    const span  = Math.ceil((minutes * 60) / secPerBlock);
    const from  = Math.max(0, head - span);

    // The RPC caps a getLogs span, so the window is walked in slices.
    const SLICE = 20000, ranges = [];
    for (let b = from; b <= head; b += SLICE) ranges.push([b, Math.min(head, b + SLICE - 1)]);

    const ethUsd = await ethPrice();
    const v4 = pools.filter(p => p.kind === 'v4');
    const v3 = pools.filter(p => p.kind !== 'v4');
    const byKey = {}; pools.forEach(p => { byKey[p.key.toLowerCase()] = p; });

    // One OR-filtered query per range for ALL V4 pools, plus one per V3 pool.
    const logCalls = [];
    for (const [a, b] of ranges) {
      if (v4.length) logCalls.push({ method: 'eth_getLogs', params: [{
        address: PM, topics: [V4_SWAP, v4.map(p => p.key)],
        fromBlock: hex(a), toBlock: hex(b) }] });
      for (const p of v3) logCalls.push({ method: 'eth_getLogs', params: [{
        address: p.key, topics: [V3_SWAP], fromBlock: hex(a), toBlock: hex(b),
        _pool: p.key }] });
    }
    const logSets = await many(logCalls, 6, 'logs');

    const events = [];
    let li = 0, truncated = 0;
    for (const [a, b] of ranges) {
      if (v4.length) {
        const set = logSets[li++];
        if (set == null) truncated++;
        else for (const lg of set) {
          const p = byKey[(lg.topics[1] || '').toLowerCase()];
          if (p) pushEvent(events, p, lg, ethUsd, true);
        }
      }
      for (const p of v3) {
        const set = logSets[li++];
        if (set == null) { truncated++; continue; }
        for (const lg of set) pushEvent(events, p, lg, ethUsd, false);
      }
    }

    // Median-based net for anything the tick test cannot catch.
    const sortedPx = events.map(e => e.px).sort((a, b) => a - b);
    const med = sortedPx.length ? sortedPx[Math.floor(sortedPx.length / 2)] : 0;
    let wild = 0;
    if (med > 0) {
      const keep = events.filter(e => e.px > med / 25 && e.px < med * 25);
      wild = events.length - keep.length;
      events.length = 0; keep.forEach(e => events.push(e));
    }

    if (!events.length) {
      res.setHeader('cache-control', 's-maxage=15, stale-while-revalidate=60');
      res.status(200).json({ candles: [], swaps: 0, secPerBlock, truncated,
        note: 'no swaps in window' });
      return;
    }

    // ANCHOR + INTERPOLATE. Spacing is 5,000 blocks because that is what the
    // measurement supported; see the header note.
    const ANCHOR = 5000;
    const aBlocks = [];
    for (let b = from; b < head; b += ANCHOR) aBlocks.push(b);
    aBlocks.push(head);
    const ares = await many(aBlocks.map(b => ({
      method: 'eth_getBlockByNumber', params: [hex(b), false] })), 25, 'blocks');
    const anchors = [];
    aBlocks.forEach((b, i) => { if (ares[i]) anchors.push([b, Number(u(ares[i].timestamp))]); });
    if (anchors.length < 2) throw new Error('could not establish block timestamps');
    const tsAt = blk => {
      if (blk <= anchors[0][0]) return anchors[0][1] + (blk - anchors[0][0]) * secPerBlock;
      for (let i = 0; i + 1 < anchors.length; i++) {
        const [b0, t0] = anchors[i], [b1, t1] = anchors[i + 1];
        if (blk <= b1) return t0 + (t1 - t0) * (blk - b0) / (b1 - b0);
      }
      const last = anchors[anchors.length - 1];
      return last[1] + (blk - last[0]) * secPerBlock;
    };

    events.sort((a, b) => a.b - b.b);
    const span2 = tf * 60, buckets = new Map();
    for (const e of events) {
      const ts = tsAt(e.b); if (!ts) continue;
      const k = Math.floor(ts / span2) * span2;
      let c = buckets.get(k);
      if (!c) { c = [k, e.px, e.px, e.px, e.px, 0, 0]; buckets.set(k, c); }
      c[2] = Math.max(c[2], e.px); c[3] = Math.min(c[3], e.px);
      c[4] = e.px; c[5] += e.vol; c[6] += 1;
    }
    const candles = [...buckets.values()].sort((a, b) => a[0] - b[0]);
    const totalVol = events.reduce((s, e) => s + e.vol, 0);

    // Volume keeps CENTS below $10 — arb bots print real swaps worth $0.23,
    // and rounding them to "$0" made true prints read as nonsense.
    const trades = events.slice(-80).map(e =>
      [Math.round(tsAt(e.b)), e.px, e.vol>=10?Math.round(e.vol):Math.round(e.vol*100)/100, e.pool]);

    // Short windows exist for the fast tape poll; a 15s CDN cache would eat
    // most of the latency the fast poll is buying. Long windows keep the
    // longer cache -- they are the expensive calls worth absorbing.
    res.setHeader('cache-control', minutes <= 15
      ? 's-maxage=5, stale-while-revalidate=20'
      : 's-maxage=15, stale-while-revalidate=60');
    res.status(200).json({
      trades,                  // newest 80 raw swaps: [ts, px, volUsd, pool]
      candles,                 // [ts, o, h, l, c, volUsd, swapCount]
      swaps: events.length,
      volumeUsd: totalVol,
      windowMinutes: minutes,
      tf, secPerBlock, fromBlock: from, head, truncated, anchors: anchors.length,
      droppedOutliers: wild,
      ethUsd,
      source: 'Swap logs read from the chain — true OHLC, not sampled'
    });
  } catch (e) {
    res.status(502).json({ error: String((e && e.message) || e) });
  }
}

// WETH/USDG 0.01% pool slot0 -> ETH in USD, so WETH-quoted swaps land in the
// same unit as USDG-quoted ones. Comparing the two without this conversion is
// exactly the mistake that condemned two real pools during the build.
async function ethPrice() {
  try {
    const r = await one('eth_call', [{
      to: '0x52e65B17fB6E5BA00Ed806f37Afcd2DaA50271Ca', data: '0x3850c7bd' }, 'latest']);
    const sp = Number(u(W(r, 0)) & ((1n << 160n) - 1n)) / 2 ** 96;
    return sp * sp * 1e12;   // WETH is token0 here; 10^(18-6)
  } catch (e) { return 0; }
}
