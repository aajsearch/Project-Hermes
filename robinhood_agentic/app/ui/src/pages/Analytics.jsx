import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";

export default function Analytics() {
  const [data, setData] = useState(null);
  useEffect(() => {
    api("/api/analytics/summary").then(setData);
  }, []);
  if (!data) return <div className="panel muted">Loading…</div>;
  return (
    <div className="panel">
      <div className="row between">
        <div>
          <strong>Edge verification</strong>
          <div className="muted">
            {data.trade_count} trades · median slippage{" "}
            {data.median_slippage_bps != null ? `${data.median_slippage_bps.toFixed(1)} bps` : "n/a"}
          </div>
        </div>
        <a className="btn btn-secondary" href="/api/analytics/export.csv">Export CSV</a>
      </div>
      <h4>Exit reasons</h4>
      <div className="feed">
        {Object.entries(data.exit_reasons || {}).map(([k, v]) => (
          <div key={k}>{k}: <b>{v}</b></div>
        ))}
      </div>
      <h4>Time buckets</h4>
      <div className="feed">
        {Object.entries(data.heatmap || {}).map(([k, v]) => (
          <div key={k}>
            {k} · n={v.n} WR={v.n ? ((v.wins / v.n) * 100).toFixed(0) : 0}% PnL=${Number(v.pnl).toFixed(2)}
          </div>
        ))}
      </div>
      <h4>Slippage points</h4>
      <div className="feed">
        {(data.slippage_points || []).slice(0, 40).map((p, i) => (
          <div key={i}>
            {p.symbol} exp={p.expected_mid} fill={p.fill_price} ({Number(p.slippage_bps).toFixed(1)} bps)
          </div>
        ))}
      </div>
    </div>
  );
}
