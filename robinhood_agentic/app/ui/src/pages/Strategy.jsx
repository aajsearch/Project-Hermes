import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";

export default function Strategy() {
  const [tech, setTech] = useState(null);
  const [opt, setOpt] = useState(null);
  const [sameDay, setSameDay] = useState(true);
  const [dryRun, setDryRun] = useState(false);

  const load = async () => {
    const [t, o, s] = await Promise.all([
      api("/api/config/tech_scalper"),
      api("/api/config/options_directional"),
      api("/api/status"),
    ]);
    setTech(t);
    setOpt(o);
    setSameDay(!!s.system.same_day_symbol_block);
    setDryRun(!!s.system.dry_run);
  };

  useEffect(() => {
    load();
  }, []);

  if (!tech || !opt) return <div className="panel muted">Loading config…</div>;
  const t = tech.config;
  const o = opt.config;

  const setScalp = (key, val) =>
    setTech({ ...tech, config: { ...t, scalp: { ...t.scalp, [key]: val } } });
  const setSel = (key, val) =>
    setTech({ ...tech, config: { ...t, selection: { ...t.selection, [key]: val } } });

  return (
    <div className="grid-2">
      <div className="panel">
        <strong>Tech Equity Scalper v{tech.version}</strong>
        <div className="field"><label>TP %</label>
          <input type="number" step="0.0001" value={t.scalp.profit_target_pct}
            onChange={(e) => setScalp("profit_target_pct", Number(e.target.value))} /></div>
        <div className="field"><label>SL %</label>
          <input type="number" step="0.0001" value={t.scalp.stop_loss_pct}
            onChange={(e) => setScalp("stop_loss_pct", Number(e.target.value))} /></div>
        <div className="field"><label>Max spread %</label>
          <input type="number" step="0.0001" value={t.selection.max_spread_pct}
            onChange={(e) => setSel("max_spread_pct", Number(e.target.value))} /></div>
        <div className="field"><label>Universe</label>
          <textarea rows={4} style={{ width: "100%" }}
            value={(t.watchlist?.all || []).join(", ")}
            onChange={(e) =>
              setTech({
                ...tech,
                config: {
                  ...t,
                  watchlist: {
                    ...t.watchlist,
                    all: e.target.value.split(",").map((s) => s.trim().toUpperCase()).filter(Boolean),
                  },
                },
              })
            } /></div>
        <button className="btn btn-ok" onClick={() => api("/api/config/tech_scalper", { method: "PUT", body: JSON.stringify({ config: tech.config }) }).then(load)}>
          Apply Tech
        </button>
        <button className="btn btn-secondary" onClick={() => api("/api/config/tech_scalper/reset", { method: "POST" }).then(load)}>Reset YAML</button>
      </div>
      <div className="panel">
        <strong>Options Directional v{opt.version}</strong>
        <div className="field"><label>TP %</label>
          <input type="number" step="0.01" value={o.trade.profit_target_pct}
            onChange={(e) => setOpt({ ...opt, config: { ...o, trade: { ...o.trade, profit_target_pct: Number(e.target.value) } } })} /></div>
        <div className="field"><label>SL %</label>
          <input type="number" step="0.01" value={o.trade.stop_loss_pct}
            onChange={(e) => setOpt({ ...opt, config: { ...o, trade: { ...o.trade, stop_loss_pct: Number(e.target.value) } } })} /></div>
        <div className="field"><label>Same-day symbol block</label>
          <select value={sameDay ? "1" : "0"} onChange={(e) => setSameDay(e.target.value === "1")}>
            <option value="1">On</option><option value="0">Off</option>
          </select>
        </div>
        <div className="field"><label>Dry run</label>
          <select value={dryRun ? "1" : "0"} onChange={(e) => setDryRun(e.target.value === "1")}>
            <option value="0">Off</option><option value="1">On</option>
          </select>
        </div>
        <button className="btn btn-ok" onClick={() => api("/api/config/options_directional", { method: "PUT", body: JSON.stringify({ config: opt.config }) }).then(load)}>
          Apply Options
        </button>
        <button className="btn btn-secondary" onClick={() => api("/api/flags", { method: "POST", body: JSON.stringify({ same_day_symbol_block: sameDay, dry_run: dryRun }) })}>
          Save Flags
        </button>
      </div>
    </div>
  );
}
