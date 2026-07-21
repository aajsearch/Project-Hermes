import React, { useState } from "react";
import { api } from "../api/client.js";

export default function CommandCenter({ status }) {
  const [haltOpen, setHaltOpen] = useState(false);
  const [variant, setVariant] = useState("flatten");
  if (!status) return <div className="panel muted">Connecting…</div>;
  const ss = status.system || {};
  const stats = status.daily_stats || {};
  const cb = status.circuit_breakers || {};
  const snaps = status.snapshots || [];
  const pnl = Number(stats.realized_pnl || 0);
  const slU = cb.equity_sl?.used || 0;
  const slM = cb.equity_sl?.max || 5;
  const oU = cb.option_losses?.used || 0;
  const oM = cb.option_losses?.max || 2;

  return (
    <>
      <div className="panel row between">
        <div>
          <div className="muted">Mode</div>
          <strong>{(ss.mode || "—").toUpperCase()}</strong>
          {ss.halt_reason && <span className="muted"> · {ss.halt_reason}</span>}
        </div>
        <div className="row">
          <button className="btn btn-secondary" onClick={() => api("/api/mode", { method: "POST", body: JSON.stringify({ mode: "autonomous" }) })}>Autonomous</button>
          <button className="btn btn-secondary" onClick={() => api("/api/mode", { method: "POST", body: JSON.stringify({ mode: "copilot" }) })}>Co-Pilot</button>
          <button className="btn btn-ok" onClick={() => api("/api/resume", { method: "POST" })}>Resume</button>
          <button className="btn btn-halt" onClick={() => setHaltOpen(true)}>GLOBAL HALT</button>
        </div>
      </div>
      <div className="panel grid-2">
        <div>
          <div className="muted">Daily realized PnL</div>
          <div className={pnl >= 0 ? "pos" : "neg"} style={{ fontSize: "1.4rem", fontWeight: 600 }}>
            ${pnl.toFixed(2)}
          </div>
          <div className="muted">{stats.trade_count || 0} trades today</div>
        </div>
        <div>
          <div className="row" style={{ marginBottom: 8 }}>
            <span className="muted">Equity SLs</span>
            <span>{slU}/{slM}</span>
            <div className="bar"><span style={{ width: `${Math.min(100, (slU / slM) * 100)}%` }} /></div>
          </div>
          <div className="row">
            <span className="muted">Option losses</span>
            <span>{oU}/{oM}</span>
            <div className="bar"><span style={{ width: `${Math.min(100, (oU / oM) * 100)}%` }} /></div>
          </div>
        </div>
      </div>
      <div className="panel">
        <strong>Live positions</strong>
        <table>
          <thead>
            <tr>
              <th>Symbol</th><th>Mark</th><th>Entry</th><th>PnL</th><th>→ TP</th><th>→ SL</th><th></th>
            </tr>
          </thead>
          <tbody>
            {!snaps.length && (
              <tr><td colSpan={7} className="muted">No open positions</td></tr>
            )}
            {snaps.map((p) => {
              const prec = p.asset === "option" ? 4 : 2;
              return (
                <tr key={p.option_id || p.symbol}>
                  <td><b>{p.symbol}</b></td>
                  <td>${Number(p.last).toFixed(prec)}</td>
                  <td>${Number(p.entry).toFixed(prec)}</td>
                  <td className={p.pnl >= 0 ? "pos" : "neg"}>
                    ${Number(p.pnl).toFixed(2)} ({Number(p.pnl_pct).toFixed(2)}%)
                  </td>
                  <td>{Number(p.dist_tp).toFixed(prec)}</td>
                  <td>{Number(p.dist_sl).toFixed(prec)}</td>
                  <td>
                    <button
                      className="btn btn-danger"
                      onClick={() =>
                        api("/api/liquidate", {
                          method: "POST",
                          body: JSON.stringify({
                            symbol: p.option_id ? null : p.symbol,
                            option_id: p.option_id || null,
                          }),
                        })
                      }
                    >
                      Liquidate
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {haltOpen && (
        <div className="modal-bg show">
          <div className="modal">
            <h3>GLOBAL HALT</h3>
            {["flatten", "soft", "entries_only"].map((v) => (
              <label key={v} style={{ display: "block", marginBottom: 8 }}>
                <input type="radio" checked={variant === v} onChange={() => setVariant(v)} /> {v}
              </label>
            ))}
            <div className="row" style={{ justifyContent: "flex-end" }}>
              <button className="btn btn-secondary" onClick={() => setHaltOpen(false)}>Cancel</button>
              <button
                className="btn btn-halt"
                onClick={async () => {
                  await api("/api/halt", { method: "POST", body: JSON.stringify({ variant }) });
                  setHaltOpen(false);
                }}
              >
                Confirm
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
