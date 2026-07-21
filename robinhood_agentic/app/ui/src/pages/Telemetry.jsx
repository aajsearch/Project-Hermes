import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";

function fmtCountdown(sec) {
  if (sec == null || sec < 0) return "--:--";
  const m = Math.floor(sec / 60);
  const s = Math.floor(sec % 60);
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

export default function Telemetry() {
  const [audit, setAudit] = useState([]);
  const [scan, setScan] = useState(null);
  const [history, setHistory] = useState([]);
  const [schedule, setSchedule] = useState(null);
  const [summary, setSummary] = useState(null);
  const [text, setText] = useState("");
  const [selectedId, setSelectedId] = useState(null);
  const [seconds, setSeconds] = useState(null);

  const refresh = async () => {
    const [a, s] = await Promise.all([api("/api/audit?limit=80"), api("/api/scan?limit=25")]);
    setAudit(a.events || []);
    setScan(s.scan);
    setHistory(s.history || []);
    setSchedule(s.schedule);
    setSummary(s.summary);
    setSeconds(s.schedule?.seconds_until_next ?? null);
    if (!selectedId && s.scan?.id) setSelectedId(s.scan.id);
  };

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const id = setInterval(() => {
      setSeconds((prev) => (prev != null && prev > 0 ? prev - 1 : prev));
    }, 1000);
    return () => clearInterval(id);
  }, []);

  const loadDetail = async (id) => {
    setSelectedId(id);
    const d = await api(`/api/scan?scan_id=${id}`);
    setScan(d.scan);
    setSummary(d.summary);
  };

  return (
    <>
      <div className="panel row">
        <button className="btn btn-secondary" onClick={() => api("/api/force-rescan", { method: "POST" }).then(refresh)}>
          Force Rescan
        </button>
        <div style={{ marginLeft: "auto", textAlign: "right" }}>
          <div style={{ fontSize: "1.2rem", fontWeight: 700 }}>{fmtCountdown(seconds)}</div>
          <div className="muted">{schedule?.scan_in_progress ? "scanning…" : "until next scan"}</div>
        </div>
        <input style={{ flex: 1 }} value={text} onChange={(e) => setText(e.target.value)} placeholder="evaluate_equity:TSLA" />
        <button
          className="btn btn-ok"
          onClick={() => api("/api/inject", { method: "POST", body: JSON.stringify({ text }) }).then(refresh)}
        >
          Inject
        </button>
      </div>
      <div className="grid-2">
        <div className="panel">
          <strong>Scan history</strong>
          <table>
            <thead><tr><th>When</th><th>Trigger</th><th>Qualified</th><th>Top</th></tr></thead>
            <tbody>
              {history.map((h) => (
                <tr key={h.id} style={{ cursor: "pointer", background: selectedId === h.id ? "#1c2430" : undefined }} onClick={() => loadDetail(h.id)}>
                  <td>{h.ts}</td>
                  <td>{h.trigger}</td>
                  <td>{h.qualified_count}/{h.evaluated}</td>
                  <td>{(h.top || []).join(", ")}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <strong style={{ display: "block", marginTop: "1rem" }}>Brain feed</strong>
          <div className="feed">
            {audit.map((e) => (
              <div key={e.id}>
                <span className="muted">{e.ts}</span> <b>{e.event_type}</b> {JSON.stringify(e.payload)}
              </div>
            ))}
          </div>
        </div>
        <div className="panel">
          <strong>Scan detail</strong>
          <div className="muted">
            {scan ? `#${scan.id} · ${scan.trigger} · ${summary?.qualified_count || 0} qualified` : "No scan yet"}
          </div>
          {summary?.reject_counts && (
            <div className="muted" style={{ marginTop: 4 }}>
              Rejects: {Object.entries(summary.reject_counts).map(([k, v]) => `${k}:${v}`).join(" · ")}
            </div>
          )}
          <table>
            <thead><tr><th>Sym</th><th>Status</th><th>Day%</th><th>Mid</th></tr></thead>
            <tbody>
              {(scan?.rows || []).map((r) => (
                <tr key={r.symbol}>
                  <td>{r.symbol}</td>
                  <td>{r.qualified ? `#${r.rank}` : r.reject_reason}</td>
                  <td>{r.day != null ? (r.day * 100).toFixed(2) : "—"}</td>
                  <td>{r.mid != null ? `$${r.mid.toFixed(2)}` : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
