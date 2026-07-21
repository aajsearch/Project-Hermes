import React, { useEffect, useState } from "react";
import { api } from "./api/client.js";
import CommandCenter from "./pages/CommandCenter.jsx";
import Telemetry from "./pages/Telemetry.jsx";
import Strategy from "./pages/Strategy.jsx";
import Analytics from "./pages/Analytics.jsx";

const TABS = [
  ["cc", "Command Center"],
  ["tel", "Telemetry"],
  ["cfg", "Strategy"],
  ["an", "Analytics"],
];

export default function App() {
  const [tab, setTab] = useState("cc");
  const [status, setStatus] = useState(null);

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const s = await api("/api/status");
        if (alive) setStatus(s);
      } catch {
        if (alive) setStatus(null);
      }
    };
    tick();
    const id = setInterval(tick, 3000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const ss = status?.system || {};
  const age = status?.heartbeat_at
    ? (Date.now() - new Date(status.heartbeat_at).getTime()) / 1000
    : 9999;
  const poll = ss.poll_seconds || 15;
  const hbClass = age < poll * 2 ? "ok" : age < poll * 6 ? "warn" : "bad";

  return (
    <div>
      <header className="header">
        <h1>Command Center</h1>
        <span className="heartbeat">
          <span className={`dot ${hbClass}`} /> Engine
        </span>
        <span className="heartbeat">
          <span className={`dot ${status?.mcp_ok ? "ok" : "bad"}`} /> MCP
        </span>
        <nav className="tabs">
          {TABS.map(([id, label]) => (
            <button
              key={id}
              className={tab === id ? "active" : ""}
              onClick={() => setTab(id)}
            >
              {label}
            </button>
          ))}
        </nav>
      </header>
      <main className="main">
        {tab === "cc" && <CommandCenter status={status} />}
        {tab === "tel" && <Telemetry />}
        {tab === "cfg" && <Strategy />}
        {tab === "an" && <Analytics />}
      </main>
    </div>
  );
}
