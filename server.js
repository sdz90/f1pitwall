// ============================================================================
// Pitwall Radio — F1 live-timing relay
// ----------------------------------------------------------------------------
// Connects to Formula 1's free official live-timing feed (SignalR) and re-serves
// the current state as OpenF1-shaped JSON, with CORS, so the browser app can use
// it exactly like it used the OpenF1 API. Point the app's "Live data base URL"
// (in ⚙ Settings) at this server, e.g. https://your-relay.onrender.com/v1
//
// This talks to the SAME free feed F1 TV uses. No F1 account or key needed.
// It is unofficial and F1's schema can change between seasons — if a live session
// shows partial/no data, the field mapping below is where to adjust.
//
// Run:  npm install && node server.js   (Node 18+)
// Env:  PORT (Render/Fly set this automatically)
// ============================================================================

const http = require("http");
const zlib = require("zlib");
const WebSocket = require("ws");

const NEGOTIATE = "https://livetiming.formula1.com/signalr/negotiate";
const CONNECT   = "wss://livetiming.formula1.com/signalr/connect";
const HUB = JSON.stringify([{ name: "Streaming" }]);
const STATIC = "https://livetiming.formula1.com/static/";

const TOPICS = [
  "Heartbeat", "SessionInfo", "TrackStatus", "LapCount", "DriverList",
  "TimingData", "TimingAppData", "TimingStats", "WeatherData",
  "RaceControlMessages", "Position.z", "CarData.z", "TeamRadio",
];

// ---- in-memory current state, merged from the feed ----
const state = {};            // topic -> latest merged object
let connected = false, lastMsg = 0, sessionPath = "";

// deep-merge a delta into a target (F1 sends partial updates)
function merge(target, delta) {
  if (Array.isArray(delta)) return delta.slice();
  if (delta && typeof delta === "object") {
    if (!target || typeof target !== "object" || Array.isArray(target)) target = {};
    for (const k of Object.keys(delta)) target[k] = merge(target[k], delta[k]);
    return target;
  }
  return delta;
}

function inflateZ(b64) {
  try { return JSON.parse(zlib.inflateRawSync(Buffer.from(b64, "base64")).toString("utf8")); }
  catch (e) { return null; }
}

function applyFeed(topic, data) {
  if (topic.endsWith(".z")) {
    const plain = typeof data === "string" ? inflateZ(data) : data;
    if (plain != null) state[topic] = plain; // compressed topics arrive whole
    return;
  }
  state[topic] = merge(state[topic], data);
  if (topic === "SessionInfo" && state.SessionInfo && state.SessionInfo.Path) sessionPath = state.SessionInfo.Path;
}

// ---- SignalR (classic ASP.NET) connection ----
async function connect() {
  try {
    const negUrl = NEGOTIATE + "?connectionData=" + encodeURIComponent(HUB) + "&clientProtocol=1.5";
    const neg = await fetch(negUrl, { headers: { "User-Agent": "BestHTTP" } });
    const cookie = (neg.headers.get("set-cookie") || "").split(";")[0];
    const rawBody = await neg.text();
    console.log("[relay] negotiate status=" + neg.status + " len=" + rawBody.length + " cookie=" + (cookie ? "yes" : "no"));
    console.log("[relay] negotiate body(first 300): " + rawBody.slice(0, 300).replace(/\s+/g, " "));
    let body;
    try { body = JSON.parse(rawBody); }
    catch (e) { throw new Error("negotiate not JSON (status " + neg.status + ")"); }
    if (!body.ConnectionToken) throw new Error("no ConnectionToken in negotiate response");
    const token = encodeURIComponent(body.ConnectionToken);
    const wsUrl = CONNECT + "?clientProtocol=1.5&transport=webSockets&connectionToken=" + token +
      "&connectionData=" + encodeURIComponent(HUB);

    const sock = new WebSocket(wsUrl, {
      headers: { "User-Agent": "BestHTTP", "Accept-Encoding": "gzip,identity", "Cookie": cookie },
    });

    sock.on("open", () => {
      connected = true;
      console.log("[relay] connected to F1 feed, subscribing…");
      sock.send(JSON.stringify({ H: "Streaming", M: "Subscribe", A: [TOPICS], I: 1 }));
    });

    sock.on("message", (raw) => {
      lastMsg = Date.now();
      let msg; try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
      // initial full state (response to Subscribe)
      if (msg.R && typeof msg.R === "object") {
        for (const topic of Object.keys(msg.R)) applyFeed(topic, msg.R[topic]);
      }
      // incremental deltas
      if (Array.isArray(msg.M)) {
        for (const m of msg.M) {
          if (m.M === "feed" && Array.isArray(m.A) && m.A.length >= 2) applyFeed(m.A[0], m.A[1]);
        }
      }
    });

    sock.on("close", () => { connected = false; console.log("[relay] closed, reconnecting in 5s"); setTimeout(connect, 5000); });
    sock.on("error", (e) => { console.log("[relay] ws error:", e.message); try { sock.close(); } catch (_) {} });
  } catch (e) {
    connected = false;
    console.log("[relay] connect failed:", e.message, "— retrying in 10s");
    setTimeout(connect, 10000);
  }
}

// ============================================================================
// F1 -> OpenF1 translation helpers
// ============================================================================
const now = () => new Date().toISOString();
function numGap(v) {
  if (v == null) return null;
  if (typeof v === "object") v = v.Value;
  if (v == null || v === "") return null;
  if (/L/i.test(v)) return String(v).trim();          // "+1 LAP", "1L"
  const n = parseFloat(String(v).replace("+", ""));
  return isNaN(n) ? null : n;
}
function timeToSec(v) {
  if (v == null) return null;
  if (typeof v === "object") v = v.Value;
  if (!v) return null;
  const p = String(v).split(":");
  const s = p.length === 2 ? parseInt(p[0], 10) * 60 + parseFloat(p[1]) : parseFloat(p[0]);
  return isNaN(s) ? null : s;
}
const drivers = () => state.DriverList || {};

function outDrivers() {
  return Object.values(drivers()).filter(d => d && d.RacingNumber).map(d => ({
    driver_number: +d.RacingNumber, name_acronym: d.Tla || null, full_name: d.FullName || null,
    first_name: d.FirstName || null, last_name: d.LastName || null,
    team_name: d.TeamName || null, team_colour: d.TeamColour || null,
  }));
}
function outPosition() {
  const L = (state.TimingData && state.TimingData.Lines) || {};
  return Object.keys(L).filter(k => L[k] && L[k].Position != null)
    .map(k => ({ driver_number: +k, position: +L[k].Position, date: now() }));
}
function outIntervals() {
  const L = (state.TimingData && state.TimingData.Lines) || {};
  return Object.keys(L).map(k => ({
    driver_number: +k,
    gap_to_leader: numGap(L[k].GapToLeader),
    interval: numGap(L[k].IntervalToPositionAhead),
    date: now(),
  }));
}
function outLaps() {
  const L = (state.TimingData && state.TimingData.Lines) || {};
  return Object.keys(L).map(k => {
    const s = L[k].Sectors || {};
    const sp = L[k].Speeds || {};
    return {
      driver_number: +k,
      lap_number: L[k].NumberOfLaps != null ? +L[k].NumberOfLaps : null,
      lap_duration: timeToSec(L[k].LastLapTime),
      duration_sector_1: timeToSec(s[0]), duration_sector_2: timeToSec(s[1]), duration_sector_3: timeToSec(s[2]),
      i1_speed: sp.I1 ? +sp.I1.Value : null, i2_speed: sp.I2 ? +sp.I2.Value : null, st_speed: sp.ST ? +sp.ST.Value : null,
      is_pit_out_lap: !!L[k].PitOut, date_start: now(),
    };
  });
}
function outStints() {
  const L = (state.TimingAppData && state.TimingAppData.Lines) || {};
  const out = [];
  for (const k of Object.keys(L)) {
    const stints = L[k].Stints || {};
    const arr = Array.isArray(stints) ? stints : Object.values(stints);
    arr.forEach((st, i) => {
      if (!st) return;
      out.push({
        driver_number: +k, stint_number: i + 1,
        compound: (st.Compound || "").toUpperCase() || null,
        tyre_age_at_start: st.StartLaps != null ? +st.StartLaps : (st.TotalLaps != null ? +st.TotalLaps : null),
        lap_start: st.StartLaps != null ? +st.StartLaps : null, lap_end: null,
      });
    });
  }
  return out;
}
function outWeather() {
  const w = state.WeatherData || {};
  if (!Object.keys(w).length) return [];
  return [{
    date: now(),
    air_temperature: w.AirTemp != null ? +w.AirTemp : null,
    track_temperature: w.TrackTemp != null ? +w.TrackTemp : null,
    humidity: w.Humidity != null ? +w.Humidity : null,
    pressure: w.Pressure != null ? +w.Pressure : null,
    wind_speed: w.WindSpeed != null ? +w.WindSpeed : null,
    wind_direction: w.WindDirection != null ? +w.WindDirection : null,
    rainfall: w.Rainfall != null ? +w.Rainfall : 0,
  }];
}
function outRaceControl() {
  const m = (state.RaceControlMessages && state.RaceControlMessages.Messages) || {};
  const arr = Array.isArray(m) ? m : Object.values(m);
  return arr.filter(Boolean).map(x => ({
    date: x.Utc || now(), lap_number: x.Lap != null ? +x.Lap : null,
    category: x.Category || null, flag: x.Flag || null, scope: x.Scope || null,
    sector: x.Sector != null ? +x.Sector : null, message: x.Message || "",
  }));
}
function outLocation() {
  const pos = state["Position.z"];
  const entries = pos && Array.isArray(pos.Position) ? pos.Position : [];
  const last = entries[entries.length - 1];
  if (!last || !last.Entries) return [];
  return Object.keys(last.Entries).map(k => ({
    driver_number: +k, x: last.Entries[k].X, y: last.Entries[k].Y, z: last.Entries[k].Z, date: now(),
  }));
}
function outCarData(driverNum) {
  const cd = state["CarData.z"];
  const entries = cd && Array.isArray(cd.Entries) ? cd.Entries : [];
  const last = entries[entries.length - 1];
  if (!last || !last.Cars) return [];
  const map = (k) => {
    const ch = (last.Cars[k] && last.Cars[k].Channels) || {};
    return { driver_number: +k, rpm: ch["0"], speed: ch["2"], n_gear: ch["3"], throttle: ch["4"], brake: ch["5"], drs: ch["45"], date: now() };
  };
  if (driverNum) return last.Cars[driverNum] ? [map(driverNum)] : [];
  return Object.keys(last.Cars).map(map);
}
function outTeamRadio() {
  const caps = (state.TeamRadio && state.TeamRadio.Captures) || {};
  const arr = Array.isArray(caps) ? caps : Object.values(caps);
  return arr.filter(Boolean).map(c => ({
    date: c.Utc || now(), driver_number: c.RacingNumber != null ? +c.RacingNumber : null,
    recording_url: c.Path ? STATIC + sessionPath + c.Path : null,
  }));
}
function outSessions() {
  const s = state.SessionInfo;
  if (!s) return [];
  const meet = s.Meeting || {};
  return [{
    session_key: "live", meeting_key: "live",
    session_name: s.Name || null, session_type: s.Type || null,
    circuit_short_name: (meet.Circuit && meet.Circuit.ShortName) || meet.Name || null,
    location: (meet.Circuit && meet.Circuit.ShortName) || null,
    country_name: (meet.Country && meet.Country.Name) || null,
    date_start: s.StartDate || null, date_end: s.EndDate || null,
    gmt_offset: s.GmtOffset || null, year: s.StartDate ? +String(s.StartDate).slice(0, 4) : new Date().getFullYear(),
  }];
}

// ============================================================================
// HTTP server — OpenF1-shaped endpoints under /v1
// ============================================================================
function route(pathname, query) {
  const dn = query.get("driver_number");
  switch (pathname) {
    case "/v1/sessions": return outSessions();
    case "/v1/drivers": return outDrivers();
    case "/v1/position": return outPosition();
    case "/v1/intervals": return outIntervals();
    case "/v1/laps": return dn ? outLaps().filter(l => l.driver_number === +dn) : outLaps();
    case "/v1/stints": return outStints();
    case "/v1/weather": return outWeather();
    case "/v1/race_control": return outRaceControl();
    case "/v1/location": return outLocation();
    case "/v1/car_data": return outCarData(dn);
    case "/v1/team_radio": return outTeamRadio();
    case "/v1/pit": return []; // not cleanly available from the live feed yet
    default: return null;
  }
}

const server = http.createServer((req, res) => {
  const u = new URL(req.url, "http://x");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Content-Type", "application/json");
  if (req.method === "OPTIONS") { res.writeHead(204); return res.end(); }

  if (u.pathname === "/" || u.pathname === "/health") {
    return res.end(JSON.stringify({
      ok: true, connected, age_ms: lastMsg ? Date.now() - lastMsg : null,
      session: (state.SessionInfo && state.SessionInfo.Name) || null,
      topics: Object.keys(state),
    }));
  }
  if (u.pathname === "/state") return res.end(JSON.stringify(state)); // raw, for debugging

  const out = route(u.pathname, u.searchParams);
  if (out === null) { res.writeHead(404); return res.end(JSON.stringify({ error: "not found" })); }
  res.end(JSON.stringify(out));
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log("[relay] http listening on " + PORT));
connect();
