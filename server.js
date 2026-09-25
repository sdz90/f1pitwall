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

const NEGOTIATE = "https://livetiming.formula1.com/signalrcore/negotiate?negotiateVersion=1";
const WSBASE    = "wss://livetiming.formula1.com/signalrcore";
const RS = "\x1e"; // SignalR Core record separator
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";
const STATIC = "https://livetiming.formula1.com/static/";
const OF1_HIST = "https://api.openf1.org/v1";  // free historical data, used to build the circuit outline

// ---- circuit outline (built once from OpenF1's free historical position data) ----
let outline = null, outlineSource = "not started", outlineTried = false;
async function buildOutline(circuit, year) {
  try {
    outlineSource = "building for " + circuit + "…";
    let sess = null;
    for (const y of [year, year - 1, year - 2]) {
      const ss = await fetch(`${OF1_HIST}/sessions?circuit_short_name=${encodeURIComponent(circuit)}&year=${y}&session_type=Race`).then(r => r.json()).catch(() => []);
      if (ss && ss.length) { sess = ss[ss.length - 1]; break; }
    }
    if (!sess) { outlineSource = "no past race session found for " + circuit; return; }
    const laps = await fetch(`${OF1_HIST}/laps?session_key=${sess.session_key}&driver_number=1`).then(r => r.json()).catch(() => []);
    const lap = laps.find(l => l.lap_number >= 8 && l.lap_duration && !l.is_pit_out_lap) || laps.find(l => l.lap_duration);
    if (!lap || !lap.date_start) { outlineSource = "no clean lap in " + sess.session_key; return; }
    const t0 = new Date(lap.date_start).toISOString();
    const t1 = new Date(new Date(lap.date_start).getTime() + (lap.lap_duration + 2) * 1000).toISOString();
    const loc = await fetch(`${OF1_HIST}/location?session_key=${sess.session_key}&driver_number=1&date>=${t0}&date<=${t1}`).then(r => r.json()).catch(() => []);
    const pts = loc.filter(p => (p.x || p.y)).map(p => ({ x: p.x, y: p.y }));
    if (pts.length > 30) { outline = pts; outlineSource = "openf1 session " + sess.session_key + " (" + pts.length + " pts)"; }
    else outlineSource = "too few location points (" + pts.length + ")";
  } catch (e) { outlineSource = "error: " + (e && e.message || e); }
}

// ---- mini-sector progress → fraction of the lap completed (0..1) ----
function trackFraction(line) {
  const sectors = line && line.Sectors;
  if (!sectors) return null;
  const arr = Array.isArray(sectors) ? sectors : Object.keys(sectors).sort((a, b) => +a - +b).map(k => sectors[k]);
  let total = 0, done = 0;
  for (const s of arr) {
    if (!s || !s.Segments) continue;
    const segs = Array.isArray(s.Segments) ? s.Segments : Object.keys(s.Segments).sort((a, b) => +a - +b).map(k => s.Segments[k]);
    for (const seg of segs) { total++; if (seg && seg.Status) done++; }
  }
  if (!total) return null;
  return { fraction: Math.max(0, Math.min(1, done / total)), done, total };
}
function outTrackPositions() {
  if (!lastMsg || (Date.now() - lastMsg) > 180000) return []; // feed quiet = session over, no live cars
  const L = (state.TimingData && state.TimingData.Lines) || {};
  const out = [];
  for (const k of Object.keys(L)) {
    const f = trackFraction(L[k]);
    if (f) out.push({ driver_number: +k, fraction: f.fraction, done: f.done, total: f.total });
  }
  return out;
}

const TOPICS = [
  "Heartbeat", "SessionInfo", "TrackStatus", "LapCount", "DriverList",
  "TimingData", "TimingAppData", "TimingStats", "WeatherData",
  "RaceControlMessages", "Position.z", "CarData.z", "TeamRadio",
  "ExtrapolatedClock", "SessionData",
];

// ---- in-memory current state, merged from the feed ----
const state = {};            // topic -> latest merged object
let connected = false, lastMsg = 0, sessionPath = "";
const seenTopics = {};   // every topic name F1 actually sends us, with a count
let bytesTotal = 0, maxMsg = 0, msgCount = 0;

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

const zSeen = {};   // topic -> {count, ok} to diagnose compressed streams
function inflateZ(b64, topic) {
  const raw = Buffer.from(b64, "base64");
  const tries = [
    () => zlib.inflateRawSync(raw),  // raw DEFLATE (F1's usual)
    () => zlib.inflateSync(raw),     // zlib-wrapped
    () => zlib.gunzipSync(raw),      // gzip, just in case
  ];
  for (const t of tries) {
    try {
      const out = JSON.parse(t().toString("utf8"));
      if (topic) zSeen[topic] = { count: ((zSeen[topic] && zSeen[topic].count) || 0) + 1, ok: true };
      return out;
    } catch (_) { /* next method */ }
  }
  if (topic) { const p = zSeen[topic] || { count: 0 }; zSeen[topic] = { count: p.count + 1, ok: false }; }
  return null;
}

const lapHist = {};          // driver_number -> [ {lap_number, lap_duration, sectors, speeds, date_start} ]
const lastLapVal = {};       // driver_number -> last seen LastLapTime value (to detect a completed lap)

function recordLaps() {
  const L = (state.TimingData && state.TimingData.Lines) || {};
  for (const k of Object.keys(L)) {
    const line = L[k];
    const val = line.LastLapTime && line.LastLapTime.Value;
    if (!val || val === lastLapVal[k]) continue;   // no new completed lap
    lastLapVal[k] = val;
    const s = line.Sectors || {};
    const sp = line.Speeds || {};
    const entry = {
      lap_number: line.NumberOfLaps != null ? +line.NumberOfLaps : null,
      lap_duration: timeToSec(val),
      duration_sector_1: timeToSec(s[0]), duration_sector_2: timeToSec(s[1]), duration_sector_3: timeToSec(s[2]),
      i1_speed: sp.I1 ? +sp.I1.Value : null, i2_speed: sp.I2 ? +sp.I2.Value : null, st_speed: sp.ST ? +sp.ST.Value : null,
      date_start: new Date().toISOString(),
    };
    (lapHist[k] = lapHist[k] || []).push(entry);
    if (lapHist[k].length > 120) lapHist[k].shift();
  }
}

function applyFeed(topic, data) {
  // diagnostics: record that F1 actually sent this topic (and whether it's a string/obj)
  if (!seenTopics[topic]) { console.log("[relay] first topic seen: " + topic + " (" + typeof data + ")"); }
  seenTopics[topic] = (seenTopics[topic] || 0) + 1;
  if (topic.endsWith(".z")) {
    const plain = typeof data === "string" ? inflateZ(data, topic) : data;
    if (plain != null) state[topic] = plain; // compressed topics arrive whole
    return;
  }
  state[topic] = merge(state[topic], data);
  if (topic === "SessionInfo" && state.SessionInfo) {
    if (state.SessionInfo.Path) sessionPath = state.SessionInfo.Path;
    const circ = state.SessionInfo.Meeting && state.SessionInfo.Meeting.Circuit && state.SessionInfo.Meeting.Circuit.ShortName;
    if (circ && !outlineTried) { outlineTried = true; buildOutline(circ, +String(state.SessionInfo.StartDate || "").slice(0, 4) || new Date().getFullYear()); }
  }
  if (topic === "TimingData") recordLaps();
}

// ---- SignalR Core connection (F1's 2026 endpoint) ----
function handleSC(m) {
  if (m.type === 6 || m.type === 7) return;               // ping / close — ignore
  if (m.type === 3) {                                     // Completion of our Subscribe = initial snapshot
    if (m.result && typeof m.result === "object") {
      const keys = Object.keys(m.result);
      for (const topic of keys) applyFeed(topic, m.result[topic]);
      console.log("[relay] initial snapshot: " + keys.length + " topics");
    }
    return;
  }
  if (m.type === 1 && m.target === "feed" && Array.isArray(m.arguments) && m.arguments.length >= 2) {
    applyFeed(m.arguments[0], m.arguments[1]);             // live delta
  }
}

async function connect() {
  try {
    const neg = await fetch(NEGOTIATE, { method: "POST", headers: { "User-Agent": UA, "Content-Length": "0" } });
    const cookie = (neg.headers.get("set-cookie") || "").split(";")[0];
    const rawBody = await neg.text();
    console.log("[relay] negotiate status=" + neg.status + " len=" + rawBody.length + " cookie=" + (cookie ? "yes" : "no"));
    console.log("[relay] negotiate body(first 300): " + rawBody.slice(0, 300).replace(/\s+/g, " "));
    let info;
    try { info = JSON.parse(rawBody); } catch (e) { throw new Error("negotiate not JSON (status " + neg.status + ")"); }
    const token = info.connectionToken || info.ConnectionToken;
    if (!token) throw new Error("no connectionToken in negotiate (status " + neg.status + ")");

    const wsUrl = WSBASE + "?id=" + encodeURIComponent(token);
    const sock = new WebSocket(wsUrl, { headers: { "User-Agent": UA, "Cookie": cookie, "Accept-Encoding": "gzip,identity" } });

    let buf = "", handshakeDone = false, pingTimer = null;
    const sendSC = (obj) => sock.send(JSON.stringify(obj) + RS);

    sock.on("open", () => {
      console.log("[relay] ws open, handshaking…");
      sock.send(JSON.stringify({ protocol: "json", version: 1 }) + RS);
    });

    sock.on("message", (raw) => {
      lastMsg = Date.now();
      const len = raw.length; bytesTotal += len; msgCount++; if (len > maxMsg) maxMsg = len;
      buf += raw.toString();
      let idx;
      while ((idx = buf.indexOf(RS)) >= 0) {
        const chunk = buf.slice(0, idx); buf = buf.slice(idx + 1);
        if (!chunk) continue;
        let m; try { m = JSON.parse(chunk); } catch (_) { continue; }
        if (!handshakeDone) {
          if (m.error) { console.log("[relay] handshake error: " + m.error); try { sock.close(); } catch (_) {} return; }
          handshakeDone = true; connected = true;
          console.log("[relay] handshake ok, subscribing…");
          sendSC({ type: 1, invocationId: "0", target: "Subscribe", arguments: [TOPICS] });
          pingTimer = setInterval(() => { try { sock.send(JSON.stringify({ type: 6 }) + RS); } catch (_) {} }, 10000);
          continue;
        }
        handleSC(m);
      }
    });

    sock.on("close", (code) => {
      connected = false; if (pingTimer) clearInterval(pingTimer);
      console.log("[relay] closed (code " + code + "), reconnecting in 5s");
      setTimeout(connect, 5000);
    });
    sock.on("error", (e) => { console.log("[relay] ws error: " + e.message); try { sock.close(); } catch (_) {} });
  } catch (e) {
    connected = false;
    console.log("[relay] connect failed: " + e.message + " — retrying in 10s");
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
  // full accumulated history: every completed lap for every driver, with sectors
  const out = [];
  for (const k of Object.keys(lapHist)) {
    for (const lap of lapHist[k]) out.push(Object.assign({ driver_number: +k }, lap));
  }
  return out;
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
function isoWithOffset(d, gmt) {
  if (!d) return null;
  if (/[zZ]$|[+-]\d\d:?\d\d$/.test(d)) return d;        // already has a timezone
  if (!gmt) return d + "Z";                              // no offset given → treat as UTC
  let sign = "+", g = String(gmt);
  if (g[0] === "-") { sign = "-"; g = g.slice(1); } else if (g[0] === "+") g = g.slice(1);
  return d + sign + g.slice(0, 5);                       // e.g. "…T13:30:00" + "+04:00"
}
function outSessionClock() {
  const ec = state.ExtrapolatedClock || {};
  const L = (state.TimingData && state.TimingData.Lines) || {};
  const knocked = Object.values(L).filter(l => l && l.KnockedOut).length;
  const name = (state.SessionInfo && state.SessionInfo.Name) || "";
  let part = null;
  if (/quali/i.test(name)) part = knocked < 5 ? "Q1" : knocked < 10 ? "Q2" : "Q3";
  return {
    remaining: ec.Remaining || null,      // "HH:MM:SS" left in the current segment
    utc: ec.Utc || null,                  // timestamp the Remaining was accurate at
    extrapolating: ec.Extrapolating === true, // is the clock currently running?
    knocked_out: knocked,
    part,                                 // Q1 / Q2 / Q3 (qualifying only)
    session_name: name,
  };
}

function outSessions() {
  const s = state.SessionInfo;
  if (!s) return [];
  const meet = s.Meeting || {};
  // If F1 gave an EndDate use it; otherwise, if the feed has gone quiet for >3 min,
  // the session is effectively over — report the last-message time as the end so the
  // app flips from LIVE to FINAL instead of hanging on "live" forever.
  const stale = lastMsg && (Date.now() - lastMsg) > 180000;
  const end = s.EndDate || (stale ? new Date(lastMsg).toISOString() : null);
  return [{
    session_key: "live", meeting_key: "live",
    session_name: s.Name || null, session_type: s.Type || null,
    circuit_short_name: (meet.Circuit && meet.Circuit.ShortName) || meet.Name || null,
    location: (meet.Circuit && meet.Circuit.ShortName) || null,
    country_name: (meet.Country && meet.Country.Name) || null,
    date_start: isoWithOffset(s.StartDate, s.GmtOffset), date_end: isoWithOffset(end, s.GmtOffset),
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
    case "/v1/track_positions": return outTrackPositions();
    case "/v1/track_outline": return { points: outline || [], source: outlineSource };
    case "/v1/session_clock": return outSessionClock();
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
      raw_topics: seenTopics,                             // every topic F1 actually sent, with counts
      compressed: zSeen,                                  // Position.z / CarData.z receipt + decompress status
      msgs: msgCount, bytes: bytesTotal, max_msg: maxMsg, // feed volume (heavy streams show up as big bytes)
      laps_recorded: Object.keys(lapHist).reduce((n, k) => n + lapHist[k].length, 0),
      outline_points: outline ? outline.length : 0, outline_source: outlineSource,
      track_positions_sample: outTrackPositions().slice(0, 3),
      clock: outSessionClock(),
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
