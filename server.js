import express from "express";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer, WebSocket } from "ws";

const WORKER = process.env.WORKER_URL;
if (!WORKER) throw new Error("WORKER_URL 未設定");

const root = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const SKIP_REQ = new Set(["host", "connection", "content-length", "accept-encoding", "transfer-encoding"]);
const SKIP_RES = new Set(["content-encoding", "content-length", "transfer-encoding", "connection"]);

app.get("/sw.js", (req, res) => {
  res.set("service-worker-allowed", "/");
  res.set("cache-control", "no-cache");
  res.type("application/javascript");
  res.sendFile(path.join(root, "public/sw.js"));
});

app.use(express.static(path.join(root, "public"), { index: "index.html" }));

app.all(["/api/*", "/svc/*"], async (req, res) => {
  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) if (!SKIP_REQ.has(k)) headers[k] = v;
  const hasBody = !["GET", "HEAD"].includes(req.method);
  let r;
  try {
    r = await fetch(WORKER + req.originalUrl, {
      method: req.method, headers, body: hasBody ? req : undefined,
      duplex: "half", redirect: "manual"
    });
  } catch (e) {
    res.status(502).send("relay: " + e.message);
    return;
  }
  res.status(r.status);
  for (const [k, v] of r.headers) if (!SKIP_RES.has(k) && k !== "set-cookie") res.setHeader(k, v);
  const sc = r.headers.getSetCookie?.() ?? [];
  if (sc.length) res.setHeader("set-cookie", sc);
  if (r.body) for await (const chunk of r.body) res.write(chunk);
  res.end();
});

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  if (!req.url.startsWith("/ws")) return socket.destroy();
  wss.handleUpgrade(req, socket, head, (client) => {
    const up = new WebSocket(WORKER.replace(/^http/, "ws") + "/api/ws");
    up.binaryType = "arraybuffer";
    const q = [];
    let open = false;
    client.on("message", (d) => (open ? up.send(d, { binary: true }) : q.push(d)));
    up.on("open", () => { open = true; for (const d of q) up.send(d, { binary: true }); });
    up.on("message", (d) => { try { client.send(d, { binary: true }); } catch {} });
    up.on("close", () => client.close());
    up.on("error", () => client.close());
    client.on("close", () => up.close());
    client.on("error", () => up.close());
  });
});

server.listen(process.env.PORT || 3000);
