// server.js
import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

// --- CORS: allow your exact env URL + Replit dev domains ---
const allowList = [];
if (process.env.ALLOWED_ORIGIN) allowList.push(process.env.ALLOWED_ORIGIN);
const domainRules = [/\.worf\.replit\.dev$/, /\.repl\.co$/];

app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);               // allow curl/Postman
    try {
      const host = new URL(origin).host;
      const ok =
        allowList.includes(origin) ||
        domainRules.some(rx => rx.test(host));
      return ok ? cb(null, true) : cb(new Error(`CORS blocked: ${origin}`));
    } catch {
      return cb(new Error("Bad Origin header"));
    }
  },
}));
app.options("*", cors());                              // preflight

// Debug + health
app.get("/health", (_req, res) => res.json({ ok: true }));
app.get("/health-origin", (req, res) =>
  res.json({ ok: true, origin: req.headers.origin || null })
);

// ---- ENV ----
const HOST = (process.env.DATABRICKS_HOST || "").replace(/\/$/, "");
const TOKEN = process.env.DATABRICKS_TOKEN;
const WAREHOUSE_ID = process.env.DATABRICKS_WAREHOUSE_ID;
if (!HOST || !TOKEN || !WAREHOUSE_ID) {
  console.warn("Missing one or more env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID");
}

// Helper to call Databricks REST
async function dbx(path, init = {}) {
  const url = `${HOST}${path}`;
  const headers = {
    "Authorization": `Bearer ${TOKEN}`,
    "Content-Type": "application/json",
    ...(init.headers || {})
  };
  const resp = await fetch(url, { ...init, headers });
  if (!resp.ok) throw new Error(`Databricks ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

// Poll until statement finishes (or timeout)
async function waitForStatement(id, timeoutMs = 30000, pollMs = 600) {
  const t0 = Date.now();
  while (true) {
    const s = await dbx(`/api/2.0/sql/statements/${id}`);
    const st = s?.status?.state;
    if (st === "SUCCEEDED" || st === "FAILED" || st === "CANCELED") return s;
    if (Date.now() - t0 > timeoutMs) return s;
    await new Promise(r => setTimeout(r, pollMs));
  }
}

// Query endpoint
app.post("/query", async (req, res) => {
  const { sql } = req.body || {};
  if (!sql) return res.status(400).json({ error: "Missing SQL" });

  try {
    // Submit & try to inline results
    const submitted = await dbx(`/api/2.0/sql/statements`, {
      method: "POST",
      body: JSON.stringify({
        statement: sql,
        warehouse_id: WAREHOUSE_ID,
        wait_timeout: "20s"
      })
    });

    if (submitted?.result?.data_array || submitted?.status?.state === "SUCCEEDED") {
      return res.json(submitted);
    }

    // Otherwise poll
    const st = await waitForStatement(submitted.statement_id);

    // If succeeded but no inline data, fetch chunk 0
    if (st?.status?.state === "SUCCEEDED" && !st?.result?.data_array) {
      const chunk0 = await dbx(`/api/2.0/sql/statements/${submitted.statement_id}/result/chunks?chunk_index=0`);
      st.result = { ...(st.result || {}), data_array: chunk0?.data_array || [], chunk_index: 0 };
    }
    return res.json(st);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err.message || err) });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Render backend running on ${port}`));
