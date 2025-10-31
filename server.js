import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

// CORS: allow your Replit origin
app.use(cors({ origin: process.env.ALLOWED_ORIGIN }));

// ---- ENV sanity ----
const HOST = (process.env.DATABRICKS_HOST || "").replace(/\/$/, ""); // no trailing slash
const TOKEN = process.env.DATABRICKS_TOKEN;
const WAREHOUSE_ID = process.env.DATABRICKS_WAREHOUSE_ID;

if (!HOST || !TOKEN || !WAREHOUSE_ID) {
  console.warn("Missing one or more env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID");
}

app.get("/health", (_req, res) => res.json({ ok: true }));

// Helper to call Databricks REST
async function dbx(path, init = {}) {
  const url = `${HOST}${path}`;
  const headers = {
    "Authorization": `Bearer ${TOKEN}`,
    "Content-Type": "application/json",
    ...(init.headers || {})
  };
  const resp = await fetch(url, { ...init, headers });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Databricks ${resp.status}: ${txt}`);
  }
  return resp.json();
}

// Poll until statement finishes (or timeout)
async function waitForStatement(statementId, timeoutMs = 30000, pollMs = 600) {
  const t0 = Date.now();
  while (true) {
    const s = await dbx(`/api/2.0/sql/statements/${statementId}`);
    const state = s?.status?.state;
    if (state === "SUCCEEDED" || state === "FAILED" || state === "CANCELED") return s;
    if (Date.now() - t0 > timeoutMs) return s; // return whatever we have
    await new Promise(r => setTimeout(r, pollMs));
  }
}

app.post("/query", async (req, res) => {
  const { sql } = req.body || {};
  if (!sql) return res.status(400).json({ error: "Missing SQL" });

  try {
    // 1) Submit statement (ask DBX to wait a bit so small queries come back inline)
    const submitted = await dbx(`/api/2.0/sql/statements`, {
      method: "POST",
      body: JSON.stringify({
        statement: sql,
        warehouse_id: WAREHOUSE_ID,
        wait_timeout: "20s" // try to inline results for quick queries
      })
    });

    // If results already present, return immediately
    if (submitted?.result?.data_array || submitted?.status?.state === "SUCCEEDED") {
      return res.json(submitted);
    }

    // 2) Otherwise poll until done
    const st = await waitForStatement(submitted.statement_id);

    // If succeeded but no inline data, fetch first chunk (most small queries fit in 1 chunk)
    if (st?.status?.state === "SUCCEEDED" && !st?.result?.data_array) {
      const chunk0 = await dbx(
        `/api/2.0/sql/statements/${submitted.statement_id}/result/chunks?chunk_index=0`
      );
      // merge chunk data into the original shape
      st.result = {
        ...(st.result || {}),
        data_array: chunk0?.data_array || [],
        chunk_index: 0
      };
    }

    return res.json(st);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err.message || err) });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Render backend running on ${port}`));
