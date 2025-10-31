import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();
app.use(express.json());
app.use(cors({ origin: process.env.ALLOWED_ORIGIN }));

app.get("/health", (req, res) => res.json({ ok: true }));

app.post("/query", async (req, res) => {
  const { sql } = req.body;
  if (!sql) return res.status(400).json({ error: "Missing SQL" });

  try {
    const resp = await fetch(
      `${process.env.DATABRICKS_HOST}/api/2.0/sql/statements/execute`,
      {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${process.env.DATABRICKS_TOKEN}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          warehouse_id: process.env.DATABRICKS_WAREHOUSE_ID,
          statement: sql
        })
      }
    );
    const data = await resp.json();
    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Render backend running on ${port}`));
