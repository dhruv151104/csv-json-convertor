const fs = require("fs");
const path = require("path");
const express = require("express");
const { Pool } = require("pg");
require("dotenv").config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const CSV_PATH = process.env.CSV_PATH;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Test DB connection
pool.connect()
  .then(client => {
    console.log("✅ Connected to PostgreSQL successfully");
    client.release();
  })
  .catch(err => {
    console.error("❌ Database connection failed:", err.message);
  });


// CSV Parsing Function

function parseCSV(content) {
  const lines = content.split("\n").filter(Boolean);
  const headers = lines[0].split(",").map(h => h.trim());
  const rows = lines.slice(1);

  return rows.map(line => {
    const values = line.split(",").map(v => v.trim());
    const obj = {};

    headers.forEach((header, idx) => {
      const keys = header.split(".");
      let current = obj;
      keys.forEach((key, i) => {
        if (i === keys.length - 1) {
          current[key] = values[idx];
        } else {
          current[key] = current[key] || {};
          current = current[key];
        }
      });
    });

    return obj;
  });
}




app.listen(PORT, () => {
  console.log(`🚀 Server started on port ${PORT}`);
});
