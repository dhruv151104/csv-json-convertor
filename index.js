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


// Insert CSV into PostgreSQL

async function insertCSVtoDB() {
  const csvFile = path.resolve(CSV_PATH);
  const content = fs.readFileSync(csvFile, "utf-8");
  const jsonData = parseCSV(content);

  for (let i = 0; i < jsonData.length; i += BATCH_SIZE) {
    const batch = jsonData.slice(i, i + BATCH_SIZE);

    const queries = batch.map(user => {
      const name = `${user.name.firstName} ${user.name.lastName}`;
      const age = parseInt(user.age);

      // Extract address fields
      const { line1, line2, city, state } = user.address || {};
      const address = { line1, line2, city, state };

      // Put remaining fields into additional_info
      const additional_info = { ...user };
      delete additional_info.name;
      delete additional_info.age;
      delete additional_info.address;

      return pool.query(
        "INSERT INTO users(name, age, address, additional_info) VALUES($1,$2,$3,$4)",
        [name, age, address, additional_info]
      );
    });

    await Promise.all(queries);
  }

  console.log(`✅ Inserted ${jsonData.length} records into PostgreSQL`);
}


// Endpoint to trigger CSV import

app.get("/import", async (req, res) => {
  try {
    await insertCSVtoDB();
    await printAgeDistribution(); 
    res.send("CSV imported successfully!");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error importing CSV");
  }
});


// ---------------------------
// Age Distribution Function
// ---------------------------
async function printAgeDistribution() {
  try {
    const { rows } = await pool.query("SELECT age FROM users");

    const total = rows.length;
    if (total === 0) {
      console.log("No users in database to calculate age distribution.");
      return;
    }

    // Initialize counters
    const distribution = {
      "<20": 0,
      "20-40": 0,
      "40-60": 0,
      ">60": 0,
    };

    // Count users in each age group
    rows.forEach(({ age }) => {
      if (age < 20) distribution["<20"]++;
      else if (age <= 40) distribution["20-40"]++;
      else if (age <= 60) distribution["40-60"]++;
      else distribution[">60"]++;
    });

    // Print percentages
    console.log("\nAge-Group % Distribution");
    for (const group in distribution) {
      const percent = ((distribution[group] / total) * 100).toFixed(0);
      console.log(`${group}  ${percent}`);
    }
    console.log("");
  } catch (err) {
    console.error("Error calculating age distribution:", err.message);
  }
}


app.listen(PORT, () => {
  console.log(`🚀 Server started on port ${PORT}`);
});
