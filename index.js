// index.js
const express = require('express');
require('dotenv').config();
const pool = require('./db');

const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
  res.send('CSV to JSON Converter API running...');
});

app.listen(PORT, () => {
  console.log(`🚀 Server started on port ${PORT}`);
});
