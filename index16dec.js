
// ----------------------
// Deye Cloud Backend Server
// ----------------------

import express from "express";
import axios from "axios";
import dotenv from "dotenv";
import crypto from "crypto";
import cron from "node-cron";
import pkg from 'winston';
import pLimit from "p-limit";
import { Sequelize, DataTypes } from "sequelize";
import { fileURLToPath } from "url";
import { dirname } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config();

const app = express();
app.use(express.json());

const { createLogger , format , transports} = pkg;

const PORT = process.env.PORT || 5001;

// Configuration from .env
const CONFIG = {
  CRON_SCHEDULE: process.env.CRON_SCHEDULE || '0 0 * * *',
  LOG_LEVEL: process.env.LOG_LEVEL || 'info',
  LOG_DIR: process.env.LOG_DIR || './logs',
  API_TIMEOUT: parseInt(process.env.API_TIMEOUT) || 30000,
  CONCURRENCY_LIMIT: parseInt(process.env.CONCURRENCY_LIMIT) || 5,
  RETRY_ATTEMPTS: parseInt(process.env.RETRY_ATTEMPTS) || 3,
  RETRY_DELAY: parseInt(process.env.RETRY_DELAY) || 1000,
};

// Set up Winston logger (consolidated; removed Pino and fs logging)
const logger = createLogger({
  level: CONFIG.LOG_LEVEL || 'info',
  // Base format for all outputs
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.errors({ stack: true }),
    format.splat(), // Vital: This handles the multiple arguments in logger.info()
    format.ms()     // Adds the (+ms) duration between log lines
  ),
  transports: [
    // 1. Console: CLEAN & READABLE (No JSON noise)
    new transports.Console({
      format: format.combine(
        format.colorize({ all: true }),
        format.printf(({ timestamp, level, message, ms, ...meta }) => {
          // Extract metadata if any (like deviceId or status)
          const metaData = Object.keys(meta).length ? JSON.stringify(meta) : '';
          return `${timestamp} [${level}] ${message} ${metaData} (${ms})`;
        })
      )
    }),
    // 2. File: STRUCTURED JSON (For debugging/analysis)
    new transports.File({ 
      filename: `${CONFIG.LOG_DIR}/daily-fetch.log`,
      format: format.json() 
    }),
  ],
});


// Ensure log directory exists
import fs, { stat } from "fs";
import path from "path";
if (!fs.existsSync(CONFIG.LOG_DIR)) {
  fs.mkdirSync(CONFIG.LOG_DIR, { recursive: true });
}

// ----------------------
// Database Config -> using Sequelize ORM
// ----------------------
const sequelize = new Sequelize(
  process.env.DB_NAME,
  process.env.DB_USER,
  process.env.DB_PASSWORD,
  {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    dialect: 'postgres',
    logging: false, // Disable SQL logs in production
    pool: { max: 5, min: 0, acquire: 30000, idle: 10000 }, // Connection pooling
  }
);
// Station Model
const Station = sequelize.define('Station', {
    index_id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
  },station_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
  },station_name: {
    type: DataTypes.STRING,
  },
});


// +-------------------+                                                                                          
// |     Device        |
// +-------------------+
// | - device_id (PK)  |
// | - device_sn       |
// | - device_type     |
// | - createdAt       |
// | - updatedAt       |
// +-------------------+                                                                                            

// Devices Model 
const Device = sequelize.define('Device', {
  device_id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  device_sn: {
    type: DataTypes.STRING,
    allowNull: false,
    unique: true,
  },
  device_type: {
    type: DataTypes.STRING,
    allowNull: false,
  },
}, {
  tableName: 'devices',
  timestamps: true,  // Maps to createdAt, updatedAt
});

// Sync DB (updated to only sync Device model)
async function syncDB() {
  try {
    await sequelize.authenticate();
    await sequelize.sync({ alter: true }); // Use { force: true } for dev resets; switch to migrations in prod
    logger.info('Database synced successfully.');
  } catch (err) {
    logger.error('Database sync error:', err);
    throw err; // Throw to prevent server start on DB failure
  }
}

// ----------------------
// Deye Cloud Config
// ----------------------
const {
  DEYE_BASE_URL,
  DEYE_APP_ID,
  DEYE_APP_SECRET,
  DEYE_EMAIL,
  DEYE_PASSWORD
} = process.env;

let accessToken = null;
let tokenExpiry = 0;

// Obtain Access Token
async function obtainToken() {
  try {
    const hashedPassword = crypto
      .createHash("sha256")
      .update(DEYE_PASSWORD)
      .digest("hex");

    const url = `${DEYE_BASE_URL}/account/token?appId=${DEYE_APP_ID}`;
    const response = await axios.post(
      url,
      {
        appSecret: DEYE_APP_SECRET,
        email: DEYE_EMAIL,
        password: hashedPassword,
        companyId: "0", // for personal accounts
      },
      { headers: { "Content-Type": "application/json" } }
    );

    if (response.data?.accessToken) {
      accessToken = response.data.accessToken;
      tokenExpiry = Date.now() + (response.data.expiresIn || 3600) * 1000;
      logger.info("Token obtained successfully");
    } else {
      logger.error("Failed to get token:", response.data);
      throw new Error("Token fetch failed");
    }
  } catch (err) {
    logger.error("Token fetch error:", err.response?.data || err.message);
    throw err;
  }
}

// Ensure valid token (with refresh logic)
async function ensureToken() {
  if (!accessToken || Date.now() > tokenExpiry - 60000) { // Refresh 1 min early
    await obtainToken();
  }
}

// Helper POST request (updated for timeout)
async function deyePost(endpoint, payload = {}, options = {}) {
  await ensureToken();

  const url = `${DEYE_BASE_URL}${endpoint}`;
  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${accessToken}`
  };

   logger.info(`Making request to: ${url}`);
  // Avoid logging full payload/token for security

  try {
    const response = await axios.post(url, payload, {
      headers,
      timeout: options.timeout || 10000
    });

    logger.info("API Response status:", response.status);
    return response.data;
  } catch (err) {
    logger.error(`Deye API Error [${endpoint}]:`, err.response?.data || err.message);
    throw err;
  }
}

// ----------------------
// API Routes
// ----------------------

// Health Check
app.get("/statusCheck", (req, res) => {
  logger.info('/statusCheck called');
  setTimeout(() => {
    res.json({ message: "Hello", status: "OK" });
  }, 3000);
});

// Get all stations
app.get("/api/stations", async (req, res) => {
  logger.info('/api/stations called');
  try {
    const data = await deyePost("/station/list", {});
    res.json({ success: true, data: data.data || data.stationList || [] });
  } catch (err) {
    logger.error("Stations API error:", err);
    res.status(500).json({
      success: false,
      error: err.response?.data || err.message,
    });
  }
});

// Get Station with device -> INVERTER
app.get("/api/station_device", async (req, res) => {
  logger.info('/api/station_device -> is called');
  // This is the payload for the API request
  const payload = {
    "page": 1,
    "size": 10,
    "deviceType": "INVERTER"
  };

  try {
    // 1. Added await
    // 2. Ensure deyePost is an async function returning the response
    const response = await deyePost("/station/listWithDevice", payload, { 
      timeout: CONFIG.API_TIMEOUT 
    });

    res.status(200).json({
      success: true,
      // 3. Use response.data if using Axios, or just response if it's pre-parsed
      data: response.data || response 
    });
  } catch (err) {
    logger.error("Stations API error:", err);
    res.status(500).json({
      success: false,
      // Improved error extraction
      error: err.response?.data || err.message,
    });
  }
});

  // Add this route to your app for fetching measurements
app.get("/api/measurements/:deviceSn", async (req, res) => {
  const { deviceSn } = req.params;
  const tableName = `measurements_sn_${deviceSn}`;
  try {
    const [results] = await sequelize.query(`SELECT * FROM ${tableName} ORDER BY timestamp DESC LIMIT 100;`);
    res.json({ success: true, data: results });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});
  
// Devices array
const devices = [
  { deviceSn: "2306178462", measurePoints: ["BatteryVoltage"] },
  { deviceSn: "2401276187", measurePoints: ["BatteryVoltage"] },
];

// Utility: Calculate timestamps
function getTimestamps() {
  const now = new Date();
  const endTimestamp = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);
  const startTimestamp = endTimestamp - (24 * 60 * 60);
  return { startTimestamp, endTimestamp };
}

// Fetch and store data for a single device with retries
async function fetchDeviceData(device) {
  const { deviceSn, measurePoints } = device;
  const { startTimestamp, endTimestamp } = getTimestamps();
  const payload = { deviceSn, startTimestamp, endTimestamp, measurePoints };

  let attempts = 0;
  // Retry loop -> with the max limit -> 3
  while (attempts < CONFIG.RETRY_ATTEMPTS) {
    try {
      logger.info(`Fetching data for device: ${deviceSn}`);
      const response = await deyePost("/device/historyRaw", payload, { timeout: CONFIG.API_TIMEOUT });

      // Validate response
      if (!response || response.code !== "1000000" || !response.dataList) {
        throw new Error(`Invalid API response: ${JSON.stringify(response)}`);
      }

      // Upsert Device (unchanged)
      const [dbDevice, created] = await Device.upsert({
        device_sn: response.deviceSn,
        device_type: response.deviceType,
      });
      logger.info(`Device ${deviceSn} ${created ? 'created' : 'updated'} in DB`);

      // Create measurement table if it doesn't exist (dynamic per device_sn)
      const tableName = `measurements_sn_${deviceSn}`;
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${tableName} (
          measurement_id SERIAL PRIMARY KEY,
          timestamp BIGINT NOT NULL,  -- Store as Unix seconds (raw integer)
          key VARCHAR(255) NOT NULL,
          value DECIMAL(10, 2) NOT NULL,
          unit VARCHAR(255) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (timestamp, key)  -- Prevent duplicate entries for same time + key
        );
        CREATE INDEX IF NOT EXISTS idx_timestamp ON ${tableName} (timestamp);
        CREATE INDEX IF NOT EXISTS idx_key ON ${tableName} (key);
      `;
      await sequelize.query(createTableQuery);
      logger.info(`Ensured table ${tableName} exists with production schema`);

      // Prepare Measurements for bulk insert (store timestamp as raw Unix BIGINT)
      const measurements = [];
      response.dataList.forEach(dataPoint => {
        const timestamp = parseInt(dataPoint.time);  // Raw Unix seconds, no conversion
        dataPoint.itemList.forEach(item => {
          if (isNaN(parseFloat(item.value))) {
            logger.warn(`Skipping invalid value for ${deviceSn}: ${item.value}`);
            return;
          }
          measurements.push({
            timestamp,
            key: item.key,
            value: parseFloat(item.value),
            unit: item.unit,
          });
        });
      });

      if (measurements.length > 0) {
        // Build bulk insert query with transaction for atomicity
        const values = measurements.map(m => `(${m.timestamp}, '${m.key}', ${m.value}, '${m.unit}')`).join(', ');
        const insertQuery = `
          BEGIN;
          INSERT INTO ${tableName} (timestamp, key, value, unit)
          VALUES ${values}
          ON CONFLICT (timestamp, key) DO NOTHING;  -- Skip duplicates
          COMMIT;
        `;
        await sequelize.query(insertQuery);
        logger.info(`Inserted ${measurements.length} measurements into ${tableName}`);
      } else {
        logger.warn(`No measurements to insert for ${deviceSn}`);
      }


        // Save response to a .txt file
        const filename = `device_${deviceSn}_data_${endTimestamp}.txt`;
        const filepath = path.join(__dirname, 'logs', filename); // Save in 'logs' folder
        const dataToWrite = `Timestamp: ${new Date().toISOString()}\nDevice: ${deviceSn}\nPayload: ${JSON.stringify(payload, null, 2)}\nResponse: ${JSON.stringify(response, null, 2)}\n\n`;

        // Ensure logs directory exists
        if (!fs.existsSync(path.join(__dirname, 'logs'))) {
          fs.mkdirSync(path.join(__dirname, 'logs'), { recursive: true });
        }

        fs.appendFileSync(filepath, dataToWrite); // Append to file
        console.log(`Response saved to ${filepath}`);

      return response;
    } catch (err) {
      attempts++;
      logger.warn(`Attempt ${attempts} failed for ${deviceSn}: ${err.message}`);
      if (attempts >= CONFIG.RETRY_ATTEMPTS) {
        logger.error(`Failed to fetch/store data for ${deviceSn} after ${CONFIG.RETRY_ATTEMPTS} attempts`, { error: err.message });
        throw err;
      }
      // Exponential backoff
      await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY * Math.pow(2, attempts - 1)));
    }
  }
}

// Schedule daily device data fetch
async function scheduleDailyDeviceDataFetch(devices) {
  await syncDB(); // Sync DB on startup

  // Schedule the job
  cron.schedule(CONFIG.CRON_SCHEDULE, async () => {
    logger.info('Starting daily device data fetch');
    const { startTimestamp, endTimestamp } = getTimestamps();
    logger.info(`Timestamps: start=${startTimestamp}, end=${endTimestamp}`);

    const limit = pLimit(CONFIG.CONCURRENCY_LIMIT);
    const promises = devices.map(device => limit(() => fetchDeviceData(device)));

    try {
      const results = await Promise.allSettled(promises);
      const successes = results.filter(r => r.status === 'fulfilled').length;
      const failures = results.filter(r => r.status === 'rejected').length;
      logger.info(`Daily fetch completed: ${successes} successes, ${failures} failures`);
      if (failures > 0) {
        logger.warn('Some fetches failed; consider sending alerts');
      }
    } catch (err) {
      logger.error('Unexpected error in daily fetch:', err);
    }
  });

  logger.info('Daily device data fetch scheduled');
}

// Graceful shutdown
process.on('SIGINT', async () => {
  logger.info('Shutting down gracefully...');
  await sequelize.close();
  process.exit(0);
});

// ----------------------
// Start Server
// ----------------------
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await obtainToken(); // Ensure token is ready
  await scheduleDailyDeviceDataFetch(devices); // Start fetching after DB sync
});

