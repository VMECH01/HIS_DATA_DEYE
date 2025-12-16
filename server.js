// // ----------------------
// // Deye Cloud Backend Server
// // ----------------------

// import express from "express";
// import axios from "axios";
// import dotenv from "dotenv";
// import crypto from "crypto";
// import cron from "node-cron";
// import winston from "winston";
// import pLimit from "p-limit";
// import { Sequelize, DataTypes } from "sequelize";
// import { fileURLToPath } from "url";
// import { dirname } from "path";

// const __filename = fileURLToPath(import.meta.url);
// const __dirname = dirname(__filename);

// dotenv.config();

// const app = express();
// app.use(express.json());

// const PORT = process.env.PORT || 5001;

// // Configuration from .env
// const CONFIG = {
//   CRON_SCHEDULE: process.env.CRON_SCHEDULE || '0 0 * * *',
//   LOG_LEVEL: process.env.LOG_LEVEL || 'info',
//   LOG_DIR: process.env.LOG_DIR || './logs',
//   API_TIMEOUT: parseInt(process.env.API_TIMEOUT) || 30000,
//   CONCURRENCY_LIMIT: parseInt(process.env.CONCURRENCY_LIMIT) || 5,
//   RETRY_ATTEMPTS: parseInt(process.env.RETRY_ATTEMPTS) || 3,
//   RETRY_DELAY: parseInt(process.env.RETRY_DELAY) || 1000,
// };

// // Set up Winston logger (consolidated; removed Pino and fs logging)
// const logger = winston.createLogger({
//   level: CONFIG.LOG_LEVEL,
//   format: winston.format.combine(
//     winston.format.timestamp(),
//     winston.format.errors({ stack: true }),
//     winston.format.json()
//   ),
//   transports: [
//     new winston.transports.Console(),
//     new winston.transports.File({ filename: `${CONFIG.LOG_DIR}/daily-fetch.log` }),
//   ],
// });

// // Ensure log directory exists
// import fs from "fs";
// import path from "path";
// if (!fs.existsSync(CONFIG.LOG_DIR)) {
//   fs.mkdirSync(CONFIG.LOG_DIR, { recursive: true });
// }

// // ----------------------
// // Database Config -> using Sequelize ORM
// // ----------------------
// const sequelize = new Sequelize(
//   process.env.DB_NAME,
//   process.env.DB_USER,
//   process.env.DB_PASSWORD,
//   {
//     host: process.env.DB_HOST,
//     port: process.env.DB_PORT,
//     dialect: 'postgres',
//     logging: false, // Disable SQL logs in production
//     pool: { max: 5, min: 0, acquire: 30000, idle: 10000 }, // Connection pooling
//   }
// );


// // +-------------------+          +---------------------------------+
// // |     Device        |          | measurements_sn_2306178462     |  (for device_sn="2306178462", device_id=2)
// // +-------------------+          +---------------------------------+
// // | - device_id (PK)  |          | - measurement_id (PK)          |
// // | - device_sn       |          | - timestamp                    |
// // | - device_type     |          | - key                          |
// // | - createdAt       |          | - value                        |
// // | - updatedAt       |          | - unit                         |
// // |                   |          | - createdAt                    |
// // |                   |          | - updatedAt                    |
// // +-------------------+          +---------------------------------+
// //           | (1)                           | (N)
// //           +-------------------------------+
// //                   Has Many (Manual Link via device_sn)

// // +-------------------+          +---------------------------------+
// // |     Device        |          | measurements_sn_2401276187     |  (for device_sn="2401276187", device_id=1)
// // +-------------------+          +---------------------------------+
// // | ... (same)        |          | ... (same attributes)          |
// // +-------------------+          +---------------------------------+
// //           | (1)                           | (N)
// //           +-------------------------------+
// //                   Has Many (Manual Link via device_sn)

// // Devices Model
// const Device = sequelize.define('Device', {
//   device_id: {
//     type: DataTypes.INTEGER,
//     primaryKey: true,
//     autoIncrement: true,
//   },
//   device_sn: {
//     type: DataTypes.STRING,
//     allowNull: false,
//     unique: true,
//   },
//   device_type: {
//     type: DataTypes.STRING,
//     allowNull: false,
//   },
// }, {
//   tableName: 'devices',
//   timestamps: true,  // Maps to createdAt, updatedAt
// });

// // DeviceMeasurements Model
// const DeviceMeasurement = sequelize.define('DeviceMeasurement', {
//   measurement_id: {
//     type: DataTypes.INTEGER,
//     primaryKey: true,
//     autoIncrement: true,
//   },
//   device_id: {
//     type: DataTypes.INTEGER,
//     allowNull: false,
//     references: { model: Device, key: 'device_id' },
//   },
//   timestamp: {
//     type: DataTypes.DATE, // Store as Date for easier querying
//     allowNull: false,
//   },
//   key: {
//     type: DataTypes.STRING,
//     allowNull: false, // e.g., "BatteryVoltage"
//   },
//   value: {
//     type: DataTypes.DECIMAL(10, 2), // Adjust precision as needed
//     allowNull: false,
//   },
//   unit: {
//     type: DataTypes.STRING,
//     allowNull: false, // e.g., "V"
//   },
// }, {
//   tableName: 'device_measurements',
//   timestamps: true,
//   indexes: [
//     { fields: ['device_id', 'timestamp'] }, // For efficient time-series queries
//     { fields: ['key'] }, // For filtering by measurement type
//   ],
// });

// // Define Relationships
// Device.hasMany(DeviceMeasurement, { foreignKey: 'device_id' });
// DeviceMeasurement.belongsTo(Device, { foreignKey: 'device_id' });

// // Sync DB
// async function syncDB() {
//   try {
//     await sequelize.authenticate();
//     await sequelize.sync({ alter: true }); // Use { force: true } for dev resets
//     logger.info('Database synced successfully.');
//   } catch (err) {
//     logger.error('Database sync error:', err);
//     throw err; // Throw to prevent server start on DB failure
//   }
// }

// // ----------------------
// // Deye Cloud Config
// // ----------------------
// const {
//   DEYE_BASE_URL,
//   DEYE_APP_ID,
//   DEYE_APP_SECRET,
//   DEYE_EMAIL,
//   DEYE_PASSWORD
// } = process.env;

// let accessToken = null;
// let tokenExpiry = 0;

// // Obtain Access Token
// async function obtainToken() {
//   try {
//     const hashedPassword = crypto
//       .createHash("sha256")
//       .update(DEYE_PASSWORD)
//       .digest("hex");

//     const url = `${DEYE_BASE_URL}/account/token?appId=${DEYE_APP_ID}`;
//     const response = await axios.post(
//       url,
//       {
//         appSecret: DEYE_APP_SECRET,
//         email: DEYE_EMAIL,
//         password: hashedPassword,
//         companyId: "0", // for personal accounts
//       },
//       { headers: { "Content-Type": "application/json" } }
//     );

//     if (response.data?.accessToken) {
//       accessToken = response.data.accessToken;
//       tokenExpiry = Date.now() + (response.data.expiresIn || 3600) * 1000;
//       logger.info("Token obtained successfully");
//     } else {
//       logger.error("Failed to get token:", response.data);
//       throw new Error("Token fetch failed");
//     }
//   } catch (err) {
//     logger.error("Token fetch error:", err.response?.data || err.message);
//     throw err;
//   }
// }

// // Ensure valid token (with refresh logic)
// async function ensureToken() {
//   if (!accessToken || Date.now() > tokenExpiry - 60000) { // Refresh 1 min early
//     await obtainToken();
//   }
// }

// // Helper POST request (updated for timeout)
// async function deyePost(endpoint, payload = {}, options = {}) {
//   await ensureToken();

//   const url = `${DEYE_BASE_URL}${endpoint}`;
//   const headers = {
//     "Content-Type": "application/json",
//     Authorization: `Bearer ${accessToken}`
//   };

//   logger.info("Making request to:", url);
//   // Avoid logging full payload/token for security

//   try {
//     const response = await axios.post(url, payload, {
//       headers,
//       timeout: options.timeout || 10000
//     });

//     logger.info("API Response status:", response.status);
//     return response.data;
//   } catch (err) {
//     logger.error(`Deye API Error [${endpoint}]:`, err.response?.data || err.message);
//     throw err;
//   }
// }

// // ----------------------
// // API Routes
// // ----------------------

// // Health Check
// app.get("/statusCheck", (req, res) => {
//   logger.info('/statusCheck called');
//   setTimeout(() => {
//     res.json({ message: "Hello", status: "OK" });
//   }, 3000);
// });

// // Get all stations
// app.get("/api/stations", async (req, res) => {
//   logger.info('/api/stations called');
//   try {
//     const data = await deyePost("/station/list", {});
//     res.json({ success: true, data: data.data || data.stationList || [] });
//   } catch (err) {
//     logger.error("Stations API error:", err);
//     res.status(500).json({
//       success: false,
//       error: err.response?.data || err.message,
//     });
//   }
// });

// // Devices array
// const devices = [
//   { deviceSn: "2306178462", measurePoints: ["BatteryVoltage"] },
//   { deviceSn: "2401276187", measurePoints: ["BatteryVoltage"] },
// ];

// // Utility: Calculate timestamps
// function getTimestamps() {
//   const now = new Date();
//   const endTimestamp = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);
//   const startTimestamp = endTimestamp - (24 * 60 * 60);
//   return { startTimestamp, endTimestamp };
// }

// // Fetch and store data for a single device with retries
// async function fetchDeviceData(device) {
//   const { deviceSn, measurePoints } = device;
//   const { startTimestamp, endTimestamp } = getTimestamps();
//   const payload = { deviceSn, startTimestamp, endTimestamp, measurePoints };

//   let attempts = 0;
//   while (attempts < CONFIG.RETRY_ATTEMPTS) {
//     try {
//       logger.info(`Fetching data for device: ${deviceSn}`);
//       const response = await deyePost("/device/historyRaw", payload, { timeout: CONFIG.API_TIMEOUT });

//       // Validate response
//       if (!response || response.code !== "1000000" || !response.dataList) {
//         throw new Error(`Invalid API response: ${JSON.stringify(response)}`);
//       }

//       // Upsert Device
//       const [dbDevice, created] = await Device.upsert({
//         device_sn: response.deviceSn,
//         device_type: response.deviceType,
//       });
//       logger.info(`Device ${deviceSn} ${created ? 'created' : 'updated'} in DB`);

//       // Prepare Measurements for bulk insert (chunk if large)
//       const measurements = [];
//       response.dataList.forEach(dataPoint => {
//         const timestamp = new Date(parseInt(dataPoint.time) * 1000);
//         dataPoint.itemList.forEach(item => {
//           measurements.push({
//             device_id: dbDevice.device_id,
//             timestamp,
//             key: item.key,
//             value: parseFloat(item.value),
//             unit: item.unit,
//           });
//         });
//       });

//       if (measurements.length > 0) {
//         // Insert in chunks to avoid DB limits
//         const chunkSize = 1000;
//         for (let i = 0; i < measurements.length; i += chunkSize) {
//           await DeviceMeasurement.bulkCreate(measurements.slice(i, i + chunkSize), { ignoreDuplicates: true });
//         }
//         logger.info(`Inserted ${measurements.length} measurements for ${deviceSn}`);
//       } else {
//         logger.warn(`No measurements to insert for ${deviceSn}`);
//       }

//       return response;
//     } catch (err) {
//       attempts++;
//       logger.warn(`Attempt ${attempts} failed for ${deviceSn}: ${err.message}`);
//       if (attempts >= CONFIG.RETRY_ATTEMPTS) {
//         logger.error(`Failed to fetch/store data for ${deviceSn} after ${CONFIG.RETRY_ATTEMPTS} attempts`, { error: err.message });
//         throw err;
//       }
//       // Exponential backoff
//       await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY * Math.pow(2, attempts - 1)));
//     }
//   }
// }

// // Schedule daily device data fetch
// async function scheduleDailyDeviceDataFetch(devices) {
//   await syncDB(); // Sync DB on startup

//   // Schedule the job
//   cron.schedule(CONFIG.CRON_SCHEDULE, async () => {
//     logger.info('Starting daily device data fetch');
//     const { startTimestamp, endTimestamp } = getTimestamps();
//     logger.info(`Timestamps: start=${startTimestamp}, end=${endTimestamp}`);

//     const limit = pLimit(CONFIG.CONCURRENCY_LIMIT);
//     const promises = devices.map(device => limit(() => fetchDeviceData(device)));

//     try {
//       const results = await Promise.allSettled(promises);
//       const successes = results.filter(r => r.status === 'fulfilled').length;
//       const failures = results.filter(r => r.status === 'rejected').length;
//       logger.info(`Daily fetch completed: ${successes} successes, ${failures} failures`);
//       if (failures > 0) {
//         logger.warn('Some fetches failed; consider sending alerts');
//       }
//     } catch (err) {
//       logger.error('Unexpected error in daily fetch:', err);
//     }
//   });

//   logger.info('Daily device data fetch scheduled');
// }

// // Graceful shutdown
// process.on('SIGINT', async () => {
//   logger.info('Shutting down gracefully...');
//   await sequelize.close();
//   process.exit(0);
// });

// // ----------------------
// // Start Server
// // ----------------------
// app.listen(PORT, async () => {
//   console.log(`Server running on port ${PORT}`);
//   await obtainToken(); // Ensure token is ready
//   await scheduleDailyDeviceDataFetch(devices); // Start fetching after DB sync
// });





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
import fs from "fs";
import path from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config();

const app = express();
app.use(express.json());

const { createLogger, format, transports } = pkg;

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

// Ensure log directory exists
if (!fs.existsSync(CONFIG.LOG_DIR)) {
  fs.mkdirSync(CONFIG.LOG_DIR, { recursive: true });
}

// Set up Winston logger
const logger = createLogger({
  level: CONFIG.LOG_LEVEL || 'info',
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.errors({ stack: true }),
    format.splat(),
    format.ms()
  ),
  transports: [
    new transports.Console({
      format: format.combine(
        format.colorize({ all: true }),
        format.printf(({ timestamp, level, message, ms, ...meta }) => {
          const metaData = Object.keys(meta).length ? JSON.stringify(meta) : '';
          return `${timestamp} [${level}] ${message} ${metaData} (${ms})`;
        })
      )
    }),
    new transports.File({ 
      filename: `${CONFIG.LOG_DIR}/daily-fetch.log`,
      format: format.json() 
    }),
  ],
});


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
    logging: false,          // Disable SQL logs in production
    pool: { max: 5, min: 0, acquire: 30000, idle: 10000 },
  }
);

// Station Model (fixed: station_id as PK)
const Station = sequelize.define('Station', {
  station_id: {
    type: DataTypes.INTEGER,
    primaryKey: true,  // Now station_id is the PK
    allowNull: false,
    unique: true,
  },
  station_name: {
    type: DataTypes.STRING,
  },
});


// Device Model (updated to link to Station)
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
  station_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
    references: {
      model: Station,
      key: 'station_id',
    },
  },
}, {
  tableName: 'devices',
  timestamps: true,
});

// Define associations
Station.hasMany(Device, { foreignKey: 'station_id' });
Device.belongsTo(Station, { foreignKey: 'station_id' });

// Sync DB
async function syncDB() {
  try {
    await sequelize.authenticate();
    await sequelize.sync({ alter: true });
    logger.info('Database synced successfully -> Ready to Read/Write data.');
  } catch (err) {
    logger.error('Database sync error:', err.message); // Simplified logging
    throw err;
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
        companyId: "0",
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

// Ensure valid token
async function ensureToken() {
  if (!accessToken || Date.now() > tokenExpiry - 60000) {
    await obtainToken();
  }
}

// Helper POST request
async function deyePost(endpoint, payload = {}, options = {}) {
  await ensureToken();

  const url = `${DEYE_BASE_URL}${endpoint}`;
  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${accessToken}`
  };

  logger.info(`Making request to: ${url}`);

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
  logger.info('/api/station_device called');
  const payload = {
    "page": 1,
    "size": 10,
    "deviceType": "INVERTER"
  };

  try {
    const response = await deyePost("/station/listWithDevice", payload, { 
      timeout: CONFIG.API_TIMEOUT 
    });

    res.status(200).json({
      success: true,
      data: response.data || response 
    });
  } catch (err) {
    logger.error("Stations API error:", err);
    res.status(500).json({
      success: false,
      error: err.response?.data || err.message,
    });
  }
});

// Fetch measurements for a device (updated to query station table)
app.get("/api/measurements/:deviceSn", async (req, res) => {
  const { deviceSn } = req.params;
  const { stationId } = req.query; // Require stationId as query param
  if (!stationId) {
    return res.status(400).json({ success: false, error: "stationId query param required" });
  }
  const tableName = `measurements_station_${stationId}`;
  try {
    const [results] = await sequelize.query(`SELECT * FROM ${tableName} WHERE device_sn = '${deviceSn}' ORDER BY timestamp DESC LIMIT 100;`);   // Limit to 100 for performance
    res.json({ success: true, data: results });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// New: Fetch all measurements for a station   => station-wise table
app.get("/api/measurements/station/:stationId", async (req, res) => {
  const { stationId } = req.params;
  const tableName = `measurements_station_${stationId}`;
  try {
    const [results] = await sequelize.query(`SELECT * FROM ${tableName} ORDER BY timestamp DESC LIMIT 1000;`);
    res.json({ success: true, data: results });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Utility: Calculate timestamps   => this will calculate start and end timestamps for previous day
function getTimestamps() {
  const now = new Date();
  const endTimestamp = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);   // Midnight UTC today  -> Time should be in the UTC timezone
  const startTimestamp = endTimestamp - (24 * 60 * 60);
  return { startTimestamp, endTimestamp };
}

// Fetch and store data for a single device with retries (updated for station table)
async function fetchDeviceData(deviceSn, stationId, measurePoints = ["BatteryVoltage"]) {
  const { startTimestamp, endTimestamp } = getTimestamps();
  const payload = { deviceSn, startTimestamp, endTimestamp, measurePoints };

  let attempts = 0;
  while (attempts < CONFIG.RETRY_ATTEMPTS) {
    try {
      logger.info(`Fetching data for device: ${deviceSn} in station: ${stationId}`);
      const response = await deyePost("/device/historyRaw", payload, { timeout: CONFIG.API_TIMEOUT });

      if (!response || response.code !== "1000000" || !response.dataList) {
        throw new Error(`Invalid API response: ${JSON.stringify(response)}`);
      }

      // Upsert Device (now with station_id)
      const [dbDevice, created] = await Device.upsert({
        device_sn: response.deviceSn,
        device_type: response.deviceType,
        station_id: stationId,
      });
      logger.info(`Device ${deviceSn} ${created ? 'created' : 'updated'} in DB for station ${stationId}`);

      // Create station-specific measurement table if it doesn't exist
      const tableName = `measurements_station_${stationId}`;
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${tableName} (
          measurement_id SERIAL PRIMARY KEY,
          device_sn VARCHAR(255) NOT NULL,
          timestamp BIGINT NOT NULL,
          key VARCHAR(255) NOT NULL,
          value DECIMAL(10, 2) NOT NULL,
          unit VARCHAR(255) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (device_sn, timestamp, key)
        );
        CREATE INDEX IF NOT EXISTS idx_device_sn ON ${tableName} (device_sn);
        CREATE INDEX IF NOT EXISTS idx_timestamp ON ${tableName} (timestamp);
        CREATE INDEX IF NOT EXISTS idx_key ON ${tableName} (key);
      `;
      await sequelize.query(createTableQuery);
      logger.info(`Ensured table ${tableName} exists`);

      // Prepare Measurements for bulk insert
      const measurements = [];
      response.dataList.forEach(dataPoint => {
        const timestamp = parseInt(dataPoint.time);
        dataPoint.itemList.forEach(item => {
          if (isNaN(parseFloat(item.value))) {
            logger.warn(`Skipping invalid value for ${deviceSn}: ${item.value}`);
            return;
          }
          measurements.push({
            device_sn: deviceSn,
            timestamp,
            key: item.key,
            value: parseFloat(item.value),
            unit: item.unit,
          });
        });
      });

      if (measurements.length > 0) {
        const values = measurements.map(m => `('${m.device_sn}', ${m.timestamp}, '${m.key}', ${m.value}, '${m.unit}')`).join(', ');
        const insertQuery = `
          BEGIN;
          INSERT INTO ${tableName} (device_sn, timestamp, key, value, unit)
          VALUES ${values}
          ON CONFLICT (device_sn, timestamp, key) DO NOTHING;
          COMMIT;
        `;
        await sequelize.query(insertQuery);
        logger.info(`Inserted ${measurements.length} measurements into ${tableName}`);
      } else {
        logger.warn(`No measurements to insert for ${deviceSn}`);
      }
      return { deviceSn, response };
    } catch (err) {
      attempts++;
      logger.warn(`Attempt ${attempts} failed for ${deviceSn}: ${err.message}`);
      if (attempts >= CONFIG.RETRY_ATTEMPTS) {
        logger.error(`Failed to fetch/store data for ${deviceSn} after ${CONFIG.RETRY_ATTEMPTS} attempts`,{ error: err.message });
        throw err;
      }
      await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY * Math.pow(2, attempts - 1)));
    }
  }
}

// Schedule daily device data fetch (station-wise, using DB data)
async function scheduleDailyDeviceDataFetch() {
  await syncDB();

  cron.schedule(CONFIG.CRON_SCHEDULE, async () => {
    logger.info('Starting daily station-wise device data fetch');

    try {
      // Step 1: Sync stations and devices from API to DB  --> Upsert logic but this funtion only runs once in 15 days to sync stations/devices
      logger.info('Syncing stations and devices from API');
      const payload = { "page": 1, "size": 50, "deviceType": "INVERTER" };
      let stationResponse = await deyePost("/station/listWithDevice", payload, { timeout: CONFIG.API_TIMEOUT });
      
      // Fix: Parse if response is a string (e.g., if Axios didn't auto-parse)
      if (typeof stationResponse === 'string') {
        stationResponse = JSON.parse(stationResponse);
      }
      
      // Fix: Robust extraction of stationList
      const apiStations = stationResponse?.data?.stationList || stationResponse?.stationList || [];
      logger.info('API stations length:', apiStations.length);

      if (apiStations.length > 0) {
        for (const apiStation of apiStations) {
          // Upsert Station (now uses station_id as PK)
          const [station, stationCreated] = await Station.upsert({
            station_id: apiStation.id,
            station_name: apiStation.name,
          });
          logger.info(`Station ${apiStation.id} (${apiStation.name}) ${stationCreated ? 'created' : 'updated'}`);

          // Upsert Devices for this station
          for (const deviceItem of apiStation.deviceListItems || []) {
            const [device, deviceCreated] = await Device.upsert({
              device_sn: deviceItem.deviceSn,
              device_type: deviceItem.deviceType,
              station_id: apiStation.id,
            });
            logger.info(`Device ${deviceItem.deviceSn} ${deviceCreated ? 'created' : 'updated'} for station ${apiStation.id}`);
          }
        }
        logger.info('Stations and devices synced successfully');
      } else {
        logger.warn('No stations from API; skipping sync');
      }

      // Step 2: Query DB for stations and devices
      logger.info('Querying DB for stations and devices');
      const dbStations = await Station.findAll({
        include: [{
          model: Device,
          as: 'Devices',
        }],
      });

      if (!dbStations.length) {
        logger.warn('No stations in DB; skipping measurement fetch');
        return;
      }

      const { startTimestamp, endTimestamp } = getTimestamps();
      logger.info(`Timestamps: start=${startTimestamp}, end=${endTimestamp}`);

      // Step 3: Fetch measurements using DB data
      for (const dbStation of dbStations) {
        const stationId = dbStation.station_id;
        const stationName = dbStation.station_name;
        const devices = dbStation.Devices || [];

        if (!devices.length) {
          logger.info(`Station ${stationId} (${stationName}) has no devices in DB; skipping`);
          continue;
        }

        logger.info(`Processing station ${stationId} (${stationName}) with ${devices.length} devices from DB`);

        const limit = pLimit(CONFIG.CONCURRENCY_LIMIT);
        const devicePromises = devices.map(device => 
          limit(() => fetchDeviceData(device.device_sn, stationId, ["BatteryVoltage"]))
        );

        const results = await Promise.allSettled(devicePromises);
        const deviceData = results
          .filter(r => r.status === 'fulfilled')
          .map(r => r.value);

        const failures = results.filter(r => r.status === 'rejected').length;
        logger.info(`Station ${stationId}: ${deviceData.length} successes, ${failures} failures`);

        // Save combined data to station-wise file
        const filename = `station_${stationId}_data_${endTimestamp}.txt`;
        const filepath = path.join(CONFIG.LOG_DIR, filename);
        const dataToWrite = {
          timestamp: new Date().toISOString(),
          stationId,
          stationName,
          devices: deviceData
        };

        fs.appendFileSync(filepath, JSON.stringify(dataToWrite, null, 2) + '\n\n');
        logger.info(`Station data saved to ${filepath}`);
      }

      logger.info('Daily station-wise fetch completed');
    } catch (err) {
      logger.error('Error in daily fetch:', err.message); // Simplified
    }
  });

  logger.info('Daily station-wise device data fetch scheduled');
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
  await obtainToken();
  await scheduleDailyDeviceDataFetch();
});
  