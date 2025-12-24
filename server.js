
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
          return `${timestamp} | [${level}] | ${message} | ${metaData} -> (${ms})`;
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
    logging: true,          // Disable SQL logs in production
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
    allowNull : false,
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

    logger.info(`API Response status: ${response.status}`);
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

// // Fetch and store data for a single device with retries (updated for station table)
// async function fetchDeviceData(deviceSn, stationId, measurePoints = ["BatteryVoltage","LoadPowerL1","LoadPowerL2","LoadPowerL3"]) {
//   const { startTimestamp, endTimestamp } = getTimestamps();
//   const payload = { deviceSn, startTimestamp, endTimestamp, measurePoints };

//   let attempts = 0;
//   while (attempts < CONFIG.RETRY_ATTEMPTS) {
//     try {
//       logger.info(`Fetching data for device: ${deviceSn} in station: ${stationId} (Attempt ${attempts + 1})`);
//       const response = await deyePost("/device/historyRaw", payload, { timeout: CONFIG.API_TIMEOUT });

//       if (!response || response.code !== "1000000" || !response.dataList) {
//         throw new Error(`Invalid API response: ${JSON.stringify(response)}`);
//       }

//       // Upsert Device (now with station_id)
//       const [dbDevice, created] = await Device.upsert({
//         device_sn: response.deviceSn,
//         device_type: response.deviceType,
//         station_id: stationId,
//       });
//       logger.info(`Device ${deviceSn} ${created ? 'created' : 'updated'} in DB for station ${stationId}`);

//       // Create station-specific measurement table if it doesn't exist
//       const tableName = `measurements_station_${stationId}`;
//       const createTableQuery = `
//         CREATE TABLE IF NOT EXISTS ${tableName} (
//           measurement_id SERIAL PRIMARY KEY,
//           device_sn VARCHAR(255) NOT NULL,
//           timestamp BIGINT NOT NULL,
//           BatteryVoltage DECIMAL(10, 2),
//           LoadPowerL1 DECIMAL(10, 2),
//           LoadPowerL2 DECIMAL(10, 2),
//           LoadPowerL3 DECIMAL(10, 2),
//           GridPowerL1 DECIMAL(10, 2),
//           GridPowerL2 DECIMAL(10, 2),
//           GridPowerL3 DECIMAL(10, 2),
//           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//           updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//           UNIQUE (device_sn, timestamp)
//         );
//         CREATE INDEX IF NOT EXISTS idx_device_sn ON ${tableName} (device_sn);
//         CREATE INDEX IF NOT EXISTS idx_timestamp ON ${tableName} (timestamp);
//       `;
//       await sequelize.query(createTableQuery);
//       logger.info(`Ensured table ${tableName} exists`);

//       // // Prepare Measurements for bulk insert
//       // const measurements = [];
//       // response.dataList.forEach(dataPoint => {
//       //   const timestamp = parseInt(dataPoint.time);
//       //   dataPoint.itemList.forEach(item => {
//       //     if (isNaN(parseFloat(item.value))) {
//       //       logger.warn(`Skipping invalid value for ${deviceSn}: ${item.value}`);
//       //       return;
//       //     }
//       //     measurements.push({
//       //       device_sn: deviceSn,
//       //       timestamp,
//       //       key: item.key,
//       //       value: parseFloat(item.value),
//       //       unit: item.unit,
//       //     });
//       //   });
//       // });

//       // if (measurements.length > 0) {
//       //   const values = measurements.map(m => `('${m.device_sn}', ${m.timestamp}, '${m.key}', ${m.value}, '${m.unit}')`).join(', ');
//       //   const insertQuery = `
//       //     BEGIN;
//       //     INSERT INTO ${tableName} (device_sn, timestamp, key, value, unit)
//       //     VALUES ${values}
//       //     ON CONFLICT (device_sn, timestamp, key) DO NOTHING;
//       //     COMMIT;
//       //   `;
//       //   await sequelize.query(insertQuery);
//       //   logger.info(`Inserted ${measurements.length} measurements into ${tableName}`);
//       // } else {
//       //   logger.warn(`No measurements to insert for ${deviceSn}`);

//       // with the col -> new logic 
//       // Group measurements by timestamp for column-wise insertion
//         const timestampMap = {};
//         response.dataList.forEach(dataPoint => {
//           const timestamp = parseInt(dataPoint.time);
//           if (!timestampMap[timestamp]) {
//             timestampMap[timestamp] = { device_sn: deviceSn, timestamp, BatteryVoltage: null, LoadPowerL1: null, LoadPowerL2: null , LoadPowerL3 : null , GridPowerL1 :null , GridPowerL2 : null };  // Initialize with known keys
//           }
//           dataPoint.itemList.forEach(item => {
//             if (isNaN(parseFloat(item.value))) {
//               logger.warn(`Skipping invalid value for ${deviceSn} at ${timestamp}: ${item.value}`);
//               return;
//             }
//             // Map key to column (add more if needed)
//             if (item.key === 'BatteryVoltage') timestampMap[timestamp].BatteryVoltage = parseFloat(item.value);
//             else if (item.key === 'LoadPowerL1') timestampMap[timestamp].LoadPowerL1 = parseFloat(item.value);
//             else if (item.key === 'LoadPowerL2') timestampMap[timestamp].LoadPowerL2 = parseFloat(item.value);
//             else if (item.key === 'LoadPowerL3') timestampMap[timestamp].LoadPowerL3 = parseFloat(item.value);
//             else if (item.key === 'GridPowerL1') timestampMap[timestamp].GridPowerL1 = parseFloat(item.value);
//             else if (item.key === 'GridPowerL2') timestampMap[timestamp].GridPowerL2 = parseFloat(item.value);
//             // Add more else-if for additional keys
//           });
//         });

//         // Prepare for bulk upsert (one row per timestamp)
//         const measurements = Object.values(timestampMap);
//         if (measurements.length > 0) {
//           const values = measurements.map(m => 
//             `('${m.device_sn}', ${m.timestamp}, ${m.BatteryVoltage || 'NULL'}, ${m.LoadPowerL1 || 'NULL'}, ${m.LoadPowerL2 || 'NULL'} ,${m.LoadPowerL3} , ${m.GridPowerL1})`
//           ).join(', ');
//           const upsertQuery = `
//             INSERT INTO ${tableName} (device_sn, timestamp, BatteryVoltage, LoadPowerL1, LoadPowerL2 ,LoadPowerL3 ,GridPowerL1,GridPowerL2 )
//             VALUES ${values}
//             ON CONFLICT (device_sn, timestamp) DO UPDATE SET
//               BatteryVoltage = EXCLUDED.BatteryVoltage,
//               LoadPowerL1 = EXCLUDED.LoadPowerL1,
//               LoadPowerL2 = EXCLUDED.LoadPowerL2,
//               LoadPowerL3 = EXCLUDED.LoadPowerL3,
//               GridPowerL1 = EXCLUDED.GridPowerL1,
//               GridPowerL2 = EXCLUDED.GridPowerL2,
//               updated_at = CURRENT_TIMESTAMP;
//           `;
//           await sequelize.query(upsertQuery);
//           logger.info(`Upserted ${measurements.length} column-wise measurements into ${tableName}`);
//         } else {
//           logger.warn(`No measurements to insert for ${deviceSn}`);
//         }
//       // }
//       return { deviceSn, response };
//     } catch (err) {
//       attempts++;
//       logger.warn(`Attempt ${attempts} failed for ${deviceSn}: ${err.message}`);
//       if (attempts >= CONFIG.RETRY_ATTEMPTS) {
//         logger.error(`Failed to fetch/store data for ${deviceSn} after ${CONFIG.RETRY_ATTEMPTS} attempts`,{ error: err.message });
//         throw err;
//       }
//       await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY * Math.pow(2, attempts - 1)));
//     }
//   }
// }


// Updated fetchDeviceData with batching (no time splitting) and merging
async function fetchDeviceData(deviceSn, stationId, measurePoints = ["BatteryVoltage", "LoadPowerL1", "LoadPowerL2", "LoadPowerL3", "GridPowerL1", "GridPowerL2", "GridPowerL3"]) {
  const { startTimestamp, endTimestamp } = getTimestamps();
  
  // Batch measurePoints to avoid API limits (e.g., 4 per batch; adjust based on API docs/testing)
  const batchSize = 4; // Test and adjust (e.g., reduce to 2 if still failing)
  const pointBatches = [];
  for (let i = 0; i < measurePoints.length; i += batchSize) {
    pointBatches.push(measurePoints.slice(i, i + batchSize));
  }

  let attempts = 0;
  while (attempts < CONFIG.RETRY_ATTEMPTS) {
    try {
      logger.info(`Fetching data for device: ${deviceSn} in station: ${stationId} (Attempt ${attempts + 1})`);
      
      // Collect all data across batches (full time range per batch)
      const allDataLists = [];
      for (const pointBatch of pointBatches) {
        const payload = { deviceSn, startTimestamp, endTimestamp, measurePoints: pointBatch };
        logger.info(`Fetching batch: ${JSON.stringify(pointBatch)} for full time range ${startTimestamp}-${endTimestamp}`);
        
        const response = await deyePost("/device/historyRaw", payload, { timeout: CONFIG.API_TIMEOUT });
        if (!response || response.code !== "1000000" || !response.dataList) {
          throw new Error(`Invalid API response for batch ${JSON.stringify(pointBatch)}: ${JSON.stringify(response)}`);
        }
        allDataLists.push(...response.dataList); // Merge dataLists from all batches
      }

      // Upsert Device (using input deviceSn for consistency across batches)
      const [dbDevice, created] = await Device.upsert({
        device_sn: deviceSn,
        device_type: "INVERTER", // Assumed default; update if needed based on API
        station_id: stationId,
      });
      logger.info(`Device ${deviceSn} ${created ? 'created' : 'updated'} in DB for station ${stationId}`);

      // Create table (unchanged, but ensure it includes all columns)
      const tableName = `measurements_station_${stationId}`;
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${tableName} (
          measurement_id SERIAL PRIMARY KEY,
          device_sn VARCHAR(255) NOT NULL,
          timestamp BIGINT NOT NULL,
          BatteryVoltage DECIMAL(10, 2),
          LoadPowerL1 DECIMAL(10, 2),
          LoadPowerL2 DECIMAL(10, 2),
          LoadPowerL3 DECIMAL(10, 2),
          GridPowerL1 DECIMAL(10, 2),
          GridPowerL2 DECIMAL(10, 2),
          GridPowerL3 DECIMAL(10, 2),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (device_sn, timestamp)
        );
        CREATE INDEX IF NOT EXISTS idx_device_sn ON ${tableName} (device_sn);
        CREATE INDEX IF NOT EXISTS idx_timestamp ON ${tableName} (timestamp);
      `;
      await sequelize.query(createTableQuery);
      logger.info(`Ensured table ${tableName} exists`);

      // Group and merge measurements by timestamp (across all batches)
      const timestampMap = {};
      allDataLists.forEach(dataPoint => {
        const timestamp = parseInt(dataPoint.time);
        if (!timestampMap[timestamp]) {
          timestampMap[timestamp] = { 
            device_sn: deviceSn, 
            timestamp, 
            BatteryVoltage: null, 
            LoadPowerL1: null, 
            LoadPowerL2: null, 
            LoadPowerL3: null, 
            GridPowerL1: null, 
            GridPowerL2: null, 
            GridPowerL3: null 
          };
        }
        dataPoint.itemList.forEach(item => {
          if (isNaN(parseFloat(item.value))) {
            logger.warn(`Skipping invalid value for ${deviceSn} at ${timestamp}: ${item.value}`);
            return;
          }
          // Map keys to columns
          if (item.key === 'BatteryVoltage') timestampMap[timestamp].BatteryVoltage = parseFloat(item.value);
          else if (item.key === 'LoadPowerL1') timestampMap[timestamp].LoadPowerL1 = parseFloat(item.value);
          else if (item.key === 'LoadPowerL2') timestampMap[timestamp].LoadPowerL2 = parseFloat(item.value);
          else if (item.key === 'LoadPowerL3') timestampMap[timestamp].LoadPowerL3 = parseFloat(item.value);
          else if (item.key === 'GridPowerL1') timestampMap[timestamp].GridPowerL1 = parseFloat(item.value);
          else if (item.key === 'GridPowerL2') timestampMap[timestamp].GridPowerL2 = parseFloat(item.value);
          else if (item.key === 'GridPowerL3') timestampMap[timestamp].GridPowerL3 = parseFloat(item.value);
          // Add more mappings if needed
        });
      });

      // Prepare for bulk upsert
      const measurements = Object.values(timestampMap);
      if (measurements.length > 0) {
        const values = measurements.map(m => 
          `('${m.device_sn}', ${m.timestamp}, ${m.BatteryVoltage || 'NULL'}, ${m.LoadPowerL1 || 'NULL'}, ${m.LoadPowerL2 || 'NULL'}, ${m.LoadPowerL3 || 'NULL'}, ${m.GridPowerL1 || 'NULL'}, ${m.GridPowerL2 || 'NULL'}, ${m.GridPowerL3 || 'NULL'})`
        ).join(', ');
        const upsertQuery = `
          INSERT INTO ${tableName} (device_sn, timestamp, BatteryVoltage, LoadPowerL1, LoadPowerL2, LoadPowerL3, GridPowerL1, GridPowerL2, GridPowerL3)
          VALUES ${values}
          ON CONFLICT (device_sn, timestamp) DO UPDATE SET
            BatteryVoltage = EXCLUDED.BatteryVoltage,
            LoadPowerL1 = EXCLUDED.LoadPowerL1,
            LoadPowerL2 = EXCLUDED.LoadPowerL2,
            LoadPowerL3 = EXCLUDED.LoadPowerL3,
            GridPowerL1 = EXCLUDED.GridPowerL1,
            GridPowerL2 = EXCLUDED.GridPowerL2,
            GridPowerL3 = EXCLUDED.GridPowerL3,
            updated_at = CURRENT_TIMESTAMP;
        `;
        await sequelize.query(upsertQuery);
        logger.info(`Upserted ${measurements.length} merged measurements into ${tableName}`);
      } else {
        logger.warn(`No measurements to insert for ${deviceSn}`);
      }
      return { deviceSn, allDataLists };
    } catch (err) {
      attempts++;
      logger.warn(`Attempt ${attempts} failed for ${deviceSn}: ${err.message}`);
      if (attempts >= CONFIG.RETRY_ATTEMPTS) {
        logger.error(`Failed to fetch/store data for ${deviceSn} after ${CONFIG.RETRY_ATTEMPTS} attempts`, { error: err.message });
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
      logger.info(`Fetched ${apiStations.length} stations from Deye API.`);

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
          limit(() => fetchDeviceData(device.device_sn, stationId, ["BatteryVoltage","LoadPowerL1","LoadPowerL2","LoadPowerL3","GridPowerL1","GridPowerL2","GridPowerL3" ]))
          // limit(() => fetchDeviceData(device.device_sn, stationId, measurePoints ))
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
  logger.info('Shutting down gracefully');
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
  