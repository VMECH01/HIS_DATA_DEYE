// // ----------------------
// // Deye Cloud Backend Server
// // ----------------------

// import express from "express";
// import axios from "axios";
// import dotenv from "dotenv";
// import crypto from "crypto";
// import moment from "moment";
// import fs from 'fs';   
// import pino from 'pino';
// import dayjs  from "dayjs";
// import PinoHttp from "pino-http";
// import { error } from "console";
// import cron from 'node-cron';
// import path from 'node:path'
// import winston from "winston";
// import pLimit from "p-limit";

// import {Sequelize , DataTypes} from 'sequelize';



// import { fileURLToPath } from 'url';
// import { dirname, join } from 'path';

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

// // Set up Winston logger
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
// const fs = require('fs');
// const path = require('path');
// if (!fs.existsSync(CONFIG.LOG_DIR)) {
//   fs.mkdirSync(CONFIG.LOG_DIR, { recursive: true });
// }

// // Utility: Calculate timestamps (UTC-based, for previous day)
// function getTimestamps() {
//   const now = new Date();
//   const endTimestamp = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);
//   const startTimestamp = endTimestamp - (24 * 60 * 60);
//   return { startTimestamp, endTimestamp };
// }


// // ----------------------
// // Database Config -> using the Sequelize ORM for the database connection 
// // ----------------------
// const sequelize = new Sequelize(
//      process.env.DB_NAME,
//      process.env.DB_USER,
//      process.env.DB_PASSWORD,
//      {
//        host: process.env.DB_HOST,
//        port: process.env.DB_PORT,
//        dialect: 'postgres',
//        logging: false, // Disable SQL logs in production
//        pool: { max: 5, min: 0, acquire: 30000, idle: 10000 }, // Connection pooling
//      }
//    );

//    // Devices Model
//    const Device = sequelize.define('Device', {
//      device_id: {
//        type: DataTypes.INTEGER,
//        primaryKey: true,
//        autoIncrement: true,
//      },
//      device_sn: {
//        type: DataTypes.STRING,
//        allowNull: false,
//        unique: true, // Ensure uniqueness
//      },
//      device_type: {
//        type: DataTypes.STRING,
//        allowNull: false,
//      },
//    }, {
//      tableName: 'devices',
//      timestamps: true, // Adds created_at, updated_at
//    });

//    // DeviceMeasurements Model
//    const DeviceMeasurement = sequelize.define('DeviceMeasurement', {
//      measurement_id: {
//        type: DataTypes.INTEGER,
//        primaryKey: true,
//        autoIncrement: true,
//      },
//      device_id: {
//        type: DataTypes.INTEGER,
//        allowNull: false,
//        references: { model: Device, key: 'device_id' },
//      },
//      timestamp: {
//        type: DataTypes.DATE, // Store as Date for easier querying
//        allowNull: false,
//      },
//      key: {
//        type: DataTypes.STRING,
//        allowNull: false, // e.g., "BatteryVoltage"
//      },
//      value: {
//        type: DataTypes.DECIMAL(10, 2), // Adjust precision as needed
//        allowNull: false,
//      },
//      unit: {
//        type: DataTypes.STRING,
//        allowNull: false, // e.g., "V"
//      },
//    }, {
//      tableName: 'device_measurements',
//      timestamps: true,
//      indexes: [
//        { fields: ['device_id', 'timestamp'] }, // For efficient time-series queries
//        { fields: ['key'] }, // For filtering by measurement type
//      ],
//    });

//    // Define Relationships
//    Device.hasMany(DeviceMeasurement, { foreignKey: 'device_id' });
//    DeviceMeasurement.belongsTo(Device, { foreignKey: 'device_id' });

//    // Sync DB (run once to create tables)
//    async function syncDB() {
//      try {
//        await sequelize.authenticate();
//        await sequelize.sync({ alter: true }); // Use { force: true } for dev resets
//        console.log('Database synced successfully.');
//      } catch (err) {
//        console.error('Database sync error:', err);
//      }
//    }

//   //  module.exports = { sequelize, Device, DeviceMeasurement, syncDB };


// // ----------------------
// // Log Config -> for logging 
// // ----------------------

// // Simple logging to a file
// fs.appendFileSync('app.log', `[${new Date().toISOString()}] - INFO - This is an informational message for the logs of the errors .\n`);

// try {
//   throw new Error("Something went wrong!");
// } catch (error) {
//   // Use error.stack to get the full details and stack trace
//   const logMessage = `[${new Date().toISOString()}] - ERROR - ${error.stack}\n`;
//   fs.appendFileSync('app.log', logMessage);
// }

// // Simple logging the response below => custom logic 

// // const pino = require('pino');

// const logger = pino({
//   level: 'info',
//   transport: {
//     target: 'pino/file',
//     options: { destination: 'request.log' }
//   },
//   timestamp: () => `,"time":"${dayjs().format()}"`
// });   


// // Middleware to measure and log request duration
// app.use((req, res, next) => {
//   const start = process.hrtime.bigint(); // High-precision start time
//   // Hook into response finish event to log after sending
//   res.on('finish', () => {
//     const end = process.hrtime.bigint();
//     const durationMs = Number(end - start) / 1e6; // Convert nanoseconds to milliseconds
//     logger.info({
//       method: req.method,
//       url: req.url,
//       status: res.statusCode,
//       duration: `${durationMs.toFixed(2)}ms` // Log as a string for readability
//     }, 'Request completed');
//   });
//   next();
// });


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


// // ----------------------
// //  Obtain Access Token
// // ----------------------

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
//       console.log(" Token obtained successfully");
//       // console.log(accessToken);
//     } else {
//       console.error(" Failed to get token:", response.data);
//     }
//   } catch (err) {
//     console.error(" Token fetch error:", err.response?.data || err.message);
//   }
// }

// // ----------------------
// //  Ensure valid token
// // ----------------------

// async function ensureToken() {
//   if (!accessToken || Date.now() > tokenExpiry) {
//     await obtainToken();
//   }
// }

// // ----------------------
// //  Helper POST request
// // ----------------------

// async function deyePost(endpoint, payload = {}) {
//   await ensureToken();

//   const url = `${DEYE_BASE_URL}${endpoint}`;
//   const headers = {
//     "Content-Type": "application/json",
//     Authorization: `Bearer ${accessToken}`
//   };

//   console.log(" Making request to:", url);
//   console.log(" Payload:", JSON.stringify(payload, null, 2));
//   console.log(" Token present:", !!accessToken);

//   try {
//     const response = await axios.post(url, payload, {
//       headers,
//       timeout: 10000
//     });

//     console.log(" API Response status:", response.status);

//     return response.data;
//   } catch (err) {
//     console.error(` Deye API Error [${endpoint}]`);
//     console.error(" Status:", err.response?.status);
//     console.error(" Response Data:", err.response?.data);

//     throw err;
//   }
// }
// // ----------------------
// //  API Routes
// // ----------------------

// // 1 => Health Check
// app.get("/statusCheck", (req, res) => {
//   logger.info('/statusCheck -> is being called')
//   setTimeout(() => {
//     res.json("Hello");
//   },3000)
// });

// // 2 =>  Get all stations  => working the api 
// app.get("/api/stations", async (req, res) => {
//   logger.info ('/api/stations -> is got called');
//   try {
//     const data = await deyePost("/station/list", {});
//     res.json({ success: true, data: data.data || data.stationList || [] });
//   } catch (err) {
//     res.status(500).json({
//       success: false,
//       error: err.response?.data || err.message,
//     });
//   }
// });

// // Devices in  Array 
// const devices = [
//   {
//     deviceSn : "2306178462",
//     measurePoints : [
//       "BatteryVoltage"
//     ]
//   },
//   {
//     deviceSn :"2401276187",
//     measurePoints : [
//       "BatteryVoltage"
//     ]
//   } ,
// ]

// // Utility: Fetch and store data for a single device with retries
// async function fetchDeviceData(device) {
//   const { deviceSn, measurePoints } = device;
//   const { startTimestamp, endTimestamp } = getTimestamps();
//   const payload = { deviceSn, startTimestamp, endTimestamp, measurePoints };

//   let attempts = 0;
//   while (attempts < CONFIG.RETRY_ATTEMPTS) {
//     try {
//       logger.info(`Fetching data for device: ${deviceSn}`, { payload });
//       const response = await deyePost("/device/historyRaw", payload, { timeout: CONFIG.API_TIMEOUT });

//       // Validate response
//       if (!response || response.code !== "1000000" || !response.dataList) {
//         throw new Error(`Invalid API response: ${JSON.stringify(response)}`);
//       }

//       // Upsert Device (insert if not exists, update if needed)
//       const [dbDevice, created] = await Device.upsert({
//         device_sn: response.deviceSn,
//         device_type: response.deviceType,
//       });
//       logger.info(`Device ${deviceSn} ${created ? 'created' : 'updated'} in DB`);

//       // Prepare Measurements for bulk insert
//       const measurements = [];
//       response.dataList.forEach(dataPoint => {
//         const timestamp = new Date(parseInt(dataPoint.time) * 1000); // Convert Unix to JS Date
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
//         await DeviceMeasurement.bulkCreate(measurements, { ignoreDuplicates: true }); // Skip duplicates
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

// // Main function: Schedule daily device data fetch
// async function scheduleDailyDeviceDataFetch(devices) {
//   await syncDB(); // Sync DB on startup

//   // Schedule the job
//   cron.schedule(CONFIG.CRON_SCHEDULE, async () => {
//     logger.info('Starting daily device data fetch');
//     const { startTimestamp, endTimestamp } = getTimestamps();
//     logger.info(`Timestamps: start=${startTimestamp} (${new Date(startTimestamp * 1000).toISOString()}), end=${endTimestamp} (${new Date(endTimestamp * 1000).toISOString()})`);

//     const limit = pLimit(CONFIG.CONCURRENCY_LIMIT); // Limit concurrency
//     const promises = devices.map(device => limit(() => fetchDeviceData(device)));

//     try {
//       const results = await Promise.allSettled(promises);
//       const successes = results.filter(r => r.status === 'fulfilled').length;
//       const failures = results.filter(r => r.status === 'rejected').length;
//       logger.info(`Daily fetch completed: ${successes} successes, ${failures} failures`);

//       // Optional: Send alerts if failures > threshold (integrate with email service like nodemailer)
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
//   const { sequelize } = require('./models');
//   await sequelize.close();
//   process.exit(0);
// });

// // Example usage
// const devices = [
//   { deviceSn: "2306178462", measurePoints: ["BatteryVoltage"] },
//   { deviceSn: "2401276187", measurePoints: ["BatteryVoltage"] },
// ];
// scheduleDailyDeviceDataFetch(devices);
// // // This id the function for the 

// // function scheduleDailyDeviceDataFetch(devices){
// //   // devices should be an array of objects, e.g.:
// //   // [
// //   //   { deviceSn: "2401276187", measurePoints: ["BatteryVoltage"] },
// //   //   { deviceSn: "2306178462", measurePoints: ["BatteryVoltage"] }
// //   // ]

// //   // schedule the call -> at the midnight (00:00:00) -> To get the data
// //   cron.schedule('0 0 * * * ' , async () => {
// //     try{
// //       // calculate the TimeStamp --> date.now() = endTimeStamp - startTimeStamp (24*60*60)
// //       // const now = new Date();
// //       // const endTimestamp = Math.floor(new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0).getTime() / 1000);
// //       // const startTimestamp = endTimestamp - (24 * 60 * 60);     // This will be time in ms

// //       // This is for the Cross checking 
// //       const now = new Date();
// //       // Use UTC to calculate start of the day in UTC
// //       const endTimestamp = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);
// //       const startTimestamp = endTimestamp - (24 * 60 * 60);


// //       // Debuge
// //       console.log(`Running Daily fetch at the ${new Date() .toISOString()}`);
// //       console.log(`startTimestamp is  : ${startTimestamp} (${new Date(startTimestamp * 1000).toISOString()})`);
// //       console.log(`endTimestamp is : ${endTimestamp} (${new Date(endTimestamp * 1000 ).toISOString()})`);

// //       // loop for the each device for iteration -> ms of data can be get missed -> not big issue 

// //       for (const device of devices){
// //         const {deviceSn , measurePoints} = device ;
// //         // Prep for the payload 
// //         const payload = {
// //           deviceSn,
// //           startTimestamp,
// //           endTimestamp,
// //           measurePoints
// //         };
// //         // Debuging 
// //         logger.info(`Fetching the device : ${deviceSn}`);
// //         console.log(`Fetching the device : ${deviceSn} & measurePoint : ${measurePoints}`);
// //         // Calling the api with the help of using the deyePost Helper function 
// //         const  response = await deyePost("/device/historyRaw",payload);
// //         // log the response -> futher modification is needed 
// //         console.log(`Successfully feteched ${deviceSn} : ${response}`) ;

// //         // ----------------------
// //         //  For the Debuging -> To save the response in the Logs folder 
// //         // ----------------------
// //         // Save response to a .txt file
// //         const filename = `device_${deviceSn}_data_${endTimestamp}.txt`;
// //         const filepath = path.join(__dirname, 'logs', filename); // Save in 'logs' folder
// //         const dataToWrite = `Timestamp: ${new Date().toISOString()}\nDevice: ${deviceSn}\nPayload: ${JSON.stringify(payload, null, 2)}\nResponse: ${JSON.stringify(response, null, 2)}\n\n`;

// //         // Ensure logs directory exists
// //         if (!fs.existsSync(path.join(__dirname, 'logs'))) {
// //           fs.mkdirSync(path.join(__dirname, 'logs'), { recursive: true });
// //         }

// //         fs.appendFileSync(filepath, dataToWrite); // Append to file
// //         console.log(`Response saved to ${filepath}`);
        
// //         // Optional: we could store the data in a database, send notifications, etc.
// //         // For example:
// //         // await saveToDatabase(deviceSn, response);
// //       }
// //     }catch(err){
// //       console.error('Error in daily fetch:', err.response?.data || err.message);
// //       // Optional: Send error notifications, e.g., via email or logging service
// //     }
// //   });
// //   // we cam put the logger funtion over here to log the data
// //   console.log('Daily device data fetch scheduled to run at midnight every day.');  
// // };

// // // calling the function for the contiouse calling 
// // scheduleDailyDeviceDataFetch(devices);


// // ----------------------
// //  Start Server
// // ----------------------
// // app.listen(PORT, async () => {
// //   console.log(`Server running on port ${PORT}`);
// //   await obtainToken();
// // });

