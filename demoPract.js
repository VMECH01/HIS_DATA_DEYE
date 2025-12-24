// --------------------------------
// Demo of Practical JavaScript Concepts
// --------------------------------
import express from 'express';
import axios  from 'axios';
import dotenv from 'dotenv';
import crypto from 'crypto';
import cron from 'node-cron';
import fs, { stat } from 'fs';
import path, { parse, resolve } from 'path';
import pkg from 'winston';
import { Sequelize ,DataTypes, INTEGER, Model } from 'sequelize';
import {fileURLToPath} from 'url';
import __dirname from 'path';
import { type } from 'os';
import { startTime } from 'pino-http';
import pLimit from 'p-limit';
import { timeStamp } from 'console';

// File path setup for ES modules
const __filename = fileURLToPath(import.meta.url);
// Path setup
// const __dirname = path.dirname(__filename);
// Winston Logger Setup
const { createLogger, format, transports } = pkg;

// Load environment variables from .env file
dotenv.config();

// Initialize Express app
const app = express();
app.use(express.json());

// port configuration
const PORT =process.env.PORT || 3001 ;

// CONFIG SETUP
const CONFIG = {
  CRON_SCHEDULE: process.env.CRON_SCHEDULE || '0 0 * * *',
  LOG_LEVEL : process.env.LOG_LEVEL || 'info',
  LOG_DIR : process.env.LOG_DIR || './logs',
  API_TIMEOUT : parseInt(process.env.API_TIMEOUT) || 30000 ,
  CONCURRENCY_LIMIT : parseInt(process.env.CONCURRENCY_LIMIT) || 5 ,
  RETRY_ATTEMPTS : parseInt(process.env.RETRY_ATTEMPTS || 3 ),
  RETRY_DELAY : parseInt(process.env.RETRY_DELAY) || 1000 ,
}

// Wiston Logger Configuration
const logger = createLogger({
  level : CONFIG.LOG_LEVEL || 'info',
  format : format.combine(
    format.timestamp({ format : 'YYYY-MM-DD HH:mm :ss'}),
    format.errors({ stack : true }),
    format.splat(),
    format.ms() 
  ),
  transports : [
    new transports.Console({
      format: format.combine(
        format.colorize({ all : true } ),
        format.printf(({ timestamp, level, message, ms  , ...meta}) => {
          const metaData = Object.keys(meta).length ? JSON.stringify(meta) : '';
          return `[${timestamp}] ${level}  ${message} ${metaData} (${ms})`;
        })
      )
    }),
    new transports.File({
      filename : ` ${CONFIG.LOG_DIR}/daily-fetch.log`,
      format : format.json
    }),
  ]
});

// ----------------------
// Database Config -> using Sequelize ORM
// ----------------------
//PostgreSQl connection setup -> its also supportes the MSSQL details
const sequelize = new sequelize(
  process.env.DB_NAME,
  process.env.DB_USER,
  process.env.DB_PASSWORD,
  {
    host : process.env.DB_HOST,
    port : process.env.DB_PORT,
    dialect : 'postgres',
    logging : false,            // Disable logging; set to console.log to see the raw SQL queries
    pool : {
      max : 5  , min : 0 , acquire : 30000 , idle : 10000
    },
  }
);

// ----------------------
// Database Models -> Station {Fixed : station_id is PK}
// ----------------------

const Station = sequelize.define('Station',{
  station_id :{
    type : DataTypes.INTEGER,
    primaryKey : true ,
    allowNull : false ,
    unqiue : true ,
  },
  station_name : {
    type : DataTypes.STRING,
    allowNull : false ,
  },
});

// ----------------------
// Database Models -> Device {Fixed : device_sn is PK}
// ----------------------
const Device = sequelize.define('Device' ,{
  device_id : {
    type : DataTypes.INTEGER,
    primaryKey : true ,
    autoIncrement : ture 
  },
   device_sn : {
    type : DataTypes.STRING ,
    allowNull : false,
    unqiue : true,
   },
   device_type: {
    type : DataTypes.STRING ,
    allowNull : false,
   },
   station_id : {
    type : DataTypes.INTEGER,
    allowNull : false ,
    references : {
      model : Station ,
      key : 'station_id'
    },
   }, 
} , {
  tableName : 'devices',
  timestamp : true ,
});

// -----------------------
// Define Associations -> 1 to N , M to M  , 1  to 1 
// -----------------------

Station.hasMany(Device,{
  foregine : 'station_id'
});

Device.belongsTo(Station , {
  foregineKey : 'station_id'
});

// -----------------------
// sync -> Data to DB
// -----------------------
async function syncDB(){
  try {
    await sequelize.authenthicate();
    logger.info('Database connection has been established successfully.');
    await sequelize.sync({ alter : true}); // this is the most distuctive way of sincing the data
    logger.info('Database synced successfully -> Ready to Read/Write data.');
  }catch (err) {
    logger.error('Database sync error:', error.message);  
    throw err;
  }
}

// -----------------------
// Deye Cloud Config 
// -----------------------

const {
  DEYE_BASE_URL,
  DEYE_APP_ID,
  DEYE_APP_SECRET,
  DEYE_EMAIL,
  DEYE_PASSWORD
} = process.env;

// -----------------------
// Token Management
// -----------------------

let accessToken = 0 ;
let tokenExpiry = 0 ;

// Function to obtain new token
async function obtainToken(){
  try{
    const hashedPassword = crypto
          .createHash("she256")
          .update(DEYE_PASSWORD)
          .digest("hex");

    // Url formation -> for the deye
    const url = `${DEYE_BASE_URL}/account/token?appId=${DEYE_APP_ID}` ;
    const response = await axios.post(
      url ,
      {
        appSecret: DEYE_APP_SECRET ,
        email : DEYE_EMAIL ,
        password  : hashedPassword,
        companyId : "0",
      },
      {
        headers : {"Content-Type" : "application/json"}
      }
    );
      // Check the response of the system 
      if(response.data?.accessToken){
        accessToken = response.data.accessToken;
        tokenExpiry = Date.now() + (response.data.expiresIn || 3600) * 1000 ;
        logger.info("Token obtained successfully");
      }else {
        logger.error("Failed to get Token :" , response.data);
        throw new Error("Token fetch failed");
      }
  }catch(err){
    logger.error("Token fetch error:" , err.response?.data || err.message );
    throw err ;
  }
};

// ---------------------
// Ensure the token is present or not
// ---------------------

async function ensureToken(accessToken) {
  if(!accessToken || Date.now() > tokenExpiry - 60000){
    logger.info("Waiting for the ObtainToken")
    await obtainToken();
  }
}

// ---------------------
// Post Method checker function
// ---------------------
// 
// 
// 
// 
// This will be developed soon 
// 
// 
// 
// 


// -----------------------
// DeyeHelper function -> Api request {POST} -> Method Only 
// -----------------------

async function deyePost(endpoint , payload = {} , options = {}) {
    // First check the Token is present or not 
    await ensureToken();
    // Then the URl -> BASE_URL + ENDPOINT + HEADER 
    const url = `${DEYE_BASE_URL}${endpoint}`;
    const headers = {
      "Content-Type":"application/json",
      Authorization : `Bearer ${accessToken}`
    };

    // The logger function for which URL we are making request
    logger.info(`Making request to : ${url}`);

    try{
      const response = await axios.post(
        url , payload , {
          headers ,
          timeout : options.timeout || 10000 
        }
      );
      // Logging the Api status_code 
      logger.info(`API Response Status Code : ${response.status}`);
      // Then return response
      return response.data;
    }catch(err){
      // Logging the error of the Api 
      logger.error(`Deye API Error [${endpoint}] : ` , err.response?.data || err.message );
      throw err ;
    }
};


// ------------------------
// API Data Fetching Function
// ------------------------

// This is the function for the getting the timestamp get the 
function getCurrentTimestamp() {
  // Get the current date & time 
  const now = new Date();
  // By using the current data as the end timestamp
  const endTimestamp = Math.floor(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate()) / 1000);
  // Getting the start timestamp by subtracting 24 hours (86400 seconds)
  const startTimestamp = endTimestamp - (24 * 60 * 60);
  // This will return the start & end timestamp
  return { startTimestamp, endTimestamp };
}

// Fetch and store the data for the single device
async function fetchDeviceData(deviceSn , stationId , measurementPoints = ["BatteryVoltage"]) {
  const {statTimestamp , endTimestamp} = getCurrentTimestamp();
  //  Then prep for the payload -> to make the requestt to call the API 
  const payload = {
    deviceSn ,
    statTimestamp,
    endTimestamp,
    measurementPoints,
  };   

  // Limiting max attempts for the retry logic
  let attempts = 0 ;
  while(attempts < CONFIG.RETRY_ATTEMPTS){
    try{
      // Then we will put the log of of the request 
      logger.info(`Fetching data for the Device : ${deviceSn} in station : ${stationId} (Attempt ${attempts + 1}) `); 
      // Making the API request -> deyePost function / device/historyRaw
      const response = await deyePost("/device/historyRaw" , payload , { timeout : CONFIG.API_TIMEOUT });

      // Checking the response
      if(!response || response.code !== "1000000" || !response.dataList){
        throw new Error(`Invalid response for device ${deviceSn} : ${JSON.stringify(response)}`);
      }

      // DataBase storage logic will be here -> upsert logic
      const [dbDevice , created ] = await Device.upsert({
        device_sn :  response.deviceSn,
        device_type : response.deviceType,
        station_id : stationId,
      });
      // Logging the success message
      logger.info(`Device ${deviceSn} ${created ? 'created' : 'updated'} successfully in DB for station ${stationId}.`);

      // Create station-specific measurement table if not exists
      const tableName = `measurements_station_${stationId}`;
      const createTableQuery = `
        CREATE TABLE IF NOT EXITS ${tableName} (
          measurement_id SERIAL PRIMARY KEY,
          device_sn VARCHAR(255) NOT NULL,
          timestamp BIGINT NOT NULL ,
          key VARCHAR(255) NOT NULL ,
          value DECIMAL NOT NULL,
          unit VARCHAR(255) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (device_sn, timestamp, key)
        );
        CREAET INDEX IF NOT EXISTS idx_device_sn ON ${tableName} (device_sn);
        CREATE INDEX IF NOT EXISTS idx_timestamp ON ${tableName} (timestamp);
        CREATE INDEX IF NOT EXISTS idx_key ON ${tableName} (key);
      `;
      await sequelize.query(createTableQuery);
      // Logging the table creation
      logger.info(`Ensured table ${tableName} exists `);

      // Prep for the Measurement Data Insertion
      const measurements = [];
      response.dataList.forEach(dataPoint => {
        const timestamp = parseInt(dataPoint.time);
        dataPoint.measurementPoints.forEach(item => {
          if(isNaN(parseInt(item.value))){
            logger.warn(`Skipping invalid value for device ${deviceSn} at ${timestamp} for key ${item.key} & value ${item.value}`);
            return ;
          }
          measurements.push({
            device_sn : deviceSn,
            timestamp ,
            key : item.key,
            value : parseFloat(item.value),
            unit : item.unit,
          });
        });
      });

      // Bulk Insert Measurements with upsert logic
      if(measurements.length > 0){
        const value = measurements.map(m => `(${m.device_sn}),${m.timestamp},${m.key},${m.value},${m.unit}`).join(", ");
        const insertQuery = `
             BEGIN;
             INSERT INTO ${tableName} (device_sn , timestamp , key , value , unit)
             VALUES ${value}
             ON CONFILCT (device_sn , timestamp , key) DO NOTING;
             COMMIT;
        `;
        await sequelize.query(insertQuery);
        logger.info(`Inserted ${measurements.length} measurements for device ${deviceSn} into table ${tableName}.`);
      }else{
        logger.info(`No valid measurements to insert for device ${deviceSn}.`);
      }
      // If everything is successful, break the loop
      return {deviceSn , response , success : true };
    }catch(err){
      attempts++;
      logger.error(`Error fetching data for device ${deviceSn} (Attempt ${attempts}): ` , err.message);
      if(attempts >= CONFIG.RETRY_ATTEMPTS){
        logger.error(`Max retry attempts reached for device ${deviceSn}. Giving up.`);
        throw err ;
      }
      // Exponential backoff before retrying
       await new Promise(resolve => setTimeout(resolve , CONFIG.RETRY_DELAY * Math.pow(2 , attempts - 1) ));
    }
  }
}


// -----------------------
// Cron Job Setup -> for the periodic data fetching
// -----------------------

  async function sheedularJob(){
    // await for syncDB ->> we need the DB to be synced first
    await syncDB();
    // Then cron job setup
    cron.schedule(CONFIG.CRON_SCHEDULE , async () => {
      logger.info(`Cron Job started for data fetching.${new Date().toISOString()}`);

      try {
      // make ready for the payload to fetch the stations from the DB
      const payload = { 
        "page":1 ,
        "pageSize":50,
        "deviceType":"INVERTER"
      };
      // Here we are making the API request to get the stations list -> deyePost function /station/listwithDevices
      let stationsResponse = await deyePost("/station/listWithDevices" , payload , { timeout : CONFIG.API_TIMEOUT});
      // Check the response -> parse if its int or not
      if(typeof stationsResponse === 'string'){
        stationsResponse = JSON.parse(stationsResponse);
      }
      // extraciton of the data from the response
      const apiStation =stationsResponse?.data?.stationList || stationsResponse?.data || [];
      logger.info(`Fetched ${apiStation.length} stations from Deye API.`);

      // check the apiStation lenght > 0 
      if(apiStation.length > 0){
        // Loop through each station and process devices
        for(const apiStation of apiStation){
          // Upsert station into DB 
          const [station , stationCreated] = await Station.upsert({
            station_id : apiStation.stationId,
            station_name : apiStation.stationName,
          });
          logger.info(`Station ${apiStation.id} ${apiStation.name} ${stationCreated ? 'created' : 'updated'} successfully in DB.`);
          // Process each device in the station
          for(const deviceItem of apiStation.deviceListItems || []){
            const [device , deviceCreated] = await Device.upsert({
              device_sn : deviceItem.deviceSn,
              device_type : deviceItem.deviceType,
              station_id : apiStation.id,
          });
          logger.info(`Device ${deviceItem.deviceSn} ${deviceCreated ? 'created' : 'updated'} successfully in DB for station ${apiStation.id}.`);
          // Message for the showing the data is synced successfully
          logger.info(`Staion & Devices synced sucessfully`);
        }
      }
      }else{
        logger.warn('No stations found in the API response. Skipping the SyncDB');
      }

      // ---------------------
      // Process - 2 
      // ---------------------

      // Querying DB for stations & Devices 
      logger.info('Quering DB for stations and Devices');
      const dbStations = await Station.findAll({
        include :[{
          model :Device ,
          as : 'Devices'
        }],
      });
      // logger after the fetch 
      logger.info(`The data in the ${dbStations}`); 

      // Check the lenght of DB
      if(!dbStations.length){
        logger.warn('No Stations in DB ->> Skipping measurement fetch')
      };

      // Now for the measurement fetch ->> getting the timeStamp  
      const {startTimestamp , endTimestamp } = getCurrentTimestamp();
      logger.info(`Timestamps : Start=${startTimestamp} , End=${endTimestamp}`);

      //---------------------
      // process - 3 
      //---------------------
      for (const dbStation of dbStations){
        const stationId = dbStation.stationId ;
        const stationName = dbStation.station_name ;
        const devices = dbStation.Device || [] ;
      
        //Check the lenght of the deviecs
        if(!devices.length){
          logger.info(`station ${stationId} (${stationName}) has no devices in DB ; skipping`);
          continue;
        }

        //Log the processing info
        logger.info(`Processing station ${stationId} (${stationName}) with ${devices.length} device from DB`);
        
        const limit = pLimit(CONFIG.CONCURRENCY_LIMIT);
        const devicePromises = devices.map(device => limit(()=> fetchDeviceData(device.device_sn ,stationId , ["BatteryVoltage"])));

        const results = await Promise.allSettled(devicePromises);
        const deviceData = results 
          .filter(r => r.status === 'fulfilled')
          .map(r => r.value);
        const failures = results.filter(r => r.status === 'rejected').length;
        logger.info(`Station ${stationId}: ${deviceData.length} successes, ${failures} failures`);

        // save combined data file wise 
        const filename = `station_${stationId}_data_${endTimestamp}.txt}` ;
        const filepath = path.join(CONFIG.LOG_DIR , filename);
        const dataToWrite = {
          timeStamp : new Date().toISOString(),
          stationId ,
          stationName ,
          devices : deviceData
        };
        fs.appendFileSync(filepath, JSON.stringify(dataToWrite, null, 2) + '\n\n');
        logger.info(`Station data saved to ${filepath}`);
      }
      logger.info('Daily station-wise fetch completed');
    }catch{
      logger.error('Error in daily fetch:', err.message); // Simplified
    }
  });
  logger.info('Daily station-wise device data fetch scheduled');
};


// ---------------
// GraceFull shutdown 
// ---------------
process.on('SIGINT' , async () => {
  logger.info('Shutting down gracefully');
  await sequelize.close();
  process.exit(0);
})


// ----------------------
// Start Server
// ----------------------
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await obtainToken();
  await scheduleDailyDeviceDataFetch();
});
  