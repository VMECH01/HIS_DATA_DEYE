<think>
First, the user wants the "full API with the db config with it." This seems to mean the complete, integrated code for the `scheduleDailyDeviceDataFetch` function, including PostgreSQL configuration, models, and all necessary parts.


## Clarifying User Request  
- The user is asking for the complete code of the `scheduleDailyDeviceDataFetch` function.
</think>Below is the **complete, integrated code** for your `scheduleDailyDeviceDataFetch` function, now fully integrated with PostgreSQL using Sequelize. This includes:

- **Database Configuration**: Using Sequelize for models, connections, and syncing.
- **Full API Logic**: The updated function with retries, concurrency, logging, and data insertion into PostgreSQL.
- **Setup Instructions**: How to set it up, including dependencies, environment variables, and running it.
- **Additional Files**: I've included the models file and a sample `.env` file for clarity.

This is designed to be production-ready, based on our previous discussions. It assumes you have a PostgreSQL instance running (e.g., via Docker or a local install). The code parses the API response (as you provided) and stores it in normalized tables for efficient querying.

### Prerequisites
1. **Install Dependencies**:
   ```
   npm init -y
   npm install node-cron winston p-limit sequelize pg dotenv
   npm install --save-dev sequelize-cli  # For migrations (optional but recommended)
   ```

2. **PostgreSQL Setup**:
   - Install PostgreSQL (e.g., `brew install postgresql` on macOS, or use Docker: `docker run --name postgres -e POSTGRES_PASSWORD=your_password -d -p 5432:5432 postgres`).
   - Create a database: `createdb device_data` (or use a GUI like pgAdmin).
   - Ensure the DB user has permissions.

3. **Environment Variables**:
   Create a `.env` file in your project root:
   ```
   # Database
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=device_data
   DB_USER=your_user  # e.g., postgres
   DB_PASSWORD=your_password

   # App Config
   CRON_SCHEDULE=0 0 * * *  # Midnight UTC
   LOG_LEVEL=info
   LOG_DIR=./logs
   API_TIMEOUT=30000  # ms
   CONCURRENCY_LIMIT=5
   RETRY_ATTEMPTS=3
   RETRY_DELAY=1000  # ms
   ```

4. **Project Structure**:
   ```
   your-project/
   ├── .env
   ├── models/
   │   └── index.js  # Sequelize models
   ├── index.js      # Main file with the function
   └── package.json
   ```

### Full Code

#### 1. `models/index.js` (Sequelize Models and DB Setup)
```javascript
const { Sequelize, DataTypes } = require('sequelize');
require('dotenv').config();

const sequelize = new Sequelize(
  process.env.DB_NAME,
  process.env.DB_USER,
  process.env.DB_PASSWORD,
  {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    dialect: 'postgres',
    logging: false, // Disable SQL logs in production; set to console.log for debugging
    pool: { max: 5, min: 0, acquire: 30000, idle: 10000 }, // Connection pooling for performance
  }
);

// Devices Model (for metadata)
const Device = sequelize.define('Device', {
  device_id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  device_sn: {
    type: DataTypes.STRING,
    allowNull: false,
    unique: true, // Prevent duplicate devices
  },
  device_type: {
    type: DataTypes.STRING,
    allowNull: false,
  },
}, {
  tableName: 'devices',
  timestamps: true, // Adds created_at, updated_at
});

// DeviceMeasurements Model (for time-series data)
const DeviceMeasurement = sequelize.define('DeviceMeasurement', {
  measurement_id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  device_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
    references: { model: Device, key: 'device_id' },
  },
  timestamp: {
    type: DataTypes.DATE, // Store as Date for easy querying
    allowNull: false,
  },
  key: {
    type: DataTypes.STRING,
    allowNull: false, // e.g., "BatteryVoltage"
  },
  value: {
    type: DataTypes.DECIMAL(10, 2), // Adjust precision as needed
    allowNull: false,
  },
  unit: {
    type: DataTypes.STRING,
    allowNull: false, // e.g., "V"
  },
}, {
  tableName: 'device_measurements',
  timestamps: true,
  indexes: [
    { fields: ['device_id', 'timestamp'] }, // Efficient for time-series queries
    { fields: ['key'] }, // For filtering by measurement type
  ],
});

// Define Relationships
Device.hasMany(DeviceMeasurement, { foreignKey: 'device_id' });
DeviceMeasurement.belongsTo(Device, { foreignKey: 'device_id' });

// Sync DB (run once to create tables; use migrations in production)
async function syncDB() {
  try {
    await sequelize.authenticate();
    console.log('Database connection established.');
    await sequelize.sync({ alter: true }); // Creates/updates tables; use { force: true } for dev resets
    console.log('Database synced successfully.');
  } catch (err) {
    console.error('Database sync error:', err);
    process.exit(1);
  }
}

module.exports = { sequelize, Device, DeviceMeasurement, syncDB };
```

#### 2. `index.js` (Main File with the Full API)
```javascript
const cron = require('node-cron');
const winston = require('winston');
const pLimit = require('p-limit');
const { Device, DeviceMeasurement, syncDB } = require('./models');
require('dotenv').config();

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

// Set up Winston logger
const logger = winston.createLogger({
  level: CONFIG.LOG_LEVEL,
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: `${CONFIG.LOG_DIR}/daily-fetch.log` }),
  ],
});

// Ensure log directory exists
const fs = require('fs');
const path = require('path');
if (!fs.existsSync(CONFIG.LOG_DIR)) {
  fs.mkdirSync(CONFIG.LOG_DIR, { recursive: true });
}

// Utility: Calculate timestamps (UTC-based, for previous day)
function getTimestamps() {
  const now = new Date();
  const endTimestamp = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);
  const startTimestamp = endTimestamp - (24 * 60 * 60);
  return { startTimestamp, endTimestamp };
}

// Utility: Fetch and store data for a single device with retries
async function fetchDeviceData(device) {
  const { deviceSn, measurePoints } = device;
  const { startTimestamp, endTimestamp } = getTimestamps();
  const payload = { deviceSn, startTimestamp, endTimestamp, measurePoints };

  let attempts = 0;
  while (attempts < CONFIG.RETRY_ATTEMPTS) {
    try {
      logger.info(`Fetching data for device: ${deviceSn}`, { payload });
      const response = await deyePost("/device/historyRaw", payload, { timeout: CONFIG.API_TIMEOUT });

      // Validate response
      if (!response || response.code !== "1000000" || !response.dataList) {
        throw new Error(`Invalid API response: ${JSON.stringify(response)}`);
      }

      // Upsert Device (insert if not exists, update if needed)
      const [dbDevice, created] = await Device.upsert({
        device_sn: response.deviceSn,
        device_type: response.deviceType,
      });
      logger.info(`Device ${deviceSn} ${created ? 'created' : 'updated'} in DB`);

      // Prepare Measurements for bulk insert
      const measurements = [];
      response.dataList.forEach(dataPoint => {
        const timestamp = new Date(parseInt(dataPoint.time) * 1000); // Convert Unix to JS Date
        dataPoint.itemList.forEach(item => {
          measurements.push({
            device_id: dbDevice.device_id,
            timestamp,
            key: item.key,
            value: parseFloat(item.value),
            unit: item.unit,
          });
        });
      });

      if (measurements.length > 0) {
        await DeviceMeasurement.bulkCreate(measurements, { ignoreDuplicates: true }); // Skip duplicates
        logger.info(`Inserted ${measurements.length} measurements for ${deviceSn}`);
      } else {
        logger.warn(`No measurements to insert for ${deviceSn}`);
      }

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

// Main function: Schedule daily device data fetch
async function scheduleDailyDeviceDataFetch(devices) {
  await syncDB(); // Sync DB on startup

  // Schedule the job
  cron.schedule(CONFIG.CRON_SCHEDULE, async () => {
    logger.info('Starting daily device data fetch');
    const { startTimestamp, endTimestamp } = getTimestamps();
    logger.info(`Timestamps: start=${startTimestamp} (${new Date(startTimestamp * 1000).toISOString()}), end=${endTimestamp} (${new Date(endTimestamp * 1000).toISOString()})`);

    const limit = pLimit(CONFIG.CONCURRENCY_LIMIT); // Limit concurrency
    const promises = devices.map(device => limit(() => fetchDeviceData(device)));

    try {
      const results = await Promise.allSettled(promises);
      const successes = results.filter(r => r.status === 'fulfilled').length;
      const failures = results.filter(r => r.status === 'rejected').length;
      logger.info(`Daily fetch completed: ${successes} successes, ${failures} failures`);

      // Optional: Send alerts if failures > threshold (integrate with email service like nodemailer)
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
  const { sequelize } = require('./models');
  await sequelize.close();
  process.exit(0);
});

// Example usage
const devices = [
  { deviceSn: "2306178462", measurePoints: ["BatteryVoltage"] },
  { deviceSn: "2401276187", measurePoints: ["BatteryVoltage"] },
];
scheduleDailyDeviceDataFetch(devices);
```

### How to Run
1. **Start PostgreSQL**: Ensure it's running (e.g., `docker start postgres`).
2. **Run the App**: `node index.js`. It will sync the DB, schedule the cron job, and start fetching at midnight.
3. **Test Manually**: To test immediately, call `scheduleDailyDeviceDataFetch(devices)` without cron, or trigger the cron logic manually.
4. **Check Logs**: Logs will be in `./logs/daily-fetch.log`.
5. **Query Data**: Use a tool like pgAdmin or connect via Sequelize:
   ```javascript
   const { DeviceMeasurement } = require('./models');
   // Example: Get all BatteryVoltage for device 1 in last 24 hours
   const data = await DeviceMeasurement.findAll({
     where: { device_id: 1, key: 'BatteryVoltage' },
     order: [['timestamp', 'ASC']],
   });
   console.log(data);
   ```

### Additional Notes
- **Migrations**: For production, use `sequelize-cli` to generate migrations instead of `sync()`. Run `npx sequelize-cli init` and create migrations for the tables.
- **Performance**: For large datasets, consider PostgreSQL partitioning or TimescaleDB.
- **Security**: Use SSL for DB connections in production (add `ssl: true` to Sequelize config).
- **Customization**: If `deyePost` isn't defined, replace it with your actual API call (e.g., using Axios).
- **Troubleshooting**: If DB connection fails, check `.env` and PostgreSQL logs.

This should give you a fully functional, robust API for daily data fetching with PostgreSQL. If you need tweaks or more features (e.g., API endpoints for querying data), let me know!