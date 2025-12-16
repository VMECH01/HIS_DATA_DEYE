<think>
First, the user wants to integrate with PostgreSQL instead of MongoDB. The response from the API is a JSON object with a `dataList` array containing timestamps and item lists.


## Evaluating Database Integration  
- The user prefers PostgreSQL over MongoDB for integration.  
- The API response is a JSON object with a `dataList` array, including timestamps and item lists.
</think>Below, I'll guide you through integrating PostgreSQL into your `scheduleDailyDeviceDataFetch` function, based on the API response structure you provided. The response includes metadata (e.g., `deviceSn`, `deviceId`, `deviceType`) and a `dataList` array of time-series data points (each with `time`, `itemList` containing measurements like `BatteryVoltage`).

### Key Considerations for PostgreSQL Integration
- **Why PostgreSQL?** It's relational, ACID-compliant, and great for structured data. For time-series data (like your `dataList`), we can use efficient indexing (e.g., on timestamps) and queries. If you have high-volume data, consider extensions like TimescaleDB for better performance.
- **Data Structure**: The API response has:
  - **Metadata**: Device info (e.g., `deviceSn`, `deviceId`, `deviceType`) – store this once per device.
  - **Time-Series Data**: `dataList` is an array of entries, each with a `time` (Unix timestamp) and `itemList` (array of key-value pairs like `{"key": "BatteryVoltage", "value": "241.30", "unit": "V"}`). We'll normalize this into relational tables to avoid duplication and enable querying (e.g., "get all BatteryVoltage for device X between dates").
- **Normalization**: Avoid storing raw JSON blobs (like in MongoDB). Use tables for devices and measurements to support joins, aggregations, and analytics.
- **ORM**: I'll use Sequelize (a popular Node.js ORM for PostgreSQL) for ease of use, migrations, and queries. Install it: `npm install sequelize pg`.
- **Assumptions**: 
  - You have a PostgreSQL instance running (e.g., locally or via Docker).
  - Environment variables for DB connection (e.g., via `dotenv`).
  - We'll handle upserts (insert or update) to avoid duplicates.
  - For production, add connection pooling, migrations, and backups.
- **Performance**: With many devices/data points, use indexes on `device_id` and `timestamp`. If data grows large, partition tables by date.

### Entity-Relationship Diagram (ERD)
Here's a simple ERD for the schema. It shows relationships between tables:

```
+----------------+       +---------------------+
|   Devices      |       | DeviceMeasurements  |
+----------------+       +---------------------+
| device_id (PK) |<------| measurement_id (PK) |
| device_sn      |       | device_id (FK)      |
| device_type    |       | timestamp           |
| created_at     |       | key                 |
| updated_at     |       | value               |
+----------------+       | unit                |
                         | created_at          |
                         +---------------------+
```

- **Devices Table**: Stores unique device metadata. Primary Key: `device_id` (auto-incrementing).
- **DeviceMeasurements Table**: Stores each measurement data point. Foreign Key: `device_id` links to Devices. Primary Key: `measurement_id` (auto-incrementing). This handles the `dataList` array – each `itemList` entry becomes a row.
- **Relationships**: One-to-many (1 Device : Many Measurements). No other tables needed for this use case, but you could add a `FetchLogs` table for tracking API calls if desired.
- **Why This Design?** 
  - Efficient for queries (e.g., "average BatteryVoltage per day").
  - Avoids redundancy (e.g., don't store `deviceSn` in every measurement row).
  - Scalable: Add more measurement types (keys) without schema changes.

### PostgreSQL Schemas (Using Sequelize)
1. **Install Dependencies**:
   ```
   npm install sequelize pg dotenv
   ```

2. **Database Configuration**:
   - Create a `.env` file:
     ```
     DB_HOST=localhost
     DB_PORT=5432
     DB_NAME=device_data
     DB_USER=your_user
     DB_PASSWORD=your_password
     ```
   - Ensure your PostgreSQL DB exists: `createdb device_data`.

3. **Sequelize Models**:
   Create a file like `models/index.js` to define the models and sync the DB.

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
       logging: false, // Disable SQL logs in production
       pool: { max: 5, min: 0, acquire: 30000, idle: 10000 }, // Connection pooling
     }
   );

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
       unique: true, // Ensure uniqueness
     },
     device_type: {
       type: DataTypes.STRING,
       allowNull: false,
     },
   }, {
     tableName: 'devices',
     timestamps: true, // Adds created_at, updated_at
   });

   // DeviceMeasurements Model
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
       type: DataTypes.DATE, // Store as Date for easier querying
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
       { fields: ['device_id', 'timestamp'] }, // For efficient time-series queries
       { fields: ['key'] }, // For filtering by measurement type
     ],
   });

   // Define Relationships
   Device.hasMany(DeviceMeasurement, { foreignKey: 'device_id' });
   DeviceMeasurement.belongsTo(Device, { foreignKey: 'device_id' });

   // Sync DB (run once to create tables)
   async function syncDB() {
     try {
       await sequelize.authenticate();
       await sequelize.sync({ alter: true }); // Use { force: true } for dev resets
       console.log('Database synced successfully.');
     } catch (err) {
       console.error('Database sync error:', err);
     }
   }

   module.exports = { sequelize, Device, DeviceMeasurement, syncDB };
   ```

   - **Notes on Schema**:
     - `device_sn` is unique to prevent duplicates.
     - `timestamp` is stored as a `DATE` (converted from Unix timestamp) for PostgreSQL's date functions.
     - Indexes on `device_id` + `timestamp` enable fast range queries (e.g., "data for device X in last 24 hours").
     - Run `syncDB()` once to create tables. In production, use migrations (e.g., `sequelize-cli`).

### Updated Code for `scheduleDailyDeviceDataFetch`
Integrate the models into your function. Update the `fetchDeviceData` function to parse the response and insert into PostgreSQL.

```javascript
// ... (Keep previous imports like cron, winston, pLimit, etc.)
const { Device, DeviceMeasurement, syncDB } = require('./models'); // Adjust path

// ... (Keep CONFIG, logger, getTimestamps, etc. from previous response)

// Utility: Fetch and store data for a single device
async function fetchDeviceData(device) {
  const { deviceSn, measurePoints } = device;
  const { startTimestamp, endTimestamp } = getTimestamps();
  const payload = { deviceSn, startTimestamp, endTimestamp, measurePoints };

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

    // Insert Measurements (bulk insert for efficiency)
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
      await DeviceMeasurement.bulkCreate(measurements, { ignoreDuplicates: true }); // Skip duplicates if any
      logger.info(`Inserted ${measurements.length} measurements for ${deviceSn}`);
    } else {
      logger.warn(`No measurements to insert for ${deviceSn}`);
    }

    return response;
  } catch (err) {
    logger.error(`Failed to fetch/store data for ${deviceSn}: ${err.message}`);
    throw err;
  }
}

// Main function (updated)
async function scheduleDailyDeviceDataFetch(devices) {
  await syncDB(); // Sync DB on startup

  // ... (Rest of the function remains similar: cron schedule, concurrency with pLimit, Promise.allSettled, etc.)
}

// ... (Keep graceful shutdown, example usage)
```

### Explanation of Changes
- **DB Integration**: `fetchDeviceData` now upserts the device and bulk-inserts measurements. This handles the `dataList` by flattening `itemList` into rows.
- **Data Parsing**: Converts Unix timestamps to JS Dates. Parses `value` as float for numerical operations.
- **Efficiency**: Bulk inserts reduce DB calls. `ignoreDuplicates` prevents re-inserting the same data if re-run.
- **Error Handling**: Logs issues and throws for retries.
- **Queries Example**: After integration, you can query like:
  ```javascript
  // Get all BatteryVoltage for a device in the last 24 hours
  const measurements = await DeviceMeasurement.findAll({
    where: {
      device_id: 1,
      key: 'BatteryVoltage',
      timestamp: { [Op.gte]: new Date(Date.now() - 24 * 60 * 60 * 1000) },
    },
    include: Device, // Join for device info
  });
  ```

### Next Steps and Best Practices
- **Migrations**: Use `sequelize-cli` for version-controlled schema changes.
- **Testing**: Mock the API and DB in tests. Verify data insertion.
- **Monitoring**: Add metrics (e.g., via Prometheus) for DB performance.
- **Scaling**: For millions of rows, use TimescaleDB or partitioning. Add vacuuming for PostgreSQL maintenance.
- **Security**: Use SSL for DB connections, parameterized queries (Sequelize handles this).
- **Backup**: Set up automated PostgreSQL backups.

If you need the full updated file, migration scripts, or help with specific queries, let me know!