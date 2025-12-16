// ----------------------
// Deye Cloud Backend Server (Improved Version)
// ----------------------

import express from "express";
import axios from "axios";
import dotenv from "dotenv";
import crypto from "crypto";
import fs from 'fs';
import pino from 'pino';
import dayjs from "dayjs";

dotenv.config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5001;

// ----------------------
// Log Config -> for logging
// ----------------------

// Simple logging to a file (kept as-is, but integrated with pino for consistency)
fs.appendFileSync('app.log', `[${new Date().toISOString()}] - INFO - Server started.\n`);

const logger = pino({
  level: 'info',
  transport: {
    target: 'pino/file',
    options: { destination: 'request.log' }
  },
  timestamp: () => `,"time":"${dayjs().format()}"`
});

// Middleware to measure and log request duration
app.use((req, res, next) => {
  const start = process.hrtime.bigint();
  res.on('finish', () => {
    const end = process.hrtime.bigint();
    const durationMs = Number(end - start) / 1e6;
    logger.info({
      method: req.method,
      url: req.url,
      status: res.statusCode,
      duration: `${durationMs.toFixed(2)}ms`
    }, 'Request completed');
  });
  next();
});

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

// ----------------------
// Obtain Access Token
// ----------------------

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
    }
  } catch (err) {
    logger.error("Token fetch error:", err.response?.data || err.message);
  }
}

// ----------------------
// Ensure valid token
// ----------------------

async function ensureToken() {
  if (!accessToken || Date.now() > tokenExpiry) {
    await obtainToken();
  }
}

// ----------------------
// Helper POST request
// ----------------------

async function deyePost(endpoint, payload = {}) {
  await ensureToken();

  const url = `${DEYE_BASE_URL}${endpoint}`;
  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${accessToken}`
  };

  logger.info({ url, payload, hasToken: !!accessToken }, "Making Deye API request");

  try {
    const response = await axios.post(url, payload, {
      headers,
      timeout: 10000
    });

    logger.info({ status: response.status }, "Deye API response received");
    return response.data;
  } catch (err) {
    logger.error({
      endpoint,
      status: err.response?.status,
      responseData: err.response?.data,
      message: err.message
    }, "Deye API Error");
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
    res.json({ message: "Hello", timestamp: dayjs().format() });
  }, 3000);
});

// Get all stations
app.get("/api/stations", async (req, res) => {
  logger.info('/api/stations called');
  try {
    const data = await deyePost("/station/list", {});
    res.json({ success: true, data: data.data || data.stationList || [] });
  } catch (err) {
    res.status(500).json({
      success: false,
      error: err.response?.data || err.message,
    });
  }
});

// Fetch device history raw data (improved with timestamp conversion and validation)
app.post("/api/device/historyRaw", async (req, res) => {
  try {
    const { deviceSn, startTimestamp, endTimestamp, measurePoints } = req.body;
    logger.info({ deviceSn, startTimestamp, endTimestamp, measurePoints }, "Fetching device history raw data");

    if (!deviceSn || !startTimestamp || !endTimestamp || !measurePoints || !Array.isArray(measurePoints) || measurePoints.length === 0) {
      return res.status(400).json({
        success: false,
        error: "deviceSn, startTimestamp, endTimestamp, and measurePoints (non-empty array) are required"
      });
    }

    // Validate and convert timestamps (expect 10-digit Unix seconds)
    const startTs = Number(startTimestamp);
    const endTs = Number(endTimestamp);
    if (isNaN(startTs) || isNaN(endTs) || startTs >= endTs) {
      return res.status(400).json({
        success: false,
        error: "Invalid timestamps: startTimestamp must be before endTimestamp (use 10-digit Unix seconds)"
      });
    }

    // Validate range: within 5 days
    const maxRange = 5 * 24 * 60 * 60 * 1000;
    if (endTs - startTs > maxRange) {
      return res.status(400).json({
        success: false,
        error: "Time range must be within 5 days"
      });
    }

    const payload = {
      deviceSn,
      startTimestamp: startTs,
      endTimestamp: endTs,
      measurePoints
    };

    const response = await deyePost("/v1.0/device/historyRaw", payload);

    // Enhance response with readable timestamps
    const enhancedDataList = response.dataList?.map(entry => ({
      ...entry,
      collectionTimeReadable: dayjs.unix(entry.collectionTime).format('YYYY-MM-DD HH:mm:ss')
    })) || [];

    res.status(200).json({
      success: true,
      data: { ...response, dataList: enhancedDataList }
    });
  } catch (err) {
    logger.error("Device History Raw Error:", err.response?.data || err.message);
    res.status(500).json({
      success: false,
      error: err.response?.data?.message || err.message,
    });
  }
});

// Get device by serial number
app.get("/api/devices/:deviceSn", async (req, res) => {
  try {
    const { deviceSn } = req.params;
    const { stationId } = req.query;

    logger.info({ deviceSn, stationId }, "Fetching device by SN");

    if (!deviceSn) {
      return res.status(400).json({
        success: false,
        error: "deviceSn is required"
      });
    }

    if (!stationId) {
      return res.status(400).json({
        success: false,
        error: "stationId query parameter is required"
      });
    }

    const payload = {
      page: 1,
      size: 100,
      stationIds: [Number(stationId)]
    };

    const response = await deyePost("/station/device", payload);

    const device = (response.deviceListItems || []).find(
      item => item.deviceSn === deviceSn
    );

    if (!device) {
      return res.status(404).json({
        success: false,
        error: `Device with SN ${deviceSn} not found in station ${stationId}`
      });
    }

    // Enhanced device info
    const enhancedDevice = {
      ...device,
      connectStatusText: device.connectStatus === 1 ? 'Online' : device.connectStatus === 0 ? 'Offline' : 'Alert',
      isOnline: device.connectStatus === 1,
      isOffline: device.connectStatus === 0,
      isAlert: device.connectStatus === 2,
      productType: device.productId // Add more mapping if needed
    };

    res.status(200).json({
      success: true,
      data: enhancedDevice
    });
  } catch (err) {
    logger.error("Device by SN Error:", err.response?.data || err.message);
    res.status(500).json({
      success: false,
      error: err.response?.data?.message || err.message,
    });
  }
});

// Fetch latest data of devices (improved: fixed path, added timestamp conversion, better logging)
app.get("/api/device/latest", async (req, res) => {
  try {
    const { deviceList } = req.query;
    const devices = deviceList ? deviceList.split(',').map(sn => sn.trim()).filter(sn => sn) : [];

    logger.info({ devices }, "Fetching latest device data");

    if (!devices || devices.length === 0 || devices.length > 10) {
      return res.status(400).json({
        success: false,
        error: "deviceList query parameter must contain 1 to 10 comma-separated device serial numbers"
      });
    }

    const payload = { deviceList: devices };
    const response = await deyePost("/v1.0/device/latest", payload); // Fixed path to match docs

    // Warn if empty
    if (!response.deviceDataList || response.deviceDataList.length === 0) {
      logger.warn("No data returned from /v1.0/device/latest. Possible reasons: Devices offline, no recent data, or invalid SNs.");
    }

    // Enhance with readable timestamps
    const enhancedDataList = response.deviceDataList?.map(device => ({
      ...device,
      collectionTimeReadable: dayjs.unix(device.collectionTime).format('YYYY-MM-DD HH:mm:ss')
    })) || [];

    res.status(200).json({
      success: true,
      data: { ...response, deviceDataList: enhancedDataList }
    });
  } catch (err) {
    logger.error("Device Latest Error:", err.response?.data || err.message);
    res.status(500).json({
      success: false,
      error: err.response?.data?.message || err.message,
    });
  }
});

// New: Combined endpoint for voltage data (latest or recent historical)
app.get("/api/device/voltage", async (req, res) => {
  try {
    const { deviceSn, measurePoints } = req.query;
    const points = measurePoints ? measurePoints.split(',').map(p => p.trim()).filter(p => p) : ['MI Voltage L2']; // Default to common voltage key

    logger.info({ deviceSn, points }, "Fetching voltage data");

    if (!deviceSn) {
      return res.status(400).json({
        success: false,
        error: "deviceSn query parameter is required"
      });
    }

    // Try latest data first
    const latestResponse = await deyePost("/v1.0/device/latest", { deviceList: [deviceSn] });
    const latestData = latestResponse.deviceDataList?.[0];

    if (latestData && latestData.dataList) {
      // Filter for voltage points
      const voltageData = latestData.dataList.filter(item => points.includes(item.key));
      if (voltageData.length > 0) {
        return res.status(200).json({
          success: true,
          type: 'latest',
          data: {
            deviceSn,
            collectionTime: latestData.collectionTime,
            collectionTimeReadable: dayjs.unix(latestData.collectionTime).format('YYYY-MM-DD HH:mm:ss'),
            voltageData
          }
        });
      }
    }

    // Fallback to recent historical data (last 1 hour)
    const now = dayjs().unix();
    const oneHourAgo = now - 3600;
    const historyPayload = {
      deviceSn,
      startTimestamp: oneHourAgo,
      endTimestamp: now,
      measurePoints: points
    };
    const historyResponse = await deyePost("/v1.0/device/historyRaw", historyPayload);

    const enhancedHistory = historyResponse.dataList?.map(entry => ({
      ...entry,
      collectionTimeReadable: dayjs.unix(entry.collectionTime).format('YYYY-MM-DD HH:mm:ss')
    })) || [];

    res.status(200).json({
      success: true,
      type: 'historical',
      data: { deviceSn, dataList: enhancedHistory }
    });
  } catch (err) {
    logger.error("Device Voltage Error:", err.response?.data || err.message);
    res.status(500).json({
      success: false,
      error: err.response?.data?.message || err.message,
    });
  }
});

// ----------------------
// Start Server
// ----------------------
app.listen(PORT, async () => {
  console.log(`🚀 Server running on port ${PORT}`);
  await obtainToken();
});


<!-- api links for checking data -->


<think>
First, the user is asking for URLs to test the APIs. Based on the improved code I provided, the server runs on port 5001 by default (from `const PORT = process.env.PORT || 5001;`).


## Identifying API Test URLs  
- The user requested URLs to test the APIs.  
- The server runs on port 5001 by default, as set by `const PORT = process.env.
</think>Below, I'll provide example URLs and instructions to test the improved APIs from your server code. Assuming your server is running locally on `http://localhost:5001` (based on the default `PORT` in your code), you can use these URLs directly. If you're running on a different port or host (e.g., in production), adjust accordingly (e.g., `https://yourdomain.com:5001`).

I'll include:
- **Method** (GET/POST).
- **Full URL** with placeholders (replace with real values, e.g., your device serial number from DeyeCloud).
- **How to Test**: Use a tool like Postman, curl, or your browser (for GET requests). For POST, you'll need to send a JSON body.
- **Expected Behavior**: Brief notes on what to expect, based on the code.
- **Prerequisites**: Ensure your server is running (`node yourfile.js`), and you have valid DeyeCloud credentials (e.g., a working device SN like `2917760161` from your account).

### 1. **Health Check**
   - **Method**: GET
   - **URL**: `http://localhost:5001/statusCheck`
   - **How to Test**: Open in browser or use curl: `curl http://localhost:5001/statusCheck`
   - **Expected**: Returns `{"message": "Hello", "timestamp": "2023-12-25T10:30:00Z"}` after a 3-second delay. Confirms the server is running.

### 2. **Get All Stations**
   - **Method**: GET
   - **URL**: `http://localhost:5001/api/stations`
   - **How to Test**: Open in browser or use curl: `curl http://localhost:5001/api/stations`
   - **Expected**: Returns a list of your DeyeCloud stations (e.g., `{"success": true, "data": [...]}`). Use this to get `stationId` for other endpoints.

### 3. **Get Device by Serial Number**
   - **Method**: GET
   - **URL**: `http://localhost:5001/api/devices/{deviceSn}?stationId={stationId}`
     - Replace `{deviceSn}` with a real SN (e.g., `2917760161`).
     - Replace `{stationId}` with a valid station ID from `/api/stations` (e.g., `12345`).
     - Example: `http://localhost:5001/api/devices/2917760161?stationId=12345`
   - **How to Test**: Open in browser or use curl: `curl "http://localhost:5001/api/devices/2917760161?stationId=12345"`
   - **Expected**: Returns details for the device (e.g., online status, product type). If not found, returns 404.

### 4. **Fetch Latest Data of Devices (Batch)**
   - **Method**: GET
   - **URL**: `http://localhost:5001/api/device/latest?deviceList={deviceSn1},{deviceSn2}`
     - Replace with comma-separated SNs (up to 10, e.g., `2917760161,anotherSn`).
     - Example: `http://localhost:5001/api/device/latest?deviceList=2917760161`
   - **How to Test**: Open in browser or use curl: `curl "http://localhost:5001/api/device/latest?deviceList=2917760161"`
   - **Expected**: Returns latest data (e.g., voltage with timestamps). If empty, check device status— it may be offline or have no recent data.

### 5. **Fetch Device History Raw Data**
   - **Method**: POST
   - **URL**: `http://localhost:5001/api/device/historyRaw`
   - **Body** (JSON): 
     ```json
     {
       "deviceSn": "2917760161",
       "startTimestamp": 1679980000,  // Unix seconds (e.g., start of a day)
       "endTimestamp": 1680066400,    // Unix seconds (e.g., end of a day, within 5 days)
       "measurePoints": ["MI Voltage L2", "Battery Voltage"]  // Voltage keys; adjust based on your device
     }
     ```
     - Replace timestamps with valid Unix seconds (10-digit). Use tools like https://www.unixtimestamp.com/ to generate.
   - **How to Test**: Use Postman (set to POST, add JSON body) or curl:
     ```
     curl -X POST http://localhost:5001/api/device/historyRaw \
     -H "Content-Type: application/json" \
     -d '{"deviceSn": "2917760161", "startTimestamp": 1679980000, "endTimestamp": 1680066400, "measurePoints": ["MI Voltage L2"]}'
     ```
   - **Expected**: Returns historical voltage data with timestamps (e.g., `{"success": true, "data": {"dataList": [...]}}`). Great for trends.

### 6. **Fetch Voltage Data (Combined Latest/Historical)**
   - **Method**: GET
   - **URL**: `http://localhost:5001/api/device/voltage?deviceSn={deviceSn}&measurePoints={points}`
     - Replace `{deviceSn}` with a real SN (e.g., `2917760161`).
     - Replace `{points}` with comma-separated voltage keys (e.g., `MI Voltage L2,Battery Voltage`). Defaults to `MI Voltage L2` if omitted.
     - Example: `http://localhost:5001/api/device/voltage?deviceSn=2917760161&measurePoints=MI Voltage L2`
   - **How to Test**: Open in browser or use curl: `curl "http://localhost:5001/api/device/voltage?deviceSn=2917760161&measurePoints=MI Voltage L2"`
   - **Expected**: Returns latest voltage if available; otherwise, recent historical data (last 1 hour). Includes readable timestamps.

### General Testing Tips
- **Tools**: Use Postman for easy POST requests. For GET, a browser works.
- **Errors**: If you get 500 errors, check your server logs (`request.log` and `app.log`). Common issues: Invalid SN, expired token, or device offline.
- **Placeholders**: Always use real values from your DeyeCloud account. Get SNs from the DeyeCloud portal or `/api/stations`.
- **Rate Limiting**: DeyeCloud may limit requests—don't spam. Your code has basic logging; monitor for timeouts.
- **Production**: If deploying, update the base URL (e.g., to your hosted server) and ensure HTTPS.
- **Voltage Keys**: Common ones include "MI Voltage L2", "Battery Voltage", etc. Check DeyeCloud docs or your device's data for exact keys.

If these don't work (e.g., still empty responses), share the exact error logs or response from one of these URLs. Let me know!