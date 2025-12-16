// --------------------------------
// Demo of Practical JavaScript Concepts
// --------------------------------
import express from 'express';
import axios  from 'axios';
import dotenv from 'dotenv';
import crypto from 'crypto';
import cron from 'node-cron';
import fs from 'fs';
import path, { parse } from 'path';
import pkg from 'winston';
import { Sequelize ,DataTypes } from 'sequelize';
import {fileURLToPath} from 'url';
import __dirname from 'path';

// File path setup for ES modules
const __filename = fileURLToPath(import.meta.url);
// Path setup
const __dirname = path.dirname(__filename);
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
  CRON_SCHEDULE: process.env.CRON_SCHEDULE || '0 * * * *',
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
  format : format.combine
});
