/* eslint-disable no-console */
import * as mqtt from 'mqtt';
import dotenv from 'dotenv';
import { Worker } from 'worker_threads';

import { logger } from './logger.js';

dotenv.config();

const {
  SERVER_URL: mosquitto = 'mqtt://127.0.0.1:1883',
  USERNAME: username = 'username',
  PASSWORD: password = 'password',
} = process.env;

const mqttClientId = `node${Math.random().toString(16).slice(2)}`;
const mqttClient = mqtt.connect(mosquitto, {
  mqttClientId,
  username,
  password,
});

mqttClient.on('connect', () => {
  mqttClient.subscribe('EDR', (err) => {
    if (err) console.err(err);
  });
});

mqttClient.on('message', (_topic, message) => {
  const worker = new Worker('./src/worker.js', { workerData: { message } });

  worker.on('error', (error) => {
    logger.error('Unable to process message', error);
  });
});
