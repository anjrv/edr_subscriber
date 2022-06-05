/* eslint-disable no-console */
import * as mqtt from 'mqtt';
import dotenv from 'dotenv';
import pako from 'pako';
import { Worker } from 'worker_threads';

import { logger } from './logger.js';

dotenv.config();

const {
  SERVER_URL: mosquitto = 'mqtt://127.0.0.1:1883',
  USERNAME: username = 'username',
  PASSWORD: password = 'password',
} = process.env;

const clientId = `node_${Math.random().toString(16).slice(2)}`;
const mqttClient = mqtt.connect(mosquitto, {
  clientId,
  username,
  password,
});

const worker = new Worker('./src/worker.js');

mqttClient.on('connect', () => {
  mqttClient.subscribe('EDR', (err) => {
    if (err) logger.error('Unable to connect to mosquitto', err);
  });
});

mqttClient.on('message', (_topic, message) => {
  try {
    const msg = pako.ungzip(message, { to: 'string' });
    worker.postMessage(msg);
  } catch (err) {
    logger.error('Unable to unzip MQTT message', err);
  }
});
