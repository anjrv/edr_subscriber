/* eslint-disable no-console */
import * as mqtt from 'mqtt';
import dotenv from 'dotenv';
import pako from 'pako';
import { Worker } from 'worker_threads';

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

const worker = new Worker('./src/worker.js');

mqttClient.on('connect', () => {
  mqttClient.subscribe('EDR', (err) => {
    if (err) console.err(err);
  });
});

mqttClient.on('message', (_topic, message) => {
  const msg = JSON.parse(pako.ungzip(message.data, { to: 'string' }));
  worker.postMessage(msg);
});
