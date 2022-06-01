/* eslint-disable no-console */
import * as mqtt from 'mqtt';
import dotenv from 'dotenv';
import pako from 'pako';
import { MongoClient } from 'mongodb';

dotenv.config();

const {
  SERVER_URL: mosquitto = 'mqtt://127.0.0.1:1883',
  DATABASE_URL: db = 'mongodb://127.0.0.1:27017',
  USERNAME: username = 'username',
  PASSWORD: password = 'password',
} = process.env;

const mongoClient = new MongoClient(db);

async function run(measurements) {
  try {
    await mongoClient.connect();

    const database = mongoClient.db('hvassahraun');
    const edr = database.collection('edr');

    const options = { ordered: true };
    const result = await edr.insertMany(measurements, options);
    console.log(`${result.insertedCount} measurements were inserted`);
  } finally {
    await mongoClient.close();
  }
}

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
  const msg = JSON.parse(pako.ungzip(message, { to: 'string' }));
  const {
    brand, manufacturer, model, id, version, data,
  } = msg;

  // Unroll user information back into every measurement
  const entry = data.map((obj) => Object.assign(obj, {
    brand, manufacturer, model, id, version,
  }));

  console.log(entry[0]);
  run(entry).catch(console.dir);
});
