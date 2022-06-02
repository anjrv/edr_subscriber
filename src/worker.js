/* eslint-disable no-console */
import dotenv from 'dotenv';
import pako from 'pako';
import { parentPort, workerData } from 'worker_threads';
import { MongoClient } from 'mongodb';

dotenv.config();

const {
  DATABASE_URL: db = 'mongodb://127.0.0.1:27017',
} = process.env;

const mongoClient = new MongoClient(db);

async function insert(measurements) {
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

async function resolve(message) {
  const msg = JSON.parse(pako.ungzip(message, { to: 'string' }));
  const {
    brand, manufacturer, model, id, version, data,
  } = msg;

  // Unroll user information back into every measurement
  const entry = data.map((obj) => Object.assign(obj, {
    brand, manufacturer, model, id, version,
  }));

  await insert(entry).catch(console.dir);
}

parentPort.postMessage(resolve(workerData.num));