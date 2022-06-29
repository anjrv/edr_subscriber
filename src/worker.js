/* eslint-disable no-console */
import dotenv from 'dotenv';
import { parentPort } from 'worker_threads';
import { MongoClient } from 'mongodb';

import { logger } from './logger.js';

dotenv.config();

const { DATABASE_URL: db = 'mongodb://127.0.0.1:27017' } = process.env;

const mongoClient = new MongoClient(db);

async function insert(date, id, session, measurements, anomalies) {
  try {
    await mongoClient.connect();

    const database = mongoClient.db('hvassahraun');
    const edr = database.collection('edr');

    const options = { ordered: true };
    const result = await edr.insertMany(measurements, options);

    console.log(`${result.insertedCount} measurements were inserted, sample:`);
    console.log(measurements[0]);
  } finally {
    await mongoClient.close();
  }
}

async function resolve(msg) {
  const message = JSON.parse(msg);
  const { start, brand, manufacturer, model, id, version, session, data } =
    message;

  const anomalies = [];

  // Unroll user information back into every measurement
  const measurements = data.map((obj) => {
    Object.assign(obj, {
      brand,
      manufacturer,
      model,
      id,
      version,
      session,
    });

    if (obj.edr > 0.25) anomalies.push(obj);
  });

  console.log(start.split("T")[0], anomalies);

  // await insert(start.split("T")[0], id, session, measurements, anomalies).catch((err) => {
  //   logger.error('Unable to insert to MongoDB', err);
  // });
}

parentPort.on('message', (msg) => {
  resolve(msg);
});
