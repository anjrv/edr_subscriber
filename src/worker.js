/* eslint-disable no-console */
import dotenv from 'dotenv';
import { parentPort } from 'worker_threads';
import { MongoClient } from 'mongodb';

import { logger } from './logger.js';

dotenv.config();

const { DATABASE_URL: db = 'mongodb://127.0.0.1:27017' } = process.env;

const mongoClient = new MongoClient(db);

async function insert(date, id, session, measurements, anomaly) {
  try {
    await mongoClient.connect();

    const database = mongoClient.db(date.split('T')[0]);

    if (anomaly) {
      await database.collection('anomalies').insertOne(anomaly);
    }

    await database
      .collection('sessions')
      .updateOne({ session }, { upsert: true });

    const options = { ordered: true };
    const result = await database
      .collection(`${date}_${id}_${session}`)
      .insertMany(measurements, options);

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

  let anomaly;
  let highest = 0.25;

  // Unroll user information back into every measurement, check for highest edr value
  const measurements = data.map((obj) => {
    Object.assign(obj, {
      brand,
      manufacturer,
      model,
      id,
      version,
      session,
    });

    if (obj.edr > highest) {
      anomaly = obj;
      highest = obj.edr;
    }
  });

  await insert(start, id, session, measurements, anomaly).catch((err) => {
    logger.error('Unable to insert to MongoDB', err);
  });
}

parentPort.on('message', (msg) => {
  resolve(msg);
});
