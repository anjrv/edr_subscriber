/* eslint-disable no-console */
import dotenv from 'dotenv';
import { parentPort } from 'worker_threads';
import { MongoClient } from 'mongodb';

import { logger } from './logger.js';

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

    console.log(`${result.insertedCount} measurements were inserted, sample:`);
    console.log(measurements[0]);
  } finally {
    await mongoClient.close();
  }
}

async function resolve(msg) {
  const message = JSON.parse(msg);
  const {
    brand, manufacturer, model, id, version, session, data,
  } = message;

  // Unroll user information back into every measurement
  const entry = data.map((obj) => Object.assign(obj, {
    brand, manufacturer, model, id, version, session,
  }));

  await insert(entry).catch((err) => {
    logger.error('Unable to insert to MongoDB', err);
  });
}

parentPort.on('message', (msg) => {
  resolve(msg);
});
