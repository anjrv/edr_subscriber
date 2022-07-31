/* eslint-disable no-console */
import dotenv from 'dotenv';
import { parentPort } from 'worker_threads';
import { MongoClient } from 'mongodb';

import { logger } from './logger.js';
import { search } from 'vedur/src/search.js'; // Requires npm linking vedur

dotenv.config();

const { DATABASE_URL: db = 'mongodb://127.0.0.1:27017' } = process.env;

const mongoClient = new MongoClient(db);

async function insert(date, session, measurements, anomaly) {
  try {
    await mongoClient.connect();

    const database = mongoClient.db(date.split('T')[0]);

    if (anomaly) {
      await database.collection('anomalies').insertOne(anomaly);
    }

    await database
      .collection('sessions')
      .updateMany(
        { session: session },
        { $setOnInsert: { session: session } },
        { upsert: true }
      );

    const options = { ordered: true };
    const result = await database
      .collection(session)
      .insertMany(measurements, options);

    console.log(`${result.insertedCount} measurements were inserted, sample:`);
    console.log(measurements[0]);
  } finally {
    await mongoClient.close();
  }
}

async function resolve(msg) {
  let message;
  try {
    message = JSON.parse(msg);
  } catch (err) {
    logger.error('Unable to parse msg', err.stack);
    return;
  }

  if (!message) return;

  const { start, brand, manufacturer, model, id, version, session, data } =
    message;

  if (!data) return;

  let windAvg = '';
  let windMax = '';
  let windDir = '';
  let windMethod = '';
  let windSource = '';

  try {
    const mid = data[Math.floor(data.length / 2)];
    const windData = await search(mid.lat, mid.lon, mid.time);

    if (!Object.keys(windData).length === 0) {
      windAvg = windData.windAvg ? windData.windAvg : ''; 
      windMax = windData.windMax ? windData.windMax : ''; 
      windDir = windData.windDir ? windData.windDir : ''; 
      windMethod = windData.windMethod ? windData.windMethod : ''; 
      windSource = windData.windSource ? windData.windSource : ''; 
    }
  } catch {
    console.error(err);
  }

  let anomaly;
  let highest = 0.25;

  // Construct identifier string
  const s = `${start}_${id}_${session}`;

  // Unroll user information back into every measurement, check for highest edr value
  data.forEach((obj) => {
    obj.brand = brand;
    obj.manufacturer = manufacturer;
    obj.model = model;
    obj.id = id;
    obj.version = version;
    obj.session = s;
    obj.windAvg = windAvg;
    obj.windMax = windMax;
    obj.windDir = windDir;
    obj.windMethod = windMethod;
    obj.windSource = windSource;

    if (obj.edr > highest) {
      anomaly = obj;
      highest = obj.edr;
    }
  });

  await insert(start, s, data, anomaly).catch((err) => {
    console.error(err);
    logger.error('Unable to insert to MongoDB', err.stack);
  });
}

parentPort.on('message', (msg) => {
  resolve(msg);
});
