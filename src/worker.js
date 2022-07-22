/* eslint-disable no-console */
import dotenv from 'dotenv';
import { parentPort } from 'worker_threads';
import { MongoClient } from 'mongodb';

import { logger } from './logger.js';

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

  // Stormglass not sufficiently stable, add own solution and try catch block
  // const mid = data[Math.floor(data.length / 2)];
  // const wind = await getWeather(mid.lon, mid.lat, mid.time);

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
    // obj.windSpeed = wind && wind[0] ? wind[0] : '';
    // obj.windDirection = wind && wind[1] ? wind[1] : '';
    obj.windSpeed = '';
    obj.windDirection = '';

    if (obj.edr > highest) {
      anomaly = obj;
      highest = obj.edr;
    }
  });

  await insert(start, s, data, anomaly).catch((err) => {
    logger.error('Unable to insert to MongoDB', err.stack);
  });
}

parentPort.on('message', (msg) => {
  resolve(msg);
});
