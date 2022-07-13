/* eslint-disable no-console */
import dotenv from 'dotenv';
import { parentPort } from 'worker_threads';
import { MongoClient } from 'mongodb';

import { logger } from './logger.js';

dotenv.config();

const { DATABASE_URL: db = 'mongodb://127.0.0.1:27017', API_KEY: api = '' } =
  process.env;

const mongoClient = new MongoClient(db);

async function getWeather(lon, lat, time) {
  if (!api) return [];

  const start = new Date(time).getTime() / 1000;

  // Available hpa: 1000, 800, 500, 200
  // Available meters: 100. 80, 50, 40, 30, 20
  // const params = 'windSpeed100m,windDirection100m'
  const params = 'windSpeed500hpa,windDirection500hpa';
  const results = [];

  await fetch(
    `https://api.stormglass.io/v2/weather/point?lat=${lat}&lng=${lon}&start=${start}&end=${start}&params=${params}`,
    {
      headers: {
        Authorization: api,
      },
    }
  )
    .then((response) => response.json())
    .then((jsonData) => {
      const first = jsonData.hours[0];
      results.push(first.windSpeed500hpa.sg);
      results.push(first.windDirection500hpa.sg);
    });

  return results;
}

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
  const message = JSON.parse(msg);
  const { start, brand, manufacturer, model, id, version, session, data } =
    message;

  if (!data) return;

  const mid = data[Math.floor(data.length / 2)];
  const wind = await getWeather(mid.lon, mid.lat, mid.time);

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
    obj.windSpeed = wind && wind[0] ? wind[0] : '';
    obj.windDirection = wind && wind[1] ? wind[1] : '';

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
