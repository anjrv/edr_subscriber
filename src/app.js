/* eslint-disable no-console */
import * as mqtt from 'mqtt';
import dotenv from 'dotenv';
import pako from 'pako';

dotenv.config();

const {
  SERVER_URL: url = 'mqtt://localhost:1883',
  USERNAME: username = 'username',
  PASSWORD: password = 'password',
} = process.env;

const clientId = `node${Math.random().toString(16).slice(2)}`;
const client = mqtt.connect(url, {
  clientId,
  username,
  password,
});

client.on('connect', () => {
  client.subscribe('EDR', (err) => {
    if (err) console.err(err);
  });
});

client.on('message', (_topic, message) => {
  const data = JSON.parse(pako.ungzip(message, { to: 'string' }));
  console.log(data);
  // Add repeat tags back into each measurement
  // Bulk insert into db ( could also collect bigger bulks )
});
