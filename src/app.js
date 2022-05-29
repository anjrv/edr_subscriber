/* eslint-disable no-console */
import * as mqtt from 'mqtt';
import dotenv from 'dotenv';
import pako from 'pako';

dotenv.config();

const {
  SERVER_URL: url = 'mqtt://localhost:1883',
} = process.env;

const client = mqtt.connect(url);

client.on('connect', () => {
  client.subscribe('EDR', (err) => {
    if (err) console.err(err);
  });
});

client.on('message', (_topic, message) => {
  const data = JSON.parse(pako.ungzip(message, {to:'string'}));
  console.log(data);
  // client.end();
});
