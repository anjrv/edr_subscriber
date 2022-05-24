/* eslint-disable no-console */
import * as mqtt from 'mqtt';
import dotenv from 'dotenv';

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
  console.log(message.toString());
  // client.end();
});
