/*
 * Mounting into an Express app you already have, rather than letting the
 * client start its own server. Useful when the process is already a web
 * service and you want deliveries to arrive on the same port.
 *
 *   node examples/express-app.js
 */
import express from 'express';

import { Client, Receiver } from '../src/index.js';

const app = express();
app.get('/', (req, res) => res.send('an ordinary app'));

/* Passing `app` means the Receiver mounts its delivery route and leaves
 * listening to you. */
const receiver = new Receiver({ app, mountPath: '/hooks/sukkal' });

const PORT = Number(process.env.PORT ?? 3000);
const server = app.listen(PORT, () => console.log(`app on :${PORT}`));
/* The broker holds this connection open between deliveries. */
server.keepAliveTimeout = 5 * 60 * 1000;
server.headersTimeout = server.keepAliveTimeout + 1000;

receiver.port = PORT;

const client = new Client({
  url: process.env.SUKKAL_URL ?? 'http://127.0.0.1:8080',
  receiver,
  /* What to put in the callback URL, when the broker reaches this
   * service by a name rather than by the address we happen to bind. */
  advertise: process.env.ADVERTISE_HOST,
});

await client.subscribe('orders.>', (msg) => {
  console.log(`${msg.subject} #${msg.index}`, msg.value);
}, { consumer: 'orders-service' });

console.log('subscribed; deliveries arrive on /hooks/sukkal/orders-service');
