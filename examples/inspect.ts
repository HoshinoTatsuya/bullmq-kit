// Queue health as Prometheus metrics.
import { createServer } from 'node:http';
import { QueueInspector, toPrometheus } from '@hoshinotatsuya/bullmq-kit/inspect';
import { Redis } from 'ioredis';

const inspector = new QueueInspector(new Redis(), { queuePrefixes: ['bull:emails', 'bull:reports'] });

createServer((_request, response) => {
  inspector.inspect().then(
    (health) => {
      response.setHeader('content-type', 'text/plain; version=0.0.4');
      response.end(toPrometheus(health));
    },
    (error: unknown) => {
      response.statusCode = 500;
      response.end(String(error));
    },
  );
}).listen(9464);
