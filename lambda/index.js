import pg from 'pg';
import { createClient } from 'redis';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';

const { Pool } = pg;
let pool = null;
let redisClient = null;
let sqsClient = null;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const getDbPool = () => {
  if (!pool) {
    pool = new Pool({
      connectionString: `postgresql://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_NAME}`,
      max: 5,
      idleTimeoutMillis: 10000,
      connectionTimeoutMillis: 3000
    });
  }

  return pool;
};

const getRedisClient = async () => {
  if (!redisClient) {
    redisClient = createClient({ url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}` });
    await redisClient.connect();
  }

  return redisClient;
};

const getSqsClient = () => {
  if (!sqsClient) {
    sqsClient = new SQSClient({
      endpoint: 'http://localstack_aws:4566',
      region: 'us-east-1',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' }
    });
  }

  return sqsClient;
};

const releaseDbClient = (dbClient) => {
  if (dbClient && typeof dbClient.release === 'function') {
    dbClient.release();
  }
};

const connectWithRetry = async (dbPool) => {
  let retries = 3;
  let backoffDelay = 200;

  while (retries > 0) {
    try {
      return await dbPool.connect();
    } catch (connError) {
      retries -= 1;
      if (retries === 0) {
        throw connError;
      }
      await delay(backoffDelay);
      backoffDelay *= 2;
    }
  }
};

const rollbackTransaction = async (dbClient, logPrefix) => {
  if (!dbClient) return;

  try {
    await dbClient.query('ROLLBACK');
  } catch (rollbackError) {
    console.error(`${logPrefix} Rollback failed:`, rollbackError.message);
  }
};

const sendDlqMessage = async (event, error, logPrefix) => {
  if (!process.env.DLQ_QUEUE_URL) return;

  try {
    await getSqsClient().send(new SendMessageCommand({
      QueueUrl: process.env.DLQ_QUEUE_URL,
      MessageBody: JSON.stringify({
        failedAt: new Date().toISOString(),
        errorMessage: error.message,
        eventPayload: event
      })
    }));
    console.log(`${logPrefix} 💾 Successfully dead-lettered failed batch context to SQS.`);
  } catch (sqsError) {
    console.error(`${logPrefix} ❌ SQS Logging Failure:`, sqsError.message);
  }
};

const parseEventRecord = (record) => {
  const payload = Buffer.from(record.value, 'base64').toString('utf-8');
  const data = JSON.parse(payload);
  if (!data.action || data.action.trim() === "") {
    throw new Error("BUSINESS_VALIDATION_FAILURE: Empty action strings are prohibited.");
  }
  return data;
};

const cleanupCreatedRedisKeys = async (redis, createdRedisKeys, logPrefix) => {
  if (!createdRedisKeys.length) return;
  console.log(`${logPrefix} 🧹 Evicting polluted Redis keys to protect cache consistency:`, createdRedisKeys);
  await redis.del(createdRedisKeys);
};

const checkDuplicate = async (redis, eventId, logPrefix, createdRedisKeys) => {
  if (eventId === undefined) return false;

  const idempotencyKey = `idempotency:${eventId}`;
  const isUnique = await redis.set(idempotencyKey, 'PROCESSED', { NX: true, EX: 300 });

  if (!isUnique) {
    console.warn(`${logPrefix} ♻️ Duplicate event detected. Rolling back batch transaction...`);
    return true;
  }

  createdRedisKeys.push(idempotencyKey);
  return false;
};

const checkRateLimit = async (redis, clientIp, logPrefix, createdRedisKeys) => {
  const rateLimitKey = `rate_limit:${clientIp}`;
  const currentHits = await redis.incr(rateLimitKey);

  if (currentHits === 1) {
    await redis.expire(rateLimitKey, 60);
    createdRedisKeys.push(rateLimitKey);
  }

  if (currentHits > 5) {
    console.warn(`${logPrefix} 🛑 Rate limit exceeded for IP: ${clientIp}. Rolling back...`);
    return { statusCode: 429, body: JSON.stringify({ error: 'Too many requests. Throttled.' }) };
  }

  return null;
};

const validateBatch = async (event, redis, createdRedisKeys, logPrefix) => {
  try {
    for (const recordKey in event.records) {
      for (const record of event.records[recordKey]) {
        const data = parseEventRecord(record);
        const clientIp = data.ip || '127.0.0.1';

        if (await checkDuplicate(redis, data.id, logPrefix, createdRedisKeys)) {
          return { statusCode: 200, body: JSON.stringify({ error: `DEDUPLICATION_TRIGGER: Duplicate event detected for ID ${data.id}`, duplicate: true }) };
        }

        const rateLimitResult = await checkRateLimit(redis, clientIp, logPrefix, createdRedisKeys);
        if (rateLimitResult) return rateLimitResult;
      }
    }

    return null;
  } catch (validationError) {
    console.error(`${logPrefix} 🚨 Batch validation error:`, validationError.message);
    return { statusCode: 400, body: JSON.stringify({ error: validationError.message }) };
  }
};

const processBatch = async (event, dbClient, logPrefix) => {
  for (const recordKey in event.records) {
    for (const record of event.records[recordKey]) {
      const data = parseEventRecord(record);
      console.log(`${logPrefix} 📥 Staging Event ID ${data.id} into transaction snapshot...`);
      await dbClient.query('INSERT INTO events(data, created_at) VALUES($1, NOW())', [data]);
    }
  }

  return { statusCode: 200, body: JSON.stringify({ message: 'Batch transaction committed successfully.' }) };
};

export const handler = async (event) => {
  const logPrefix = process.env.LOG_PREFIX || '[STREAM-PROCESSOR]';
  console.log(`${logPrefix} Triggered with event payload:`, JSON.stringify(event));

  if (!event?.records) {
    return { statusCode: 400, body: JSON.stringify({ error: 'Missing records structure.' }) };
  }

  const dbPool = getDbPool();
  const redis = await getRedisClient();
  const createdRedisKeys = [];

  const preflight = await validateBatch(event, redis, createdRedisKeys, logPrefix);
  if (preflight) {
    const errorObj = JSON.parse(preflight.body);
    console.error(`${logPrefix} 🚨 Preflight validation failed: "${errorObj.error}". Routing batch to SQS DLQ...`);

    await sendDlqMessage(event, new Error(errorObj.error), logPrefix);

    await cleanupCreatedRedisKeys(redis, createdRedisKeys, logPrefix);
    return preflight;
  }

  let dbClient = null;

  try {
    dbClient = await connectWithRetry(dbPool);
  } catch (connError) {
    await cleanupCreatedRedisKeys(redis, createdRedisKeys, logPrefix);
    await sendDlqMessage(event, connError, logPrefix);
    return { statusCode: 503, body: JSON.stringify({ error: 'Service Unavailable.' }) };
  }

  let transactionActive = false;

  try {
    await dbClient.query('BEGIN');
    transactionActive = true;

    const result = await processBatch(event, dbClient, logPrefix);

    await dbClient.query('COMMIT');
    transactionActive = false;
    releaseDbClient(dbClient);

    return result;
  } catch (error) {
    console.error(`${logPrefix} 🚨 Database ACID Error intercepted: "${error.message}". Rolling back...`);

    if (transactionActive) {
      await rollbackTransaction(dbClient, logPrefix);
    }
    releaseDbClient(dbClient);

    await cleanupCreatedRedisKeys(redis, createdRedisKeys, logPrefix);
    await sendDlqMessage(event, error, logPrefix);

    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message, rolledBack: true })
    };
  }
};
