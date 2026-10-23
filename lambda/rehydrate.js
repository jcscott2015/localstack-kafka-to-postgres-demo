import pg from 'pg';
import { createClient } from 'redis';

const { Pool } = pg;

export const rehydrate = async () => {
    const logPrefix = '🚰 [CACHE-REHYDRATOR]';
    console.log(`${logPrefix} Starting cache warming sequence...`);

    // Fallback to localhost when running from your Mac terminal thread
    const dbHost = process.env.DB_HOST === 'postgres_db' ? 'localhost' : (process.env.DB_HOST || 'localhost');
    const redisHost = process.env.REDIS_HOST === 'redis' ? 'localhost' : (process.env.REDIS_HOST || 'localhost');

    const pool = new Pool({
        connectionString: `postgresql://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${dbHost}:${process.env.DB_PORT}/${process.env.DB_NAME}`,
        connectionTimeoutMillis: 2000
    });

    const redisClient = createClient({
        url: `redis://${redisHost}:${process.env.REDIS_PORT}`
    });

    redisClient.on('error', (err) => console.error(`${logPrefix} Redis Error:`, err.message));
    await redisClient.connect();

    try {
        console.log(`${logPrefix} Scanning PostgreSQL for records processed within the last 5 minutes...`);
        const queryStr = `
      SELECT DISTINCT (data->>'id') as event_id,
             EXTRACT(EPOCH FROM (NOW() - created_at)) as elapsed_seconds
      FROM events
      WHERE created_at >= NOW() - INTERVAL '5 minutes';
    `;

        const { rows } = await pool.query(queryStr);
        console.log(`${logPrefix} Found ${rows.length} records requiring cache restoration.`);

        if (rows.length === 0) {
            console.log(`${logPrefix} 🧊 Cache is already synchronized or database is empty. Exiting.`);
            return { rehydratedCount: 0 };
        }

        const pipeline = redisClient.multi();

        for (const row of rows) {
            const currentEventId = row.event_id; // FIXED: Safely extract column value map string
            if (!currentEventId) continue;

            const redisKey = `idempotency:${currentEventId}`; // FIXED: Map variable name correctly
            const remainingTtl = Math.max(1, Math.ceil(300 - parseFloat(row.elapsed_seconds)));

            pipeline.set(redisKey, 'PROCESSED', { EX: remainingTtl });
        }

        await pipeline.exec();
        console.log(`${logPrefix} 🎉 Successfully rehydrated ${rows.length} distributed locks into Redis memory.`);

        return { rehydratedCount: rows.length };

    } catch (error) {
        console.error(`${logPrefix} 💥 Critical Rehydration Failure:`, error.message);
        throw error;
    } finally {
        await pool.end();
        await redisClient.disconnect();
        console.log(`${logPrefix} Infrastructure connections closed cleanly.`);
    }
};

if (process.argv.some(arg => arg.endsWith('rehydrate.js'))) {
    try {
        await rehydrate();
        process.exit(0);
    } catch (e) {
        console.error('🚰 Cache warming aborted on script runner catch:', e.message);
        process.exit(1);
    }
}

