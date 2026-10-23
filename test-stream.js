import { Kafka } from 'kafkajs';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';

const kafka = new Kafka({ clientId: 'demo-producer', brokers: ['localhost:9092'] });
const lambda = new LambdaClient({ endpoint: 'http://localhost:4566', region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' } });

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'localstack-bridge-group' });

async function run() {
    await producer.connect();
    await consumer.connect();
    const topic = 'demo-topic';
    await consumer.subscribe({ topic, fromBeginning: true });

    // Array containing unique messages and intentional duplicate events
    const messageQueue = [
        { id: 5001, user: 'Alice', action: 'page_view' }, // 1. Unique
        { id: 5002, user: 'Bob', action: 'click' },     // 2. Unique
        { id: 5001, user: 'Alice', action: 'page_view' }, // 3. DUPLICATE RETRY of 5001
        { id: 5003, user: 'Charlie', action: 'purchase' },// 4. Unique
        { id: 5002, user: 'Bob', action: 'click' }      // 5. DUPLICATE RETRY of 5002
    ];
    // const messageQueue = [
    //     { id: 7001, user: 'DevDev', action: 'login' },
    //     { id: 7002, user: 'MaliciousActor', action: '' } // 💥 CRIPPLED PACKET: Empty action string forces failure mapping!
    // ];
    // const messageQueue = [
    //     // Bypasses the Redis preflight filters because Redis has never seen this IP or ID, 
    //     // but crashes instantly with a 500 when Postgres hits the duplicate index constraint!
    //     { id: 9999, user: "Ghost", action: "ghost_login", ip: "192.168.1.111" }
    // ];

    console.log(`📡 Shipping test streams containing retry duplicates into Kafka topic...`);
    for (const msg of messageQueue) {
        await producer.send({
            topic,
            messages: [{ value: JSON.stringify({ ...msg, ip: '192.168.1.99' }) }]
            // messages: [{ value: JSON.stringify({ ...msg, ip: '203.0.113.1' }) }] // Using TEST-NET-3 documentation IP (RFC 5737) instead of a private/hardcoded address

        });
    }

    await consumer.run({
        eachMessage: async ({ message }) => {
            const rawValue = message.value.toString();
            const lambdaPayload = {
                eventSource: 'aws:kafka',
                records: { [`${topic}-0`]: [{ topic, partition: 0, offset: message.offset, value: Buffer.from(rawValue).toString('base64') }] }
            };

            try {
                // Reverted to synchronous mode to catch explicit result loops instantly
                const response = await lambda.send(new InvokeCommand({
                    FunctionName: 'kafka-processor',
                    Payload: JSON.stringify(lambdaPayload)
                }));

                const rawResult = Buffer.from(response.Payload).toString('utf-8');
                console.log(`📡 Response payload received from background container:`, rawResult);
            } catch (err) {
                console.error('❌ Error invoking Lambda:', err.message);
            }
        }
    });
}

try {
    await run();
} catch (error) {
    console.error('💥 Fatal error:', error);
    process.exit(1);
}
