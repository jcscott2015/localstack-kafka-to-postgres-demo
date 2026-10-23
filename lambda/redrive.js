export const redrive = async () => {
    const logPrefix = '🛟 [DLQ-REDRIVER]';
    console.log(`${logPrefix} Starting dead-letter triage and re-drive worker loop...`);

    const queueUrl = 'http://localhost:4566/000000000000/demo-processing-dlq';
    const kafkaProxyUrl = 'http://localhost:8082/topics/demo-topic';

    try {
        console.log(`${logPrefix} Polling LocalStack SQS queue via HTTP fetch...`);

        // 1. Fetch raw messages directly from the local cloud endpoint
        const response = await fetch(`${queueUrl}?Action=ReceiveMessage&MaxNumberOfMessages=10`, {
            headers: { 'Accept': 'application/json' }
        });

        const textData = await response.text();
        let messages = [];

        if (textData.includes('<Message>')) {
            const matches = textData.match(/<Message>[\s\S]*?<\/Message>/g) || [];
            messages = matches.map(msgStr => {
                const body = msgStr.match(/<Body>([\s\S]*?)<\/Body>/)?.[1] || '';
                const receiptHandle = msgStr.match(/<ReceiptHandle>([\s\S]*?)<\/ReceiptHandle>/)?.[1] || '';
                const messageId = msgStr.match(/<MessageId>([\s\S]*?)<\/MessageId>/)?.[1] || '';
                return { Body: body, ReceiptHandle: receiptHandle, MessageId: messageId };
            });
        } else if (textData.trim().startsWith('{')) {
            const parsedJson = JSON.parse(textData);
            messages = parsedJson?.ReceiveMessageResponse?.ReceiveMessageResult?.messages || [];
        }

        if (messages.length === 0) {
            console.log(`${logPrefix} 🧊 No dead-letter messages present for triage. Exiting cleanly.`);
            return { redrivedCount: 0 };
        }

        console.log(`${logPrefix} Intercepted ${messages.length} failed event records. Processing patches...`);

        for (const msg of messages) {
            const envelope = JSON.parse(msg.Body);
            console.log(`${logPrefix} 🔍 Inspecting failed envelope...`);

            let rawPayload = envelope.originalPayload;
            if (!rawPayload && envelope.eventPayload?.records) {
                const firstRecordArray = Object.values(envelope.eventPayload.records)[0];
                if (firstRecordArray && firstRecordArray[0]) {
                    rawPayload = Buffer.from(firstRecordArray[0].value, 'base64').toString('utf-8');
                }
            }

            if (!rawPayload) {
                console.warn(`${logPrefix} ⚠️ Unable to extract raw payload. Skipping message ID: ${msg.MessageId}`);
                continue;
            }

            const originalData = JSON.parse(rawPayload);

            // --- AUTOMATED DATA REPAIR PATCH ---
            if (!originalData.action || originalData.action.trim() === "") {
                console.log(`${logPrefix} 🩹 Patching crippled empty 'action' string on Record ID: ${originalData.id}`);
                originalData.action = 'UNKNOWN_REPAIRED_FALLBACK';
            }

            console.log(`${logPrefix} 🚀 Re-injecting repaired record ID ${originalData.id} via HTTP REST Proxy...`);

            // 2. Re-inject payload into Kafka using Redpanda REST Proxy API natively
            await fetch(kafkaProxyUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/vnd.kafka.json.v2+json' },
                body: JSON.stringify({
                    records: [{ value: originalData }]
                })
            });

            // 3. Delete the successfully triaged message from SQS memory
            await fetch(`${queueUrl}?Action=DeleteMessage&ReceiptHandle=${encodeURIComponent(msg.ReceiptHandle)}`);
            console.log(`${logPrefix} 🗑️ Successfully deleted Message ID ${msg.MessageId} from SQS.`);
        }

        console.log(`${logPrefix} Re-drive triage run completed successfully.`);
        return { redrivedCount: messages.length };

    } catch (error) {
        console.error(`${logPrefix} 💥 Critical Re-drive Failure:`, error.message);
        throw error;
    }
};

if (process.argv.some(arg => arg.endsWith('redrive.js'))) {
    try {
        await redrive();
        process.exit(0);
    } catch (e) {
        console.error(`💥 Redrive script encountered an error:`, e.message);
        process.exit(1);
    }
}
