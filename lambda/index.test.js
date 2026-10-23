
import { describe, it, expect, vi, beforeEach } from 'vitest';

const mockConnectSpy = vi.fn();
const mockQuerySpy = vi.fn();
const mockRedisIncr = vi.fn();
const mockRedisExpire = vi.fn();
const mockRedisSet = vi.fn();
const mockRedisDel = vi.fn();
const mockSqsSend = vi.fn();

vi.mock('pg', () => ({
    default: {
        Pool: class {
            constructor() {
                this.connect = mockConnectSpy;
            }

            query(...args) {
                return mockQuerySpy(...args);
            }
        }
    }
}));

vi.mock('redis', () => ({
    createClient: () => ({
        connect: vi.fn().mockResolvedValue(),
        incr: mockRedisIncr,
        expire: mockRedisExpire,
        set: mockRedisSet,
        del: mockRedisDel,
        on: vi.fn()
    })
}));

vi.mock('@aws-sdk/client-sqs', () => ({
    SQSClient: class { send(...args) { return mockSqsSend(...args); } },
    SendMessageCommand: class { constructor(props) { this.props = props; } }
}));

const { handler } = await import('./index.js');

describe('Pure ESM Lambda Handler Unit Tests', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockConnectSpy.mockResolvedValue({ query: mockQuerySpy, release: vi.fn() });
        mockRedisExpire.mockResolvedValue();
        mockRedisDel.mockResolvedValue(1);
        mockSqsSend.mockResolvedValue({ MessageId: 'mock-sqs-id' });
        vi.stubEnv('DLQ_QUEUE_URL', 'http://localhost:4566/mock-queue');
    });

    it('✅ should successfully process records when everything is healthy', async () => {
        const mockEvent = {
            eventSource: 'aws:kafka',
            records: {
                'demo-topic-0': [{ value: Buffer.from(JSON.stringify({ id: 101, ip: '1.1.1.1', action: 'page_view' })).toString('base64') }]
            }
        };

        mockRedisSet.mockResolvedValue(true);
        mockRedisIncr.mockResolvedValue(1);
        mockQuerySpy.mockResolvedValue({ rowCount: 1 });

        const result = await handler(mockEvent);

        expect(result.statusCode).toBe(200);
        expect(JSON.parse(result.body).message).toBe('Batch transaction committed successfully.');
        expect(mockRedisSet).toHaveBeenCalledWith('idempotency:101', 'PROCESSED', { NX: true, EX: 300 });
        expect(mockQuerySpy).toHaveBeenCalledWith('BEGIN');
        expect(mockQuerySpy).toHaveBeenCalledWith('COMMIT');
    });

    it('♻️ should short-circuit and report a duplicate without opening a DB transaction', async () => {
        const mockEvent = {
            eventSource: 'aws:kafka',
            records: {
                'demo-topic-0': [{ value: Buffer.from(JSON.stringify({ id: 101, ip: '1.1.1.1', action: 'page_view' })).toString('base64') }]
            }
        };

        mockRedisSet.mockResolvedValue(false);

        const result = await handler(mockEvent);

        expect(result.statusCode).toBe(200);
        expect(JSON.parse(result.body).duplicate).toBe(true);
        expect(mockRedisDel).not.toHaveBeenCalled();
        expect(mockQuerySpy).not.toHaveBeenCalled();
    });

    it('⚠️ should return a 400 status code if the payload is missing records structure', async () => {
        const malformedEvent = { somethingElse: 'data' };

        const result = await handler(malformedEvent);

        expect(result.statusCode).toBe(400);
        expect(JSON.parse(result.body).error).toBe('Missing records structure.');
        expect(mockQuerySpy).not.toHaveBeenCalled();
        expect(mockRedisIncr).not.toHaveBeenCalled();
    });

    it('🛑 should throttle requests if Redis hit count exceeds 5', async () => {
        const mockEvent = {
            eventSource: 'aws:kafka',
            records: {
                'demo-topic-0': [{ value: Buffer.from(JSON.stringify({ id: 102, ip: '1.1.1.1', action: 'click' })).toString('base64') }]
            }
        };

        mockRedisSet.mockResolvedValue(true);
        mockRedisIncr.mockResolvedValue(6);

        const result = await handler(mockEvent);

        expect(result.statusCode).toBe(429);
        expect(JSON.parse(result.body).error).toBe('Too many requests. Throttled.');
        expect(mockQuerySpy).not.toHaveBeenCalled();
    });

    it('🚨 should handle database query failures gracefully and send the payload to the SQS DLQ', async () => {
        const mockEvent = {
            eventSource: 'aws:kafka',
            records: {
                'demo-topic-0': [{ value: Buffer.from(JSON.stringify({ id: 999, ip: '10.0.0.5', action: 'purchase' })).toString('base64') }]
            }
        };

        mockRedisSet.mockResolvedValue(true);
        mockRedisIncr.mockResolvedValue(1);
        mockQuerySpy.mockImplementation(async (sql) => {
            if (sql.startsWith('BEGIN')) return { rowCount: 1 };
            if (sql.startsWith('INSERT')) throw new Error('Connection timeout to database');
            return { rowCount: 1 };
        });

        const result = await handler(mockEvent);

        expect(result.statusCode).toBe(500);
        expect(JSON.parse(result.body).error).toBe('Connection timeout to database');
        expect(mockQuerySpy).toHaveBeenCalledWith('BEGIN');
        expect(mockQuerySpy).toHaveBeenCalledWith('ROLLBACK');
        expect(mockRedisDel).toHaveBeenCalledWith(['idempotency:999', 'rate_limit:10.0.0.5']);
        expect(mockSqsSend).toHaveBeenCalledTimes(1);
    });

    it('🧱 should retry a database pool connection before failing with a 503', async () => {
        const mockEvent = {
            eventSource: 'aws:kafka',
            records: {
                'demo-topic-0': [{ value: Buffer.from(JSON.stringify({ id: 777, ip: '10.0.0.7', action: 'login' })).toString('base64') }]
            }
        };

        mockConnectSpy
            .mockRejectedValueOnce(new Error('temporary pool failure'))
            .mockRejectedValueOnce(new Error('temporary pool failure'))
            .mockResolvedValue({ query: mockQuerySpy, release: vi.fn() });

        mockRedisSet.mockResolvedValue(true);
        mockRedisIncr.mockResolvedValue(1);
        mockQuerySpy.mockResolvedValue({ rowCount: 1 });

        const result = await handler(mockEvent);

        expect(result.statusCode).toBe(200);
        expect(mockConnectSpy).toHaveBeenCalledTimes(3);
    });
});

