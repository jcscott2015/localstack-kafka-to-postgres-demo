CREATE TABLE IF NOT EXISTS kafka_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255),
    payload JSONB,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the event stream table matrix
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 🛡️ THE BULLETPROOF LAYER: Enforces uniqueness on the JSON id field at the database level!
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_unique_id ON events ((data->>'id'));
