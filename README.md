# LocalStack Kafka to Postgres Demo

This project runs a local event-processing stack. A Kafka producer publishes test events, a LocalStack Lambda validates and persists them to PostgreSQL, Redis provides short-lived idempotency and rate-limit keys, and failed batches are sent to a LocalStack SQS dead-letter queue (DLQ).

Mermaid Flowchart: [Demo Architecture](demo-architecture.md)

## Prerequisites

- Docker and Docker Compose
- Node.js and pnpm
- Terraform
- AWS CLI, for `make verify-dlq`

## Configuration

Create a root `.env` file before deploying. `make deploy` converts it to the Lambda environment map in `terraform.tfvars.json`.

```dotenv
DB_USER=demo_user
DB_PASSWORD=demo_password
DB_HOST=postgres_db
DB_PORT=5432
DB_NAME=demo_db
REDIS_HOST=redis
REDIS_PORT=6379
DLQ_QUEUE_URL=http://localstack_aws:4566/000000000000/demo-processing-dlq
LOG_PREFIX=[STREAM-PROCESSOR]
```

The container names in `DB_HOST`, `REDIS_HOST`, and `DLQ_QUEUE_URL` are used by the Lambda running on the Docker network. The maintenance scripts automatically use `localhost` when they run from the host.

## Run the Stack

```sh
pnpm install
pnpm --dir lambda install
make up
make init
make deploy
```

`make up` starts PostgreSQL, Redpanda Kafka, Redis, and LocalStack. `make deploy` bundles `lambda/index.js`, creates the LocalStack SQS DLQ and Lambda resources, and passes `.env` values into the Lambda.

Send the sample Kafka event stream and inspect persisted rows:

```sh
make test-stream
make verify
```

The stream simulator invokes the deployed `kafka-processor` Lambda through LocalStack after consuming each Kafka message.

## Processing Behavior

For each Kafka batch, the Lambda:

1. Decodes Base64 record values and rejects missing or blank `action` fields.
2. Stores an `idempotency:<id>` Redis key for five minutes and rejects duplicate event IDs.
3. Limits each IP address to five requests per minute.
4. Writes all validated records to PostgreSQL in one transaction.
5. Rolls back the transaction, removes Redis keys created for the failed batch, and writes the event envelope to the SQS DLQ when validation or database processing fails.

Connection attempts to PostgreSQL are retried up to three times with exponential backoff. A connection failure returns HTTP 503; transaction failures return HTTP 500 with `rolledBack: true`.

## Operations

```sh
make lambda-test       # Run the Lambda Vitest suite
make verify            # Query the PostgreSQL events table
make verify-dlq        # Read up to five visible DLQ messages
make rehydrate-cache   # Restore recent idempotency keys from PostgreSQL
make redrive-dlq       # Repair empty actions and republish DLQ records to Kafka
make logs              # Follow LocalStack logs
make shell-db          # Open psql in PostgreSQL
```

`make rehydrate-cache` restores idempotency keys for events written during the previous five minutes. `make redrive-dlq` reads failed envelopes from the DLQ, replaces an empty `action` with `UNKNOWN_REPAIRED_FALLBACK`, republishes the repaired event through the Kafka REST proxy, then removes the DLQ message.

## Cleanup

```sh
make down   # Stop containers and remove the Docker network
make clean  # Also remove build outputs and Terraform state
```
