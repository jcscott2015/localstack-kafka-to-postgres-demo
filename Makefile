# Define phony targets so make doesn't conflict with local files/folders of the same name
.PHONY: init up down restart lambda-test tfvars deploy test-stream verify logs clean shell-db Help

# Default command when running just 'make'
.DEFAULT_GOAL := help

## 🐳 INFRASTRUCTURE ACTIONS
up: ## Spin up PostgreSQL, Redpanda Kafka, and LocalStack containers
	@echo "📡 Starting up Docker containers..."
	docker-compose up -d
	@echo "⏳ Waiting for services to initialize..."
	@sleep 5
	@make verify

down: ## Stop and tear down all containers and networks
	@echo "🛑 Stopping containers..."
	docker-compose down

restart: down up ## Hard-restart the container infrastructure cluster

## 🧪 LOCAL TESTING ACTIONS
lambda-test: ## Run the automated Vitest suite inside the lambda microservice folder
	@echo "🧪 Running Lambda Unit Tests via Vitest..."
	pnpm --prefix lambda test

## 🚀 TERRAFORM & DEPLOYMENT ACTIONS
init: ## Initialize and upgrade all Terraform binary provider modules
	@echo "⚙️ Initializing Terraform..."
	terraform init -upgrade

tfvars: ## Automatically parse the root .env file and generate the masked terraform.tfvars.json mapping layer
	@echo "🔐 Parsing .env and generating secure variables mask..."
	@node -e 'const fs = require("fs"); const env = fs.readFileSync(".env", "utf8").split("\n").filter(l => l.trim() && !l.startsWith("#")).reduce((acc, l) => { const [k, v] = l.split("="); acc[k.trim()] = v.trim(); return acc; }, {}); console.log(JSON.stringify({ lambda_environment: env }));' > terraform.tfvars.json

deploy: tfvars ## Compile your code bundle via esbuild and apply changes directly into LocalStack
	@echo "🚀 Bundling lambda and deploying architecture to LocalStack..."
	terraform apply --auto-approve

## 🏃 SIMULATION & VALIDATION
test-stream: ## Execute your modern top-level await Kafka stream simulation script
	@echo "📡 Executing Kafka stream simulator..."
	node test-stream.js

verify: ## Query the Postgres events table to view your streaming rows live
	@echo "📊 Querying PostgreSQL database 'events' table structure:"
	@docker exec -it postgres_db psql -U demo_user -d demo_db -c "SELECT * FROM events;" 2>/dev/null || echo "⚠️ Database table 'events' not ready or initialized yet."

verify-dlq: ## Query your emulated Amazon SQS queue to view stuck dead-lettered payloads
	@echo "🗂️ Inspecting AWS SQS Dead-Letter Queue ('demo-processing-dlq') metrics:"
	@AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:4566 sqs receive-message --queue-url=http://localhost:4566/000000000000/demo-processing-dlq --max-number-of-messages 5 --visibility-timeout 0 --region us-east-1 2>/dev/null || echo "⚠️ SQS queue empty or no dead-letter messages present."

rehydrate-cache: ## Scan PostgreSQL rows and instantly warm up active idempotency locks inside Redis
	@echo "🚰 Warming cache from historical database rows..."
	@cd lambda && node -e 'const fs=require("fs"); if(fs.existsSync("../.env")){fs.readFileSync("../.env", "utf8").split("\n").filter(l=>l.trim()&&!l.startsWith("#")).forEach(l=>{const idx=l.indexOf("="); if(idx!==-1){process.env[l.substring(0,idx).trim()]=l.substring(idx+1).trim();}}); } import("./rehydrate.js").then(m => m.rehydrate());'

redrive-dlq: ## Drain failed event records from SQS, apply diagnostic patches, and re-inject to Kafka
	@echo "🛟 Activating automated DLQ Re-drive engine worker loop..."
	@cd lambda && node -e 'const fs=require("fs"); if(fs.existsSync("../.env")){fs.readFileSync("../.env", "utf8").split("\n").filter(l=>l.trim()&&!l.startsWith("#")).forEach(l=>{const idx=l.indexOf("="); if(idx!==-1){process.env[l.substring(0,idx).trim()]=l.substring(idx+1).trim();}}); } import("./redrive.js").then(m => m.redrive());'

shell-db: ## Open an interactive psql shell directly inside your Postgres container
	@docker exec -it postgres_db psql -U demo_user -d demo_db

logs: ## Tail the console log logs coming out of LocalStack
	@docker logs -f localstack_aws

clean: down ## Wipe all temporary builds, distribution caches, zip files, and terraform states completely
	@echo "🧹 Cleaning up target builds, states, caches, and archives..."
	@rm -rf lambda/dist lambda/node_modules lambda.zip terraform.tfvars.json .terraform .terraform.lock.hcl terraform.tfstate terraform.tfstate.backup node_modules
	@docker-compose down --volumes --remove-orphans

## 💡 DOCUMENTATION UTILITY
help: ## Display this dynamic, self-documenting menu mapping list
	@echo "🔧 Local Demo Architecture Workflow Engine commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
