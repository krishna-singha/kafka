# Apache Kafka

This repository contains small example scripts that demonstrate producing and consuming messages with Apache Kafka using kafkajs.

## Table of contents
- Overview
- Prerequisites
- Installation
- Start Kafka (Docker)
- Create topics
- Running the examples
- Files and responsibilities
- Troubleshooting

## Overview

These scripts provide a minimal local development playground for Kafka:

- `admin.js` – creates a set of topics with configured partitions.
- `producer.js` – interactive producer that reads lines from stdin and sends messages.
- `consumer.js` – starts a consumer for a consumer group and subscribes to a single topic or all topics.
- `client.js` – shared kafkajs client configuration used by the other scripts.
- `docker.compose.yaml` – single-node Kafka broker configuration for local testing.

## Prerequisites

- Docker & Docker Compose (to run Kafka locally)
- Node.js (v14+ recommended)
- npm

## Installation

1. Install dependencies:

```bash
npm install
```

2. (Optional) Edit broker settings in `client.js` if your Kafka broker is not on `localhost:9092`.

## Start Kafka (Docker)

Bring up the single-node Kafka broker defined in `docker.compose.yaml`:

```bash
docker compose -f docker.compose.yaml up -d --build
```

Verify the container is running:

```bash
docker ps --filter name=kafka
docker logs kafka --tail 200
```

## Create topics

Run the admin script to create the example topics (this uses the broker configured in `client.js`):

```bash
node admin.js
```

`admin.js` creates topics such as `ride-requests`, `driver-location`, `payment-events`, `notifications`, and others. The number of partitions for each topic is defined in the script.

## Running the examples

Producer (interactive):

```bash
node producer.js
```

After starting, the producer shows the expected input format and prompts with `>`.

Input format:

```
<topic> <name> <value>
```

Examples you can type into the producer prompt:

```
ride-requests John North
driver-location Mike South
payment-events John completed
notifications Alice RideStarted
```

Consumer:

```bash
node consumer.js <group> <topic|all>
```

Examples:

```bash
node consumer.js group-1 ride-requests
node consumer.js group-2 driver-location
node consumer.js analytics-group analytics-events
node consumer.js group-1 all
```

The consumer prints detailed metadata for each received message (topic, partition, key, offset, timestamp) and supports subscribing to all topics by passing `all` as the topic argument.

Stopping:
- Press `Ctrl+C` in the producer/consumer terminals to disconnect and exit gracefully.

## Files and responsibilities

- [admin.js](admin.js): Creates topics and sets partition counts.
- [producer.js](producer.js): Interactive CLI producer. Uses a simple partitioning strategy (example: `north` => partition 0, `south` => partition 1).
- [consumer.js](consumer.js): Consumer that subscribes to a topic or to all topics (regex) and prints message contents.
- [client.js](client.js): Exports a configured `kafkajs` `Kafka` instance. Change `brokers` or `clientId` here to adapt to different environments.
- [docker.compose.yaml](docker.compose.yaml): Local single-node Kafka broker configuration.
- [package.json](package.json): Project dependencies (uses `kafkajs`).

## Troubleshooting

- If producers/consumers cannot connect, confirm the broker address in [client.js](client.js) matches the container's advertised listener and host.
- Check Kafka container logs:

```bash
docker logs kafka --tail 200
```

- If ports are in use, stop conflicting services or change the port mapping in `docker.compose.yaml`.
- If you see schema/JSON parsing errors, inspect the raw message output in the consumer — `consumer.js` falls back to printing the raw value when JSON.parse fails.
