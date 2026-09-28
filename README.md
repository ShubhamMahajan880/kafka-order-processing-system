# Kafka Order Processing System

Event-driven Order Processing System built using:

- Java 21
- Spring Boot 4.1.1
- Apache Kafka
- Spring Kafka
- MySQL
- Spring Data JPA
- Hibernate
- Docker
- Maven
- Postman
- Eclipse IDE

## Project Overview

This project implements an event-driven Order Processing System using Spring Boot Microservices and Apache Kafka.

The system contains four independent microservices that communicate asynchronously through Kafka events instead of relying entirely on direct synchronous communication.

The project demonstrates:

- Asynchronous communication
- Kafka Producers and Consumers
- Kafka Topics
- Kafka Partitions
- Kafka Message Keys
- Consumer Groups
- Consumer Offsets
- Consumer Scaling
- Failure Recovery
- Retry Handling
- Idempotency
- Dead Letter Topics (DLT)

## Architecture

Customer
   |
   | POST /orders
   ↓
Order Service
   |
   | ORDER_CREATED
   ↓
Kafka: order-created
   |
   +-------------------------+
   |                         |
   ↓                         ↓
Payment Service        Notification Service
   |
   +------------------+
   |                  |
   ↓                  ↓
PAYMENT_SUCCESS   PAYMENT_FAILED
   |
   ↓
Kafka: payment-success
   |
   ↓
Delivery Service
   |
   | DELIVERY_CREATED
   ↓
Kafka: delivery-created
   |
   ↓
Notification Service

## End-to-End Flow

1. Customer sends an order request to Order Service.
2. Order Service stores the order in MySQL.
3. Order Service publishes an ORDER_CREATED event to Kafka.
4. Payment Service consumes the ORDER_CREATED event.
5. Payment Service processes the payment.
6. Successful payment publishes PAYMENT_SUCCESS.
7. Failed payment publishes PAYMENT_FAILED.
8. Delivery Service consumes PAYMENT_SUCCESS.
9. Delivery Service creates a delivery and publishes DELIVERY_CREATED.
10. Notification Service consumes the relevant events and simulates customer notifications.

## Microservices

| Service | Port | Responsibility |
|---|---:|---|
| Order Service | 8085 | Create and publish orders |
| Payment Service | 8086 | Process payments |
| Delivery Service | 8087 | Create delivery |
| Notification Service | 8088 | Simulate notifications |

## Order Service

Port:

8085

Responsibility:

- Accept order requests
- Store orders in MySQL
- Generate order IDs
- Create ORDER_CREATED events
- Publish events to Kafka

Endpoint:

POST http://localhost:8085/orders

Example request:

{
  "customerId": 101,
  "customerName": "Rahul",
  "productId": 501,
  "productName": "Laptop",
  "quantity": 1,
  "amount": 75000,
  "deliveryAddress": "Bangalore"
}

Example response:

{
  "orderId": 1,
  "status": "CREATED"
}

## Payment Service

Port:

8086

Consumer Group:

payment-service-group

Consumes:

order-created

Responsibilities:

- Consume ORDER_CREATED events
- Process payment
- Publish PAYMENT_SUCCESS
- Publish PAYMENT_FAILED
- Handle retry scenarios
- Handle duplicate events using eventId
- Route failed messages to the Dead Letter Topic

Payment logic used in the project:

- Amount greater than 0 → PAYMENT_SUCCESS
- Amount less than or equal to 0 → PAYMENT_FAILED

## Delivery Service

Port:

8087

Consumer Group:

delivery-service-group

Consumes:

payment-success

Responsibilities:

- Consume successful payment events
- Create delivery records
- Generate tracking numbers
- Publish DELIVERY_CREATED events

## Notification Service

Port:

8088

Consumer Group:

notification-service-group

Consumes:

- order-created
- payment-success
- payment-failed
- delivery-created

Responsibilities:

- Consume order events
- Consume payment events
- Consume delivery events
- Simulate customer notifications

## Kafka Topics

| Topic | Partitions | Purpose |
|---|---:|---|
| order-created | 3 | Order creation events |
| payment-success | 3 | Successful payment events |
| payment-failed | 3 | Failed payment events |
| delivery-created | 3 | Delivery creation events |
| payment-success.DLT | 3 | Failed messages after retry exhaustion |

## Kafka Message Key

The Order Service uses orderId as the Kafka message key when publishing ORDER_CREATED events.

Example:

Key:

orderId = 100

Value:

ORDER_CREATED event

Using orderId as the message key allows events for the same order to be routed consistently to the same Kafka partition.

## Kafka Partitions

The main Kafka topics use 3 partitions.

Example:

order-created

Partition 0
Partition 1
Partition 2

Partitions allow Kafka to distribute messages and support parallel consumption.

## Consumer Groups

### Payment Service

payment-service-group

Consumes:

order-created

### Delivery Service

delivery-service-group

Consumes:

payment-success

### Notification Service

notification-service-group

Consumes:

- order-created
- payment-success
- payment-failed
- delivery-created

## Consumer Scaling

Payment Service was tested using multiple service instances belonging to the same consumer group.

Payment Service Instance 1
            |
            |
Payment Service Instance 2
            |
            ↓
    payment-service-group
            |
            ↓
      Kafka Partitions

When multiple consumers belong to the same consumer group, Kafka distributes partitions among the active consumers.

This demonstrates consumer scaling using Kafka consumer groups.

## Consumer Offsets

Kafka consumer groups maintain offsets for consumed messages.

The project was tested using Kafka consumer-group commands to inspect:

- Current Offset
- Log End Offset
- Lag
- Consumer ID
- Topic
- Partition

The demonstrated consumer groups reached zero lag after successful processing.

## Failure & Recovery

A failure scenario was intentionally tested by stopping the Payment Service while an order event was available in Kafka.

Flow:

Order Service
   |
   ↓
ORDER_CREATED
   |
   ↓
order-created
   |
   ↓
Payment Service OFF
   |
   ↓
Message remains available in Kafka
   |
   ↓
Payment Service ON
   |
   ↓
Payment Service consumes pending event
   |
   ↓
PAYMENT_SUCCESS
   |
   ↓
Delivery Service
   |
   ↓
DELIVERY_CREATED

After restarting the Payment Service, the pending event was consumed and processing continued.

This demonstrates Kafka-based message retention and service recovery.

## Retry Handling

Retry handling was implemented in the Payment Service using Spring Kafka error handling.

When message processing fails, Kafka retries the message before routing it to the Dead Letter Topic.

Retry flow:

Message
   |
   ↓
Initial Attempt
   |
   ↓
Retry #1
   |
   ↓
Retry #2
   |
   ↓
Retry Exhausted
   |
   ↓
payment-success.DLT

The configured retry mechanism uses a fixed retry interval before sending the failed message to the DLT.

## Dead Letter Topic

The project implements the following Dead Letter Topic:

payment-success.DLT

An intentionally invalid message was sent to the order-created topic:

THIS IS NOT A VALID JSON

The Payment Service failed while processing the invalid message.

After the configured retry attempts were exhausted, the message was successfully routed to:

payment-success.DLT

DLT flow:

order-created
   |
   ↓
Payment Service
   |
   ↓
JSON Processing Failure
   |
   ↓
Retry #1
   |
   ↓
Retry #2
   |
   ↓
Retry Exhausted
   |
   ↓
payment-success.DLT

## Idempotency

Idempotency was implemented using eventId.

Example event:

{
  "eventId": "DUP-10001",
  "eventType": "ORDER_CREATED",
  "orderId": 100,
  "customerId": 101,
  "amount": 50000,
  "deliveryAddress": "Bangalore"
}

The same event was intentionally sent twice.

First event:

eventId = DUP-10001
        |
        ↓
Event processed
        |
        ↓
Payment successful

Same event sent again:

eventId = DUP-10001
        |
        ↓
Event already processed
        |
        ↓
Duplicate event ignored

The Payment Service logged:

Duplicate event ignored. EventId: DUP-10001

This prevents duplicate processing of the same event.

Note:

The current project demonstration uses an in-memory set for tracking processed event IDs.

In a production system, idempotency keys would typically be persisted in a database or distributed store such as Redis.

## Payment Success Flow

Valid payment event:

ORDER_CREATED
   |
   ↓
Payment Service
   |
   ↓
Amount > 0
   |
   ↓
PAYMENT_SUCCESS
   |
   ↓
payment-success
   |
   ↓
Delivery Service
   |
   ↓
DELIVERY_CREATED

## Payment Failure Flow

Invalid payment amount:

ORDER_CREATED
   |
   ↓
Payment Service
   |
   ↓
Amount <= 0
   |
   ↓
PAYMENT_FAILED
   |
   ↓
payment-failed
   |
   ↓
Notification Service

## Kafka Commands

### List Kafka Topics

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

### Consume order-created

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order-created --from-beginning

### Consume payment-success

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-success --from-beginning

### Consume payment-failed

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-failed --from-beginning

### Consume delivery-created

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic delivery-created --from-beginning

### Consume Dead Letter Topic

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-success.DLT --from-beginning

## Consumer Group Commands

### Payment Service

docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group payment-service-group

### Delivery Service

docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group delivery-service-group

### Notification Service

docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group notification-service-group

## Topic Partition Commands

### order-created

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic order-created

### payment-success

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic payment-success

### payment-failed

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic payment-failed

### delivery-created

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic delivery-created

### payment-success.DLT

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic payment-success.DLT

## Docker & Kafka Setup

Kafka broker:

localhost:9092

Kafka is running through Docker.

The project uses Docker-based Kafka infrastructure for local development and testing.

## How to Run

1. Start Docker Desktop.
2. Start Kafka and Zookeeper.
3. Verify Kafka is running on localhost:9092.
4. Create the required Kafka topics.
5. Configure MySQL.
6. Start Order Service on port 8085.
7. Start Payment Service on port 8086.
8. Start Delivery Service on port 8087.
9. Start Notification Service on port 8088.
10. Import the Postman collection.
11. Send POST /orders.
12. Verify the events using Kafka console consumers.
13. Verify consumer groups and offsets.
14. Test Payment Service failure and recovery.
15. Test retry handling.
16. Test the Dead Letter Topic.
17. Test idempotency using the same eventId.

## MySQL Configuration

Database used by the project:

18lpa_preparation

The services use MySQL with Spring Data JPA and Hibernate.

Important:

Database passwords and other secrets should not be committed to GitHub.

Use environment variables or local configuration for sensitive credentials.

## Project Structure

kafka-order-processing-system/
|
├── README.md
├── .gitignore
|
├── docs/
|   |
|   ├── Kafka-Order-Processing-System-Report.pdf
|   |
|   ├── architecture/
|   |   └── architecture.png
|   |
|   └── screenshots/
|       ├── 01-order-service.png
|       ├── 02-payment-success.png
|       ├── 03-payment-failed.png
|       ├── 04-delivery-created.png
|       ├── 05-kafka-partitions.png
|       ├── 06-consumer-groups.png
|       ├── 07-consumer-scaling.png
|       ├── 08-failure-recovery.png
|       ├── 09-retry.png
|       ├── 10-dlt.png
|       └── 11-idempotency.png
|
├── postman/
|   └── Kafka-Order-Processing-System.postman_collection.json
|
├── project-kafka-order-service/
|   ├── pom.xml
|   └── src/
|
├── project-kafka-payment-service/
|   ├── pom.xml
|   └── src/
|
├── project-kafka-delivery-service/
|   ├── pom.xml
|   └── src/
|
└── project-kafka-notification-service/
    ├── pom.xml
    └── src/

## Testing

The project was tested using:

- Postman
- Kafka Console Producer
- Kafka Console Consumer
- Kafka Consumer Group commands
- Kafka Topic commands
- Eclipse console logs
- MySQL

The following scenarios were demonstrated:

- Successful order creation
- Successful payment
- Failed payment
- Delivery creation
- Notification consumption
- Kafka partitions
- Consumer groups
- Consumer scaling
- Payment Service failure
- Kafka message retention during service failure
- Payment Service recovery
- Retry handling
- Dead Letter Topic
- Duplicate event handling
- Idempotency using eventId

## Key Kafka Concepts Demonstrated

- Producer / Consumer
- Topics
- Partitions
- Message Keys
- Offsets
- Consumer Groups
- Consumer Scaling
- Asynchronous Communication
- Event-Driven Architecture
- Failure Recovery
- Retry
- Idempotency
- Dead Letter Topic
- Message Retention
- Microservice Communication

## Project Documentation

The complete project report is available under:

docs/Kafka-Order-Processing-System-Report.pdf

The report contains:

- Project architecture
- Microservice implementation
- Kafka configuration
- Kafka topics
- Kafka partitions
- Consumer groups
- Consumer scaling
- Failure and recovery demonstration
- Retry implementation
- Dead Letter Topic implementation
- Idempotency implementation
- Testing evidence
- Screenshots

## Project Evidence

Screenshots are available under:

docs/screenshots/

The screenshots include evidence for:

- Order Service
- Payment Success
- Payment Failure
- Delivery Creation
- Kafka Topics
- Kafka Partitions
- Consumer Groups
- Consumer Scaling
- Failure Recovery
- Retry
- Dead Letter Topic
- Idempotency

## Postman Collection

The Postman collection is available under:

postman/Kafka-Order-Processing-System.postman_collection.json

Import the collection into Postman to test the Order Service API.

## Key Learning Outcomes

Through this project, I gained practical experience with:

1. Building event-driven microservices using Spring Boot.
2. Integrating Apache Kafka with Spring Boot.
3. Publishing and consuming Kafka events.
4. Working with Kafka topics and partitions.
5. Using Kafka message keys.
6. Understanding consumer groups and offsets.
7. Scaling Kafka consumers.
8. Handling service failures and recovery.
9. Implementing retry mechanisms.
10. Implementing event-level idempotency.
11. Handling failed messages using Dead Letter Topics.
12. Understanding asynchronous microservice communication.

## Production Considerations

This project focuses on demonstrating Kafka and microservice concepts in a practical development environment.

For a production-grade implementation, additional considerations would include:

- Persistent idempotency storage
- Distributed caching where required
- Kafka replication and fault tolerance
- Event schema management
- Event versioning
- Centralized logging
- Monitoring and alerting
- Distributed tracing
- Secure Kafka authentication and authorization
- Externalized configuration
- Secret management
- Container orchestration
- Database transaction consistency
- Reliable event publishing strategies

## Future Enhancements

Possible future improvements include:

- Implementing persistent idempotency using MySQL or Redis
- Adding centralized configuration
- Adding service discovery
- Adding API Gateway
- Adding distributed tracing
- Adding monitoring with Prometheus and Grafana
- Adding Kafka Schema Registry
- Adding stronger event versioning
- Adding authentication and authorization
- Deploying the complete system using Docker Compose or Kubernetes

## Author

Shubham Mahajan

Software Developer | Java Backend | Spring Boot | Apache Kafka

## Project Purpose

This project was developed to gain practical experience with:

Java + Spring Boot + Apache Kafka + Microservices + Event-Driven Architecture

The project demonstrates how asynchronous communication, Kafka topics, partitions, consumer groups, offsets, consumer scaling, failure recovery, retries, idempotency, and Dead Letter Topics work together in a practical microservices-based Order Processing System.

## Repository Contents

This repository contains:

- Four Spring Boot microservices
- Kafka configuration
- Kafka producer and consumer implementations
- MySQL/JPA integration
- Retry and error handling
- Idempotency implementation
- Dead Letter Topic implementation
- Postman collection
- Architecture documentation
- Project screenshots
- Complete project report

## License

This project is created for learning, demonstration, and portfolio purposes.
