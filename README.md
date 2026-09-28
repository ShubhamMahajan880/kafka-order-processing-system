# Kafka Order Processing System

An event-driven Order Processing System built using Spring Boot Microservices and Apache Kafka.

The project demonstrates asynchronous communication between microservices using Kafka, along with partitions, consumer groups, consumer scaling, failure recovery, retries, idempotency, and Dead Letter Topics (DLT).

---

## 🚀 Project Overview

The system simulates an order processing workflow where an order moves through multiple independent microservices using Kafka events.

Instead of directly calling another service synchronously, services communicate through Kafka topics.

### Main Flow

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
   ↓
Payment Service
   |
   +----------------------+
   |                      |
   ↓                      ↓
PAYMENT_SUCCESS      PAYMENT_FAILED
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

Notification Service also consumes:
- order-created
- payment-success
- payment-failed
- delivery-created

---

## 🏗️ Microservices

| Service | Port | Responsibility |
|---|---:|---|
| Order Service | 8085 | Creates orders and publishes ORDER_CREATED events |
| Payment Service | 8086 | Processes payments and publishes payment events |
| Delivery Service | 8087 | Creates deliveries after successful payment |
| Notification Service | 8088 | Consumes events and simulates customer notifications |

---

## 🛠️ Technologies Used

- Java 21
- Spring Boot 4.1.1
- Spring Kafka
- Apache Kafka
- MySQL
- Spring Data JPA
- Hibernate
- Docker
- Maven
- Postman
- Eclipse IDE

---

## 📨 Kafka Topics

| Topic | Partitions | Purpose |
|---|---:|---|
| `order-created` | 3 | Order Service → Payment Service / Notification Service |
| `payment-success` | 3 | Payment Service → Delivery Service / Notification Service |
| `payment-failed` | 3 | Payment Service → Notification Service |
| `delivery-created` | 3 | Delivery Service → Notification Service |
| `payment-success.DLT` | 3 | Stores messages that fail after retry attempts |

The `orderId` is used as the Kafka message key when publishing order events.

---

## 👥 Kafka Consumer Groups

### Payment Service

```text
payment-service-group
