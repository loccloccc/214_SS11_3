# Order Service

Non-blocking Order API sử dụng Spring Boot 3 + WebFlux + Kafka.

## Kiến trúc

```
Client
  ↓ POST /api/v1/orders
  ↓ OrderController (Mono<OrderResponse>)
  ↓ OrderService    (tạo orderId, build event)
  ↓ OrderEventProducer (publish → Kafka)
  ↓ Topic: storex-order-events (5 partitions)
  ↓ HTTP 202 Accepted { "orderId": "ORD-..." }
```

## BUG-03 — Kafka Key = orderId

Mọi event liên quan đến cùng một đơn hàng phải vào **cùng partition**:

```
order.created  ─┐
order.paid     ─┤─ hash(ORD-001) → Partition 3
order.shipped  ─┤
order.completed─┘
```

**Sai (KHÔNG làm):**
```java
kafkaTemplate.send(topic, event);        // Key = null → round-robin
```

**Đúng (đã implement):**
```java
kafkaTemplate.send(topic, orderId, event); // Key = orderId → same partition
```

## Chạy Project

### 1. Khởi động Kafka bằng Docker Compose

```bash
docker-compose up -d
```

Đợi ~20 giây để Kafka và topic `storex-order-events` được tạo.

### 2. Build & Run Spring Boot

```bash
mvn clean package -DskipTests
mvn spring-boot:run
```

Hoặc chạy file JAR:
```bash
java -jar target/order-service-1.0.0-SNAPSHOT.jar
```

### 3. Test bằng curl

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUS-001",
    "productId": "PROD-001",
    "quantity": 2,
    "totalAmount": 150000
  }'
```

Expected response:
```
HTTP/1.1 202 Accepted
{
  "orderId": "ORD-A1B2C3D4"
}
```

### 4. Test bằng Postman

- Method: `POST`
- URL: `http://localhost:8080/api/v1/orders`
- Body (raw JSON):
```json
{
  "customerId": "CUS-001",
  "productId": "PROD-001",
  "quantity": 2,
  "totalAmount": 150000
}
```
- Expected: Status `202 Accepted`

### 5. Xem message trên Kafka UI

Mở browser: http://localhost:8090
→ Cluster `local` → Topics → `storex-order-events` → Messages

## Chạy Tests

```bash
mvn test
```

## Cấu trúc Project

```
order-service/
├── pom.xml
├── docker-compose.yml
├── README.md
└── src/
    ├── main/
    │   ├── java/org/example/orderservice/
    │   │   ├── OrderServiceApplication.java
    │   │   ├── controller/OrderController.java
    │   │   ├── service/OrderService.java
    │   │   ├── producer/OrderEventProducer.java    ← BUG-03 fix here
    │   │   ├── event/OrderCreatedEvent.java
    │   │   ├── dto/CreateOrderRequest.java
    │   │   ├── dto/OrderResponse.java
    │   │   └── config/KafkaProducerConfig.java     ← BUG-03 explained here
    │   └── resources/application.yml
    └── test/
        └── java/org/example/orderservice/
            ├── controller/OrderControllerTest.java
            ├── service/OrderServiceTest.java
            └── producer/OrderEventProducerTest.java ← BUG-03 verified here
```
"# 214_SS11_3" 
