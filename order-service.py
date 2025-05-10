import requests
from kafka import KafkaProducer
import json
import uuid

# Configuration
inventory_service_url = "http://localhost:8082/api/inventory"
kafka_topic = "notificationTopic"
kafka_broker = "localhost:9092"  # Change if your Kafka is hosted elsewhere

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def check_inventory(sku_code):
    """Call inventory service and return True if in stock"""
    try:
        response = requests.get(inventory_service_url, params={"skuCode": sku_code})
        response.raise_for_status()
        inventory = response.json()
        return inventory[0]["inStock"] if inventory else False
    except Exception as e:
        print(f"Error checking inventory: {e}")
        return False

def send_notification(order_number, email):
    """Send message to Kafka topic in required format"""
    message = {
        "eventType": "ORDER_PLACED",
        "orderNumber": order_number,
        "email": email
    }
    producer.send(kafka_topic, message)
    producer.flush()
    print(f"✅ Notification sent for order {order_number}")

def process_order(sku_code, email):
    print(f"➡️  Checking inventory for: {sku_code}")
    if check_inventory(sku_code):
        order_number = str(uuid.uuid4())[:8]
        print(f"✅ Product {sku_code} is in stock. Placing order #{order_number}...")
        send_notification(order_number, email)
    else:
        print(f"❌ Product {sku_code} is out of stock.")

if __name__ == "__main__":
    sku_code = input("Enter SKU code: ")
    email = input("Enter customer email: ")
    process_order(sku_code, email)

