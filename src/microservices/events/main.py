import os
import json
import logging
import threading
import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='[Events Service] %(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')

producer = KafkaProducer(
    bootstrap_servers=kafka_brokers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

def start_consumers():
    topics = ['movie-events', 'user-events', 'payment-events']
    
    for topic in topics:
        thread = threading.Thread(target=consume_topic, args=(topic,), daemon=True)
        thread.start()
        logger.info(f"Started consumer for topic: {topic}")

def consume_topic(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_brokers,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=1000
    )
    
    logger.info(f"Consumer for {topic_name} started, waiting for messages...")
    
    while True:
        try:
            message_pack = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in message_pack.items():
                for message in messages:
                    event_data = message.value
                    logger.info(f"[{topic_name}] Received event: {json.dumps(event_data, indent=2)}")
        except Exception as e:
            logger.error(f"Error consuming from {topic_name}: {str(e)}")
            break

@app.route('/api/events/health', methods=['GET'])
def health():
    return jsonify({"status": True}), 200

@app.route('/api/events/movie', methods=['POST'])
def create_movie_event():
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        
        required_fields = ['movie_id', 'title', 'action']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        event_id = f"movie-{data['movie_id']}-{data['action']}-{uuid.uuid4().hex[:8]}"
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        event = {
            "id": event_id,
            "type": "movie",
            "timestamp": timestamp,
            "payload": data
        }
        
        future = producer.send('movie-events', value=event, key=str(data['movie_id']))
        record_metadata = future.get(timeout=10)
        
        response = {
            "status": "success",
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "event": event
        }
        
        logger.info(f"Movie event sent to Kafka: {event_id}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
        
        return jsonify(response), 201
        
    except KafkaError as e:
        logger.error(f"Kafka error: {str(e)}")
        return jsonify({"error": "Failed to send event to Kafka"}), 500
    except Exception as e:
        logger.error(f"Error creating movie event: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/events/user', methods=['POST'])
def create_user_event():
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        
        required_fields = ['user_id', 'action', 'timestamp']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        event_id = f"user-{data['user_id']}-{data['action']}-{uuid.uuid4().hex[:8]}"
        
        event = {
            "id": event_id,
            "type": "user",
            "timestamp": data['timestamp'],
            "payload": data
        }
        
        future = producer.send('user-events', value=event, key=str(data['user_id']))
        record_metadata = future.get(timeout=10)
        
        response = {
            "status": "success",
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "event": event
        }
        
        logger.info(f"User event sent to Kafka: {event_id}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
        
        return jsonify(response), 201
        
    except KafkaError as e:
        logger.error(f"Kafka error: {str(e)}")
        return jsonify({"error": "Failed to send event to Kafka"}), 500
    except Exception as e:
        logger.error(f"Error creating user event: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/events/payment', methods=['POST'])
def create_payment_event():
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        
        required_fields = ['payment_id', 'user_id', 'amount', 'status', 'timestamp']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        event_id = f"payment-{data['payment_id']}-{data['status']}-{uuid.uuid4().hex[:8]}"
        
        event = {
            "id": event_id,
            "type": "payment",
            "timestamp": data['timestamp'],
            "payload": data
        }
        
        future = producer.send('payment-events', value=event, key=str(data['user_id']))
        record_metadata = future.get(timeout=10)
        
        response = {
            "status": "success",
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "event": event
        }
        
        logger.info(f"Payment event sent to Kafka: {event_id}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
        
        return jsonify(response), 201
        
    except KafkaError as e:
        logger.error(f"Kafka error: {str(e)}")
        return jsonify({"error": "Failed to send event to Kafka"}), 500
    except Exception as e:
        logger.error(f"Error creating payment event: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    logger.info("Starting Events Service...")
    logger.info(f"Kafka brokers: {kafka_brokers}")
    
    start_consumers()
    
    port = int(os.getenv('PORT', '8082'))
    logger.info(f"Events Service API listening on port {port}")
    
    app.run(host='0.0.0.0', port=port, debug=False)

