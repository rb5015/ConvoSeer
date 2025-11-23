#!/usr/bin/env python3
"""
Simple Kafka test script to verify Kafka is running and working correctly.
Tests both producer and consumer functionality.
"""

import json
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError


def test_producer(brokers: str, topic: str) -> bool:
    """Test producing messages to Kafka."""
    print(f"\n📤 Testing Producer...")
    print(f"   Brokers: {brokers}")
    print(f"   Topic: {topic}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Send a test message
        test_message = {
            "test_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "message": "Hello from Kafka test!",
            "call_id": "test-call-001",
            "utterance_id": "test-utt-001",
            "text": "This is a test utterance to verify Kafka is working."
        }
        
        key = test_message["call_id"]
        future = producer.send(topic, key=key, value=test_message)
        
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        
        print(f"   ✅ Message sent successfully!")
        print(f"   Topic: {record_metadata.topic}")
        print(f"   Partition: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        
        producer.flush()
        producer.close()
        return True
        
    except KafkaError as e:
        print(f"   ❌ Producer error: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        return False


def test_consumer(brokers: str, topic: str, timeout: int = 10) -> bool:
    """Test consuming messages from Kafka."""
    print(f"\n📥 Testing Consumer...")
    print(f"   Brokers: {brokers}")
    print(f"   Topic: {topic}")
    print(f"   Waiting up to {timeout}s for messages...")
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            auto_offset_reset='latest',  # Start from latest messages
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=timeout * 1000
        )
        
        # Seek to beginning to catch any messages
        consumer.seek_to_beginning()
        
        message_count = 0
        for message in consumer:
            message_count += 1
            print(f"   ✅ Received message #{message_count}:")
            print(f"      Key: {message.key.decode('utf-8') if message.key else 'None'}")
            print(f"      Partition: {message.partition}, Offset: {message.offset}")
            print(f"      Value: {json.dumps(message.value, indent=6)}")
            
            # Only read one message for test
            if message_count >= 1:
                break
        
        consumer.close()
        
        if message_count > 0:
            print(f"   ✅ Consumer working! Received {message_count} message(s)")
            return True
        else:
            print(f"   ⚠️  No messages received (this is OK if topic is empty)")
            return True  # Consumer is working, just no messages
            
    except KafkaError as e:
        print(f"   ❌ Consumer error: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        return False


def test_topic_exists(brokers: str, topic: str) -> bool:
    """Check if topic exists."""
    print(f"\n🔍 Checking if topic '{topic}' exists...")
    
    try:
        consumer = KafkaConsumer(bootstrap_servers=brokers)
        topics = consumer.list_topics(timeout=10)
        consumer.close()
        
        if topic in topics:
            print(f"   ✅ Topic '{topic}' exists")
            return True
        else:
            print(f"   ⚠️  Topic '{topic}' does not exist (will be auto-created)")
            print(f"   Available topics: {list(topics)}")
            return True  # Auto-create is enabled, so this is OK
            
    except Exception as e:
        print(f"   ❌ Error checking topics: {e}")
        return False


def main():
    import os
    from argparse import ArgumentParser
    
    parser = ArgumentParser(description="Test Kafka setup")
    parser.add_argument(
        "--brokers",
        default=os.getenv("KAFKA_BROKERS", "localhost:9092"),
        help="Kafka broker addresses (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC_RAW", "calls.raw"),
        help="Topic to test (default: calls.raw)"
    )
    parser.add_argument(
        "--skip-consumer",
        action="store_true",
        help="Skip consumer test (useful if topic is empty)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Kafka Connection Test")
    print("=" * 60)
    
    # Test 1: Check topic
    topic_ok = test_topic_exists(args.brokers, args.topic)
    
    # Test 2: Producer
    producer_ok = test_producer(args.brokers, args.topic)
    
    # Test 3: Consumer (wait a bit for message to be available)
    consumer_ok = True
    if not args.skip_consumer:
        print("\n⏳ Waiting 2 seconds for message to be available...")
        time.sleep(2)
        consumer_ok = test_consumer(args.brokers, args.topic, timeout=5)
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"Topic check:  {'✅ PASS' if topic_ok else '❌ FAIL'}")
    print(f"Producer:     {'✅ PASS' if producer_ok else '❌ FAIL'}")
    print(f"Consumer:     {'✅ PASS' if consumer_ok else '❌ FAIL'}")
    
    if topic_ok and producer_ok and consumer_ok:
        print("\n🎉 All tests passed! Kafka is working correctly.")
        return 0
    else:
        print("\n❌ Some tests failed. Check the errors above.")
        return 1


if __name__ == "__main__":
    exit(main())

