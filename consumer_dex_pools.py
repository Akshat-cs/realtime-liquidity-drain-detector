# Consumer for ETH DEX Pools Kafka stream
# Subscribes to eth.dexpools.proto and prints full messages

import uuid
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from google.protobuf.descriptor import FieldDescriptor
from evm import dex_pool_block_message_pb2
import logging
import os
import config
import datetime
import signal
import sys

# Kafka consumer configuration
group_id_suffix = uuid.uuid4().hex
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'{config.username}-dex-pools-{group_id_suffix}',  
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': config.username,
    'sasl.password': config.password,
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
topic = 'eth.dexpools.proto'
consumer.subscribe([topic])

# Control flag for graceful shutdown
shutdown_event = False
processed_count = 0
decode_error_count = 0

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Helper functions for printing protobuf messages --- #

def convert_bytes_to_hex(value):
    """Convert bytes to hexadecimal string"""
    return '0x' + value.hex()

def convert_bytes_to_int(value):
    """Convert bytes to integer (big-endian)"""
    return int.from_bytes(value, byteorder='big')

def print_protobuf_message(msg, indent=0):
    """
    Recursively print all fields in a protobuf message
    """
    prefix = ' ' * indent
    
    for field in msg.DESCRIPTOR.fields:
        field_name = field.name
        value = getattr(msg, field_name)

        # Skip empty fields
        if field.label == FieldDescriptor.LABEL_REPEATED:
            if not value:
                continue
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            if not msg.HasField(field_name):
                continue
        elif not value:
            continue

        # Handle repeated fields (arrays/lists)
        if field.label == FieldDescriptor.LABEL_REPEATED:
            print(f"{prefix}{field_name} (repeated):")
            for idx, item in enumerate(value):
                if field.type == FieldDescriptor.TYPE_MESSAGE:
                    print(f"{prefix}  [{idx}]:")
                    print_protobuf_message(item, indent + 4)
                elif field.type == FieldDescriptor.TYPE_BYTES:
                    hex_str = convert_bytes_to_hex(item)
                    int_val = convert_bytes_to_int(item)
                    print(f"{prefix}  [{idx}]: {hex_str} (uint: {int_val})")
                else:
                    print(f"{prefix}  [{idx}]: {item}")

        # Handle nested message fields
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            print(f"{prefix}{field_name}:")
            print_protobuf_message(value, indent + 2)

        # Handle bytes fields (convert to hex and optionally to int)
        elif field.type == FieldDescriptor.TYPE_BYTES:
            hex_str = convert_bytes_to_hex(value)
            # Try to show as integer if it's small enough (likely a number)
            if len(value) <= 32:  # Reasonable size for a number
                try:
                    int_val = convert_bytes_to_int(value)
                    print(f"{prefix}{field_name}: {hex_str} (uint: {int_val})")
                except:
                    print(f"{prefix}{field_name}: {hex_str}")
            else:
                print(f"{prefix}{field_name}: {hex_str}")

        # Handle oneof fields
        elif field.containing_oneof:
            if msg.WhichOneof(field.containing_oneof.name) == field_name:
                print(f"{prefix}{field_name} (oneof): {value}")

        # Handle regular fields
        else:
            print(f"{prefix}{field_name}: {value}")

def convert_bytes_to_int(value):
    """Convert bytes to integer (big-endian)"""
    if isinstance(value, bytes):
        return int.from_bytes(value, byteorder='big')
    return int(value)

def process_message(buffer):
    """Process a single protobuf message and print pool info in simple format"""
    global processed_count, decode_error_count
    
    try:
        # Parse the DexPoolBlockMessage
        dex_pool_block = dex_pool_block_message_pb2.DexPoolBlockMessage()
        dex_pool_block.ParseFromString(buffer)
        
        # Process each pool event
        for pool_event in dex_pool_block.PoolEvents:
            try:
                pool = pool_event.Pool
                liquidity = pool_event.Liquidity
                
                # Get pool address
                pool_address = convert_bytes_to_hex(pool.SmartContract) if hasattr(pool, 'SmartContract') else 'unknown'
                
                # Get currency A info
                currency_a_address = convert_bytes_to_hex(pool.CurrencyA.SmartContract) if hasattr(pool.CurrencyA, 'SmartContract') else 'unknown'
                currency_a_symbol = pool.CurrencyA.Symbol if hasattr(pool.CurrencyA, 'Symbol') else 'UNKNOWN'
                currency_a_decimals = pool.CurrencyA.Decimals if hasattr(pool.CurrencyA, 'Decimals') else 18
                
                # Get currency B info
                currency_b_address = convert_bytes_to_hex(pool.CurrencyB.SmartContract) if hasattr(pool.CurrencyB, 'SmartContract') else 'unknown'
                currency_b_symbol = pool.CurrencyB.Symbol if hasattr(pool.CurrencyB, 'Symbol') else 'UNKNOWN'
                currency_b_decimals = pool.CurrencyB.Decimals if hasattr(pool.CurrencyB, 'Decimals') else 18
                
                # Get amounts (schema now sends floats directly - human-readable)
                amount_a_raw = liquidity.AmountCurrencyA if hasattr(liquidity, 'AmountCurrencyA') else 0
                amount_b_raw = liquidity.AmountCurrencyB if hasattr(liquidity, 'AmountCurrencyB') else 0
                
                # Handle float values (new schema)
                if isinstance(amount_a_raw, (float, int)):
                    amount_a_human = float(amount_a_raw)
                elif isinstance(amount_a_raw, bytes):
                    # Backward compatibility: convert bytes to int, then to human-readable
                    amount_a_int = convert_bytes_to_int(amount_a_raw)
                    amount_a_human = amount_a_int / (10 ** currency_a_decimals) if currency_a_decimals > 0 else float(amount_a_int)
                else:
                    amount_a_human = 0.0
                
                if isinstance(amount_b_raw, (float, int)):
                    amount_b_human = float(amount_b_raw)
                elif isinstance(amount_b_raw, bytes):
                    # Backward compatibility: convert bytes to int, then to human-readable
                    amount_b_int = convert_bytes_to_int(amount_b_raw)
                    amount_b_human = amount_b_int / (10 ** currency_b_decimals) if currency_b_decimals > 0 else float(amount_b_int)
                else:
                    amount_b_human = 0.0
                
                # Print in simple format
                print(f"{pool_address} | {amount_a_human:,.2f} {currency_a_symbol} ({currency_a_address}) | {amount_b_human:,.2f} {currency_b_symbol} ({currency_b_address})")
                
            except Exception as err:
                logger.error(f"Error processing pool event: {err}", exc_info=True)
        
        processed_count += 1
        
    except DecodeError as err:
        decode_error_count += 1
        buffer_size = len(buffer) if buffer else 0
        if decode_error_count <= 3 or decode_error_count % 10 == 0:
            logger.warning(f"Protobuf decoding error (count: {decode_error_count}, buffer size: {buffer_size} bytes): {err}")
    except Exception as err:
        logger.error(f"Error processing message: {err}", exc_info=True)

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_event
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event = True

# --- Main execution --- #

def main():
    global processed_count, shutdown_event
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info(f"Starting consumer for topic: {topic}")
    logger.info(f"Consumer group ID: {conf['group.id']}")
    logger.info("Press Ctrl+C to stop\n")
    
    try:
        while not shutdown_event:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            try:
                process_message(msg.value())
            except Exception as err:
                logger.exception(f"Failed to process message: {err}")
                
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Error in main polling loop: {e}")
    finally:
        # Graceful shutdown
        logger.info("Initiating graceful shutdown...")
        
        # Close Kafka consumer
        consumer.close()
        logger.info(f"Shutdown complete. Total messages processed: {processed_count}")
        if decode_error_count > 0:
            logger.info(f"Total decode errors: {decode_error_count}")

if __name__ == "__main__":
    main()

