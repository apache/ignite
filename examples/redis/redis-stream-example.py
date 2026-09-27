import redis
import time
import threading
from datetime import datetime

# Connect to Redis (adjust host/port as needed)
r = redis.StrictRedis(host='localhost', port=11212, db=0, 
                      socket_timeout=600000,socket_connect_timeout=600000,decode_responses=True,retry= None)

def clear_stream(stream_name):
    """Clear all messages from a stream"""
    try:
        # Get all message IDs
        messages = r.xrange(stream_name, count=10000)
        if messages:
            # Delete all messages
            message_ids = [msg[0] for msg in messages]
            r.xdel(stream_name, *message_ids)
            print(f"Cleared {len(message_ids)} messages from {stream_name}")
    except Exception as e:
        print(f"Error clearing stream: {e}")

def example_xadd_basic():
    """Basic XADD examples"""
    print("\n=== Basic XADD Examples ===")
    stream_name = "user-actions2"
    
    # Clear existing data
    clear_stream(stream_name)
    
    # 1. Simple message with auto-generated ID
    message_id = r.xadd(stream_name, {
        'user_id': '1001',
        'action': 'login',
        'timestamp': str(time.time())
    })
    print(f"Added message with ID: {message_id}")
    
    # 2. Add multiple messages
    for i in range(3):
        msg_id = r.xadd(stream_name, {
            'user_id': f'100{i+1}',
            'action': 'click',
            'page': 'home',
            'timestamp': str(time.time())
        })
        print(f"Added message {i+1}: {msg_id}")
    
    # 3. Add message with specific ID (usually not recommended)
    specific_id = r.xadd(stream_name, 
                        {'event': 'special'},
                        id='1234567890-0')
    print(f"Added message with specific ID: {specific_id}")
    
    # 4. Add message with MAXLEN (limit stream size)
    r.xadd(stream_name, 
           {'user_id': '9999', 'action': 'test'}, 
           maxlen=100)
    print("Added message with MAXLEN=100")
    
    # 5. Add message with approximate trimming (better performance)
    r.xadd(stream_name, 
           {'user_id': '8888', 'action': 'test2'}, 
           maxlen=100, approximate=True)
    print("Added message with approximate trimming")

def example_xrange_basic():
    """Basic XRANGE examples"""
    print("\n=== Basic XRANGE Examples ===")
    stream_name = "user-actions2"
    
    # 1. Get all messages (oldest to newest)
    all_messages = r.xrange(stream_name)
    print(f"All messages ({len(all_messages)} total):")
    for msg_id, fields in all_messages[:3]:  # Show first 3 only
        print(f"  {msg_id}: {fields}")
    
    # 2. Get messages with count limit
    first_5 = r.xrange(stream_name, count=5)
    print(f"\nFirst 5 messages: {len(first_5)}")
    
    # 3. Get messages in specific ID range
    if len(all_messages) >= 2:
        start_id = all_messages[1][0]  # Second message ID
        end_id = all_messages[-1][0]   # Last message ID
        range_messages = r.xrange(stream_name, min=start_id, max=end_id)
        print(f"\nMessages from {start_id} to {end_id}: {len(range_messages)}")
    
    # 4. Get messages after a specific ID (excluding the ID)
    if len(all_messages) >= 1:
        after_id = all_messages[0][0]
        after_messages = r.xrange(stream_name, min=f'({after_id}')
        print(f"\nMessages after {after_id}: {len(after_messages)}")
    
    # 5. Get messages by time range (IDs contain timestamps)
    # Get messages from last 10 seconds
    current_time_ms = int(time.time() * 1000)
    ten_seconds_ago = current_time_ms - 10000
    recent_messages = r.xrange(stream_name, 
                               min=f'{ten_seconds_ago}-0', 
                               max='+')
    print(f"\nMessages from last 10 seconds: {len(recent_messages)}")

def example_xread_basic():
    """Basic XREAD examples"""
    print("\n=== Basic XREAD Examples ===")
    stream_name = "notifications"
    
    # Clear and prepare data
    clear_stream(stream_name)
    
    # Add some test messages
    for i in range(5):
        r.xadd(stream_name, {
            'message': f'Test message {i+1}',
            'level': 'INFO',
            'timestamp': str(time.time())
        })
    
    # 1. Read from beginning (using '0')
    messages = r.xread({stream_name: '0'}, count=10)
    print("Reading from beginning (0):")
    for stream, msgs in messages:
        print(f"  Stream: {stream}")
        for msg_id, fields in msgs:
            print(f"    {msg_id}: {fields}")
    
    # 2. Read only new messages (using '$')
    print("\nReading only new messages ($):")
    new_messages = r.xread({stream_name: '$'})
    if new_messages:
        print(f"  Got {len(new_messages[0][1])} new messages")
    else:
        print("  No new messages")
    
    # 3. Read with block (wait for messages)
    print("\nBlocking read (will wait 3 seconds)...")
    # Add a message in 1 second using thread
    def add_message_delayed():
        time.sleep(1)
        r.xadd(stream_name, {
            'message': 'Delayed message',
            'level': 'ALERT',
            'timestamp': str(time.time())
        })
        print("  Added delayed message")
    
    threading.Thread(target=add_message_delayed).start()
    
    # Block for up to 3 seconds
    blocked_messages = r.xread({stream_name: '$'}, block=30000, count=10)
    if blocked_messages:
        for stream, msgs in blocked_messages:
            for msg_id, fields in msgs:
                print(f"  Received: {msg_id} -> {fields}")
    else:
        print("  No messages received (timeout)")    


def example_consumer_producer():
    """Complete producer-consumer example"""
    print("\n=== Producer-Consumer Example ===")
    stream_name = "task:queue"
    
    # Clear existing data
    clear_stream(stream_name)
    
    # Producer function
    def producer(num_tasks=5):
        print("Producer: Adding tasks...")
        for i in range(num_tasks):
            task_id = r.xadd(stream_name, {
                'task_id': f'TASK_{i+1}',
                'data': f'Task data {i+1}',
                'priority': 'normal',
                'created_at': str(time.time())
            })
            print(f"  Produced: {task_id}")
            time.sleep(0.5)
        print(f"Producer: Added {num_tasks} tasks")
    
    # Consumer function
    def consumer(consumer_name, batch_size=2):
        print(f"Consumer {consumer_name}: Starting...")
        last_id = '0'  # Start from beginning
        
        while True:
            # Read messages
            messages = r.xread({stream_name: last_id}, count=batch_size, block=1000)
            
            if not messages:
                print(f"Consumer {consumer_name}: No more messages, stopping")
                break
            
            for stream, msgs in messages:
                for msg_id, fields in msgs:
                    print(f"Consumer {consumer_name}: Processing {msg_id} -> {fields}")
                    # Simulate processing
                    time.sleep(0.3)
                    # Update last_id
                    last_id = msg_id
            
            # Stop after processing all messages
            if len(msgs) < batch_size:
                print(f"Consumer {consumer_name}: Processed {len(msgs)} messages")
                break
    
    # Run producer
    producer(5)
    
    # Run consumer
    consumer("Worker-1", batch_size=2)
    
    # Show remaining messages
    remaining = r.xlen(stream_name)
    print(f"\nRemaining messages in stream: {remaining}")

def example_pagination():
    """Pagination using XRANGE"""
    print("\n=== Pagination Example ===")
    stream_name = "logs:paginated"
    
    # Clear and add test data
    clear_stream(stream_name)
    for i in range(25):
        r.xadd(stream_name, {
            'log_id': str(i+1),
            'message': f'Log entry {i+1}',
            'timestamp': str(time.time())
        })
    
    # Pagination function
    def paginate_stream(stream_name, page_size=10, page_num=1):
        """Get a specific page of messages (oldest to newest)"""
        if page_num == 1:
            # First page: from beginning
            messages = r.xrange(stream_name, count=page_size)
        else:
            # Get the last message of previous page
            prev_page = r.xrange(stream_name, count=page_size * (page_num - 1))
            if not prev_page:
                return []
            last_id = prev_page[-1][0]
            # Get next page (exclude last_id)
            messages = r.xrange(stream_name, min=f'({last_id}', count=page_size)
        return messages
    
    # Demonstrate pagination
    page_size = 10
    total_messages = r.xlen(stream_name)
    total_pages = (total_messages + page_size - 1) // page_size
    
    print(f"Total messages: {total_messages}, Page size: {page_size}")
    print(f"Total pages: {total_pages}\n")
    
    for page in range(1, min(4, total_pages + 1)):
        messages = paginate_stream(stream_name, page_size, page)
        print(f"Page {page}: {len(messages)} messages")
        for msg_id, fields in messages[:3]:  # Show first 3 of each page
            print(f"  {msg_id}: {fields['log_id']} - {fields['message']}")
        print()

def example_reverse_reading():
    """Reading messages from newest to oldest using XREVRANGE"""
    print("\n=== Reverse Reading Example (Newest First) ===")
    stream_name = "recent:updates"
    
    # Clear and add test data
    clear_stream(stream_name)
    for i in range(15):
        r.xadd(stream_name, {
            'update_id': str(i+1),
            'content': f'Update {i+1}',
            'timestamp': str(time.time())
        })
    
    # 1. Get newest 5 messages
    newest_5 = r.xrevrange(stream_name, count=5)
    print("Newest 5 messages:")
    for msg_id, fields in newest_5:
        print(f"  {msg_id}: {fields}")
    
    # 2. Get all messages from newest to oldest
    all_reverse = r.xrevrange(stream_name, count=20)
    print(f"\nAll messages in reverse order: {len(all_reverse)}")
    for msg_id, fields in all_reverse[:3]:
        print(f"  {msg_id}: {fields['content']}")
    
    # 3. Get messages newer than specific ID
    if len(all_reverse) >= 3:
        specific_id = all_reverse[2][0]  # Third newest
        newer_messages = r.xrevrange(stream_name, max=specific_id, count=5)
        print(f"\nMessages newer than {specific_id}: {len(newer_messages)}")

def example_stream_monitoring():
    """Monitor stream for new messages"""
    print("\n=== Stream Monitoring Example ===")
    stream_name = "monitor:stream"
    
    # Clear existing data
    clear_stream(stream_name)
    
    # Simulate real-time monitoring
    def monitor_stream():
        print("Monitor: Starting real-time monitoring...")
        last_id = '$'  # Start from newest only
        
        for _ in range(3):  # Check 3 times
            # Block for up to 2 seconds
            messages = r.xread({stream_name: last_id}, block=2000, count=10)
            
            if messages:
                for stream, msgs in messages:
                    for msg_id, fields in msgs:
                        print(f"  [ALERT] {msg_id}: {fields}")
                        last_id = msg_id
            else:
                print("  No new messages in this interval")
            
            # Add a test message if needed (simulate other process)
            if _ == 1:
                r.xadd(stream_name, {
                    'alert': 'High CPU usage',
                    'value': '95%',
                    'timestamp': str(time.time())
                })
                print("  [SIMULATED] Added test alert")
    
    # Run monitor
    monitor_stream()

def example_error_handling():
    """Error handling and best practices"""
    print("\n=== Error Handling Example ===")
    stream_name = "robust:stream"
    
    # 1. Check if stream exists before operations
    def safe_read_stream(stream_name, count=10):
        """Safely read from stream with error handling"""
        try:
            if r.exists(stream_name):
                messages = r.xread({stream_name: '0'}, count=count)
                return messages if messages else []
            else:
                print(f"Stream {stream_name} does not exist")
                return []
        except redis.exceptions.ResponseError as e:
            print(f"Redis error: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error: {e}")
            return []
    
    # 2. Safe write with validation
    def safe_add_message(stream_name, fields, max_retries=3):
        """Safely add message with retry logic"""
        for attempt in range(max_retries):
            try:
                if not fields:
                    raise ValueError("Fields cannot be empty")
                message_id = r.xadd(stream_name, fields)
                return message_id
            except redis.exceptions.ConnectionError:
                print(f"Connection error, retry {attempt + 1}/{max_retries}")
                time.sleep(0.5)
            except Exception as e:
                print(f"Failed to add message: {e}")
                return None
        return None
    
    # Test error handling
    result = safe_read_stream("nonexistent_stream")
    print(f"Reading nonexistent stream: {len(result)} messages")
    
    msg_id = safe_add_message(stream_name, {'test': 'value'})
    if msg_id:
        print(f"Successfully added message: {msg_id}")
    
    # 3. Check stream info
    if r.exists(stream_name):
        stream_length = r.xlen(stream_name)
        print(f"Stream {stream_name} length: {stream_length}")

def example_batch_operations():
    """Batch operations for better performance"""
    print("\n=== Batch Operations Example ===")
    stream_name = "batch300-stream"
    
    # Clear existing data
    clear_stream(stream_name)
    
    # 1. Batch add using pipeline
    print("Batch adding 100 messages using pipeline...")
    pipeline = r.pipeline(transaction=True)
    for i in range(100):
        pipeline.xadd(stream_name, {
            'index': str(i),
            'data': f'Batch data {i}',
            'timestamp': str(time.time())
        })
    results = pipeline.execute()
    print(f"Added {len(results)} messages")
    
    # 2. Batch read in chunks
    def batch_read_all(stream_name, chunk_size=20):
        """Read all messages in chunks"""
        all_messages = []
        last_id = '0'
        
        while True:
            chunk = r.xread({stream_name: last_id}, count=chunk_size)
            if not chunk:
                break
            
            for stream, msgs in chunk:
                if not msgs:
                    break
                all_messages.extend(msgs)
                last_id = msgs[-1][0]  # Last message ID of this chunk
                
                if len(msgs) < chunk_size:
                    break
        
        return all_messages
    
    # Read all messages in batches
    all_msgs = batch_read_all(stream_name, chunk_size=30)
    print(f"Batch read {len(all_msgs)} messages")
    
    # Show first and last few
    if all_msgs:
        print(f"First message: {all_msgs[0][0]}")
        print(f"Last message: {all_msgs[-1][0]}")

def main():
    """Main function to run all examples"""
    print("=" * 60)
    print("REDIS STREAMS EXAMPLES")
    print("=" * 60)
    
    try:
        # Test connection
        r.ping()
        print("✓ Connected to Redis successfully\n")
        
        # Run examples
        #example_xadd_basic()
        #example_xrange_basic()
        #example_xread_basic()
        example_consumer_producer()
        #example_pagination()
        #example_reverse_reading()
        #example_stream_monitoring()
        #example_error_handling()
        example_batch_operations()
        
        print("\n" + "=" * 60)
        print("ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except redis.exceptions.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
        print("\nPlease ensure Redis is running on localhost:20700")
        print("You may need to adjust the host/port in the connection string")
    except Exception as e:
        print(f"✗ Error: {e}")

if __name__ == "__main__":
    main()