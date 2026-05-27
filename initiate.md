source venv/bin/activate (every terminal needs to initiate first)

Terminal 1 
- docker compose up -d
- docker compose ps
-docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list (to see if kafka is ready)\

Terminal 2 (Start the event producer)
- python producer/event_producer.py

Terminal 3 (Start the event pipeline)
- python flink-processor/pipeline.py

Check Kafka DLQ 
``bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-events-dlq \
  --from-beginning \
  --max-messages 5
```

## Step 11: Stop Everything

```bash
# Stop producer and Flink job: Ctrl+C in each terminal

# Stop and remove Docker containers (keeps data)
docker compose down

# Stop and WIPE all data (clean reset)
docker compose down -v
```
