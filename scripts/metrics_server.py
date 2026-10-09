"""Run business metrics independently of the Flink submission client."""
import os
from pathlib import Path
import signal
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from monitoring.metrics import start_metrics_server


def main():
    stopped = threading.Event()
    for name in ('SIGINT', 'SIGTERM'):
        signal.signal(getattr(signal, name), lambda *_: stopped.set())
    start_metrics_server({
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', '5432')),
        'dbname': os.getenv('POSTGRES_DB', 'grabevents'),
        'user': os.getenv('POSTGRES_USER', 'grabuser'),
        'password': os.getenv('POSTGRES_PASS', 'grabpass'),
    }, os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092'),
       os.getenv('DLQ_TOPIC', 'user-events-dlq'))
    stopped.wait()


if __name__ == '__main__':
    main()
