"""Names are isolated; endpoint settings support host and Compose workflows."""
from dataclasses import dataclass
import os

EVENT_TOPIC = 'journeypulse-events'
QUARANTINE_TOPIC = 'journeypulse-events-quarantine'
TOPICS = (EVENT_TOPIC, QUARANTINE_TOPIC)
PARTITIONS = 3
REPLICATION_FACTOR = 1
RETENTION_MS = 604800000  # Seven days, for the local single-broker demo.


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_environment(cls):
        return cls(
            os.getenv('JOURNEYPULSE_KAFKA_BOOTSTRAP', 'localhost:29092'),
            os.getenv('JOURNEYPULSE_POSTGRES_HOST', 'localhost'),
            int(os.getenv('JOURNEYPULSE_POSTGRES_PORT', '5432')),
            os.getenv('JOURNEYPULSE_POSTGRES_DB', 'grabevents'),
            os.getenv('JOURNEYPULSE_POSTGRES_USER', 'grabuser'),
            os.getenv('JOURNEYPULSE_POSTGRES_PASSWORD', 'grabpass'),
        )

    def database_parameters(self):
        return dict(host=self.postgres_host, port=self.postgres_port,
                    dbname=self.postgres_db, user=self.postgres_user,
                    password=self.postgres_password, connect_timeout=5)
