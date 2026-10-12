"""Check repeatable setup and refusal to mutate incompatible shared resources."""
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock

from journeypulse.bootstrap import ensure_topics, ensure_database, TOPIC_CONFIG
from journeypulse.config import TOPICS, PARTITIONS, REPLICATION_FACTOR
from journeypulse.contracts import EVENT_NAMES, event_schema


def admin_fixture():
    admin = MagicMock()
    admin.list_topics.return_value = list(TOPICS) + ['user-events', 'user-events-dlq']
    admin.describe_topics.return_value = [
        {'topic': name, 'error_code': 0, 'partitions': [
            {'leader': 1, 'replicas': [1]} for _ in range(PARTITIONS)]} for name in TOPICS]
    admin.describe_configs.return_value = [SimpleNamespace(resources=[
        (0, '', 2, name, [(key, value, False, 1, False, []) for key, value in TOPIC_CONFIG.items()])
        for name in TOPICS])]
    return admin


def connection_fixture(definitions=None):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    values = definitions if definitions is not None else [(event_schema(name),) for name in EVENT_NAMES]
    cursor.fetchone.side_effect = [(1,), (2,)] + values
    return connection, cursor


class JourneyPulseFoundationTests(unittest.TestCase):
    def test_existing_topics_are_only_verified(self):
        admin = admin_fixture()
        ensure_topics(admin, create=True)
        admin.create_topics.assert_not_called()
        admin.alter_configs.assert_not_called()
        admin.delete_topics.assert_not_called()

    def test_missing_topics_created_with_dedicated_names(self):
        admin = admin_fixture()
        admin.list_topics.return_value = ['user-events', 'user-events-dlq']
        ensure_topics(admin, create=True)
        self.assertEqual(admin.create_topics.call_count, 2)
        created = [call.args[0][0] for call in admin.create_topics.call_args_list]
        self.assertEqual([topic.name for topic in created], list(TOPICS))
        for topic in created:
            self.assertEqual(topic.num_partitions, PARTITIONS)
            self.assertEqual(topic.replication_factor, REPLICATION_FACTOR)
            self.assertEqual(topic.topic_configs, TOPIC_CONFIG)

    def test_read_only_check_refuses_missing_topics(self):
        admin = admin_fixture()
        admin.list_topics.return_value = []
        with self.assertRaises(ValueError):
            ensure_topics(admin, create=False)
        admin.create_topics.assert_not_called()

    def test_mismatched_topic_topology_fails_without_altering(self):
        for field, value in [('partitions', []), ('error_code', 1)]:
            admin = admin_fixture()
            admin.describe_topics.return_value[0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                ensure_topics(admin, create=True)
            admin.alter_configs.assert_not_called()

    def test_mismatched_retention_fails_without_altering(self):
        admin = admin_fixture()
        response = admin.describe_configs.return_value[0]
        name = TOPICS[0]
        response.resources[0] = (0, '', 2, name, [('retention.ms', '1', False, 1, False, [])])
        with self.assertRaises(ValueError):
            ensure_topics(admin, create=True)
        admin.alter_configs.assert_not_called()

    def test_catalogue_bootstrap_preserves_existing_definitions(self):
        connection, cursor = connection_fixture()
        ensure_database(connection, create=True)
        inserts = [call for call in cursor.execute.call_args_list
                   if 'INSERT INTO journeypulse.event_definitions' in call.args[0]]
        self.assertEqual(len(inserts), 5)
        for call in inserts:
            self.assertIn('ON CONFLICT', call.args[0])
            self.assertIn('DO NOTHING', call.args[0])
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertFalse(any('UPDATE ' in sql or 'DROP ' in sql for sql in statements))

    def test_changed_registered_contract_fails_and_rolls_back(self):
        connection, _ = connection_fixture([({'changed': True},)])
        with self.assertRaises(ValueError):
            ensure_database(connection, create=True)
        self.assertIs(connection.__exit__.call_args.args[0], ValueError)

    def test_read_only_check_uses_no_ddl_or_insert(self):
        connection, cursor = connection_fixture()
        ensure_database(connection, create=False)
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(statements[0], 'SET TRANSACTION READ ONLY')
        self.assertFalse(any('INSERT ' in sql or 'CREATE ' in sql for sql in statements))

    def test_missing_registered_contract_is_rejected(self):
        connection, _ = connection_fixture([None])
        with self.assertRaises(ValueError):
            ensure_database(connection, create=False)


if __name__ == '__main__':
    unittest.main()
