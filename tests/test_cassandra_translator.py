import unittest
from unified_query_maker.translators.cassandra_translator import CassandraTranslator


class TestCassandraTranslator(unittest.TestCase):
    def setUp(self):
        self.translator = CassandraTranslator()

    def test_translate_basic_query(self):
        uql = {
            "select": ["name", "age"],
            "from": "users",
            "where": {
                "must": [{"age": {"gt": 30}}, {"status": "active"}],
                "must_not": [{"role": "admin"}],
            },
        }
        expected = "SELECT name, age FROM users WHERE (age > 30 AND status = 'active') AND NOT (role = 'admin');"
        result = self.translator.translate(uql)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
