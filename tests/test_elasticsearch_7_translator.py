import unittest
from unified_query_maker.translators.elasticsearch_7_translator import (
    Elasticsearch7Translator,
)


class TestElasticsearch7Translator(unittest.TestCase):
    def setUp(self):
        self.translator = Elasticsearch7Translator()

    def test_translate_basic_query(self):
        uql = {
            "select": ["name", "age"],
            "from": "users",
            "where": {
                "must": [{"age": {"gt": 30}}, {"status": "active"}],
                "must_not": [{"role": "admin"}],
            },
        }
        expected = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"age": {"gt": 30}}},
                        {"term": {"status": "active"}},
                    ],
                    "must_not": [{"term": {"role": "admin"}}],
                }
            }
        }
        result = self.translator.translate(uql)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
