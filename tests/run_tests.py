import unittest


def run_tests():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.discover("tests", pattern="test_*.py"))

    runner = unittest.TextTestRunner()
    runner.run(suite)


if __name__ == "__main__":
    run_tests()
