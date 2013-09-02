
import unittest
import Version

class AaaTheFirst_test(unittest.TestCase):
    """The first test to run, just outputs some debuugin info on the program"""

    def print_test(self):
        print("Starting unit-test execution. Version:")
        print(Version.getFullVersionString())


if __name__ == '__main__':
    unittest.main()