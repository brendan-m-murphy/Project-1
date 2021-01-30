from unittest import TestCase, main
from string_iterator import StringIteratorIO


class StringIteratorTestCase(TestCase):
    def test_read_2bytes(self):
        gen = (str(i) for i in range(4))
        with StringIteratorIO(gen) as g:
            result = [g.read(2), g.read(2)]
        expected = ['01', '23']
        self.assertEqual(expected, result)

    def test_read_all_bytes(self):
        gen = (str(i) for i in range(4))
        with StringIteratorIO(gen) as g:
            result = g.read()
        expected = '0123'
        self.assertEqual(expected, result)


if __name__ == '__main__':
    main()
