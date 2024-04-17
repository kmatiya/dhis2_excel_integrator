import unittest

from main import get_org_unit

class TestClass(unittest.TestCase):
	def test_tedzani(self):
		self.assertEqual(get_org_unit('Tedzani'), 'sKN6JyTFe9M')
	