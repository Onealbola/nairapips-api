import unittest
from package_configuration import defaults, validate


class ConfigurationErrorTests(unittest.TestCase):
    def test_inactive_blank_prices_save(self):
        self.assertEqual(validate(defaults()), defaults())

    def test_charge_error_names_exact_choice_and_unit(self):
        doc = defaults()
        doc['options']['funded_resets'][1]['price'] = 5000
        with self.assertRaisesRegex(ValueError, 'Included funded resets.*choice 1.*5000.*% of base price.*1000'):
            validate(doc)

    def test_active_blank_names_choice_and_explains_zero(self):
        doc = defaults()
        doc['options']['funded_resets'][1]['enabled'] = True
        with self.assertRaisesRegex(ValueError, 'Included funded resets.*choice 1.*0 means included free'):
            validate(doc)
