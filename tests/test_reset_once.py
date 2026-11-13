import unittest
from unittest.mock import Mock
from reset_once import consumed,install
class ResetOnceTests(unittest.TestCase):
 def test_consumed_source_cannot_quote_or_request_another_reset_even_without_columns(self):
  for evidence in [dict(reset_consumed_at='now'),dict(reset_replacement_account_id='fresh'),dict(archive_reason='BREACH | reset_consumed replacement_account_id=fresh replacement_mt5=123'),dict(archive_reason='[NP_RESET_CONSUMED:order]'),dict(archive_reason='NP_CONSUMED:PAID_RESET:order')]:
   with self.subTest(evidence=evidence):
    previous=Mock(return_value={'eligible':True});g={'_np_reset_policy':previous};install(g)
    result=g['_np_reset_policy'](dict(id='source',**evidence))
    self.assertFalse(result['eligible']);self.assertEqual(result['reason'],'reset_already_consumed');previous.assert_not_called()
 def test_unused_source_and_payout_renewal_do_not_consume_breach_reset(self):
  previous=Mock(return_value={'eligible':True});g={'_np_reset_policy':previous};install(g)
  self.assertTrue(g['_np_reset_policy']({'archive_reason':'NP_CONSUMED:PAYOUT:123'})['eligible'])
