import unittest,json
from unittest.mock import Mock
from breach_email_delivery import check
class DeliveryTests(unittest.TestCase):
 def test_admin_delivery_does_not_mask_trader_bounce_and_never_resends(self):
  case=dict(state='accepted',provider_id='<id>',recipient='trader@example.test',recipients=['trader@example.test'],admin_copies=['admin@example.test'])
  stored={'value':json.dumps(case)};s=Mock();s.g={'BREVO_API_KEY':'fixture'}
  events=[dict(email='trader@example.test',messageId='<id>',event='hardBounce',reason='mailbox not found'),dict(email='admin@example.test',messageId='<id>',event='delivered')]
  s.http.get.return_value.json.return_value={'events':events};s.save.return_value=True
  self.assertTrue(check(s,stored));result=s.save.call_args.args[1]
  self.assertEqual([r['status'] for r in result['delivery']],['bounced','delivered']);self.assertEqual([r['audience'] for r in result['delivery']],['trader','admin_copy']);s.http.post.assert_not_called()
  self.assertFalse(check(s,{'value':json.dumps(result)}));self.assertEqual(s.http.get.call_count,2)
 def test_pending_or_uncertain_messages_are_not_sent_or_polled(self):
  for state in ['pending','unconfirmed','failed']:
   s=Mock();self.assertFalse(check(s,{'value':json.dumps(dict(state=state,provider_id='id'))}));s.http.get.assert_not_called();s.http.post.assert_not_called()
