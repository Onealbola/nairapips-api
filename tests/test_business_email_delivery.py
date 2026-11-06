import copy,unittest
from business_email_delivery import DeliveryChecks,outcome,install
from test_business_email import EmailTests,Response

class Outcomes(unittest.TestCase):
    def event(self,event,email='owner@example.test',mid='<test>',**extra):return dict(email=email,messageId=mid,event=event,date='2026-11-06T18:00:00Z',**extra)
    def test_delivery_is_not_claimed_without_exact_email_and_message_id(self):
        self.assertEqual(outcome([self.event('delivered')],'other@example.test','<test>')['status'],'unknown')
        self.assertEqual(outcome([self.event('delivered')],'owner@example.test','<other>')['status'],'unknown')
        r=outcome([self.event('delivered')],'owner@example.test','<test>');self.assertEqual(r['status'],'delivered');self.assertIn('Inbox placement is not confirmed',r['detail'])
    def test_bounce_delay_and_no_event(self):
        for event,status in [('hardBounces','bounced'),('hard_bounce','bounced'),('softBounces','deferred'),('deferred','deferred'),('blocked','blocked'),('requests','queued')]:
            self.assertEqual(outcome([self.event(event)],'owner@example.test','<test>')['status'],status)
        self.assertEqual(outcome([],'owner@example.test','<test>')['status'],'unknown')

class Checks(EmailTests):
    def setUp(self):
        super().setUp();self.app.config['NP_EMAIL_DELIVERY_BOOTSTRAP']=False;install(self.app,self.g)
    def test_saved_accepted_message_can_be_checked_without_any_resend(self):
        data,_=self.draft(mode='selected',trader_ids=[self.http.members[0]['id']]);self.start(data);identity=data['campaign']['id'];self.send_one(identity)
        before=len(self.http.sent);real_get=self.http.get
        def provider(url,params,**kw):
            if url.startswith('https://api.brevo.com'):
                return Response({'events':[dict(email=params['email'],messageId=params['messageId'],event='delivered',date='2026-11-06T18:00:00Z')]})
            return real_get(url,params,**kw)
        self.http.get=provider;check=DeliveryChecks(self.s);result=check.check(identity)
        self.assertEqual(result['recipients'][0]['delivery']['status'],'delivered');self.assertEqual(result['recipients'][0]['status'],'accepted');self.assertEqual(len(self.http.sent),before)
        response=self.c.get('/admin/business_email/campaigns/'+identity);self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['recipients'][0]['delivery']['status'],'delivered');self.assertEqual(response.json['delivery_counts']['delivered'],1)
        self.role=None;self.assertEqual(self.c.post('/admin/business_email/campaigns/'+identity+'/check-delivery',json={}).status_code,401)
    def test_provider_log_failure_does_not_turn_acceptance_into_delivery(self):
        data,_=self.draft(mode='selected',trader_ids=[self.http.members[0]['id']]);self.start(data);identity=data['campaign']['id'];self.send_one(identity);real_get=self.http.get
        self.http.get=lambda url,params,**kw:Response({},403) if url.startswith('https://api.brevo.com') else real_get(url,params,**kw)
        result=DeliveryChecks(self.s).check(identity);self.assertEqual(result['recipients'][0]['delivery']['status'],'unavailable');self.assertEqual(len(self.http.sent),1)

if __name__=='__main__':unittest.main()
