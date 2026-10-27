import base64,copy,json,unittest
from unittest.mock import Mock
from flask import Flask,jsonify
from announcement_edit import register_announcement_edit,version,content_patch
class EditTests(unittest.TestCase):
 def setUp(self):
  self.app=Flask(__name__);self.allowed=True;self.http=Mock();self.row={'id':'notice','title':'Old','subject':'Old','message':'Before','created_at':'2026-10-01','read_by':['t'],'read_at':'date','target_trader_id':'t','delivery_email':True,'created_by':'admin','status':'active'}
  self.http.get.return_value.json.return_value=[self.row]
  self.http.patch.return_value.json.return_value=[dict(self.row,title='New',subject='New',message='After')]
  self.audit=Mock();self.invalidate=Mock()
  runtime=dict(requests=self.http,SUPABASE_URL='https://db',SUPABASE_KEY='key',_require_admin=lambda:({'id':'admin'},None) if self.allowed else (None,(jsonify(error='Unauthorized'),401)),_np_offer_merge_meta=lambda r:r,_audit_safe=self.audit,np_invalidate_admin_bootstrap=self.invalidate,_invalidate_trader_bootstrap_cache=Mock())
  register_announcement_edit(self.app,runtime);self.client=self.app.test_client()
 def save(self,**extra):return self.client.post('/admin/edit_announcement',json=dict(id='notice',title='New',message='After',version=version(self.row),**extra))
 def test_updates_same_id_content_only_without_touching_notification_state(self):
  r=self.save();self.assertEqual(r.status_code,200)
  data=self.http.patch.call_args.kwargs['json'];self.assertEqual(data,dict(title='New',subject='New',message='After'))
  self.assertEqual(r.json['announcement']['created_at'],'2026-10-01');self.assertEqual(r.json['announcement']['read_by'],['t']);self.audit.assert_called_once();self.invalidate.assert_called_once()
  self.assertEqual(self.http.patch.call_args.kwargs['params']['message'],'eq.Before')
 def test_authentication_precedes_reads_and_writes(self):
  self.allowed=False;self.assertEqual(self.save().status_code,401);self.http.get.assert_not_called();self.http.patch.assert_not_called()
 def test_get_editor_and_concurrent_edit_conflict(self):
  self.assertEqual(self.client.get('/admin/edit_announcement?id=notice').json['version'],version(self.row))
  r=self.client.post('/admin/edit_announcement',json=dict(id='notice',title='New',message='After',version='old'));self.assertEqual(r.status_code,409);self.http.patch.assert_not_called()
 def test_audience_or_delivery_changes_and_empty_messages_rejected(self):
  self.assertEqual(self.save(delivery_email=True).status_code,400)
  self.assertEqual(self.client.post('/admin/edit_announcement',json=dict(id='notice',title='',message='')).status_code,400);self.http.patch.assert_not_called()
 def test_lost_compare_and_set_is_not_reported_as_saved(self):
  self.http.patch.return_value.json.return_value=[];self.assertEqual(self.save().status_code,409);self.audit.assert_not_called()
 def test_metadata_audience_and_delivery_preserved(self):
  meta=dict(subject='Old',target_trader_id='t',delivery_email=True,offer_code='KEEP')
  row=dict(self.row,created_by='NP_PRIVATE_OFFER_META:'+base64.urlsafe_b64encode(json.dumps(meta).encode()).decode())
  patch=content_patch(row,'New','After');updated=json.loads(base64.urlsafe_b64decode(patch['created_by'].split(':')[1]))
  self.assertEqual(updated,dict(meta,subject='New'));self.assertNotIn('created_at',patch);self.assertNotIn('read_by',patch)
