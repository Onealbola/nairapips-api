"""Authenticated, in-place announcement wording edits. Never sends or republishes."""
import base64,hashlib,json
from flask import request,jsonify

def version(row):
    fields={k:row.get(k) for k in ('id','title','subject','message','body','created_by')}
    return hashlib.sha256(json.dumps(fields,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def wording(row):
    return str(row.get('subject') or row.get('title') or ''),str(row.get('message') or row.get('body') or '')

def content_patch(row,title,message):
    patch={k:v for k,v in [('title',title),('subject',title),('message',message),('body',message)] if k in row}
    creator=str(row.get('created_by') or '')
    if creator.startswith('NP_PRIVATE_OFFER_META:'):
        metadata=json.loads(base64.urlsafe_b64decode(creator.split(':',1)[1]).decode())
        changed=False
        for k,v in [('title',title),('subject',title),('message',message),('body',message)]:
            if k in metadata:metadata[k]=v;changed=True
        if changed:patch['created_by']='NP_PRIVATE_OFFER_META:'+base64.urlsafe_b64encode(json.dumps(metadata,separators=(',',':'),ensure_ascii=False).encode()).decode()
    return patch

def register_announcement_edit(app,runtime):
    @app.route('/admin/edit_announcement',methods=['GET','POST'])
    def edit_announcement():
        admin,error=runtime['_require_admin']()
        if error is not None:return error
        body=request.get_json(silent=True) or {} if request.method=='POST' else request.args
        aid=str(body.get('id') or '').strip()
        if not aid or len(aid)>128:return jsonify(success=False,error='Choose an existing announcement.'),400
        if request.method=='POST':
            if set(body)-{'id','title','message','version'}:return jsonify(success=False,error='Only announcement wording can be edited.'),400
            title,message=body.get('title'),body.get('message')
            if not isinstance(title,str) or not isinstance(message,str) or not 1<=len(title.strip())<=250 or not 1<=len(message.strip())<=20000:return jsonify(success=False,error='Enter a title (up to 250 characters) and message (up to 20,000 characters).'),400
            title,message=title.strip(),message.strip()
        http=runtime['requests'];key=runtime.get('SUPABASE_SERVICE_ROLE_KEY') or runtime['SUPABASE_KEY']
        url=runtime['SUPABASE_URL'].rstrip('/')+'/rest/v1/announcements';headers={'apikey':key,'Authorization':'Bearer '+key}
        try:
            response=http.get(url,headers=headers,params={'select':'*','id':'eq.'+aid,'limit':'2'},timeout=(2,5));response.raise_for_status();rows=response.json()
            if not isinstance(rows,list) or len(rows)!=1:return jsonify(success=False,error='Announcement not found.'),404
            row=rows[0]
            if request.method=='GET':
                clean=runtime['_np_offer_merge_meta'](row);title,message=wording(clean)
                return jsonify(success=True,id=aid,title=title,message=message,version=version(row))
            if body.get('version')!=version(row):return jsonify(success=False,error='This announcement changed. Reopen Edit to load the latest wording.'),409
            patch=content_patch(row,title,message)
            params={'id':'eq.'+aid}
            # Compare-and-set preserves concurrent edits, including schemas without updated_at.
            for k in ('title','subject','message','body','created_by'):
                if k in row:params[k]='is.null' if row[k] is None else 'eq.'+str(row[k])
            response=http.patch(url,headers={**headers,'Prefer':'return=representation'},params=params,json=patch,timeout=(2,7));response.raise_for_status();saved=response.json()
            if not isinstance(saved,list) or len(saved)!=1:return jsonify(success=False,error='The announcement changed while saving. Reopen Edit and try again.'),409
            for name,args in [('np_invalidate_admin_bootstrap',('announcements',)),('_invalidate_trader_bootstrap_cache',())]:
                try:runtime[name](*args)
                except Exception:pass
            try:runtime['_audit_safe']('announcements','edit_existing','Edited existing announcement '+aid+'; no new notification sent.',admin,aid)
            except Exception:pass
            return jsonify(success=True,announcement=runtime['_np_offer_merge_meta'](saved[0]),message='Announcement updated.')
        except Exception:
            return jsonify(success=False,error='Announcement update could not be confirmed. Reload the message before trying again.'),503
