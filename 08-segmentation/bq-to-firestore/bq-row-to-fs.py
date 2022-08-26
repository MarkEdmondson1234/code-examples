import json
import firebase_admin # 5.2.0 
from firebase_admin import credentials
from firebase_admin import firestore
import datetime

_MAX_LOSSLESS=9007199254740992

def export_fs(call, fs_collection_name, db, fs_fields):

  if len(fs_fields) != len(call):
    return 'error: number of fields does not match number of fields in call'

  try:
    id = call[0]
    data = dict(zip(fs_fields, call))
    db.collection(fs_collection_name).document(id).set(data)
    return 'added {} to Firestore: {}' % id, datetime.datetime.now()

  except Exception as e:
    return 'error: failed to add to Firestore: {}', datetime.datetime.now()

def fs_add(request):
  try:
    return_value = []
    request_json = request.get_json()
    calls = request_json['calls']
    print('request: {}'.format(request_json))
  except Exception as inst:
    return json.dumps( { "errorMessage": 'something unexpected in input' } ), 400

  if request_json and 'userDefinedContext' in request_json:
    userDefinedContext = request_json['userDefinedContext']
    if 'fs_collection_name' in userDefinedContext:
      fs_collection_name = userDefinedContext['fs_collection_name']
    else:
      return json.dumps( { "errorMessage": 'no fs_collection_name specified' } ), 400

    if 'fs_fields' in userDefinedContext:
      fs_fields = userDefinedContext['fs_fields'].split(",")
    else:
      return json.dumps( { "errorMessage": 'no fs_fields specified' } ), 400

    if 'fs_project_id' in userDefinedContext:
      fs_project_id = userDefinedContext['fs_project_id']
    else:
      return json.dumps( { "errorMessage": 'no fs_project_id specified' } ), 400
  else:
    return json.dumps( { "errorMessage": 'no userDefinedContext specified' } ), 400

  if len(fs_fields) == 0:
    return json.dumps( { "errorMessage": 'length of fs_fields=0' } ), 400
  
  
  # Use the application default credentials
  cred = credentials.ApplicationDefault()
  firebase_admin.initialize_app(cred, {
    'projectId': fs_project_id,
  })

  db = firestore.client()

  print('Authenticated')
  print('fs_collection_name: {}' % fs_collection_name)
  print('fs_fields: {}' % fs_fields)
  print('fs_project_id: {}' % fs_project_id)


  if len(calls) > 500:
    return json.dumps( { "errorMessage": "too many rows - 500 limit" } ), 400

  for call in calls:
    value = export_fs(call=call, fs_collection_name=fs_collection_name, db=db, fs_fields=fs_fields)
    return_value.append(value)

  replies = [str(x) if x > _MAX_LOSSLESS or x < -_MAX_LOSSLESS else x for x in return_value]
  return_json = json.dumps( { "replies" :  replies} )
  return return_json






  