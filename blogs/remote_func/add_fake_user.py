import json

def add_fake_user(request):
  request_json = request.get_json(silent=True)
  replies = []
  calls = request_json['calls']
  for call in calls:
    userno = call[0]
    corp = call[1]
    replies.append({
      'username': f'user_{userno}',
      'email': f'user_{userno}@{corp}.com'
    })
  return json.dumps({
    # each reply is a STRING (JSON not currently supported)
    'replies': [json.dumps(reply) for reply in replies]
  })
