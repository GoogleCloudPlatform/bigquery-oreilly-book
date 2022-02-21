import json

def add_fake_user(request):
   request_json = request.get_json(silent=True)
   replies = []
   calls = request_json['calls']
   for call in calls:
       bike_id = call[0]
       rental_id = call[1]
       replies.append({
           'username': f'user_{bike_id}',
           'email': f'user_{bike_id}@{rental_id}.com'
       })
   return json.dumps({
       # each reply is a STRING (JSON not currently supported)
       'replies': [json.dumps(reply) for reply in replies]
   })
