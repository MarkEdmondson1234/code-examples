import os, json
from google.cloud import pubsub_v1 # google-cloud-pubsub==2.8.0

def http_to_pubsub(request):
    request_json = request.get_json()
    request_args = request.args

    print('Request json: {}'.format(request_json))

    if request_json:
        res = trigger(json.dumps(request_json).encode('utf-8'), request.path)
        return res
    else:
        return 'No data found', 204


def trigger(data, topic_name):
  publisher = pubsub_v1.PublisherClient()

  topic_name = 'projects/{project_id}/topics{topic}'.format(
    project_id=os.getenv('GCP_PROJECT'),
    topic=topic_name,
  )

  print ('Publishing message to topic {}'.format(topic_name))
  
  # create topic if necessary
  try:
    future = publisher.publish(topic_name, data)
    future_return = future.result()
    print('Published message {}'.format(future_return))

    return future_return

  except Exception as e:
    print('Topic {} does not exist? Attempting to create it'.format(topic_name))
    print('Error: {}'.format(e))

    publisher.create_topic(name=topic_name)
    print ('Topic created ' + topic_name)

    return 'Topic Created', 201