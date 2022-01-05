import json
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import bigquery


project_id = "artur-liszewski"
subscription_id = "nowy-topic-sub"
# Number of seconds the subscriber should listen for messages
timeout = 10.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"{message.data}".replace('b','').replace('\'',''))
    msg = json.loads(message.data.decode('utf8'))
    message.ack()
    data = [
        {u"execution_time": msg['execution_time'], u"number": msg['number'], u"timestamp": msg['timestamp'], u"deployment": msg['deployment']},
    ]

    client = bigquery.Client()
    table_id = 'python_flask.python_flask_table'
    errors = client.insert_rows_json(table_id, data)  # Make an API request.
    if errors == []:
        print("New rows have been added.")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.