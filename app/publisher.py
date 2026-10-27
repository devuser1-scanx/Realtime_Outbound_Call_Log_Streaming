from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import BatchSettings


class Publisher:
    """Thin wrapper over the Pub/Sub client. publish() returns a future of the message id."""

    def __init__(self, project_id: str, topic: str):
        self._client = pubsub_v1.PublisherClient(
            batch_settings=BatchSettings(max_messages=100, max_latency=0.01)
        )
        self._topic_path = self._client.topic_path(project_id, topic)

    def publish(self, data: bytes, attributes: dict):
        return self._client.publish(self._topic_path, data, **attributes)

    def close(self):
        self._client.stop()
