import logging
from typing import Optional, List

from bytewax.outputs import DynamicSink, StatelessSinkPartition
import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage

logger = logging.getLogger()

class AzureQueueOutput(DynamicSink):
    """A class representing a Azure queue output.

    Args:
        vector_size (int): The size of the vector.
        collection_name (str, optional): The name of the collection.
            Defaults to constants.VECTOR_DB_OUTPUT_COLLECTION_NAME.
        client (Optional[UpstashClient], optional): The Upstash client. Defaults to None.
    """

    def __init__(
        self,
        account_url: str = "https://realtimeragstorage.queue.core.windows.net",
        client: Optional[QueueClient] = None
    ):
        self._account_url = account_url
    
        try:
            print("Azure Queue storage - Python quickstart sample")
            
            if client:
                self.client = client
            else:
                # Create a unique name for the queue
                queue_name = "quickstartqueues-" + str(uuid.uuid4())

                default_credential = DefaultAzureCredential()

                # Create the QueueClient object
                # We'll use this object to create and interact with the queue
                self.client = QueueClient(account_url=self._account_url, 
                                        queue_name=queue_name, 
                                        credential=default_credential)
                
                print("Creating queue: " + queue_name)

                # Create the queue
                self.client.create_queue()
        except Exception as ex:
            print('Exception:')
            print(ex)

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition:
        """Builds a AzureQueueSink object.

        Args:
            step_id (str): The step_id of the output operator.
            worker_index (int): The index of the worker.
            worker_count (int): The total number of workers.

        Returns:
            AzureQueueSink: An AzureQueueSink object.
        """
        return AzureQueueSink(self.client)


class AzureQueueSink(StatelessSinkPartition):
    """
    A sink that adds messages to an Azure Storage Queue.

    Args:
        client (Index): The Azure Queue client to use for writing.
    """

    def __init__(
        self,
        client: QueueClient
    ):
        self._client = client

    def write_batch(self, str: str):
        """
        Writes a message to the configured Azure Storage Queue.

        Args:
            str: The message to write to the queue.
        """
        print(f"Adding message to the queue...{str}")
        
        try:
            # Send message to the queue
            self._client.send_message(str)
            
        except Exception as ex:
            print('Exception:')
            print(ex)

