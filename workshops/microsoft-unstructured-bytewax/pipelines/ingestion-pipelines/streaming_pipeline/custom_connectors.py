"""Connectors for local text files with delay."""
from pathlib import Path
from typing import Callable, List, Dict, Any, Optional, Union
from datetime import datetime, timedelta, timezone
import json
import time
import os

import requests
from typing_extensions import override
from bytewax.connectors.files import FileSource, _FileSourcePartition
from bytewax.outputs import StatelessSinkPartition, DynamicSink
from pinecone.grpc import PineconeGRPC as Pinecone
from pinecone import ServerlessSpec

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

from bytewax import inputs
from streaming_pipeline import constants

#
# Haystack custom connector to read from file for testing locally
#
def _get_path_dev(path: Path) -> str:
    return hex(path.stat().st_dev)

class _SimulationSourcePartition(_FileSourcePartition):
    def __init__(self, path: Path, batch_size: int, resume_state: Optional[int], delay: timedelta):
        super().__init__(path, batch_size, resume_state)
        self._delay = delay
        self._next_awake = datetime.now(timezone.utc)

    @override
    def next_batch(self) -> List[str]:
        if self._delay:
            self._next_awake += self._delay
        return super().next_batch()

    @override
    def next_awake(self) -> Optional[datetime]:
        return self._next_awake

class SimulationSource(FileSource):
    """Read a path line-by-line from the filesystem with a delay between batches."""

    def __init__(
        self,
        path: Union[Path, str],
        batch_size: int = 10,
        delay: timedelta = timedelta(seconds=5),
        get_fs_id: Callable[[Path], str] = _get_path_dev,
    ):
        """Init.

        :arg path: Path to file.

        :arg batch_size: Number of lines to read per batch. Defaults
            to 10.

        :arg delay: Delay between batches. Defaults to 1 second.

        :arg get_fs_id: Called with the parent directory and must
            return a consistent (across workers and restarts) unique
            ID for the filesystem of that directory. Defaults to using
            {py:obj}`os.stat_result.st_dev`.

            If you know all workers have access to identical files,
            you can have this return a constant: `lambda _dir:
            "SHARED"`.

        """
        super().__init__(path, batch_size, get_fs_id)
        self._delay = delay

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _SimulationSourcePartition:
        _fs_id, path = for_part.split("::", 1)
        assert path == str(self._path), "Can't resume reading from different file"
        return _SimulationSourcePartition(self._path, self._batch_size, resume_state, self._delay)

#
# Haystack custom connector for writing vectors and metadata to Azure AI search
#
class _AzureSearchPartition(StatelessSinkPartition[Any]):
    @override
    def write_batch(self, dictionary) -> None:
        search_api_version = '2024-07-01'
        index_name = os.getenv("AZURE_AI_SEARCH_INDEX_NAME")
        search_api_key = os.getenv("AZURE_AI_SEARCH_API_KEY")
        azure_search_service = os.getenv("AZURE_AI_SEARCH_SERVICE_NAME")
        search_endpoint = f'https://{azure_search_service}.search.windows.net/indexes/{index_name}/docs/index?api-version={search_api_version}'  
        
        headers = {  
            'Content-Type': 'application/json',
            'api-key': search_api_key
        }  

        dictionary = dictionary[0]
        # Use the flattened meta directly
        flattened_meta = dictionary['meta']
        
        # Convert DataFrame to the format expected by Azure Search  
        body = json.dumps({  
            "value": [  
                {  
                    "@search.action": "upload",  
                    "id": dictionary['id'],  
                    "content": dictionary['content'],  
                    "meta": flattened_meta,  # Use flattened meta
                    "date": dictionary['date'],
                    "vector": dictionary['vector']  # Include the generated embeddings  
                } 
            ]  
        })  
        
        # Upload documents to Azure Search  
        response = requests.post(search_endpoint, 
                                 headers=headers, data=body) 

        return {"status": "success" if response.status_code == 200 else response.text}

class AzureSearchSink(DynamicSink[Any]):
    """Write each output item to Azure Search
    """

    @override
    def build(
        self, _step_id: str, _worker_index: int, _worker_count: int
    ) -> _AzureSearchPartition:
        return _AzureSearchPartition()

#
# Haystack custom connector for writing vectors and metadata to Pinecone
#  
class PineconeVectorSink(StatelessSinkPartition[Any]):
    """
    A sink that writes document embeddings and metadata to a Pinecone index.

    Args:
        client (Optional[PineconeGRPC]): The Pinecone client. Defaults to None.
        index_name (Optional[str]): The name of the index.
            Defaults to constants.VECTOR_DB_OUTPUT_INDEX_NAME.
        namespace (Optional[str]): The name of the namespace. Defaults to "".
    """

    def __init__(
        self,
        client: Optional[Pinecone] = None,
        index_name: Optional[str] = constants.VECTOR_DB_OUTPUT_INDEX_NAME,
        namespace: Optional[str] = ""
    ):
        self._client = client
        self._index_name = index_name
        self._index = self._client.Index(index_name)
        self._namespace = namespace

    @override
    def write_batch(self, dictionary) -> None:
        dictionary = dictionary[0]
        # print(f"metadata type={type(dictionary['metadata'])}")
        
        # The upsert operation writes vectors into a namespace. 
        # If a new value is upserted for an existing vector id, it will overwrite the previous value.
        response = self._index.upsert(
            vectors=[
                {
                    "id": dictionary['id'],
                    "metadata": dictionary['metadata'],
                    # "metadata": json.loads(dictionary['metadata']), # convert metadata to dictionary
                    "values": dictionary['values']      # Include the generated embeddings  
                }
            ],
            namespace=self._namespace
        )
        # print(f"Upsert response={response}")


class PineconeVectorOutput(DynamicSink[Any]):
    """A class representing a Pinecone vector output.

    Args:
        client (Optional[PineconeGRPC]): The Pinecone client. Defaults to None.
        index_name (Optional[str]): The name of the index.
            Defaults to constants.VECTOR_DB_OUTPUT_INDEX_NAME.
        namespace (Optional[str]): The name of the namespace. Defaults to "".
    """

    def __init__(
        self,
        client: Optional[Pinecone] = None,
        index_name: Optional[str] = constants.VECTOR_DB_OUTPUT_INDEX_NAME,
        namespace: Optional[str] = ""
    ):
        self._index_name = index_name
        self._namespace = namespace

        if client:
            self._client = client
        else:
            self._client = build_pinecone_client()

        # Create index if it does not exist
        if index_name not in self._client.list_indexes().names():
            self._client.create_index(
                name=index_name,
                dimension=int(constants.EMBEDDING_DIMENSIONS),
                metric="cosine",
                spec=ServerlessSpec(
                    cloud='aws',
                    region='us-east-1'
                )
            )
            # wait for index to be initialized
            while not self._client.describe_index(index_name).status["ready"]:
                time.sleep(1)

    @override
    def build(
        self, _step_id: str, _worker_index: int, _worker_count: int
    ) -> PineconeVectorSink:
        """Builds a PineconeVectorSink object.

        Args:
            _step_id (str):
            _worker_index (int): The index of the worker.
            _worker_count (int): The total number of workers.

        Returns:
            PineconeVectorSink: A PineconeVectorSink object.
        """

        return PineconeVectorSink(self._client, self._index_name, self._namespace)


def build_pinecone_client(url: Optional[str] = None, api_key: Optional[str] = None):
    """
    Builds a Pinecone object with the given URL and API key.

    Args:
        api_key (Optional[str]): The API key to use for authentication. If not provided,
            it will be read from the PINECONE_API_KEY environment variable.

    Raises:
        KeyError: If the PINECONE_API_KEY environment variables are not set
            and no values are provided as arguments.

    Returns:
        Pinecone: A Pinecone object connected to the specified Pinecone server.
    """

    if api_key is None:
        try:
            api_key = os.environ["PINECONE_API_KEY"]
        except KeyError:
            raise KeyError(
                "PINECONE_API_KEY must be set as environment variable or manually passed as an argument."
            )
        
    # Initilize and authenticate the Pinecone client
    client = Pinecone(api_key=api_key)

    return client


## Usage Example
# from simulated_connector import SimulationSource
# flow = Dataflow("simulate")
# inp = op.input("sim_inp", flow, SimulationSource("sec_out.jsonl"))
# op.inspect("inp", inp)