import datetime
from pathlib import Path
from typing import List, Optional

from bytewax.dataflow import Dataflow
## from bytewax.inputs import Input
## from bytewax.outputs import Output
## from bytewax.testing import TestingInput
## from pydantic import TypeAdapter
## from pydantic import parse_obj_as
## from qdrant_client import QdrantClient

from streaming_pipeline import mocked
from streaming_pipeline.alpaca_batch import AlpacaNewsBatchInput
## from streaming_pipeline.alpaca_stream import AlpacaNewsStreamInput
## from streaming_pipeline.embeddings import EmbeddingModelSingleton
## from streaming_pipeline.models import NewsArticle
## from streaming_pipeline.qdrant import QdrantVectorOutput

import json
from bytewax import operators as op
from bytewax.connectors.files import FileSink
from bytewax.inputs import Source
from bytewax.testing import TestingSource

def build(
    is_batch: bool = False,
    from_datetime: Optional[datetime.datetime] = None,
    to_datetime: Optional[datetime.datetime] = None,
    model_cache_dir: Optional[Path] = None,
    debug: bool = False,
) -> Dataflow:
    """
    Builds a dataflow pipeline for processing news articles.

    Args:
        is_batch (bool): Whether the pipeline is processing a batch of articles or a stream.
        from_datetime (Optional[datetime.datetime]): The start datetime for processing articles.
        to_datetime (Optional[datetime.datetime]): The end datetime for processing articles.
        model_cache_dir (Optional[Path]): The directory to cache the embedding model.
        debug (bool): Whether to enable debug mode.

    Returns:
        Dataflow: The dataflow pipeline for processing news articles.
    """

    ## model = EmbeddingModelSingleton(cache_dir=model_cache_dir)
    # Do a mock run in streaming and debug mode
    is_input_mocked = debug is True and is_batch is False

    # Create bytewax flow
    flow = Dataflow("alpaca_retriever")
    # Get news using Alpaca API for given number of days
    news_stream = op.input("input", flow, 
                           _build_input(
                               is_batch, from_datetime, to_datetime, is_input_mocked=is_input_mocked
                               ))

    # Type checking and type casting
    ## flow.flat_map(lambda messages: TypeAdapter(List[NewsArticle]).validate_python(messages))
    ## flow.flat_map(lambda messages: parse_obj_as(List[NewsArticle], messages))
    
    ## Chunking, embedding and storing in qdrant
    ## flow.map(lambda article: article.to_document())
    ## flow.map(lambda document: document.compute_chunks(model))
    ## flow.map(lambda document: document.compute_embeddings(model))
    ## flow.output("output", _build_output(model, in_memory=debug))
    ## flow.output("output", StdOutput())

    # Serialize Dict to JSON str 
    if debug:
        op.inspect("news_stream", news_stream)
        def serialize(news):
            if news['symbols']:
                return (news['symbols'][0], json.dumps(news))
            else:
                return ('', json.dumps(news))
        
        # Serialize Dict to JSON str to send to file or kafka
        serialized = op.map("serialize", news_stream, serialize)
        op.output("output", serialized, FileSink('news_out_2.jsonl'))
        
    # else:
        # # original code for Kafka
        # def serialize_k(news)-> KafkaSinkMessage[Dict, Dict]:
        #     return KafkaSinkMessage(
        #         key=json.dumps(news['symbols'][0]),
        #         value=json.dumps(news),
        #     )

        # print(f"Connecting to brokers at: {BROKERS}, Topic: {OUT_TOPIC}")

        # serialized = op.map("serialize", inp, serialize_k)

        # broker_config = {
        #     "security_protocol":"SASL_SSL",
        #     "sasl_mechanism":"SCRAM-SHA-256",
        #     "sasl_plain_username":"demo",
        #     "sasl_plain_password":"Qq2EnlzHpzv3RZDAMjZzfZCwrFZyhK"
        #     }
        # kop.output("out1", serialized, brokers=BROKERS, topic=OUT_TOPIC)
    
    return flow

# Get news using Alpaca API for given number of days
def _build_input(
    is_batch: bool = False,
    from_datetime: Optional[datetime.datetime] = None,
    to_datetime: Optional[datetime.datetime] = None,
    is_input_mocked: bool = False,
) -> Source:
    # Do a mock run in streaming and debug mode
    if is_input_mocked is True:
        return TestingSource(mocked.financial_news)

    if is_batch:
        assert (
            from_datetime is not None and to_datetime is not None
        ), "from_datetime and to_datetime must be provided when is_batch is True"

        return AlpacaNewsBatchInput(
            from_datetime=from_datetime, to_datetime=to_datetime, tickers=["*"]
        )
    # else:
    #     return AlpacaNewsStreamInput(tickers=["*"])


# def _build_output(model: EmbeddingModelSingleton, in_memory: bool = False) -> Output:
#     if in_memory:
#         return QdrantVectorOutput(
#             vector_size=model.max_input_length,
#             client=QdrantClient(":memory:"),
#         )
#     else:
#         return QdrantVectorOutput(
#             vector_size=model.max_input_length,
#         )
