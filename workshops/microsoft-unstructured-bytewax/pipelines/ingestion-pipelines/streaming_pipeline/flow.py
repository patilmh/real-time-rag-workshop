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

## from streaming_pipeline.alpaca_stream import AlpacaNewsStreamInput
## from streaming_pipeline.embeddings import EmbeddingModelSingleton
## from streaming_pipeline.models import NewsArticle
## from streaming_pipeline.qdrant import QdrantVectorOutput
## from streaming_pipeline.azure_queue import AzureQueueOutput
from streaming_pipeline import mocked
from streaming_pipeline.alpaca_batch import AlpacaNewsBatchInput
from streaming_pipeline.custom_connectors import PineconeVectorOutput
from streaming_pipeline.rag_custom_pipeline import ParserFetcherEmbedder

import json
from datetime import timedelta
from bytewax import operators as op
from bytewax.connectors.files import FileSink
from bytewax.inputs import Source
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.outputs import DynamicSink

def build(
    is_batch: bool = False,
    from_datetime: Optional[datetime.datetime] = None,
    to_datetime: Optional[datetime.datetime] = None,
    model_cache_dir: Optional[Path] = None,
    debug: bool = False,
    download_needed: bool = False,
    date_field: str = ""
) -> Dataflow:
    """
    Builds a dataflow pipeline for processing news articles.

    Args:
        is_batch (bool): Whether the pipeline is processing a batch of articles or a stream.
        from_datetime (Optional[datetime.datetime]): The start datetime for processing articles.
        to_datetime (Optional[datetime.datetime]): The end datetime for processing articles.
        model_cache_dir (Optional[Path]): The directory to cache the embedding model.
        debug (bool): Whether to enable debug mode.
        download_needed (bool): Will the flow need to download using HTTP
        date_field (str): Date field to be extracted from SEC filing or Alpaca news

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
                            )
    )

    # Type checking and type casting
    ## flow.flat_map(lambda messages: TypeAdapter(List[NewsArticle]).validate_python(messages))
    ## flow.flat_map(lambda messages: parse_obj_as(List[NewsArticle], messages))
    
    ## Chunking, embedding and storing in qdrant
    ## flow.map(lambda article: article.to_document())
    ## flow.map(lambda document: document.compute_chunks(model))
    ## flow.map(lambda document: document.compute_embeddings(model))
    ## flow.output("output", _build_output(model, in_memory=debug))
    ## flow.output("output", StdOutput())

    # Serialize and write data given by Alpaca API to a file
    if debug:
        # Serialize Dict to JSON str
        def serialize(news):
            if news['symbols']:
                return (news['symbols'][0], json.dumps(news))
            else:
                return ('', json.dumps(news))
        
        # Serialize Dict to JSON str to send to file or queue
        serialized = op.map("serialize", news_stream, serialize)

        # Write to file in debug mode
        op.output("fileout", serialized, FileSink('data/news_out.jsonl'))

    # Process each event to parse out HTML tags, clean, embed
    # Embedding can be turned off when testing locally with embed_data flag
    # Process each event to parse out HTML tags, clean, embed
    parser_embedder = ParserFetcherEmbedder(
        metadata_fields=['title','headline','form_type','symbols','url'],
        download_needed=download_needed,
        embed_data=False,
        date_field=date_field
    )

    def process_event(event):
        """Wrapper to handle the processing of each event."""
        if event:
            dict_document = parser_embedder.run(event)
            return dict_document
        return None

    # Parse out HTML tags, clean, embed the news data
    extract_html = op.filter_map("parse_embed", news_stream, process_event)

    # Print out data extracted from Alpaca news in debug mode
    if debug:
        # op.inspect("insp_out", extract_html)
        op.output("std_out", extract_html, StdOutSink())
    
    # Pinecone recommendation is to upsert large number of vectors in batches
    # collect or batch operation needs a keyed stream
    keyed_stream = op.key_on("key", extract_html, lambda _: "ALL")

    # Batch for either 5 elements, or 10 second. This should emit at
    # the size limit since we are giving a large timeout.
    batched_stream = op.collect(
        "batch_items", keyed_stream, max_size=100, timeout=timedelta(seconds=120)
    )
    op.inspect("ins_batch", batched_stream)

    # Write to serverless Pinecone vector database in the default namespace
    # op.output("pc_out", extract_html, PineconeVectorOutput(namespace=""))

    # Write to Azure Queue Storage - this might only be needed for SEC filings
    ## op.output("q_out", serialized, _build_q_output())
        
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
    ## else:
    ##     return AlpacaNewsStreamInput(tickers=["*"])

# def _build_q_output() -> DynamicSink:
#     return AzureQueueOutput()

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
