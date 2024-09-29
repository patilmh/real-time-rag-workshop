from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from streaming_pipeline.custom_connectors import SimulationSource, PineconeVectorOutput
from streaming_pipeline.rag_custom_pipeline import safe_deserialize, ParserFetcherEmbedder
from streaming_pipeline import initialize
from datetime import timedelta
import traceback
import logging

def test_flow(
        debug: bool = False,
        download_needed: bool = False
):
    """
    Builds a Bytewax flow for batch processing of news data.

    Args:
        download_needed (bool): Will the flow need to download using HTTP
        latest_n_days (int): Number of days to extract news from.
        debug (bool): Whether to run the flow in debug mode.

    Returns:
        flow (prefect.Flow): The Bytewax flow for batch processing of news data.
    """

    try:
        # Initializes the logger and environment variables.
        initialize(logging_config_path="logging.yaml", env_file_path=".env")

        logger = logging.getLogger(__name__)

        logger.info(f"test_flow debug={debug} download_needed={download_needed}")

        # Process each event to parse out HTML tags, clean, embed
        parser_embedder = ParserFetcherEmbedder(
            metadata_fields=['title','headline','form_type','symbols','url'],
            debug=debug,
            download_needed=download_needed,
            embed_data=True,
            date_field="updated_at"
        )

        def process_event(event):
            """Wrapper to handle the processing of each event."""
            if event:
                dict_document = parser_embedder.run(event)
                return dict_document
            return None

        # Build bytewax test flow to read from jsonl file and populate to Pinecone vector DB
        flow = Dataflow("rag-pipeline-test")
        input_data = op.input("input", flow, SimulationSource("data/test.jsonl", batch_size=1))
        deserialize_data = op.map("deserialize", input_data, safe_deserialize)
        extract_html = op.filter_map("parse_embed", deserialize_data, process_event)

        # Print out data extracted from Alpaca news in debug mode
        if debug:
            # op.inspect("insp_out", extract_html)
            op.output("stdout", extract_html, StdOutSink())

        # Pinecone recommendation is to upsert large number of vectors in batches
        # collect or batch operation needs a keyed stream
        keyed_stream = op.key_on("key", extract_html, lambda _: "ALL")

        # Batch for either 5 elements, or 10 second. This should emit at
        # the size limit since we are giving a large timeout.
        batched_stream = op.collect(
            "batch_items", keyed_stream, max_size=5, timeout=timedelta(seconds=10)
        )
        # op.inspect("ins_batch", batched_stream)

        # Write to serverless Pinecone vector database in a test namespace
        op.output("pc_out", batched_stream, PineconeVectorOutput(namespace="test"))

    except Exception as e:
        logging.error(traceback.format_exc())

    return flow

# python command
# python -m bytewax.run "local_dataflow:test_flow(debug=True, download_needed=False)"