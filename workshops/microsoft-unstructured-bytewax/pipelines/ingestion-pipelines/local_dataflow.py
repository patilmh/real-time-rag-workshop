from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from streaming_pipeline.custom_connectors import SimulationSource, PineconeVectorOutput
from streaming_pipeline.rag_custom_pipeline import safe_deserialize, ParserFetcherEmbedder

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

    # Process each event to parse out HTML tags, clean, embed
    parser_embedder = ParserFetcherEmbedder(
        metadata_fields=['title','headline','form_type','symbols','url'],
        download_needed=download_needed,
        embed_data=True,
        date_field="updated_at")

    def process_event(event):
        """Wrapper to handle the processing of each event."""
        if event:
            dict_document = parser_embedder.run(event)
            return dict_document
        return None

    # Build bytewax test flow to read from jsonl file and populate Azure search index
    flow = Dataflow("rag-pipeline-test")
    input_data = op.input("input", flow, SimulationSource("data/test.jsonl", batch_size=1))
    deserialize_data = op.map("deserialize", input_data, safe_deserialize)
    extract_html = op.filter_map("prep_data", deserialize_data, process_event)

    # Print out data extracted from Alpaca news in debug mode
    if debug:
        # op.inspect("insp_out", extract_html)
        op.output("stdout", extract_html, StdOutSink())

    # Write to serverless Pinecone vector database in a test namespace
    op.output("pc_out", extract_html, PineconeVectorOutput(namespace="test"))

    return flow

# python command
# python -m bytewax.run "local_dataflow:test_flow(debug=True, download_needed=False)"