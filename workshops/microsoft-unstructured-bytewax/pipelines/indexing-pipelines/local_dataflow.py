from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from custom_connectors import SimulationSource, AzureSearchSink
from rag_custom_pipeline import safe_deserialize, JSONLReader

def build_flow(
        download_needed: bool = False,
        latest_n_days: int = 1,
        debug: bool = False
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

    jsonl_reader = JSONLReader(download_needed=download_needed, 
                               metadata_fields=['title', \
                                                'headline', \
                                                'form_type', \
                                                'author', \
                                                'symbols', \
                                                'url'])


    def process_event(event):
        """Wrapper to handle the processing of each event."""
        if event:
            dict_document = jsonl_reader.run(event)
            return dict_document
        return None


    flow = Dataflow("rag-pipeline-test")
    input_data = op.input("input", flow, SimulationSource("data/test.jsonl", batch_size=1))
    deserialize_data = op.map("deserialize", input_data, safe_deserialize)
    extract_html = op.filter_map("build_indexes", deserialize_data, process_event)

    op.inspect("out", extract_html)
    # op.output("output", extract_html, StdOutSink())
    # op.output("output", extract_html, AzureSearchSink())

    return flow

# python command
# python -m bytewax.run local_dataflow:flow
# python -m bytewax.run "local_dataflow:build_flow(download_needed=False)"