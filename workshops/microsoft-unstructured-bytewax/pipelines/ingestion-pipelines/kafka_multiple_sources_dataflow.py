from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.testing import run_main
from bytewax.connectors.kafka import KafkaSource, AzureSearchSink
from streaming_pipeline.custom_connectors import SimulationSource
from streaming_pipeline.rag_custom_pipeline import safe_deserialize, JSONLReader

jsonl_reader = JSONLReader(download_needed=False, 
                            metadata_fields=['title', \
                                            'headline', \
                                            'form_type', \
                                            'author', \
                                            'symbols', \
                                            'url'])

def process_event_edgar(event):
    pass

def process_event_news(event):
    pass

def process_event(event):
    """Wrapper to handle the processing of each event."""
    if event:
        dict_document = jsonl_reader.run(event)
        return dict_document
    return None

flow = Dataflow("rag-pipeline")
# edgar_k_input = op.input("input", flow, KafkaSource())
edgar_input = op.input("edgar_inp", flow, SimulationSource("data/sec_out.jsonl"))

# news_k_input = op.input("input", flow, KafkaSource())
news_input = op.input("news_inp", flow, SimulationSource("data/news_out.jsonl"))

edgar_deser = op.map("deserialize", edgar_input, safe_deserialize)
edgar_dicts = op.map("extract_html", edgar_deser, process_event_edgar)

news_deser = op.map("deserialize", news_input, safe_deserialize)
news_dicts = op.map("extract_html", news_deser, process_event_news)

merged_stream = op.merge("merge", news_dicts, edgar_dicts)
op.inspect("out", merged_stream)

# op.output("output", merged_stream, AzureSearchSink())
