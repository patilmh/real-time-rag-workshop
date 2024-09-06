import os
from typing import List, Dict

## from bytewax import operators as op
## from bytewax.connectors.files import FileSink
from bytewax.connectors.files import FileOutput
from bytewax.dataflow import Dataflow
## from bytewax.connectors.kafka import operators as kop
## from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.connectors.kafka import KafkaOutput


import json
import logging

from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicInput, StatelessSource
## from bytewax.inputs import DynamicSource, StatelessSourcePartition
from websocket import create_connection

API_KEY = os.getenv("ALPACA_API_KEY")
API_SECRET = os.getenv("ALPACA_SECRET")
ticker_list = ["*"]

# Creating an object 
logger=logging.getLogger() 

# Setting the threshold of logger to DEBUG 
logger.setLevel(logging.DEBUG) 

# This code is not being used in the Ingestion pipeline
# Attempted using streaming example code for bytewax==0.17.1 instead of the original code for bytewax==0.19.0
# because the original streaming code doesn't work. This has been replaced with flow.py, run_batch.py, etc.

class AlpacaSource(StatelessSource):
#class AlpacaSource(StatelessSourcePartition):
    def __init__(self, worker_tickers):
        # set the workers tickers
        self.worker_tickers = worker_tickers
    
        # establish a websocket connection to alpaca
        self.ws = create_connection("wss://stream.data.alpaca.markets/v1beta1/news")
        logger.info(self.ws.recv())
        
        # authenticate to the websocket
        self.ws.send(
            json.dumps(
                {"action":"auth",
                 "key":f"{API_KEY}",
                 "secret":f"{API_SECRET}"}
            )
        )
        logger.info(self.ws.recv())
        
        # subscribe to the tickers
        self.ws.send(
            json.dumps(
                {"action":"subscribe","news":self.worker_tickers}
            )
        )
        logger.info(self.ws.recv())

    def next(self):
    #def next_batch(self):
        return self.ws.recv()

class AlpacaNewsInput(DynamicInput):
#class AlpacaNewsInput(DynamicSource):
    """Input class to receive streaming news data
    from the Alpaca real-time news API.
    
    Args:
        tickers: list - should be a list of tickers, use "*" for all
    """
    def __init__(self, tickers):
        self.TICKERS = tickers
    
    # distribute the tickers to the workers. If parallelized
    # workers will establish their own websocket connection and
    # subscribe to the tickers they are allocated
    def build(self, worker_index, worker_count):
    #def build(self, step_id, worker_index, worker_count):
        prods_per_worker = int(len(self.TICKERS) / worker_count)
        worker_tickers = self.TICKERS[
            int(worker_index * prods_per_worker) : int(
                worker_index * prods_per_worker + prods_per_worker
            )
        ]
        return AlpacaSource(worker_tickers)

## bytewax version 0.19.0 code attempt
# flow = Dataflow("news_loader")
# news_input = op.input("news_input", flow, AlpacaNewsInput(tickers=["*"]))
# op.inspect("input", news_input)
# inp = op.flat_map("json_loader", news_input, lambda x: json.loads(x))

## bytewax version 0.16.0 code attempt
flow = Dataflow()
flow.input("input", AlpacaNewsInput(tickers=["*"]))
flow.inspect(print)
flow.flat_map(lambda x: json.loads(x))

## original code
# flow = Dataflow("news_loader")
# inp = op.input("news_input", flow, NewsSource(ticker_list)).then(op.flat_map, "flatten", lambda x: x)
# op.inspect("input", inp)

def serialize(news):
    return (news['symbols'][0], json.dumps(news))

## bytewax version 0.16.0 code attempt
flow.map(serialize)
flow.output("output", FileOutput('news_out_2.jsonl'))

# original code for file sink
# serialized = op.map("serialize", inp, serialize)
# op.output("output", serialized, FileSink('data/news_out.jsonl'))

# python command
# python -m bytewax.run news_ingestion.py