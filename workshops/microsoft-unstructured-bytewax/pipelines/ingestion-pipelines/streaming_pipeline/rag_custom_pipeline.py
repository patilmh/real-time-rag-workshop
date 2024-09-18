from haystack import Pipeline
# from haystack.components.embedders import AzureOpenAIDocumentEmbedder
from haystack.components.preprocessors import DocumentCleaner
from haystack.components.embedders import OpenAIDocumentEmbedder
from pathlib import Path
from haystack.utils import Secret

from streaming_pipeline.unstructured_component import UnstructuredParser
from streaming_pipeline.parser_component import CustomParser
from streaming_pipeline import constants
import logging
from haystack import component, Document
from typing import Any, Dict, List, Optional, Union
from haystack.dataclasses import ByteStream

import json
import sys
import os
from dateutil import parser
from datetime import datetime

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def flatten_meta(meta):
    """
    Flatten a nested dictionary to a single-level dictionary.
    
    :param meta: Nested dictionary to flatten.
    :return: Flattened dictionary.
    """
    def _flatten(d, parent_key=''):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}_{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(_flatten(v, new_key).items())
            else:
                items.append((new_key, str(v) if not isinstance(v, (str, int, float, bool)) else v))
        return dict(items)
    
    return _flatten(meta)


def safe_deserialize(data):
    """
    Safely deserialize JSON data, handling various formats.
    
    :param data: JSON data to deserialize.
    :return: Deserialized data or None if an error occurs.
    """
    try:
        parsed_data = json.loads(data)
        if isinstance(parsed_data, list):
            if len(parsed_data) == 2 and (parsed_data[0] is None or isinstance(parsed_data[0], str)):
                event = parsed_data[1]
                logger.info(f"parsed_data[0]: {parsed_data[0]}")
            else:
                logger.info(f"Skipping unexpected list format: {data}")
                return None
        elif isinstance(parsed_data, dict):
            event = parsed_data
        else:
            logger.info(f"Skipping unexpected data type: {data}")
            return None
        
        # logger.info(f"event: {event}")
        if 'link' in event:
            event['url'] = event.pop('link')
        
        if "url" in event:
            return event
        else:
            logger.info(f"Missing 'url' key in data: {data}")
            return None

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error ({e}) for data: {data}")
        return None
    except Exception as e:
        logger.error(f"Error processing data ({e}): {data}")
        return None


class ParserFetcherEmbedder:
    def __init__(self, 
                 metadata_fields=None,
                 debug=False,
                 download_needed=False, 
                 embed_data=False, 
                 date_field=""
    ):
        """
        Initialize the JSONLReader with optional metadata fields and a link keyword.
        
        :param metadata_fields: List of fields in the JSONL to retain as metadata.
        :param download_needed: Will the flow need to download data using HTTP
        :param embed_data: Skip embedding data when testing locally
        :param date_field: Date field to be extracted from SEC filing or Alpaca news
        """
        self.download_needed = download_needed
        self.debug = debug
        self.embed_data = embed_data
        self.metadata_fields = metadata_fields or []
        self.date_field = date_field or ""
        
        # Get environment variables
        # AZURE_OPENAI_KEY = os.getenv('AZURE_OPENAI_API_KEY')
        # AZURE_OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')
        # AZURE_OPENAI_EMBEDDING_MODEL = os.getenv('AZURE_OPENAI_EMBEDDING_MODEL')
        UNSTRUCTURED_API_KEY = os.getenv("UNSTRUCTURED_API_KEY")

        # Create components
        if(self.download_needed):
            # SEC filings have to be downloaded from EDGAR before indexing
            document_parser = UnstructuredParser(unstructured_key=UNSTRUCTURED_API_KEY,
                                            chunking_strategy="by_page",
                                            strategy="auto",
                                            model="yolox")
        else:
            # Alpaca API call gives title, summary, content. So no HTTP download is needed.
            document_parser = CustomParser(download_needed=download_needed)

        regex_pattern = (
            r'<.*?>'  # HTML tags
            r'|\t'  # Tabs
            r'|\n+'  # Newlines
            r'|&nbsp;'  # Non-breaking spaces
            r'|[^a-zA-Z0-9.,\'\s]'  # Any non-alphanumeric character (excluding whitespace)
        )
        document_cleaner = DocumentCleaner(
                            remove_empty_lines=True,
                            remove_extra_whitespaces=True,
                            remove_repeated_substrings=False,
                            remove_substrings=None,  
                            remove_regex=regex_pattern
                        )

        document_embedder = OpenAIDocumentEmbedder(model=constants.OPENAI_EMBEDDING_MODEL, 
                                                   dimensions=constants.EMBEDDING_DIMENSIONS)
        # document_embedder = AzureOpenAIDocumentEmbedder(azure_endpoint=AZURE_OPENAI_ENDPOINT,
        #                                                 api_key=Secret.from_token(AZURE_OPENAI_KEY),
        #                                                 azure_deployment=AZURE_OPENAI_EMBEDDING_MODEL) 

        # Initialize Haystack pipeline
        self.pipeline = Pipeline()

        # Add components to pipeline
        self.pipeline.add_component("parser", document_parser)
        self.pipeline.add_component("cleaner", document_cleaner)
    
        # Connect components
        self.pipeline.connect("parser", "cleaner")
        if(self.embed_data):
            # Skip adding embedder for local testing
            self.pipeline.add_component("embedder", document_embedder)
            self.pipeline.connect("cleaner", "embedder")

    def run(self, event: List[Union[str, Path, ByteStream]]) -> Dict:
        """
        Process each source file, read URLs and their associated metadata,
        fetch HTML content using a pipeline, and convert to Haystack Documents.
        :param event: A list of source files, URLs, or ByteStreams.
        :return: A dictionary containing the processed document and the result of writing to Azure Search.
        """

        # Event type here is usually <class 'dict'>
        # if(self.debug):
        #     logger.info(f"type={type(event)}, event={event}")

        # Extract URL and modify it if necessary
        url = event.get("url")
        if url and '-index.html' in url:
            url = url.replace('-index.html', '.txt')

        # Extract required metadata fields from the event
        metadata = {field: event.get(field) for field in self.metadata_fields if field in event}
        
        # Assume a pipeline fetches and processes this URL
        try:
            if(self.download_needed):
                doc = self.pipeline.run({"parser": {"sources": [url]}})
            else:
                doc = self.pipeline.run({"parser": {"event": event}})
        except Exception as e:
            logger.error(f"Error running pipeline for URL {url}: {e}")
            raise

        if(self.debug):
            logger.info(f"doc={doc}")
            
        if(self.embed_data):
            # Get embedding data if the embedder was run
            document_obj = doc['embedder']['documents'][0]

            # Get the embedding
            embedding = document_obj.embedding
            # Ensure embedding is converted to a list, if it is a NumPy array
            if embedding is not None and hasattr(embedding, 'tolist'):
                embedding = embedding.tolist()

            # Safely access the embedding metadata
            # embedding_metadata = doc.get('embedder', {}).get('meta', {})
            # metadata.update(embedding_metadata)
        else:
            # Get cleaned data if the embedder was not run
            document_obj = doc['cleaner']['documents'][0]

        # Insert Alpaca news or filing text in metadata if needed
        metadata["content"] = document_obj.content,

        # Extract date from appropriate field for SEC filing or Alpaca news
        date = ""
        if self.date_field:
            date = event.get(self.date_field)
            metadata["date"] = date
            # store epoch since date string cannot be used filter in Pinecone
            _datetime = parser.parse(date)
            metadata["epoch"] = int(_datetime.timestamp())

        # Flatten the metadata if it is nested
        metadata = flatten_meta(metadata)
        if(self.debug):
            logger.info(f"updated metadata={metadata}")

        # Create dictionary with id, content, metadata, date
        dictionary = {
            "id": document_obj.id,
            "metadata": metadata
            #"metadata": json.dumps(metadata),  # Serialize the flattened meta to a JSON string
        }

        # Add embedding to dictionary if the embedder was run
        if(self.embed_data):
            dictionary["values"] = embedding

        return dictionary
    
    # def document_to_dict(self, document: Document) -> Dict:
    #     """
    #     Convert a Haystack Document object to a dictionary.
    #     """
    #     # Ensure embedding is converted to a list, if it is a NumPy array
    #     embedding = document.embedding
    #     if embedding is not None and hasattr(embedding, 'tolist'):
    #         embedding = embedding.tolist()
    #     flattened_meta = flatten_meta(document.meta)
    #     return {
    #         "id": document.id,
    #         "metadata": json.dumps(flattened_meta),  # Serialize the flattened meta to a JSON string
    #         "values": embedding,
    #     }

