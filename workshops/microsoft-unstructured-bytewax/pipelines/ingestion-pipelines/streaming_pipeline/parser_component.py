import re
from typing import Any, Dict, List, Union
from pathlib import Path
import requests
import time
import uuid
from dotenv import load_dotenv
from haystack import Document, component
from haystack.dataclasses import ByteStream

# Setup logging
import logging

import hashlib
from unstructured.cleaners.core import (
    clean,
    clean_non_ascii_chars,
    replace_unicode_quotes,
)
from unstructured.partition.html import partition_html

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv(".env")

@component
class CustomParser:
    """
    A component for fetching, parsing, cleaning contents from source files or URLs
    """
    def __init__(self, download_needed=False):
        """
        Initialize the custom parser.
        :param download_needed: Will the flow need to download data using HTTP.
        """
        self.download_needed = download_needed

    @component.output_types(documents=List[Document])
    def run(self, event: Dict):
        """
        Parse HTML, extract content, and return as Haystack Document.
        :param event: An Alpaca event.
        :return: A Document object representing the data from Alpaca event.
        """
        documents = []
        doc_id = hashlib.md5(event.get("content").encode()).hexdigest()

        article_elements = partition_html(text=event.get("content"))
        cleaned_content = clean_non_ascii_chars(
            replace_unicode_quotes(clean(" ".join([str(x) for x in article_elements])))
        )
        cleaned_headline = clean_non_ascii_chars(
            replace_unicode_quotes(clean(event.get("headline")))
        )
        cleaned_summary = clean_non_ascii_chars(
            replace_unicode_quotes(clean(event.get("summary")))
        )

        text = cleaned_headline + " " + cleaned_summary + " " + cleaned_content
        documents.append(Document(content=text, id=doc_id))

        # logger.info(f"documents: {documents}")

        return {"documents": documents}
