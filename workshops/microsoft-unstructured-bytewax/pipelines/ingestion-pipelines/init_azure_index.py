from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential  
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient  

from azure.search.documents.indexes.models import (  
    AzureOpenAIVectorizerParameters,  
    AzureOpenAIVectorizer,  
    ExhaustiveKnnParameters,  
    ExhaustiveKnnAlgorithmConfiguration,
    HnswParameters,  
    HnswAlgorithmConfiguration,  
    SemanticPrioritizedFields,    
    SearchField,  
    SearchFieldDataType,  
    SearchIndex,  
    SemanticConfiguration,  
    SemanticField,  
    SemanticSearch,  
    VectorSearch,  
    VectorSearchAlgorithmKind,  
    VectorSearchAlgorithmMetric,  
    VectorSearchProfile,  
)  

import os
from dotenv import find_dotenv, load_dotenv  
load_dotenv(find_dotenv(), override=True)

AZURE_OPENAI_KEY = os.getenv('AZURE_OPENAI_API_KEY')
AZURE_OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')
AZURE_OPENAI_EMBEDDING_MODEL = os.getenv('AZURE_OPENAI_EMBEDDING_MODEL')

# Configure the vector search configuration  
vector_search = VectorSearch(  
    algorithms=[  
        HnswAlgorithmConfiguration(  
            name="myHnsw",  
            kind=VectorSearchAlgorithmKind.HNSW,  
            parameters=HnswParameters(  
                m=4,  
                ef_construction=400,  
                ef_search=500,  
                metric=VectorSearchAlgorithmMetric.COSINE,  
            ),  
        ),  
        ExhaustiveKnnAlgorithmConfiguration(  
            name="myExhaustiveKnn",  
            kind=VectorSearchAlgorithmKind.EXHAUSTIVE_KNN,  
            parameters=ExhaustiveKnnParameters(  
                metric=VectorSearchAlgorithmMetric.COSINE,  
            ),  
        ),  
    ],  
    profiles=[  
        VectorSearchProfile(  
            name="myHnswProfile",  
            algorithm_configuration_name="myHnsw",
            vectorizer_name="myOpenAI",
        ),  
        VectorSearchProfile(  
            name="myExhaustiveKnnProfile",  
            algorithm_configuration_name="myExhaustiveKnn",
            vectorizer_name="myOpenAI",  
        ),  
    ],
    vectorizers=[  
        AzureOpenAIVectorizer(  
            vectorizer_name="myOpenAI",  
            kind="azureOpenAI",  
            parameters=AzureOpenAIVectorizerParameters(  
                resource_url=AZURE_OPENAI_ENDPOINT,  
                deployment_name=AZURE_OPENAI_EMBEDDING_MODEL,  
                api_key=AZURE_OPENAI_KEY,
                model_name="text-embedding-3-small" 
            ),  
        ),  
    ],   
)  

endpoint = os.getenv("AZURE_AI_SEARCH_SERVICE_ENDPOINT")
key = os.getenv("AZURE_AI_SEARCH_API_KEY")  
credential = AzureKeyCredential(key)
index_client = SearchIndexClient(endpoint=endpoint, credential=credential)

semantic_config = SemanticConfiguration(
    name="my-semantic-config",
    prioritized_fields=SemanticPrioritizedFields(
        title_field=SemanticField(field_name="meta"),
        # keywords_fields=[SemanticField(field_name="Category")],
        # content_fields=[SemanticField(field_name="chunk")]
    )
)

# Create the semantic settings with the configuration  
semantic_search = SemanticSearch(configurations=[semantic_config])

fields = [  
    # SearchField(name="chunk_id", type=SearchFieldDataType.String, searchable= True,filterable=True,sortable=True,facetable=True,key=True,analyzer_name="keyword"),  
    SearchField(name="id",type=SearchFieldDataType.String,searchable=True,filterable=True,sortable=False,facetable=False,key=True), 
    SearchField(name="content", type=SearchFieldDataType.String,searchable=True,filterable=False,sortable=False,facetable=False,key=False),  
    SearchField(name="meta", type=SearchFieldDataType.String,searchable=True,filterable=True,sortable=False,facetable=True,key=False),
    SearchField(name="date", type=SearchFieldDataType.DateTimeOffset,searchable=False,filterable=True,sortable=True,facetable=True,key=False),    
    SearchField(name="vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),searchable=True,filterable=False,sortable=False,vector_search_dimensions=1536,vector_search_profile_name="myHnswProfile")
] 

# Create the search index with the semantic settings  
index = SearchIndex(name="bytewax-index", fields=fields, vector_search=vector_search,
                    semantic_search=semantic_search)  
result = index_client.create_or_update_index(index)  
# print(f"{result.name} created")

print(f"Creating bytewax-index search index")