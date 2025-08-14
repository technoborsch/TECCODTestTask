from opensearchpy import OpenSearch, RequestsHttpConnection
import random

INDEX_NAME = "documents_index"
CONTENT_TYPES = ["article", "news", "tutorial", "report"]


def get_opensearch_client():
    return OpenSearch(
        hosts=[{'host': 'opensearch', 'port': 9200}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        connection_class=RequestsHttpConnection
    )


def create_index(client):
    if not client.indices.exists(index=INDEX_NAME):
        client.indices.create(
            index=INDEX_NAME,
            body={
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                },
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "content": {"type": "text"},
                        "content_type": {"type": "keyword"}
                    }
                }
            }
        )


def generate_documents(count=5):
    documents = []
    titles = [
        "Introduction to Python",
        "Advanced Data Analysis",
        "Machine Learning Basics",
        "Web Development with Flask",
        "OpenSearch Tutorial"
    ]
    contents = [
        "Python is a popular programming language for data science and web development.",
        "Data analysis involves processing and interpreting data to extract insights.",
        "Machine learning algorithms can learn patterns from data and make predictions.",
        "Flask is a lightweight web framework for Python that's easy to get started with.",
        "OpenSearch is a scalable search and analytics engine based on Apache Lucene."
    ]

    for i in range(count):
        doc = {
            "title": random.choice(titles),
            "content": random.choice(contents),
            "content_type": random.choice(CONTENT_TYPES)
        }
        documents.append(doc)

    return documents


def index_documents(client, documents):
    for i, doc in enumerate(documents):
        client.index(
            index=INDEX_NAME,
            body=doc,
            id=i + 1,
            refresh=True
        )


def search_documents(client, query, content_type):
    search_body = {
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title", "content"]
                    }
                },
                "filter": {
                    "term": {
                        "content_type": content_type
                    }
                }
            }
        },
        "_source": ["title", "content"]
    }

    response = client.search(
        index=INDEX_NAME,
        body=search_body
    )

    results = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        results.append({
            "title": source['title'],
            "snippet": source['content'][:50] + "..." if len(source['content']) > 50 else source['content']
        })

    return results
