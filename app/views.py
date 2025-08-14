from flask import render_template, request, Blueprint
from .opensearch_client import *

main = Blueprint('main', __name__)


@main.route('/', methods=['GET', 'POST'])
def index():
    client = get_opensearch_client()
    create_index(client)

    if client.count(index="documents_index")['count'] == 0:
        documents = generate_documents(5)
        index_documents(client, documents)

    results = []
    if request.method == 'POST':
        query = request.form.get('query', '')
        content_type = request.form.get('content_type', CONTENT_TYPES[0])
        results = search_documents(client, query, content_type)

    return render_template('index.html', content_types=CONTENT_TYPES, results=results)
