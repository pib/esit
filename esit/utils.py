import json
from pyelasticsearch.exceptions import ElasticHttpNotFoundError


def get_index_metadata(client, index_name):
    # In case of alias, get actual index name
    real_index_name = client.aliases(index_name).keys()[0]

    settings = client.get_settings(index_name)
    mappings = client.get_mapping(index_name)

    for remkey in ('index.uuid', 'index.version.created'):
        if remkey in settings:
            del settings[remkey]

    index_meta = {'settings': settings[real_index_name]['settings'],
                  'mappings': mappings[real_index_name]}

    return index_meta


def put_index_metadata(client, index_name, index_meta):
    client.create_index(index_name, index_meta)
    client.refresh(index_name)


def copy_index_metadata(client, src_index, dest_index):
    index_meta = get_index_metadata(client, src_index)
    put_index_metadata(client, dest_index, index_meta)


def alias_index(client, alias_name, index_name):
    settings = {
        "actions": [
            {"add": {"index": index_name,
                     "alias": alias_name}}
        ]
    }
    client.update_aliases(settings)


def move_alias(client, alias_name, index_name):
    try:
        old_index_name = client.aliases(alias_name).keys()[0]
    except ElasticHttpNotFoundError:
        alias_index(client, alias_name, index_name)
        return

    settings = {
        "actions": [
            {"remove": {"index": old_index_name, "alias": alias_name}},
            {"add": {"index": index_name, "alias": alias_name}},
        ]
    }
    client.update_aliases(settings)


def write_index_metadata(index_meta, filename):
    with open(filename, 'w') as f:
        json.dump(index_meta, f, indent=4,
                  separators=(',', ': '), sort_keys=True)


def read_index_metadata(filename):
    with open(filename, 'r') as f:
        return json.load(f)


def index_documents(client, index_name, documents):
    lines = []
    for doc in documents:
        action = {"index": {
            "_index": index_name,
            "_type": doc['_type'],
            "_id": doc['_id'],
        }}
        lines.append(json.dumps(action))
        lines.append(json.dumps(doc['_source']))

    body = '\n'.join(lines) + '\n'
    client.send_request('POST', ['_bulk'], body, encode_body=False)


def copy_documents(client, src_index, dest_index, progress_fn=None, transform=None):
    total = client.count('*', index=src_index)['count']
    sofar = 0

    if progress_fn is not None:
        progress_fn(0, total)

    for page in iter_documents(client, src_index):
        lines = []
        for hit in page:
            if transform:
                hit = transform(hit)
            action = {"index": {
                "_index": dest_index,
                "_type": hit['_type'],
                "_id": hit['_id'],
            }}
            lines.append(json.dumps(action))
            lines.append(json.dumps(hit['_source']))
            sofar += 1

        body = '\n'.join(lines) + '\n'
        client.send_request('POST', ['_bulk'], body, encode_body=False)

        if progress_fn is not None:
            progress_fn(sofar, total)


def iter_documents(client, index_name, per_page=100):
    offset = 0
    while True:
        res = client.search("*", index=index_name,
                            size=per_page, es_from=offset)
        if len(res['hits']['hits']) == 0:
            break
        yield res['hits']['hits']
        offset += per_page
