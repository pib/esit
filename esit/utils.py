import json


def get_index_metadata(client, index_name):
    settings = client.get_settings(index_name)
    mappings = client.get_mapping(index_name)
    return {'settings': settings[index_name]['settings'], 'mappings': mappings[index_name]}


def put_index_metadata(client, index_name, index_meta):
    client.create_index(index_name, index_meta)
    client.refresh(index_name)


def write_index_metadata(index_meta, filename):
    with open(filename, 'w') as f:
        json.dump(index_meta, f, indent=4,
                  separators=(',', ': '), sort_keys=True)


def read_index_metadata(filename):
    with open(filename, 'r') as f:
        index_meta = json.load(f)


def copy_documents(client, src_index, dest_index, progress_fn=None):
    total = client.count('*', index=src_index)['count']
    sofar = 0

    if progress_fn is not None:
        progress_fn(0, total)

    for page in iter_documents(client, src_index):
        lines = []
        for hit in page:
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
