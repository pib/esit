from .common import command, copy_docs
from .. import utils


@command
def get(client, args):
    """
    Save the index settings and mappings to a JSON file.

    Usage: esit get <index> <json_file>
    """

    index_meta = utils.get_index_metadata(client, args['<index>'])
    utils.write_index_metadata(index_meta, args['<json_file>'])


@command
def put(client, args):
    """
    Create an index with the index settings and mappings in a JSON file.

    Usage: esit put <index> <json_file>
    """

    index_meta = utils.read_index_metadata(args['<json_file>'])
    utils.put_index_metadata(client, args['<index>'], index_meta)


@command
def copy(client, args):
    """
    Copy an index to a new index, optionally including documents.

    Usage: esit copy <src_index> <dest_index> [-d] [-m]

    Options:
      -d, --copy-docs  Copy the documents as well.
      -m, --no-meta    Don't copy the metadata (settings and mappings).
    """

    if not args['--no-meta']:
        utils.copy_index_metadata(
            client, args['<src_index>'], args['<dest_index>'])

    if args['--copy-docs']:
        copy_docs(client, args['<src_index>'], args['<dest_index>'])
