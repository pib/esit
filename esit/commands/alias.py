from .common import command, copy_docs
from .. import utils


@command
def wrap(client, args):
    """
    Wrap an alias around an index (copy old->new, remove old, alias old->new).

    Usage: esit wrap <src_index> <dest_index>
    """
    src, dest = args['<src_index>'], args['<dest_index>']

    utils.copy_index_metadata(client, src, dest)
    copy_docs(client, src, dest)
    client.delete_index(src)
    utils.alias_index(client, src, dest)


@command
def point(client, args):
    """
    Move an alias from its current index to the specified one

    Usage: esit point <alias> <dest_index>
    """
    src, dest = args['<alias>'], args['<dest_index>']
    utils.move_alias(client, src, dest)
