from .common import command, copy_docs
from .. import utils


@command
def wrap(client, args):
    """
    Wrap an alias around an index (copy old->new, remove old, alias old->new).

    Usage: esit wrap <src_index> <dest_index>
    """
    src, dest = args['<src_index>'], args['<dest_index>']

    utils.wrap_index(src, dest)


@command
def point(client, args):
    """
    Move an alias from its current index to the specified one

    Usage: esit point <alias> <dest_index>
    """
    src, dest = args['<alias>'], args['<dest_index>']
    utils.move_alias(client, src, dest)


@command
def info(client, args):
    """
    Get general information about a given index or alias.

    Usage: esit info <index>
    """
    index_name = args['<index>']
    index_info = utils.index_info(client, index_name)
    print index_info
