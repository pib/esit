"""ElasticSearch Index Tools

Usage:
  esit [-v...] [options] <command> [<args>...]
  esit -h
  esit -V

Common options:
  -v, --verbose              Be more verbose (can be used more than once, i.e. -vvv)
  -s <srv>, --server=<srv>   Use a different server [default: localhost:9200].
  -h, --help                 Show this help message.
  -V, --version              Show version.

Available commands:
{commands}

See 'esit <command> -h | --help' for more information on a specific command.
"""

from esit import __version__ as version
from esit import utils
from docopt import docopt
from pyelasticsearch import ElasticSearch
import json
from .term import ProgressBar

COMMANDS = {}


def _command(fn):
    COMMANDS[fn.__name__] = fn
    return fn


@_command
def get(client, args):
    """Save the index settings and mappings to a JSON file.

    Usage: esit get <index> <json_file>
    """

    index_meta = utils.get_index_metadata(client, args['<index>'])
    utils.write_index_metadata(index_meta, args['<json_file>'])


@_command
def put(client, args):
    """Create an index with the index settings and mappings in a JSON file.

    Usage: esit put <index> <json_file>
    """

    index_meta = utils.read_index_metadata(args['<json_file>'])
    utils.put_index_metadata(client, args['<index>'], index_meta)


@_command
def copy(client, args):
    """Copy an index to a new index, optionally including documents.

    Usage: esit copy <src_index> <dest_index> [-d] [-m]

    Options:
      -d, --copy-docs  Copy the documents as well.
      -m, --no-meta    Don't copy the metadata (settings and mappings).
    """

    if not args['--no-meta']:
        index_meta = utils.get_index_metadata(client, args['<src_index>'])
        utils.put_index_metadata(client, args['<dest_index>'], index_meta)

    if args['--copy-docs']:
        bar = ProgressBar('copy (eta %(eta_td)s) %(percent).0f%%')

        def update_progress(sofar, total):
            bar.max = total
            bar.goto(sofar)
        try:
            utils.copy_documents(
                client, args['<src_index>'], args['<dest_index>'], update_progress)
        finally:
            bar.message = "done (took %(elapsed_td)s) %(percent).0f%%"
            bar.update()
            bar.finish()


def _commands():
    max_name_len = 0
    commands = []
    for name, fn in COMMANDS.items():
        description = fn.__doc__.split('\n')[0]
        max_name_len = max(max_name_len, len(name))
        commands.append((name, description))
    return __doc__.format(
        commands='\n'.join(['  {}  {}'.format(n.ljust(max_name_len), d)
                            for n, d in commands]))


def set_verbose(verbose):
    import logging
    logging.basicConfig()

    if verbose > 1:
        logging.getLogger('pyelasticsearch').setLevel(logging.DEBUG)
        logging.getLogger('requests').setLevel(logging.DEBUG)
    elif verbose > 0:
        logging.getLogger('pyelasticsearch').setLevel(logging.DEBUG)


def run():
    args = docopt(_commands(),
                  version=version,
                  options_first=True)
    sub_argv = [args['<command>']] + args['<args>']

    # Little debug bit to check docopt output
    if args['<command>'] == 'args':
        print args
        return

    set_verbose(args['--verbose'])
    sub_command = COMMANDS[args['<command>']]
    sub_args = docopt(sub_command.__doc__, argv=sub_argv)

    client = ElasticSearch(
        'http://{}'.format(args.get('--server', 'localhost:9200')))
    sub_command(client, sub_args)

if __name__ == '__main__':
    run()
