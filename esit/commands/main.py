"""ElasticSearch Index Tools

Usage:
  esit [-v...] [options] <command> [<args>...]
  esit -h
  esit -V

Common options:
  -v, --verbose              Be more verbose (maybe more than once, i.e. -vvv)
  -s <srv>, --server=<srv>   Use a different server [default: localhost:9200].
  -h, --help                 Show this help message.
  -V, --version              Show version.

Available commands:
{commands}

See 'esit <command> -h | --help' for more information on a specific command.
"""

from esit import __version__ as version
from docopt import docopt
from pyelasticsearch import ElasticSearch
from textwrap import dedent
from .common import COMMANDS


def _commands():
    max_name_len = 0
    commands = []
    for name in sorted(COMMANDS.keys()):
        fn = COMMANDS[name]
        description = fn.__doc__.strip().split('\n')[0]
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
    sub_args = docopt(dedent(sub_command.__doc__).strip(), argv=sub_argv)

    client = ElasticSearch(
        'http://{}'.format(args.get('--server', 'localhost:9200')))
    sub_command(client, sub_args)

if __name__ == '__main__':
    run()
