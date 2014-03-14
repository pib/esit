from .common import command, copy_docs, progress_funcs
from .. import utils
from pyelasticsearch.exceptions import ElasticHttpNotFoundError

import datetime
import imp
import os.path
import re


@command
def migrate(client, args):
    """
    Run a migration script to update an aliased index.

    Usage: esit migrate <alias> <migrate_script> [options]

    Options:
      -c, --create  Create a new index, rather than migrating an existing one.
      -t, --test    Don't point the alias to the new index.

    This command will create a new index, copy all the docs from the
    existing index pointed to by <alias> (optionally transforming them
    if a transform_document function is provided), and point the alias
    to the new index.

    migrate_script should be the path to a python file with some
    module-level variables:
      index_name          A name for the newly-created index. Can optionally
                          include replacement variables {alias} and {date}.
      index_metadata      A dict in the format used in the ElasticSearch
                          create-index API call.
      transform_document  (optional) Function which takes an document dict as
                          an argument and returns a possibly-modified dict.
                          This can be used when changes which can't be handled
                          by simply reindexing are done.
      documents           (optional) List of documents to insert into the
                          newly-migrated index. They should be in the same
                          format of the "hits" list of an ES search.

    """
    migrate_mod_path = os.path.abspath(args['<migrate_script>'])
    mod_name = os.path.splitext(os.path.basename(migrate_mod_path))[0]
    migrate_mod = imp.load_source(mod_name, migrate_mod_path)

    src = args['<alias>']
    dest = migrate_mod.index_name.format(alias=src, date=datetime.date.today())

    utils.put_index_metadata(client, dest, migrate_mod.index_metadata)
    transform = getattr(migrate_mod, 'transform_document', None)

    if not args['--create']:
        copy_docs(client, src, dest, transform=transform)

    if hasattr(migrate_mod, 'documents'):
        utils.index_documents(client, dest, migrate_mod.documents)

    if not args['--test']:
        utils.move_alias(client, src, dest)


@command
def upgrade(client, args):
    """
    Run migrations to update to the latest version.

    Usage: esit upgrade <migrate_dir>

    This command is mostly equivalent to running the migrate command
    for each migration script in the migrate directory, with some
    extra functionality.

    There should be a file named index.py in the migrate directory
    with the following module-level variables:

      aliases             A dict mapping alias names to dicts containing:

          alias_pattern   (optional) A regex pattern to use in discovering
                          aliases to run migrations on.

          initial_index   For initial runs, if there is an index with the
                          name of the target alias, copy it to this index
                          and point the alias to this index before doing
                          the actual migrations. {alias} can be used to
                          generate distinct index names when alias_pattern
                          is used.

          latest          The name of the most recent index which migrations
                          will lead to. Once this index exists and is pointed
                          to by the alias, the upgrade is done. {alias} can be
                          used to generate distinct index names when
                          alias_pattern is used.

          transitions     A dict with keys equal to an index name and the
                          values being the script which will migrate from that
                          index to the next one. {alias} can be used to
                          generate distinct index names when alias_pattern
                          is used.
    """
    migrate_dir = os.path.abspath(args['<migrate_dir>'])
    index_mod_path = os.path.join(migrate_dir, 'index.py')
    index_mod = imp.load_source('index_mod', index_mod_path)

    current_aliases = get_current_aliases(client)
    for alias, conf in index_mod.aliases.items():
        alias_re = re.compile(
            conf.get('alias_pattern', '^{}$'.format(alias)))
        match_found = False
        for alias_name, index_name in current_aliases.items():
            if alias_re.match(alias_name):
                match_found = True
                upgrade_index(
                    client, alias_name, index_name, conf, migrate_dir)
        if not match_found and 'alias_pattern' not in conf:
            # Create initial index for non-pattern aliases
            create_initial_index(client, alias, conf['initial_index'])
            upgrade_index(client, alias, conf[
                          'initial_index'], conf, migrate_dir)


def get_current_aliases(client):
    """ Returns a dict of all aliases currently in elasticsearch and
    the indexes they point to """
    aliases = {}
    for name, index in client.aliases().items():
        index_aliases = index['aliases'].keys()
        if len(index_aliases) == 0:
            aliases[name] = name
        else:
            aliases[index_aliases[0]] = name
    return aliases


def create_initial_index(client, alias, index):
    client.create_index(index)
    client.refresh(index)
    utils.alias_index(client, alias, index)


def upgrade_index(client, alias, current_index, conf, migrate_dir):
    """ upgrade the specified alias to the latest version specified in
    conf
    """
    if alias == current_index:
        current_index = conf['initial_index'].format(alias=alias)
        print alias, '->', current_index
        progress, finish = progress_funcs()
        try:
            utils.wrap_index(client, alias, current_index, progress)
            client.refresh(alias)
        finally:
            finish()

    transitions = format_transitions(alias, conf['transitions'])
    latest = conf['latest'].format(alias=alias)
    while current_index != latest:
        print current_index, '->', transitions[current_index]
        migrate_script = os.path.join(migrate_dir, transitions[current_index])
        migrate(client, {
            '<migrate_script>': migrate_script,
            '<alias>': alias,
            '--create': False,
            '--test': False,
        })
        client.refresh(alias)
        current_index = client.aliases(alias).keys()[0]


def format_transitions(alias, transitions):
    return {k.format(alias=alias): v for k, v in transitions.items()}
