from .common import command, copy_docs
from .. import utils

import datetime
import imp
import os.path


@command
def migrate(client, args):
    """
    Run a migration script to update an aliased index.

    Usage: esit migrate <alias> <migrate_script>

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

    """
    migrate_mod_path = os.path.abspath(args['<migrate_script>'])
    migrate_mod = imp.load_source('migrate_mod', migrate_mod_path)

    src = args['<alias>']
    dest = migrate_mod.index_name.format(alias=src, date=datetime.date.today())

    utils.put_index_metadata(client, dest, migrate_mod.index_metadata)
    transform = getattr(migrate_mod, 'transform_document', None)
    copy_docs(client, src, dest, transform=transform)
    utils.move_alias(client, src, dest)
