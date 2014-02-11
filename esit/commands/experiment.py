from .common import command, copy_docs
from .. import utils

from jinja2 import Template

import datetime
import imp
import os.path


@command
def experiment(client, args):
    """
    Run an experiment script on an index.

    Usage: esit experiment <index_name> <experiment_script> [options]

    Options:
      -o=<output>, --output=<output>  Output file [default: experiment.html].
      -f, --force                     Delete target indexes if they exist.
      -c, --no-copy                   Assume the target indexes already exist.

    This command will make copies of the index for each entry in the
    <experiment_script>'s module-level "indexes" variable, which
    should be a list of dicts containing keys "name", "metadata", and
    "transform" (treated the same as the three fields used in the
    migrate command). If metadata is left out, the source index's
    metadata will be used.

    After the index copies are made, queries specified in the
    module-level "queries" dict with keys being each query's name and
    values being dicts with the following keys:
      index       Which index to run this query on.
      query       The query to run, in ElasticSearch query DSL format.
      query_args  Extra URL query arguments to pass on the the query
                  (good for specifying size of result set, for example).

    If "queries" is a callable, it will be called with the client and
    base index as arguments and expected to return a dict with the
    above format.

    After the results of the queries have been collected, a Jinja2
    template string in the module-level "template" variable will be
    rendered with a dictionary named "results" in its context. The
    result of rendering the template will be stored in the file
    specified by the --output argument. If there is a module-level
    dict called "extra_context", its contents will also be passed to
    the template.

    """
    experiment_mod_path = os.path.abspath(args['<experiment_script>'])
    experiment_mod = imp.load_source('experiment_mod', experiment_mod_path)

    src = args['<index_name>']
    src_meta = utils.get_index_metadata(client, src)
    if not args['--no-copy']:
        for index in experiment_mod.indexes:
            dest = index['name'].format(alias=src,
                                        date=datetime.date.today())
            if args['--force']:
                client.delete_index(dest)

            meta = index.get('metadata', src_meta)
            transform = index.get('transform_document', None)
            utils.put_index_metadata(client, dest, meta)
            copy_docs(client, src, dest, transform=transform)
            client.refresh(dest)

    results = {}
    queries = experiment_mod.queries
    if callable(queries):
        queries = queries(client, src)

    for name, query in queries.items():
        kwargs = query.get('query_args', {})
        results[name] = client.search(
            query['query'], index=query['index'], **kwargs)

    template = Template(experiment_mod.template)
    if hasattr(experiment_mod, 'extra_context'):
        output = template.render(
            results=results, **experiment_mod.extra_context)
    else:
        output = template.render(results=results)

    open(args['--output'], 'w').write(output.encode('utf8'))
