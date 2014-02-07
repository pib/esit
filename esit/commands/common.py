from .term import ProgressBar
from .. import utils

COMMANDS = {}


def command(fn):
    COMMANDS[fn.__name__] = fn
    return fn


def copy_docs(client, src_index, dest_index, transform=None):
    bar = ProgressBar('copy (eta %(eta_td)s) %(percent).0f%%')

    def update_progress(sofar, total):
        bar.max = total
        bar.goto(sofar)
    try:
        utils.copy_documents(
            client, src_index, dest_index, update_progress, transform)
    finally:
        bar.message = "done (took %(elapsed_td)s) %(percent).0f%%"
        bar.update()
        bar.finish()
