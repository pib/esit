from .term import ProgressBar
from .. import utils

COMMANDS = {}


def command(fn):
    COMMANDS[fn.__name__] = fn
    return fn


def progress_funcs():
    bar = ProgressBar('copy (eta %(eta_td)s) %(percent).0f%%')

    def update_progress(sofar, total):
        bar.max = total
        if total != 0:
            bar.goto(sofar)

    def finish():
        bar.message = "done (took %(elapsed_td)s) %(percent).0f%%"
        if bar.max != 0:
            bar.update()
            bar.finish()

    return update_progress, finish


def copy_docs(client, src_index, dest_index, transform=None):
    update, finish = progress_funcs()
    try:
        utils.copy_documents(
            client, src_index, dest_index, update, transform)
    finally:
        finish()
