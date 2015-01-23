# -*- coding: utf-8 -*-
import bz2
import gzip
import re

DISALLOWED_STREAM_CHARACTERS_RE = re.compile(r'[^-_a-zA-Z0-9]')


def scribify(stream_name):
    """Convert an arbitrary stream name to be appropriate to use as a Scribe category name."""
    return DISALLOWED_STREAM_CHARACTERS_RE.sub('_', stream_name)


def open_compressed_file(filename, mode='r'):
    """Open a file as raw, gzip, or bz2, based on the filename."""
    if filename.endswith('.bz2'):
        return bz2.BZ2File(filename, mode=mode)
    elif filename.endswith('.gz'):
        return gzip.GzipFile(filename, mode=mode)
    else:
        return open(filename, mode=mode)
