import itertools, io, sys
from ucsv import unicodecsv as csv
from itertools import groupby
from collections import defaultdict
import os.path

def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk: return
       yield chunk

class DictWriter(object):
    def __init__(self, *args, **kwargs):
        self.inner = export_csv_iter(*args, **kwargs)
        self.writerow = self.inner.send
        self.inner.next()

    def writerows(self, dicts):
        for d in dicts:
            self.inner.send(d)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.inner.close()

def export_csv(filename, dicts, calculate_fieldnames=False, *args, **kwargs):
    if calculate_fieldnames:
        dicts = list(dicts)
        kwargs['fieldnames'] = get_all_keys(dicts)
    with DictWriter(filename, *args, **kwargs) as w:
        w.writerows(dicts)

def export_csv_iter(filename, fieldnames=None, dialect=None, append=False, writeheader=True):
    if not dialect: dialect = get_dialect(filename)
    row = yield
    if fieldnames is None: fieldnames = row.keys()
    closefd = filename != '-'
    if filename == '-': filename = sys.stdout.fileno()
    with io.open(filename, 'at' if append else 'wt', newline='', encoding=dialect.encoding, closefd=closefd) as f:
        with csv.DictWriter(f, dialect=dialect, fieldnames=fieldnames) as csv_out:
            if writeheader and not append: csv_out.writeheader()
            while True:
                csv_out.writerow(row)
                row = yield


def export_csv_tuples_iter(filename, header=None, dialect=None, append=False):
    if not dialect: dialect = get_dialect(filename)
    closefd = filename != '-'
    if filename == '-': filename = sys.stdout.fileno()
    rows = yield
    with io.open(filename, 'at' if append else 'wt', newline='', encoding=dialect.encoding, closefd=closefd) as f:
        with csv.writer(f, dialect=dialect) as csv_out:
            while True:
                csv_out.writerows(rows)
                rows = yield


def export_csv_tuples(filename, tuples, header=None, dialect=None):
    if not dialect: dialect = get_dialect(filename)
    with io.open(filename, 'wt', newline='', encoding=dialect.encoding) as f:
        with csv.writer(f, dialect=dialect) as csv_out:
            if header: csv_out.writerow(header)
            for t in tuples:
                csv_out.writerow(t)

DIALECTS = {
        'txt' : csv.excel_tab,
        'csv' : csv.PETDialect,
        'tsv' : csv.excel_tsv,
}
def register_filename_dialect(extension, dialect):
    DIALECTS[extension] = dialect

def get_dialect(filename):
    extension = os.path.splitext(filename.lower())[1][1:]
    if extension in DIALECTS:
        return DIALECTS[extension]
    if filename == '-': return csv.excel
    raise ValueError

def import_csv(*args, **kwargs):
    return list(import_csv_iter(*args, **kwargs))

def import_csv_iter(filename, *args, **kwargs):
    if not kwargs.get('dialect'): kwargs['dialect'] = get_dialect(filename)
    closefd = filename != '-'
    if filename == '-': filename = sys.stdin.fileno()
    with io.open(filename, 'rt', encoding=kwargs['dialect'].encoding, closefd=closefd) as f:
        for i, e in enumerate(csv.DictReader(f, *args, **kwargs)):
            yield e

def import_csv_tuples_iter(filename, *args, **kwargs):
    if not kwargs.get('dialect'): kwargs['dialect'] = get_dialect(filename)
    closefd = filename != '-'
    if filename == '-': filename = sys.stdin.fileno()
    with io.open(filename, 'rt', encoding=kwargs['dialect'].encoding, closefd=closefd) as f:
        for e in csv.reader(f, *args, **kwargs):
            yield e

def get_common_keys(rows, force_include=lambda k: False):
    counter = defaultdict(int)
    for row in rows:
        for k in row:
            counter[k] += 1
    return sorted([k for k, v in counter.items() if (v == len(rows)) or force_include(k)])

def get_all_keys(rows, force_include=None):
    keys = set()
    for row in rows:
        keys.update(row.keys())
    return sorted(keys)

def get_csvs_common_keys(csvs, keys=get_common_keys):
    def get_first_row(csv):
        return import_csv_iter(csv).next()
    first_rows = [get_first_row(csv) for csv in csvs]
    return keys(first_rows)

def import_csvs(csvs):
    return list(itertools.chain(*[import_csv(c) for c in csvs]))

def merge_csvs(csvs, output_filename, keys=get_common_keys):
    keys = get_csvs_common_keys(csvs, keys=keys)
    with DictWriter(output_filename, fieldnames=keys) as output:
        for csv in csvs:
            output.writerows(import_csv_iter(csv))

def dedupe_csv(input_filename, output_filename, key):
    with DictWriter(output_filename) as output:
        exported = set()
        rows = import_csv_iter(input_filename)
        for row in rows:
            k = key(row)
            if k in exported: continue
            exported.add(k)
            output.writerow(row)

def slim_csv(input_filename, output_filename, fieldnames):
    with DictWriter(output_filename, fieldnames=fieldnames) as output:
        output.writerows(import_csv_iter(input_filename))
