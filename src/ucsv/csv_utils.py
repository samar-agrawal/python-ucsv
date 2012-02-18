import itertools, io
from ucsv import unicodecsv as csv
from itertools import groupby
from collections import defaultdict
from hzutils.hzasq import *

class PETDialect(csv.Dialect):
    delimiter = ';'
    quoting = csv.QUOTE_ALL
    doublequote = True
    quotechar = '"'
    lineterminator = '\r\n'
    encoding = 'utf-8'
setattr(csv.excel_tab, 'encoding', 'utf-16')

def export_csv(filename, dicts, *args, **kwargs):
    w = export_csv_iter(filename, *args, **kwargs)
    w.next()
    for d in dicts:
        w.send(d)
    w.close()

def export_csv_iter(filename, fieldnames=None, dialect=None, append=False, writeheader=True):
    if not dialect: dialect = get_dialect(filename)
    row = yield
    if fieldnames is None: fieldnames = row.keys()
    with io.open(filename, 'at' if append else 'wt', newline='', encoding=dialect.encoding) as f:
        with csv.DictWriter(f, dialect=dialect, fieldnames=fieldnames) as csv_out:
            if writeheader and not append: csv_out.writeheader()
            while True:
                csv_out.writerow(row, flush=False)
                row = yield


def export_csv_tuples(filename, tuples, header=None, dialect=None):
    if not dialect: dialect = get_dialect(filename)
    with io.open(filename, 'wt', newline='', encoding=dialect.encoding) as f:
        with csv.writer(f, dialect=dialect) as csv_out:
            if header: csv_out.writerow(header)
            for t in tuples:
                csv_out.writerow(t)
        
def get_dialect(filename):
    if filename.endswith("txt"): return csv.excel_tab
    if filename.endswith("csv"): return PETDialect
    raise ValueError
        
def import_csv(filename, dialect=None):
    return list(import_csv_iter(filename, dialect))

def import_csv_iter(filename, dialect=None):  
    if not dialect: dialect = get_dialect(filename)
    with io.open(filename, 'rt', encoding=dialect.encoding) as f:
        for e in csv.DictReader(f, dialect=dialect):
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
    output = export_csv_iter(output_filename, fieldnames=keys)
    output.next()
    for csv in csvs:
        for row in import_csv_iter(csv):
            output.send(row)
    
def grouped_csv(input_filename, output_filename, dialect=None, key=lambda e: e['sku_config']):
    if not dialect: dialect = get_dialect(input_filename)
    other_key = lambda e: e[0]
    rows = import_csv(input_filename)
    static_keys = None
    for group, items in groupby(sorted(rows, key=key), key=key):
        items = list(set([i for item in items for i in item.items()]))
        this_static_keys = set()
        for group, items in groupby(sorted(items, key=other_key), key=other_key):
            if len(list(items)) == 1: this_static_keys.add(group)
        static_keys = static_keys & this_static_keys if static_keys else this_static_keys
    def get_new_rows():
        for group, items in groupby(sorted(rows, key=key), key=key):
            yield list(items)[0]
    export_csv(output_filename, get_new_rows(), fieldnames=sorted(static_keys))

def slim_csv(input_filename, output_filename, fieldnames):
    sanitize = lambda e: e.replace('\r\n', '<br/>').replace('\n', '<br/>')
    rows = (dict((k, sanitize(r.get(k, ''))) for k in fieldnames) for r in import_csv_iter(input_filename))
    export_csv(output_filename, rows, fieldnames)
