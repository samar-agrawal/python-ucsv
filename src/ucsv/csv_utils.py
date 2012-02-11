import itertools, io
import unicodecsv as csv
from itertools import groupby

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
    if fieldnames is None: fieldnames = sorted(row.keys())
    with io.open(filename, 'at' if append else 'wt', newline='', encoding=dialect.encoding) as f:
        csv_out = csv.DictWriter(f, dialect=dialect, fieldnames=fieldnames)
        if writeheader and not append: csv_out.writeheader()
        while True:
            csv_out.writerow(row)
            row = yield

def export_csv_tuples(filename, tuples, header=None, dialect=None):
    if not dialect: dialect = get_dialect(filename)
    with io.open(filename, 'wt', newline='', encoding=dialect.encoding) as f:
        csv_out = csv.writer(f, dialect=dialect)
        if header: csv_out.writerow(header)
        for t in tuples:
            csv_out.writerow(t)
        f.flush()
        
def get_dialect(filename):
    if filename.endswith("txt"): return csv.excel_tab
    if filename.endswith("csv"): return PETDialect
    raise ValueError
        
def import_csv(filename, dialect=None):
    if not dialect: dialect = get_dialect(filename)
    with io.open(filename, 'rt', encoding=dialect.encoding) as f:
        return list( csv.DictReader(f, dialect=dialect) )
        
def import_csv_iter(filename, dialect=None):  
    if not dialect: dialect = get_dialect(filename)
    with io.open(filename, 'rt', encoding=dialect.encoding) as f:
        for e in csv.DictReader(f, dialect=dialect):
            yield e
    
def merge_csvs(csvs, output_filename):
    drs = list(itertools.chain(*[import_csv(c) for c in csvs]))
    dr_concat = lambda acc, val: acc | set(val.keys())
    dr_reducer = lambda acc, val: acc & set(val.keys())
    drs_potential_keys = reduce(dr_concat, drs, set(drs[0].keys()))
    drs_potential_keys = sorted(list(drs_potential_keys))
    drs_keys = reduce(dr_reducer, drs, set(drs[0].keys()))
    drs_keys = sorted(list(drs_keys))
    for k in (set(drs_potential_keys) - set(drs_keys)):
        if 'size' in k:
            drs_keys.append(k)
    drs_small = [dict((k, v.get(k, '')) for k in drs_keys) for v in drs]

    export_csv(output_filename, drs_small, fieldnames=drs_keys)
    
def grouped_csv(input_filename, output_filename, dialect=None, key=lambda e: e['sku_config']):
    if not dialect: dialect = get_dialect(input_filename)
    other_key = lambda e: e[0]
    rows = import_csv(input_filename, dialect=dialect)
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
    rows = import_csv(input_filename)
    rows = [dict((k, sanitize(r.get(k, ''))) for k in fieldnames) for r in rows]
    export_csv(output_filename, rows, fieldnames)

