# -*- coding: utf-8 -*-
import csv, io
from csv import *
from cStringIO import StringIO


class PETDialect(csv.Dialect):
    delimiter = ';'
    quoting = csv.QUOTE_ALL
    doublequote = True
    quotechar = '"'
    lineterminator = '\r\n'
    encoding = 'utf-8'

class excel_tsv(csv.Dialect):
    delimiter = '\t'
    quoting = csv.QUOTE_ALL
    doublequote = True
    quotechar = '"'
    lineterminator = '\r\n'
    encoding = 'utf-8'

class mysql_tsv(csv.Dialect):
    delimiter = '\t'
    quoting = csv.QUOTE_NONE
    doublequote = True
    quotechar = '"'
    lineterminator = '\r\n'
    escapechar = '\\'
    encoding = 'utf-8'

setattr(csv.excel, 'encoding', 'utf-8')
setattr(csv.excel_tab, 'encoding', 'utf-16')

encode = lambda e: unicode(e).encode('utf-8') if e is not None else ''
decode = lambda e: e.decode('utf-8') if e is not None else u''

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


class writer(object):
    def __init__(self, f, *args, **kwargs):
        self.queue = StringIO()
        self.writer = csv.writer(self.queue, *args, **kwargs)
        self.stream = f
        if isinstance(self.stream, basestring):
            self.stream = io.open(f, 'wt', newline='', encoding=kwargs['dialect'].encoding)


    def flush(self):
        data = decode(self.queue.getvalue())
        self.stream.write(data)
        self.queue.truncate(0)

    def writerow(self, row, flush=True):
        self.writer.writerow([encode(s) for s in row])
        if flush: self.flush()

    def writerows(self, rows):
        for row in rows:
            self.writerow(row, False)
        self.flush()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.flush()
        self.stream.close()

    def __getattr__(self, name):
        return getattr(self.writer, name)


class DictWriter(object):
    def __init__(self, f, *args, **kwargs):
        self.queue = StringIO()
        if 'fieldnames' in kwargs:
            kwargs['fieldnames'] = [k.encode('ascii', 'ignore') for k in kwargs['fieldnames']]
        self.writer = csv.DictWriter(self.queue, *args, **kwargs)
        self.stream = f

    def flush(self):
        data = decode(self.queue.getvalue())
        self.stream.write(data)
        self.queue.truncate(0)

    def writerow(self, row, flush=True):
        self.writer.writerow(OrderedDict((k.encode('ascii', 'ignore'), encode(row.get(k, ''))) for k in self.writer.fieldnames))
        if flush: self.flush()

    def writeheader(self):
        self.writerow(OrderedDict((f, f) for f in self.writer.fieldnames), flush=False)
        if len(self.queue.getvalue()) > 10000:
            self.flush()

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.flush()

    def __getattr__(self, name):
        return getattr(self.writer, name)

import pdb; st = pdb.set_trace
class UTF8Encoder(object):
    def __init__(self, f): self.f = f
    def read(*args, **kwargs): return encode(self.f.read(*args, **kwargs))
    def __iter__(self, *args, **kwargs):
        for e in self.f:
            yield encode(e)

class DictReader(object):
    def __init__(self, f, dict=OrderedDict, encode=True, *args, **kwargs):
        self.dict = dict
        self.map_fieldnames = kwargs.pop('map_fieldnames', None)
        self.fieldnames = kwargs.pop('fieldnames', None)
        self.reader = csv.reader(UTF8Encoder(f) if encode else f, *args, **kwargs)
        if not self.fieldnames: self.fieldnames = self.reader.next()

    def next(self):
        row = self.reader.next()
        if self.map_fieldnames:
            return self.dict((self.map_fieldnames(decode(k)), decode(v)) for k, v in zip(self.fieldnames, row))
        else:
            return self.dict((decode(k), decode(v)) for k, v in zip(self.fieldnames, row))

    def __iter__(self):
        return self

    def __getattr__(self, name):
        return getattr(self.reader, name)

class reader(object):
    def __init__(self, f, *args, **kwargs):
        self.reader = csv.reader(UTF8Encoder(f), *args, **kwargs)

    def next(self):
        row = self.reader.next()
        return [decode(e) for e in row]

    def __getattr__(self, name):
        return getattr(self.reader, name)

    def __iter__(self):
        return self


