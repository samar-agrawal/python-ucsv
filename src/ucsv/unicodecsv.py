# -*- coding: utf-8 -*-
import csv
from csv import *
import codecs
from cStringIO import StringIO

setattr(csv.excel_tab, 'encoding', 'utf-16')

encode = lambda e: unicode(e).encode('utf-8')
decode = lambda e: e.decode('utf-8')


class reader(object):
    def __init__(self, f, *args, **kwargs):
        self.queue = StringIO()
        self.queue.write(encode(f.read()))
        self.queue.seek(0)        
        self.reader = csv.reader(self.queue, *args, **kwargs)
        
    def next(self):
        row = self.reader.next()
        return [decode(e) for e in row]
        
    def __getattr__(self, name):
        return getattr(self.reader, name)
        
    def __iter__(self):
        return self


class writer(object):
    def __init__(self, f, *args, **kwargs):
        self.queue = StringIO()
        self.writer = csv.writer(self.queue, *args, **kwargs)
        self.stream = f
        
    def flush(self):
        data = decode(self.queue.getvalue())
        self.stream.write(data)
        self.queue.truncate(0)    
        
    def writerow(self, row):
        self.writer.writerow([encode(s) for s in row])
        self.flush()
        
    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
            
    def __getattr__(self, name):
        return getattr(self.writer, name)

            
class DictWriter(object):
    def __init__(self, f, *args, **kwargs):
        self.queue = StringIO()
        self.writer = csv.DictWriter(self.queue, *args, **kwargs)
        self.stream = f
        
    def flush(self):
        data = decode(self.queue.getvalue())
        self.stream.write(data)
        self.queue.truncate(0)
        
    def writerow(self, row, flush=True):
        self.writer.writerow(dict((encode(k), encode(v)) for k, v in row.items()))
        if flush: self.flush()

    def writeheader(self):
        self.writerow(dict((f, f) for f in self.writer.fieldnames), flush=False)
        self.flush()
        
    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
            
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
    def __init__(self, f, dict=dict, *args, **kwargs):
        self.dict = dict
        self.reader = csv.DictReader(UTF8Encoder(f), *args, **kwargs)
        
    def next(self):
        row = self.reader.next()
        return self.dict((decode(k), decode(v)) for k, v in row.items())
        
    def __iter__(self):
        return self    
            
    def __getattr__(self, name):
        return getattr(self.reader, name)
