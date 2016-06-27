# -*- coding: utf-8 -*-

import sys
import couchdb
from couchdb.design import ViewDefinition
from prettytable import PrettyTable

DB = sys.argv[1]

server = couchdb.Server('http://localhost:5984')
db = server[DB]


def dateTimeCountMapper(doc):
    from dateutil.parser import parse
    from datetime import datetime as dt
    if doc.get('Date'):
        try:
            _doc = str(doc.get('Date')).replace('[', '(').replace(']', ')')
            _date = list(dt.timetuple(parse(_doc[:-3])))
            yield (_date, 1)
        except Exception as e:
            with open("/Users/cpence/projects/divorce/snitch/data/mbox.couchdb.log", mode='a') as f:
                f.write('\n\nException message: %s \n\n\t\tCurrent Document: %s' % (e.message, doc))


def summingReducer(keys, values, rereduce):
    return sum(values)


view = ViewDefinition('index', 'doc_count_by_date_time', dateTimeCountMapper, reduce_fun=summingReducer, language='python')
view.sync(db)

# Print out message counts by time slice such that they're
# grouped by year, month, day

field_names = ['Date', 'Count']
pt = PrettyTable(field_names=field_names)
pt.align = 'l'

for row in db.view('index/doc_count_by_date_time', group_level=3):
    pt.add_row(['-'.join([str(i) for i in row.key]), row.value])

print pt
