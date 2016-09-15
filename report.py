#!/usr/bin/env python

from biii import open_db
from collections import defaultdict

import psycopg2.extras as extras

conn, cur = open_db(cursor_factory=extras.RealDictCursor)
cur.execute((
    "select t.term, t.tid, n.nid, n.title, n.type "
    "from node n, term t, tags l "
    "where n.nid = l.node and l.term = t.tid"
))
rs = defaultdict(lambda: defaultdict(list))
try:
    for node in cur:
        node = dict(node)
        key = "'%s' (%s)" % (node["term"], node["tid"])
        rs[key][node["type"]].append(node)
    for term, terms in sorted(rs.items()):
        print term
        for type, nodes in sorted(terms.items()):
            print "\t", type
            for node in nodes:
                print "\t\t", node["title"], "(%s)" % node["nid"]
    print "UNUSED TAGS:"
    cur.execute((
        "select t1.tid, t1.term from term t1 where not exists ("
        "   select 1 from tags t2 where t2.term = t1.tid "
        ")"))
    for unused in cur:
        print "\t%(term)s (%(tid)s)" % unused
finally:
    conn.close()
