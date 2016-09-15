#!/usr/bin/env python

from biii import open_db
import psycopg2.extras as extras

conn, cur = open_db(cursor_factory=extras.RealDictCursor)
cur.execute("select * from node")
try:
    for node in cur:
        node = dict(node)
        nid = node.pop("nid")
        for k, v in sorted(node.items()):
            if v and '"' not in str(v):
                print ':%s :%s "%s" .' % (nid, k, v)
    cur.execute((
        "select n.nid, t.term from node n, term t, tags l "
        "where n.nid = l.node and l.term = t.tid"
    ))
    for tag in cur:
        if tag["term"]:
            print ':%s :tag "%s".' % (tag["nid"], tag["term"])
finally:
    conn.close()
