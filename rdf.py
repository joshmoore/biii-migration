#!/usr/bin/env python

from biii import open_db
import psycopg2.extras as extras

print "@prefix : <#>."
conn, cur = open_db(cursor_factory=extras.RealDictCursor)
cur.execute("select * from node")
try:
    for node in cur:
        node = dict(node)
        nid = node.pop("nid")
        for k, v in sorted(node.items()):
            if v:
                print ':node_%s :%s %r .' % (nid, k, v)

    # Load tags
    cur.execute((
        "select n.nid, t.term from node n, term t, tags l "
        "where n.nid = l.node and l.term = t.tid"
    ))
    for tag in cur:
        if tag["term"]:
            print ':node_%s :tag "%s".' % (tag["nid"], tag["term"])

    # Load fields (safe_text)
    cur.execute((
        "select n.nid, s.field, s.value from node n, safe_text s "
        "where n.nid = s.nid"
    ))
    for text in cur:
        if text["value"]:
            print ':node_%s :%s %r.' % (
                text["nid"], text["field"], text["value"])
finally:
    conn.close()
