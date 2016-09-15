#!/usr/bin/env python

from biii import open_db
import psycopg2.extras as extras

def string_to_c(s, max_length = 140, unicode=False):
    # see http://stackoverflow.com/questions/1675181/get-str-repr-with-double-quotes-python
    ret = []

    # Try to split on whitespace, not in the middle of a word.
    split_at_space_pos = max_length - 10
    if split_at_space_pos < 10:
        split_at_space_pos = None

    position = 0
    if unicode:
        position += 1
        ret.append('L')

    ret.append('"')
    position += 1
    for c in s:
        newline = False
        if c == "\n":
            to_add = "\\\n"
            newline = True
        elif ord(c) < 32 or 0x80 <= ord(c) <= 0xff:
            to_add = "\\x%02x" % ord(c)
        elif ord(c) > 0xff:
            if not unicode:
                raise ValueError, "string contains unicode character but unicode=False"
            to_add = "\\u%04x" % ord(c)
        elif "\\\"".find(c) != -1:
            to_add = "\\%c" % c
        else:
            to_add = c

        ret.append(to_add)
        position += len(to_add)
        if newline:
            position = 0

        if split_at_space_pos is not None and position >= split_at_space_pos and " \t".find(c) != -1:
            ret.append("\\\n")
            position = 0
        elif position >= max_length:
            ret.append("\\\n")
            position = 0

    ret.append('"')

    return "".join(ret)

print "@prefix : <#>."

conn, cur = open_db(cursor_factory=extras.RealDictCursor)
cur.execute("select * from node")
try:
    for node in cur:
        node = dict(node)
        nid = node.pop("nid")
        for k, v in sorted(node.items()):
            if v:
                print ':node_%s :%s %s^^xsd:string.' % (nid, k, string_to_c(v))

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
            print ':node_%s :%s %s^^xsd:string .' % (
                text["nid"], text["field"], string_to_c(text["value"]))
finally:
    conn.close()
