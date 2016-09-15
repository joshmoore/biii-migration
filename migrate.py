#!/usr/bin/env python

from glob import glob
from json import load, dump
from collections import defaultdict
import psycopg2

def parse(fname):
    with open(fname, "r") as f:
        data = load(f)
        _vid = data["vid"]
        for key, value in data.items():
            if not value: continue
            if isinstance(value, dict):
                if "und" in value:
                    value = value["und"]
            yield _vid, key, value

columns = set()
counts = defaultdict(int)
types = defaultdict(set)
total = len(glob("*.fix"))
printed = set()

for fname in glob("*.fix"):
    for _vid, key, value in parse(fname):
        counts[key] += 1
        types[key].add(type(value))
        if type(value) == unicode:
            columns.add(key.replace("#", ""))
        elif key in (
            "body",  # TODO: has more fields
            "field_tags",
            "field_url",
            "field_url_link",
            "field_workflow_author",
            "field_data_url",
        ):
            # These are handled specially below.
            pass
        elif type(value) == list:
            if key not in printed:
                printed.add(key)
                print key, value
        elif type(value) == dict:
            if key not in printed:
                printed.add(key)
                print key, value

values = counts.items()
values.sort(lambda a, b: cmp(a[1], b[1]))
for k, v in values:
    T = [str(x) for x in types[k]]
    if T != [str(unicode)]:
        print "%30s\t%8s\t%30s" % (k, v, ", ".join(T))

conn = psycopg2.connect("dbname='biii'")
cur = conn.cursor()
cur.execute("select * from information_schema.tables where table_name=%s", ('node',))
if not bool(cur.rowcount):
    cols = " text, ".join([x for x in sorted(columns) if x != "vid"])
    cur.execute("create table node (vid text primary key, %s text)" % cols)
    conn.commit()

for fname in glob("*.fix"):
    cols = list()
    vals = list()
    for _vid, key, value in parse(fname):
        if not value: continue
        if key in columns:
            cols.append(key)
            vals.append(value)
        elif "#%s" in columns:
            cols.append(key.replace("#", ""))
            vals.append(value)
        elif "field_tags" == key:
            for tid in value:
                tid = tid["tid"]
                query = (
                    "insert into tags (node, term) select %s, %s where not exists ("
                    "  select node, term from tags where node = %s and term = %s"
                    ")"
                )
                cur.execute(query, [_vid, tid, _vid, tid])
                conn.commit()
        elif key in ("field_url", "field_url_link", "field_workflow_author", "body"):
            for tid in value:
                saf = tid.get("safe_value", None)
                val = tid["value"]
                fmt = tid["format"]
                query = (
                    "insert into safe_text (vid, field, safe_value, value, format) "
                    "select %s, %s, %s, %s, %s where not exists ("
                    "  select vid, field from safe_text where vid = %s and field = %s"
                    ")"
                )
                cur.execute(query, [_vid, key, saf, val, fmt, _vid, key])
                conn.commit()
        elif key in ("field_data_url",):
            for tid in value:
                val = tid["value"]
                query = (
                    "insert into safe_text (vid, field, safe_value, value, format) "
                    "select %s, %s, %s, %s, %s where not exists ("
                    "  select vid, field from safe_text where vid = %s and field = %s"
                    ")"
                )
                cur.execute(query, [_vid, key, None, val, None, _vid, key])
                conn.commit()

    query = (
        "insert into node (%s) select %s where not exists ("
        "  select vid from node where vid = '%s'"
        ")"
    ) % (
        ", ".join(cols),
        ", ".join(["%s" for x in vals]),
        _vid,
    )
    cur.execute(query, vals)
    conn.commit()
