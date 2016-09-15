#!/usr/bin/env python

from biii import Node
from biii import PATTERN
from biii import SAFE_TEXT_FIELDS
from biii import TAG_FIELDS
from biii import lang_ins
from biii import open_db
from biii import safe1_ins
from biii import safe2_ins
from biii import tags_ins

from glob import glob
from collections import defaultdict
import psycopg2


columns = set()
counts = defaultdict(int)
types = defaultdict(set)
total = len(glob(PATTERN))

handled = set()
printed = set()

# 1. Load primary data
nodes = [Node(fname) for fname in glob(PATTERN)]

# 2. Parse the various columns
for node in nodes:
    for _nid, key, value in node:
        counts[key] += 1
        types[key].add(type(value))
        if type(value) == unicode:
            columns.add(key.replace("#", ""))
            handled.add(key)
        elif key in SAFE_TEXT_FIELDS \
            or key in TAG_FIELDS \
                or key == "field_data_url":
            # These are handled specially below.
            handled.add(key)
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
    if k in printed:
        print "%30s\t%8s\t%30s" % (k, v, ", ".join(T))


# 3. Setup db and create nodes
conn, cur = open_db()

for node in nodes:
    cols = list()
    vals = list()
    for _nid, key, value in node:
        if key in columns:
            cols.append(key)
            vals.append(value)
        elif "#%s" in columns:
            cols.append(key.replace("#", ""))
            vals.append(value)

    query = (
        "insert into node (%s) select %s where not exists ("
        "  select nid from node where nid = '%s'"
        ")"
    ) % (
        ", ".join(cols),
        ", ".join(["%s" for x in vals]),
        _nid,
    )
    cur.execute(query, vals)
    conn.commit()

# 4. Add links etc.
for node in nodes:
    for _nid, key, value in node:
        if "field_language" == key:
            for tid in value:
                tid = tid["target_id"]
                try:
                    cur.execute(lang_ins, [_nid, tid, _nid, tid])
                except psycopg2.IntegrityError, ie:
                    print fname, _nid, key, value
                    raise ie
        elif key in TAG_FIELDS:
            for tid in value:
                tid = tid["tid"]
                cur.execute(tags_ins, [_nid, tid, _nid, tid])
        elif key in SAFE_TEXT_FIELDS:
            for tid in value:
                saf = tid.get("safe_value", None)
                val = tid["value"]
                fmt = tid["format"]
                cur.execute(safe1_ins, [_nid, key, saf, val, fmt, _nid, key])
        elif key in ("field_data_url",):
            for tid in value:
                val = tid["value"]
                cur.execute(safe2_ins, [_nid, key, None, val, None, _nid, key])
    conn.commit()

conn.close()
