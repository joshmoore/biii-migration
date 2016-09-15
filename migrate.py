#!/usr/bin/env python

from glob import glob
from json import load
from collections import defaultdict
import psycopg2

#
# Constants
#

PATTERN = "json/*.json"

SAFE_TEXT_FIELDS = (
    "field_url", "field_url_link",
    "field_workflow_author",
    "field_author_s_", "body"
)

TAG_FIELDS = (
    "field_tags",
    "field_ttest",  # Used minimally in workflows
)

TODO = """
 - body has more fields
 - cleanup 'mhmzmgso'
 - field_task_annotation_descriptio = sample data term or url...
"""

#
# SQL strings
#

schema_sql = "select * from information_schema.tables where table_name=%s"

node_sql = "create table node (nid text primary key, %s text)"

term_sql = "create table term (tid text primary key, term text)"

tags_sql = (
    "create table tags (node text references node(nid), "
    "term text references term(tid))"
)

langs_sql = (
    "create table langs (node text references node(nid), "
    "lang text references node(nid))"
)

safe_sql = (
    "create table safe_text (nid text references node(nid), "
    "field text, safe_value text, value text, format text)"
)

node_idx = "create index node_index on node(nid)"

term_idx = "create index term_index on term(tid)"

tags_idx = "create index tag_index on tags(node, term)"

lang_ins = (
    "insert into langs (node, lang) select %s, %s where not exists ("
    "  select node, lang from langs where node = %s and lang = %s"
    ")"
)

safe1_ins = (
    "insert into safe_text (nid, field, safe_value, value, format) "
    "select %s, %s, %s, %s, %s where not exists ("
    "  select nid, field from safe_text where nid = %s and field = %s"
    ")"
)

safe2_ins = (
    "insert into safe_text (nid, field, safe_value, value, format) "
    "select %s, %s, %s, %s, %s where not exists ("
    "  select nid, field from safe_text where nid = %s and field = %s"
    ")"
)

tags_ins = (
    "insert into tags (node, term) select %s, %s where not exists ("
    "  select node, term from tags where node = %s and term = %s"
    ")"
)

#
# Helpers
#


class Node(object):

    def __init__(self, fname):
        self.fname = fname
        with open(fname, "r") as f:
            self.data = load(f)
        self.nid = self.data["nid"]

    def __iter__(self):
        for key, value in self.data.items():
            if not value:
                continue
            if isinstance(value, dict):
                if "und" in value:
                    value = value["und"]
            yield self.nid, key, value


def open_db():
    """
    Open a cursor to the biii database, creating tables once.
    """

    conn = psycopg2.connect("dbname='biii'")
    cur = conn.cursor()
    cur.execute(schema_sql, ('node',))
    if not bool(cur.rowcount):
        cols = " text, ".join([x for x in sorted(columns) if x != "nid"])
        cur.execute(node_sql % cols)
        cur.execute(term_sql)
        cur.execute(tags_sql)
        cur.execute(langs_sql)
        cur.execute(safe_sql)
        with open("terms.tsv", "r") as f:
            cur.copy_from(f, "term")
        cur.execute(node_idx)
        cur.execute(term_idx)
        cur.execute(tags_idx)
        conn.commit()
    return conn, cur


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
