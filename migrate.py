#!/usr/bin/env python

from glob import glob
from json import load, dump
from collections import defaultdict
import psycopg2

def parse(fname):
    with open(fname, "r") as f:
        data = load(f)
        _nid = data["nid"]
        for key, value in data.items():
            if not value: continue
            if isinstance(value, dict):
                if "und" in value:
                    value = value["und"]
            yield _nid, key, value

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

columns = set()
counts = defaultdict(int)
types = defaultdict(set)
total = len(glob(PATTERN))

handled = set()
printed = set()


for fname in glob(PATTERN):
    for _nid, key, value in parse(fname):
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

conn = psycopg2.connect("dbname='biii'")
cur = conn.cursor()
cur.execute("select * from information_schema.tables where table_name=%s", ('node',))
if not bool(cur.rowcount):
    cols = " text, ".join([x for x in sorted(columns) if x != "nid"])
    cur.execute("create table node (nid text primary key, %s text)" % cols)
    cur.execute("create table term (tid text primary key, term text)")
    cur.execute("create table tags (node text references node(nid), term text references term(tid))")
    cur.execute("create table langs (node text references node(nid), lang text references node(nid))")
    cur.execute("create table safe_text (nid text references node(nid), field text, safe_value text, value text, format text)")
    with open("terms.tsv", "r") as f:
        cur.copy_from(f, "term")
    cur.execute("create index node_index on node(nid)")
    cur.execute("create index term_index on term(tid)")
    cur.execute("create index tag_index on tags(node, term)")
    conn.commit()

for fname in glob(PATTERN):
    cols = list()
    vals = list()
    for _nid, key, value in parse(fname):
        if not value: continue
        if key in columns:
            cols.append(key)
            vals.append(value)
        elif "#%s" in columns:
            cols.append(key.replace("#", ""))
            vals.append(value)
        elif "field_language" == key:
            for tid in value:
                tid = tid["target_id"]
                query = (
                    "insert into langs (node, lang) select %s, %s where not exists ("
                    "  select node, lang from langs where node = %s and lang = %s"
                    ")"
                )
                try:
                    cur.execute(query, [_nid, tid, _nid, tid])
                    conn.commit()
                except psycopg2.IntegrityError, ie:
                    print fname, _nid, key, value
                    raise ie
        elif key in TAG_FIELDS:
            for tid in value:
                tid = tid["tid"]
                query = (
                    "insert into tags (node, term) select %s, %s where not exists ("
                    "  select node, term from tags where node = %s and term = %s"
                    ")"
                )
                cur.execute(query, [_nid, tid, _nid, tid])
                conn.commit()
        elif key in SAFE_TEXT_FIELDS:
            if key == "field_ttest":
                import pdb
                pdb.set_trace()
            for tid in value:
                saf = tid.get("safe_value", None)
                val = tid["value"]
                fmt = tid["format"]
                query = (
                    "insert into safe_text (nid, field, safe_value, value, format) "
                    "select %s, %s, %s, %s, %s where not exists ("
                    "  select nid, field from safe_text where nid = %s and field = %s"
                    ")"
                )
                cur.execute(query, [_nid, key, saf, val, fmt, _nid, key])
                conn.commit()
        elif key in ("field_data_url",):
            for tid in value:
                val = tid["value"]
                query = (
                    "insert into safe_text (nid, field, safe_value, value, format) "
                    "select %s, %s, %s, %s, %s where not exists ("
                    "  select nid, field from safe_text where nid = %s and field = %s"
                    ")"
                )
                cur.execute(query, [_nid, key, None, val, None, _nid, key])
                conn.commit()

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
