#!/usr/bin/env python

from glob import glob
from json import load
from collections import defaultdict
import psycopg2

#
# Constants
#

PATTERN = "json/*.json"

RATING_FIELDS = (
    "field_rating",
    "field_workflow_rating",
)

SAFE_TEXT_FIELDS = (
    "field_url", "field_url_link",
    "field_workflow_author",
    "field_author_s_", "body",
    "field_platform",
    "field_platforms",
    "field_type",
    # Licensing
    "field_license",
    "field_licenses_costs",
    "field_licensing",
    "field_additional_license",
    # Other
    "field_target_audience",
    "field_ecosystem",
    "field_dimensions_of_the_data",
    "field_input_data_type",
    "field_output_data_type",
    # Rest
    "field_references",
    "field_interoperates_with",
    "field_installation",
    "field_development_tools",
    "field_ways_to_deploy",
    "field_implementations",
    "field_license_costs",
    "field_interfaces_to_other_langua",
    "field_training_topic",
    "field_comments",
    "field_biological_object",
    "field_contributor",
)

TAG_FIELDS = (
    "field_tags",
    "field_ttest",  # Used minimally in workflows
)

TARGET_FIELDS = (
    "field_language",
    "field_package_library_wf",
    "field_package_library",
    "field_workflow",
    "field_sample_data",
)

VALUE_FIELDS = (
    # Note: we assume attributes are empty
    "field_data_url",
    "field_link",
    "field_dimension",
    "field_annotations_url",
    "field_scale",
    "field_filename_or_url",
    "field_size_mb_",
    "field_url_func",
    "field_example_image_url",
)

TODO = """
 - body has more fields
 - cleanup 'mhmzmgso'
 - field_task_annotation_descriptio = sample data term or url...
 - synonyms, esp. around "wf"/"workflow"
"""

#
# SQL strings
#

schema_sql = "select * from information_schema.tables where table_name=%s"

node_sql = "create table node (nid text primary key, %s text)"

biblio_sql = (
    "create table biblio (nid text references node(nid), %s text, "
    "primary key (nid))"
)

term_sql = "create table term (tid text primary key, term text)"

tags_sql = (
    "create table tags (node text references node(nid), "
    "term text references term(tid))"
)

links_sql = (
    "create table links (parent text references node(nid), "
    "child text references node(nid), "
    "type text)"
)

safe_sql = (
    "create table safe_text (nid text references node(nid), "
    "field text, safe_value text, value text, format text, title text)"
)

node_idx = "create index node_index on node(nid)"

term_idx = "create index term_index on term(tid)"

tags_idx = "create index tag_index on tags(node, term)"

link_ins = (
    "insert into links (parent, child, type) select %s, %s, %s "
    "where not exists ("
    "  select parent, child from links "
    "where parent = %s and child = %s and type = %s"
    ")"
)

biblio_ins = (
    "insert into biblio (%s) select %s where not exists ("
    "  select nid from biblio where nid = '%s'"
    ")"
)

node_ins = (
    "insert into node (%s) select %s where not exists ("
    "  select nid from node where nid = '%s'"
    ")"
)

rating_ins = (
    "insert into safe_text (nid, field, value) "
    "select %s, %s, %s where not exists ("
    "  select nid, field from safe_text where nid = %s and field = %s"
    ")"
)

safe1_ins = (
    "insert into safe_text (nid, field, safe_value, value, format, title) "
    "select %s, %s, %s, %s, %s, %s where not exists ("
    "  select nid, field from safe_text where nid = %s and field = %s"
    ")"
)

safe2_ins = (
    "insert into safe_text (nid, field, safe_value, value, format, title) "
    "select %s, %s, %s, %s, %s, %s where not exists ("
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


def open_db(columns=None, biblio=None, cursor_factory=None):
    """
    Open a cursor to the biii database, creating tables once.
    """

    conn = psycopg2.connect("dbname='biii'")
    cur = conn.cursor(cursor_factory=cursor_factory)
    cur.execute(schema_sql, ('node',))
    # If no columns are passed, don't try to initialize
    if columns and not bool(cur.rowcount):
        cols = " text, ".join([x for x in sorted(columns) if x != "nid"])
        bibs = " text, ".join([x for x in sorted(biblio) if x != "nid"])
        cur.execute(node_sql % cols)
        cur.execute(biblio_sql % bibs)
        cur.execute(term_sql)
        cur.execute(tags_sql)
        cur.execute(links_sql)
        cur.execute(safe_sql)
        with open("terms.tsv", "r") as f:
            cur.copy_from(f, "term")
        cur.execute(node_idx)
        cur.execute(term_idx)
        cur.execute(tags_idx)
        conn.commit()
    return conn, cur
