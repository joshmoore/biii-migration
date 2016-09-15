Steps:
 * create DB: biii
 * run ./terms.py to terms.tsv
 * biii=# create table term (tid text primary key, term text);
 * biii=# create table tags (node text references node(vid), term text references term(tid));
 * biii=# create table safe_text (vid text references node(vid), field text, safe_value text, value text, format text);
 * $ psql biii -c "copy term from stdin" < terms.tsv
 COPY 2568
 * downloaded with nodes.py
 * converted with find / jq '.[0]' for size empty
 * parsed with migrate.py

biii=# create index node_index on node(vid);
biii=# create index term_index on term(tid);
biii=# create index tag_index on tags(node, term);
