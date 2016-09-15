#!/usr/bin/env python

import requests
import datetime

from lxml import html
from StringIO import StringIO
from json import load, dump

term_url = "http://biii.upf.edu/taxonomy/term/%s"
client = requests.session()

def get_content(client, term):
    page = client.get(term_url % term)
    tree = html.fromstring(page.text)
    html_element = tree.xpath(".//h1")
    try:
        return html_element[0].text.encode("utf-8")
    except IndexError:
        return "FAIL: %s" % term

with open("terms.tsv", "w") as f:
    for node in range(1, 2570):
        print >>f, "%s\t%s" % (node, get_content(client, node))
