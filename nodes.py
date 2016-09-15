#!/usr/bin/env python

import requests
import datetime

from lxml import html
from StringIO import StringIO
from json import load, dump

import os

username = os.environ.get("BIII_USERNAME", "username")
password = os.environ.get("BIII_PASSWORD", "password")

login_url = "http://biii.upf.edu/user/"
export_url = "http://biii.upf.edu/node/%s/node_export/json"
client = requests.session()
client.get(login_url)
data = {
  'name': username,
  'pass': password,
  'form_id':"user_login",
  'op': 'Log in'
}
client.post(login_url, data=data, headers=dict(Referer=login_url))

def get_content(client, node):
    try:
        page = client.get(export_url % node)
    except requests.exceptions.ConnectionError, ce:
        return str(ce)
    tree = html.fromstring(page.text)
    html_element = tree.xpath(".//textarea[@id='edit-export']")
    with open("out_%s.json" % node, "w") as f:
        if not html_element:
            return "empty"
        #f.write(str(load(StringIO(html_element[0].text))))
        f.write(html_element[0].text)
    return "ok"

for node in range(2647, 3000):
    print datetime.datetime.now().time().isoformat(),
    print node,
    print get_content(client, node)
