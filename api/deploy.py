#!/usr/bin/env python

import shutil
import subprocess
import json
import os.path
import urllib.parse

config_secrets = json.load(open(os.path.expanduser("~/.config/gumbo-read-only/config.json")))

with open("app.yaml.template", "rt") as fd:
    template = fd.read()
app_yaml = template.replace("$USER", config_secrets["user"]).replace("$PASSWORD", urllib.parse.quote(config_secrets["password"], safe=''))
with open("app.yaml", "wt") as fd:
    fd.write(app_yaml)

shutil.copytree("../client/gumbo_client","gumbo_client",dirs_exist_ok=True)
subprocess.check_call("gcloud app deploy --project depmap-gumbo", shell=True)
