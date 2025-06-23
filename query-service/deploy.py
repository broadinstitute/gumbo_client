#!/usr/bin/env python

import shutil
import subprocess
import json
import os.path
import urllib.parse


def generate_requirements_file():
    print("Generating requirements.txt based on poetry environment...")
    requirements = subprocess.run(
        [
            "poetry",
            "export",
            "-f",
            "requirements.txt",
            "--without-hashes",
            "--only",
            "main",
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout

    with open("requirements.txt", "wt") as fd:
        fd.write(requirements)


def deploy():
    config_secrets = json.load(
        open(os.path.expanduser("~/.config/gumbo-read-only/config.json"))
    )

    with open("app.yaml.template", "rt") as fd:
        template = fd.read()

    app_yaml = template.replace("$USER", config_secrets["user"]).replace(
        "$PASSWORD", urllib.parse.quote(config_secrets["password"], safe="")
    )
    with open("app.yaml", "wt") as fd:
        fd.write(app_yaml)

    generate_requirements_file()

    subprocess.check_call("gcloud app deploy --project depmap-gumbo", shell=True)


if __name__ == "__main__":
    deploy()
