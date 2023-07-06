#!/bin/bash

# USAGE: install.sh path_to_this_repo

cd "$1"/python-package
python3 setup.py sdist bdist_wheel || python3 setup.py sdist
