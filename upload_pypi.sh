#!/bin/bash

rm -rf build dist
python setup.py sdist
twine upload dist/*
