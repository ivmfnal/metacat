import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname), "r").read()

def get_version():
    g = {}
    exec(open(os.path.join("metacat", "version.py"), "r").read(), g)
    return g["Version"]


setup(
    name = "metacat",
    version = get_version(),
    author = "Igor Mandrichenko",
    author_email = "ivm@fnal.gov",
    description = ("General purpose metadata database"),
    license = "BSD 3-clause",
    keywords = "metadata, data management, database, web service",
    url = "https://github.com/ivmfnal/metacat",
    packages=['metacat', 'metacat.mql', 'metacat.db', 'metacat.util', 'metacat.webapi', 'metacat.ui', 'metacat.filters', 'metacat.auth'],
    #long_description=read('README.rst'),
    install_requires=["pythreader >= 2.6", "pyjwt", "pyyaml", "lark"],
    zip_safe = False,
    classifiers=[
    ],
    entry_points = {
            "console_scripts": [
                "metacat = metacat.ui.metacat_ui:main",
            ]
        }
)