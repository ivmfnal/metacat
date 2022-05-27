import os
from setuptools import setup

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
    description = ("MetaCat is a general purpose metadata database. This package is the client side portion of the product."),
    license = "BSD 3-clause",
    keywords = "metadata, data management, database, web service",
    url = "https://github.com/ivmfnal/metacat",
    packages=['metacat', 'metacat.db', 'metacat.util', 'metacat.webapi', 'metacat.ui', 'metacat.auth', 'metacat.ui.cli'],
    install_requires=["pyjwt", "requests"],
    zip_safe = False,
    classifiers=[
    ],
    entry_points = {
            "console_scripts": [
                "metacat = metacat.ui.metacat_ui:main",
            ]
        }
)