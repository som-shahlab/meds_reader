import os
import shutil
import sys

# Configuration file for the Sphinx documentation builder.

# -- Project information

project = "meds_reader"
copyright = "2024, Ethan Steinberg"
author = "Ethan Steinberg"

release = "0.1"
version = "0.1.0"

# -- General configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "myst_parser",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
}

# -- Options for HTML output

html_theme = "furo"

html_theme_options = {
    # Disable showing the sidebar. Defaults to 'false'
    "nosidebar": True,
}

autodoc_member_order = "bysource"
autoclass_content = "init"
autodoc_typehints_format = "fully-qualified"

shutil.copyfile("../../src/meds_reader/__init__.pyi", "meds_reader.py")
sys.path.insert(0, os.path.abspath("."))

skipping = False

with open("README_copy.md", "w") as dest:
    with open("../../README.md", "r") as source:
        for line in source.readlines():
            if line == "<!-- Start OnlyGithub -->\n":
                skipping = True

            if not skipping:
                dest.write(line)

            if line == "<!-- End OnlyGithub -->\n":
                skipping = False
