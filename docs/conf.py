# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
from datetime import datetime, timezone
from pathlib import Path


def get_version():
    version_file = Path(".").absolute().parent / "pyathena" / "__init__.py"
    with version_file.open() as f:
        for line in f:
            if line.startswith("__version__"):
                return line.strip().split('"')[1]


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "PyAthena"
copyright = f"{datetime.now(timezone.utc).year}, laughingman7743"
author = "laughingman7743"
version = f"v{get_version()}"
release = f"v{get_version()}"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.githubpages"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
