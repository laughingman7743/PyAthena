# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import subprocess
from datetime import datetime, timezone


def get_version():
    # Use git commands to get version from the checked-out ref.
    # sphinx-multiversion checks out each ref to a temp directory and runs Sphinx there.
    # Use current working directory to get the version of the checked-out ref.
    try:
        # Try to get exact tag (for tagged commits)
        result = subprocess.run(
            ["git", "describe", "--tags", "--exact-match"],
            capture_output=True,
            text=True,
            check=True,
        )
        tag = result.stdout.strip()
        if tag and tag.startswith("v"):
            return tag[1:]  # Remove 'v' prefix
        return tag
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    # Try to get version using git describe (for non-tagged commits)
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--always"],
            capture_output=True,
            text=True,
            check=True,
        )
        version_str = result.stdout.strip()
        if version_str and version_str.startswith("v"):
            return version_str[1:]  # Remove 'v' prefix
        return version_str
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    # Fallback to _version.py (for local development builds)
    try:
        from pyathena._version import __version__

        return __version__
    except ImportError:
        pass

    # Final fallback to importlib.metadata
    try:
        from importlib.metadata import version

        return version("PyAthena")
    except Exception:
        return "unknown"


# -- Setup function ----------------------------------------------------------


def config_inited(app, config):
    """Handler for config-inited event to set version dynamically."""
    smv_current_version = getattr(config, "smv_current_version", None)

    if smv_current_version:
        # sphinx-multiversion sets this to the ref name (e.g., "v3.19.0" or "master")
        if smv_current_version.startswith("v") and smv_current_version[1:2].isdigit():
            # It's a version tag like "v3.19.0"
            ver = smv_current_version[1:]  # Remove 'v' prefix
        else:
            # It's a branch name like "master", use git to get version
            ver = get_version()
    else:
        # Not running under sphinx-multiversion, use git
        ver = get_version()

    config.version = f"v{ver}"
    config.release = f"v{ver}"


def setup(app):
    """Sphinx setup hook."""
    app.connect("config-inited", config_inited)


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "PyAthena"
copyright = f"{datetime.now(timezone.utc).year}, laughingman7743"
author = "laughingman7743"
# Version will be set dynamically in setup() function
version = ""
release = ""

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.githubpages",
    "sphinx_multiversion",
]

# Napoleon settings for Google-style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}

# Autosummary settings
autosummary_generate = True
autosummary_imported_members = False

# Intersphinx mapping
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "pyarrow": ("https://arrow.apache.org/docs/", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]
html_css_files = [
    "custom.css",
]

# Furo theme options
html_theme_options = {
    "source_repository": "https://github.com/laughingman7743/PyAthena/",
    "source_branch": "master",
    "source_directory": "docs/",
}

# Sidebar templates
html_sidebars = {
    "**": [
        "sidebar/brand.html",
        "sidebar/search.html",
        "sidebar/scroll-start.html",
        "sidebar/navigation.html",
        "versioning.html",
        "sidebar/scroll-end.html",
    ]
}

# -- Sphinx-multiversion configuration ----------------------------------------

# Whitelist pattern for tags (semantic versioning: vX.Y.Z)
smv_tag_whitelist = r"^v\d+\.\d+\.\d+$"  # Match vX.Y.Z tags

# Whitelist pattern for branches
smv_branch_whitelist = r"^master$"  # Only build master branch

# Whitelist pattern for remotes
smv_remote_whitelist = r"^origin$"  # Only build from origin remote

# Output all versions to the root directory
smv_outputdir_format = "{ref.name}"

# Specify the latest version (used for stable redirect)
smv_latest_version = "master"
