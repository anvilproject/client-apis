"""Utility methods."""
import re


def make_identifier(*args):
    """Create legal fhir id."""
    return re.sub(r"[^A-Za-z0-9\-\.]", "-", ".".join(str(a) for a in args))[-64:]


def join(*args):
    """Create pipe separated string."""
    return "|".join(str(a) for a in args)


# note coordinate with StructureDefintions /fhir/config.yaml::canonical
CANONICAL = "http://fhir.ncpi-project-forge.io"
