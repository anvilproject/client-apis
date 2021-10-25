"""Utility methods."""
import re
import uuid

from anvil.terra.workspace import Workspace


def make_identifier(*args):
    """Create legal fhir id."""
    return re.sub(r"[^A-Za-z0-9\-\.]", "-", ".".join(str(a) for a in args))[-64:]


def join(*args):
    """Create pipe separated string."""
    return "|".join(str(a) for a in args)


def make_id(workspace_name, resource_id):
    """Create legal fhir id, a reproducible SHA-1 hash of a workspace_name and resource_id."""
    namespace = uuid.UUID(bytes=bytes(bytearray(("xxxxxxxxxxxxxxxx" + workspace_name)[-16:], 'utf-8')))
    return str(uuid.uuid5(namespace, resource_id))


def make_workspace_id(resource):
    """Deduce workspace id."""
    workspace_name = None
    if issubclass(resource.__class__, Workspace):
        workspace_name = resource.attributes.workspace.name
    elif hasattr(resource, 'workspace_name'):
        workspace_name = resource.workspace_name
    assert workspace_name, f"missing workspace_name {resource}"
    return make_identifier(workspace_name)


# note coordinate with StructureDefintions /fhir/config.yaml::canonical
CANONICAL = "https://nih-ncpi.github.io/ncpi-fhir-ig"
