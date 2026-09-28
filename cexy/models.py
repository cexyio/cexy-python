"""Typed API models, generated from spec/openapi.sdk.json (see cexy/_generated/)."""

from cexy._generated import models as _models
from cexy._generated.models import *  # noqa: F403

__all__ = sorted(
    [k for k, v in vars(_models).items() if isinstance(v, type) and v.__module__ == _models.__name__]
    + ["LedgerReference"]  # a typing alias (tolerant union), not a class
    + [k for k in vars(_models) if k.endswith("Id") and vars(_models)[k] is str]  # id aliases
)
