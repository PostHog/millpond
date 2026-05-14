"""PyIceberg version-pin canary.

millpond/iceberg.py depends on two private PyIceberg symbols to convert a
PyArrow schema into an Iceberg ``Schema`` with sequential field IDs:

  * ``pyiceberg.io.pyarrow._pyarrow_to_schema_without_ids``
  * ``pyiceberg.schema.assign_fresh_schema_ids``

PyIceberg is pre-1.0 and rearranges internals between minor versions. This
test imports both symbols and asserts the installed PyIceberg version is
the one this module was last verified against. When the import fails or
the version moves, treat that as a signal to revisit ``iceberg.py`` —
either the private API has changed, or it's time to rewrite the schema
conversion to use only public surface.
"""

from __future__ import annotations

from importlib.metadata import version

import pyiceberg

# Update this constant when revalidating against a new PyIceberg release.
_VERIFIED_AGAINST = "0.11.1"


def test_pyiceberg_version_is_pinned():
    installed = version("pyiceberg")
    assert installed == _VERIFIED_AGAINST, (
        f"PyIceberg {installed} installed, but millpond/iceberg.py was last "
        f"verified against {_VERIFIED_AGAINST}. Review the private-API imports "
        f"in millpond/iceberg.py before bumping this constant."
    )
    assert pyiceberg.__version__ == _VERIFIED_AGAINST


def test_private_symbols_still_importable():
    # If either import raises, the private path moved and the module
    # body in millpond/iceberg.py won't load — fail loud now instead of
    # at startup in production.
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    from pyiceberg.schema import assign_fresh_schema_ids

    assert callable(_pyarrow_to_schema_without_ids)
    assert callable(assign_fresh_schema_ids)
