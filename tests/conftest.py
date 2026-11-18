import pytest

from millpond import metrics

# Initialize metrics with a test pipeline label so tests that don't mock metrics
# can call .labels() without hitting "Incorrect label names" errors.
metrics.init("test-topic-test-table")


# (module, attribute, empty value) for every module-global that exists to make
# a warning fire ONCE per pod lifetime. They are process state, so a test that
# asserts "warned exactly once" passes or fails on what ran before it — and
# with `-p randomly` active that is a different answer every run. Clearing them
# around each test makes those assertions mean what they say.
_WARN_STATE = (
    ("millpond.main", "_sort_missing_fields_warned", set()),
    ("millpond.main", "_sort_unsortable_warned", set()),
    ("millpond.main", "_uuid_filter_values_warned", set()),
    ("millpond.main", "_uuid_filter_values_memo", None),
    ("millpond.hoglake", "_uuid_rewrite_warned", set()),
    ("millpond.ducklake", "_uuid_realign_warned", set()),
)


@pytest.fixture(autouse=True)
def _reset_warn_once_state():
    """Reset every warn-once / memo global around each test.

    Autouse and unconditional: a suite where one test's leftover state decides
    whether another test catches a bug is a suite that reports a different
    result per ordering. Imports are inside the fixture so a module a given
    test session never touches is never imported for this.
    """
    import importlib

    def apply():
        for module_name, attribute, empty in _WARN_STATE:
            module = importlib.import_module(module_name)
            current = getattr(module, attribute)
            if isinstance(current, set):
                current.clear()
            else:
                setattr(module, attribute, empty)

    apply()
    yield
    apply()
