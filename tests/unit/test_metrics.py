"""`millpond/metrics.py` definitions.

The call sites are tested with the module mocked out, which is the right
way to test them and the reason a bucket ladder or a label set can be
edited without a single test noticing. These assert the shapes a
dashboard, an alert and a recording rule bind to — the names, the `le`
boundaries, the common labels, and that `init()` actually bound the
public name a flush reaches for.
"""

import math

import pytest

from millpond import metrics

# The fanout pair. `_flush_size_records` rides along as the control: the
# label set is asserted against it rather than against a literal, so a
# future common label (a third dimension on every series) moves all three
# together or fails here.
_FANOUT = (metrics._flush_files, metrics._flush_file_rows)


def _buckets(metric) -> list[float]:
    """The histogram's `le` boundaries, read off the exposition rather
    than off `_upper_bounds`: `le` is the label a PromQL query names, and
    the point of asserting a ladder is that the ladder a query can see is
    the intended one.

    De-duplicated across children, because every `init()` any test in the
    session ran left a child of its own on the raw metric and they all
    carry the same boundaries.
    """
    return sorted(
        {
            float(sample.labels["le"])
            for family in metric.collect()
            for sample in family.samples
            if sample.name.endswith("_bucket")
        }
    )


def _label_names(metric) -> set[str]:
    return {name for family in metric.collect() for sample in family.samples for name in sample.labels if name != "le"}


def test_flush_files_resolves_every_doubling_to_2048():
    # Powers of two because the question is an order of magnitude of
    # fanout — ten objects a flush or a thousand — and a flush that
    # doubles its object count is the event worth seeing. 2048 is the
    # ceiling because past it the only answer that matters is "too many",
    # which `+Inf` gives.
    assert _buckets(metrics._flush_files) == [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, math.inf]


def test_flush_file_rows_resolves_the_tens_of_rows_population():
    # A late-arriving tail produces objects holding dozens of rows beside
    # one holding the whole current partition. A ladder starting at 100
    # (as `flush_size_records` does, where it is right) would put every
    # one of the small objects in the same bucket as the large one and
    # answer "all your objects are small-ish" to every question.
    assert _buckets(metrics._flush_file_rows) == [
        1,
        5,
        10,
        25,
        50,
        100,
        500,
        1000,
        5000,
        10000,
        50000,
        100000,
        500000,
        1000000,
        math.inf,
    ]


@pytest.mark.parametrize("metric", _FANOUT)
def test_the_fanout_histograms_carry_the_flush_family_labels(metric):
    assert _label_names(metric) == _label_names(metrics._flush_size_records)


@pytest.mark.parametrize(
    ("metric", "name"),
    [(metrics._flush_files, "millpond_flush_files"), (metrics._flush_file_rows, "millpond_flush_file_rows")],
)
def test_the_fanout_histogram_names(metric, name):
    assert [family.name for family in metric.collect()] == [name]


@pytest.mark.parametrize(("public", "raw"), [("flush_files", "_flush_files"), ("flush_file_rows", "_flush_file_rows")])
def test_init_bound_the_public_name(public, raw):
    # `init()` replaces each public name with a label-bound child so call
    # sites can `.observe()` without naming pipeline/broker_source. A
    # metric added to the module and forgotten in `init()` keeps the RAW
    # histogram as its public name and raises "missing label values" on
    # the first flush that touches it — in production, from inside the
    # write-retry loop. tests/conftest.py ran `init()` at import.
    bound = getattr(metrics, public)
    assert bound is not getattr(metrics, raw)
    bound.observe(1)  # would raise ValueError on the raw metric


# The hoglake phase pair: the two remote phases of one flush, so main.py's
# `flush_duration_seconds` decomposes. Semantics on the metrics themselves.
_PHASES = (metrics._hoglake_prepare_seconds, metrics._hoglake_commit_seconds)


def test_prepare_seconds_resolves_the_whales_flush():
    # Chosen for the whales flush (2 GiB, ~155k rows, 4-10 s of encode
    # plus the uploads): the 10-60 s span is where its quantiles live
    # and gets 15 and 45 beside the 1-2-5 steps; the top is the 480 s
    # liveness deadline so a fatal attempt is visible as one rather than
    # lost in +Inf; nothing under 0.1 s uploads anything.
    assert _buckets(metrics._hoglake_prepare_seconds) == [
        0.1,
        0.25,
        0.5,
        1,
        2.5,
        5,
        10,
        15,
        20,
        30,
        45,
        60,
        120,
        240,
        480,
        math.inf,
    ]


def test_commit_seconds_resolves_the_lock_round_trip():
    # A deferred-stats commit is a lock acquisition and a file-list
    # insert; an inline-stats one on a wide flush is tens of seconds
    # under the lock. The ladder has to tell 50 ms from 250 ms at the
    # bottom and stop at the write timeout rather than the liveness
    # deadline, since past 60 s the transport has the floor.
    assert _buckets(metrics._hoglake_commit_seconds) == [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, math.inf]


def test_the_prepare_histogram_carries_the_flush_family_labels_plus_result():
    # `result` is the one label of its own: `ok` returned, `failed`
    # raised. Both outcomes, so the series covers the same attempts
    # `variant_encode_seconds` does.
    assert _label_names(metrics._hoglake_prepare_seconds) == _label_names(metrics._flush_duration_seconds) | {"result"}


def test_the_commit_histogram_carries_the_flush_family_labels():
    assert _label_names(metrics._hoglake_commit_seconds) == _label_names(metrics._flush_duration_seconds)


@pytest.mark.parametrize(
    ("metric", "name"),
    [
        (metrics._hoglake_prepare_seconds, "millpond_hoglake_prepare_seconds"),
        (metrics._hoglake_commit_seconds, "millpond_hoglake_commit_seconds"),
    ],
)
def test_the_phase_histogram_names(metric, name):
    assert [family.name for family in metric.collect()] == [name]


def test_init_bound_the_phase_histograms():
    # Same trap as the fanout pair: a histogram added to the module and
    # forgotten in `init()` keeps the raw metric as its public name and
    # raises "missing label values" on the first flush, in production.
    # The prepare is label-wrapped (`result` is the call site's), the
    # commit pre-bound.
    assert metrics.hoglake_prepare_seconds is not metrics._hoglake_prepare_seconds
    metrics.hoglake_prepare_seconds.labels(result="ok").observe(1)
    assert metrics.hoglake_commit_seconds is not metrics._hoglake_commit_seconds
    metrics.hoglake_commit_seconds.observe(1)
