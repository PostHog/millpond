"""Sources for the keep-filter's include-value set.

`build(cfg)` returns ONE source object with `current()/start()/stop()`:

- static (no URL configured): today's behavior — the set parsed from
  MILLPOND_FILTER_VALUES at startup, immutable for the process lifetime.
- shadow (URL + mode=shadow): `current()` still serves the static set;
  a PRIVATE background prober polls the URL and exports diff-vs-static
  gauges so the authority flip can be gated on the symmetric difference
  holding at zero. The prober's set is deliberately not exposed —
  reading it as authority in shadow mode should require reaching into
  private attributes, not a one-line mistake.
- authoritative (URL + mode=authoritative): `current()` serves the
  polled set, seeded from the static values; startup BLOCKS until the
  first successful poll (see `start()` — a halt is recoverable from
  Kafka, a silent drop is not).

The consequence asymmetry drives every safety choice: an erroneous
ADDITION writes surplus rows a downstream reader ignores; an erroneous
REMOVAL silently drops records on the floor with no recovery. Hence:

- additions apply on the first successful poll that shows them;
- removals require `removal_confirm_polls` CONSECUTIVE successful polls
  with the value absent; failed polls advance nothing;
- a REFUSED poll (below) advances nothing either — refusal must not
  pre-charge the removal countdown;
- a poll failure keeps the last-known-good set, with staleness
  observable via `millpond_include_values_last_success_timestamp_seconds`;
- a successful poll is REFUSED (set kept, `millpond_include_values_
  refused_total{reason}`) when it would (a) empty a non-empty set,
  (b) seed an EMPTY initial set, (c) confirm removal of more than half
  of a multi-value set at once, or (d) flip the set's scalar type
  (int↔str) — a type flip would silently exclude every record at the
  filter's cast site, which is a mass drop wearing a different hat;
- the damping state and last-known-good set are IN-MEMORY: a restart
  falls back to the static bootstrap, which is why authoritative mode
  refuses to start unsynced rather than running on a possibly-stale
  bootstrap.

Nothing in this module knows what the endpoint is or what the values
mean — the URL and auth header are plain config.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
import urllib.request

from millpond import metrics

log = logging.getLogger(__name__)

Values = tuple[int, ...] | tuple[str, ...]

# Cap on the endpoint response body. The largest plausible legitimate
# payload (thousands of scalar values) is a few hundred KB; anything
# bigger is a misconfigured URL, and an unbounded read() is an OOM.
_MAX_RESPONSE_BYTES = 4 * 1024 * 1024

_LOG_CHANGES_INDIVIDUALLY_MAX = 10


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: urllib re-sends custom headers (our auth token)
    to the redirect target, including cross-host. For a component that
    takes an arbitrary URL plus a secret header, a 3xx is an error."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect())


def _normalize(elements: list) -> Values:
    """Normalize a JSON array into the homogeneous, deduplicated tuple
    shape the filter expects — same rule as config._parse_filter_values:
    an all-int array becomes tuple[int, ...], anything else becomes
    tuple[str, ...]. (bool is an int subclass in Python; a JSON `true`
    must not silently become 1, so bools force the string path.)"""
    if any(isinstance(e, (dict, list)) for e in elements):
        raise ValueError("include-values array must contain only scalars")
    if all(isinstance(e, int) and not isinstance(e, bool) for e in elements):
        return tuple(sorted(set(elements)))
    return tuple(sorted({str(e) for e in elements}))


def _values_type(values) -> type | None:
    return type(values[0]) if values else None


class StaticIncludeValues:
    """The startup-parsed static set. `current()` never changes."""

    mode = "static"

    def __init__(self, values: Values | None):
        self._values = values

    def current(self) -> Values | None:
        return self._values

    def start(self) -> None:  # uniform lifecycle across sources
        pass

    def stop(self) -> None:
        pass


class HttpIncludeValues:
    """Polls `url` for a JSON array of include values on a background
    thread and maintains the current set under the refusal / damping /
    keep-last-on-error semantics in the module docstring.

    `current()` is safe from any thread: the set is an immutable tuple
    swapped by plain attribute assignment; all other state is touched
    only by the poll thread.
    """

    mode = "authoritative"

    def __init__(
        self,
        *,
        url: str,
        poll_interval_s: float,
        removal_confirm_polls: int,
        auth_header: tuple[str, str] | None = None,
        bootstrap: Values | None = None,
        shadow_reference: Values | None = None,
        request_timeout_s: float = 10.0,
        startup_timeout_s: float = 60.0,
    ):
        self._url = url
        self._poll_interval_s = poll_interval_s
        self._removal_confirm_polls = removal_confirm_polls
        self._auth_header = auth_header
        self._shadow_reference = shadow_reference
        self._request_timeout_s = request_timeout_s
        self._startup_timeout_s = startup_timeout_s

        self._values: Values | None = bootstrap
        # value -> consecutive successful-AND-accepted polls it has been
        # absent from. Failed polls and refused polls advance nothing.
        self._absent_polls: dict[int | str, int] = {}
        self._synced = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def current(self) -> Values | None:
        return self._values

    def start(self) -> None:
        """Start the poll thread and BLOCK until the first successful
        poll, raising after `startup_timeout_s`.

        No proceed-on-bootstrap escape hatch, deliberately: the damping
        state is in-memory, so a restart during an endpoint outage that
        silently ran on the (possibly stale) bootstrap would perform
        de-facto removals with zero polls of evidence. Refusing to start
        halts ingestion — recoverable, alertable, and the data waits in
        Kafka — where running on a stale set silently drops records.
        """
        self._thread = threading.Thread(target=self._run, name="include-values-poll", daemon=True)
        self._thread.start()
        if not self._synced.wait(self._startup_timeout_s):
            self.stop()
            raise RuntimeError(
                f"include-values source got no successful poll of {self._url} within {self._startup_timeout_s}s"
            )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    # -- poll loop ---------------------------------------------------------

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception as e:
                metrics.include_values_poll_failures_total.inc()
                log.warning("include-values poll failed (keeping last set): %s", e)
            # ±10% jitter so a fleet of replicas deployed in phase doesn't
            # poll the endpoint in lockstep.
            self._stop.wait(self._poll_interval_s * random.uniform(0.9, 1.1))

    def _fetch(self) -> list:
        req = urllib.request.Request(self._url)
        if self._auth_header is not None:
            req.add_header(*self._auth_header)
        with _OPENER.open(req, timeout=self._request_timeout_s) as resp:
            body = resp.read(_MAX_RESPONSE_BYTES + 1)
        if len(body) > _MAX_RESPONSE_BYTES:
            raise ValueError(f"include-values response exceeds {_MAX_RESPONSE_BYTES} bytes")
        parsed = json.loads(body)
        if not isinstance(parsed, list):
            raise ValueError(f"include-values endpoint returned {type(parsed).__name__}, expected a JSON array")
        return parsed

    def _refuse(self, reason: str, detail: str) -> None:
        metrics.include_values_refused_total.labels(reason=reason).inc()
        log.error("include-values poll refused (%s): %s — keeping current set", reason, detail)

    def _poll_once(self) -> None:
        remote = _normalize(self._fetch())
        remote_set = set(remote)

        current = self._values if self._values is not None else ()
        current_set = set(current)

        # Type-flip refusal. If the endpoint's scalar type disagrees with
        # the held set's (ints vs strings), unioning them would produce a
        # mixed set and — worse — a full flip makes the filter's value-
        # array cast fail against the column and drop ENTIRE batches as
        # filter_field_missing. A type change is an endpoint bug or a
        # coordinated migration; either way it goes through static config,
        # not an unattended poll. (First sight with no held set accepts
        # whatever type the endpoint serves.)
        held_type = _values_type(current)
        remote_type = _values_type(remote)
        if held_type is not None and remote_type is not None and held_type is not remote_type:
            self._refuse(
                "type_flip",
                f"held set is {held_type.__name__}, endpoint served {remote_type.__name__}",
            )
            self._record_success_metrics(remote_set)
            return

        # Empty-remote refusal, unconditional: an empty array is never
        # removal evidence and never an acceptable first state. Against a
        # held set it must not even ADVANCE the absence countdown (a run
        # of empty responses is an endpoint bug, and letting it pre-charge
        # the counters would let one later junk value mass-remove); with
        # no held set, accepting () would arm the filter with an empty
        # include set and drop every record. Legitimately shrinking to
        # zero values is an operator action through static config.
        if not remote_set:
            if current_set or self._values is None:
                self._refuse("empty", "endpoint served an empty array")
                if current_set:
                    self._record_success_metrics(remote_set)
                return

        additions = remote_set - current_set

        # Damped removals — computed TENTATIVELY. The counters are only
        # committed if this poll is accepted; a refused poll must not
        # pre-charge the countdown (a run of refused-empty polls followed
        # by one junk value would otherwise mass-remove instantly).
        tentative_counts: dict[int | str, int] = {}
        confirmed_removals: set = set()
        for v in current_set - remote_set:
            tentative_counts[v] = self._absent_polls.get(v, 0) + 1
            if tentative_counts[v] >= self._removal_confirm_polls:
                confirmed_removals.add(v)

        candidate = (current_set | additions) - confirmed_removals

        if not candidate and current_set:
            self._refuse("empty", f"result would empty a non-empty set ({len(current_set)} values)")
            self._record_success_metrics(remote_set)
            return
        # Bulk-removal refusal: confirming removal of more than half of a
        # multi-value set in one poll is a fleet-wide config wipe, not
        # routine churn. (Removing 1 of 2 is allowed; the guard is against
        # the endpoint replacing the world.)
        if len(current_set) > 1 and len(confirmed_removals) > max(1, len(current_set) // 2):
            self._refuse(
                "bulk_removal",
                f"would remove {len(confirmed_removals)} of {len(current_set)} values in one poll",
            )
            self._record_success_metrics(remote_set)
            return

        # Accepted: commit the countdown state and the new set.
        self._absent_polls = {v: n for v, n in tentative_counts.items() if v not in confirmed_removals}

        if candidate != current_set or self._values is None:
            new_values = _normalize(list(candidate))
            self._log_changes(additions, confirmed_removals)
            for _ in additions:
                metrics.include_values_changes_total.labels(action="add").inc()
            for _ in confirmed_removals:
                metrics.include_values_changes_total.labels(action="remove").inc()
            self._values = new_values

        self._record_success_metrics(remote_set)
        self._synced.set()

    def _log_changes(self, additions: set, removals: set) -> None:
        # Individually at WARNING while small (rare, operator-relevant);
        # summarized when large (e.g. first sync against a full endpoint)
        # so one poll can't flood the log.
        if len(additions) <= _LOG_CHANGES_INDIVIDUALLY_MAX:
            for v in sorted(additions, key=str):
                log.warning("include-values: adding %r", v)
        elif additions:
            log.warning(
                "include-values: adding %d values (sample: %s)",
                len(additions),
                sorted(additions, key=str)[:_LOG_CHANGES_INDIVIDUALLY_MAX],
            )
        for v in sorted(removals, key=str):
            log.warning(
                "include-values: removing %r (absent %d consecutive polls)",
                v,
                self._removal_confirm_polls,
            )

    def _record_success_metrics(self, remote_set: set) -> None:
        metrics.include_values_size.set(len(self._values or ()))
        metrics.include_values_pending_removals.set(len(self._absent_polls))
        metrics.include_values_last_success_timestamp_seconds.set(time.time())
        if self._shadow_reference is not None:
            ref = set(self._shadow_reference)
            metrics.include_values_shadow_only_static.set(len(ref - remote_set))
            metrics.include_values_shadow_only_remote.set(len(remote_set - ref))


class ShadowIncludeValues:
    """Shadow mode: `current()` serves the STATIC set; the HTTP prober is
    private and exists only for its diff/staleness metrics. Its polled
    set is intentionally unreachable through this object's public
    surface."""

    mode = "shadow"

    def __init__(self, static_values: Values | None, prober: HttpIncludeValues):
        self._values = static_values
        self._prober = prober

    def current(self) -> Values | None:
        return self._values

    def start(self) -> None:
        # The prober's sync is not load-bearing in shadow mode — the
        # authority is static — so start the thread without blocking.
        self._prober._thread = threading.Thread(target=self._prober._run, name="include-values-poll", daemon=True)
        self._prober._thread.start()

    def stop(self) -> None:
        self._prober.stop()


def build(cfg) -> StaticIncludeValues | ShadowIncludeValues | HttpIncludeValues:
    """Build the include-values source from config. Exactly one object
    comes back; the caller starts/stops it around the consume loop and
    reads `current()` per batch. `.mode` is exported as
    `millpond_include_values_mode` so fleet-level queries (the shadow→
    authoritative flip gate, staleness alerts) can tell which replicas
    are actually running which source instead of passing vacuously on
    replicas that never got the URL.
    """
    if cfg.include_values_url is None:
        return StaticIncludeValues(cfg.filter_values)

    auth = None
    if cfg.include_values_auth_header_name is not None:
        auth = (cfg.include_values_auth_header_name, cfg.include_values_auth_token)

    http = HttpIncludeValues(
        url=cfg.include_values_url,
        poll_interval_s=cfg.include_values_poll_interval_s,
        removal_confirm_polls=cfg.include_values_removal_polls,
        auth_header=auth,
        bootstrap=cfg.filter_values,
        shadow_reference=cfg.filter_values if cfg.include_values_mode == "shadow" else None,
        request_timeout_s=cfg.include_values_request_timeout_s,
        startup_timeout_s=cfg.include_values_startup_timeout_s,
    )
    if cfg.include_values_mode == "shadow":
        return ShadowIncludeValues(cfg.filter_values, http)
    return http
