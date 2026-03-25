"""Produces PostHog-like events to Kafka with realistic schema variability.

Event types have different property sets, sparse fields, nested objects,
arrays, high-cardinality strings, and occasional type inconsistencies --
all the things that stress schema inference and evolution.

Faithfully models:
- Real PostHog event types ($pageview, $pageleave, $autocapture, $identify,
  $groupidentify, $feature_flag_called, $exception, custom events)
- Realistic elements_chain format for $autocapture
- Power-law distinct_id distribution (some users generate many events)
- Session grouping ($session_id / $window_id on most events)
- Group analytics ($groups on ~20% of events)
- team_id / project_id top-level fields
- Timestamp jitter (not all events at exactly now())
- Property edge cases: dots, spaces, unicode, long names
"""

import json
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
PARTITION_COUNT = int(os.environ["KAFKA_PARTITION_COUNT"])
RECORDS_PER_SECOND = int(os.environ.get("RECORDS_PER_SECOND", "1000"))
TOTAL_RECORDS = int(os.environ.get("TOTAL_RECORDS", "10000"))

# ---------------------------------------------------------------------------
# Pre-computed pools (allocated once, sampled repeatedly for speed)
# ---------------------------------------------------------------------------

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

PATHNAMES = [
    "/dashboard", "/insights/new", "/insights", "/events", "/persons",
    "/feature_flags", "/sessions", "/signup", "/login", "/settings",
    "/data-management/events", "/data-management/properties",
    "/web-analytics", "/experiments", "/surveys", "/notebooks",
    "/project/settings", "/toolbar", "/apps", "/batch-exports",
]

URLS = [f"https://app.posthog.com/project/1{p}" for p in PATHNAMES]

REFERRERS = [
    "https://www.google.com/",
    "https://twitter.com/",
    "https://news.ycombinator.com/",
    "https://github.com/PostHog/posthog",
    "https://www.linkedin.com/",
    "https://duckduckgo.com/",
    "",   # direct
    None, # unknown
]

FEATURE_FLAGS = [
    "new-dashboard-layout", "session-replay-v2", "billing-redesign",
    "query-performance", "data-pipelines", "hog-ql", "web-analytics",
    "notebook-mode", "error-tracking", "surveys-v2", "early-access-feature",
    "person-on-events-v2", "hogql-insights", "data-warehouse",
]

OS_LIST = ["Mac OS X", "Windows", "Linux", "iOS", "Android"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge", "Mobile Safari", "Chrome Mobile"]
BROWSER_VERSIONS = [str(v) for v in range(95, 126)]
DEVICE_TYPES = ["Desktop", "Mobile", "Tablet"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA", "AU", "NL", "SE", "KR", "SG"]
CITIES = [
    "San Francisco", "London", "Berlin", "Paris", "Tokyo", "Sao Paulo",
    "Mumbai", "Toronto", "Sydney", "Amsterdam", "Stockholm", "Seoul", "Singapore",
]
TIMEZONES = [
    "America/Los_Angeles", "America/New_York", "Europe/London",
    "Europe/Berlin", "Asia/Tokyo", "America/Sao_Paulo",
]
UTM_SOURCES = ["google", "twitter", "linkedin", "newsletter", "producthunt", "hackernews", None]
UTM_MEDIUMS = ["cpc", "social", "email", "organic", None]
UTM_CAMPAIGNS = ["launch-2024", "winter-promo", "devrel-outreach", "retarget-trial", None]

LIB_NAMES = ["web", "posthog-js", "posthog-python", "posthog-node", "posthog-go", "posthog-ruby"]
LIB_VERSIONS = [f"1.{maj}.{mn}" for maj in range(80, 135) for mn in range(0, 4)]

COMPANY_NAMES = [
    "Acme Corp", "Initech", "Globex", "Hooli", "Pied Piper", "Umbrella Corp",
    "Stark Industries", "Wayne Enterprises", "Dunder Mifflin", "Cyberdyne",
    "Weyland-Yutani", "Soylent Corp", "Tyrell Corp", "Massive Dynamic",
]
COMPANY_INDUSTRIES = [
    "SaaS", "Fintech", "Healthcare", "E-commerce", "Developer Tools",
    "Security", "AI/ML", "Education", "Media", "Infrastructure",
]

# Elements for realistic autocapture chains
TAG_NAMES = ["a", "button", "input", "div", "span", "form", "label", "li", "td", "svg"]
CSS_CLASSES = [
    "LemonButton", "LemonButton--primary", "LemonButton--secondary",
    "LemonInput", "LemonSelect", "LemonModal", "LemonTable",
    "ant-btn", "ant-btn-primary", "ant-input", "ant-select",
    "btn", "btn-primary", "btn-secondary", "btn-sm",
    "link", "link--muted", "text-muted", "font-semibold",
    "flex", "items-center", "gap-2", "px-4", "py-2",
]
BUTTON_TEXTS = [
    "Save", "Submit", "Cancel", "Next", "Back", "Create", "Delete",
    "Create insight", "View events", "Add filter", "Run query",
    "Save dashboard", "Share", "Export", "Invite team member",
    "Enable", "Disable", "Refresh", "Load more", "Dismiss",
    "", None,
]
DATA_ATTRS = [
    "save-dashboard", "create-insight", "open-menu", "toggle-flag",
    "new-insight", "delete-item", "expand-row", "close-modal",
    "nav-dashboards", "nav-insights", "nav-events", "nav-persons",
    "submit-form", "open-settings", "copy-link",
]
HREFS = [
    "/dashboard", "/insights/new", "/events", "/persons",
    "/feature_flags", "/settings", "/docs", None,
]

# Exception data
EXCEPTION_TYPES = [
    "TypeError", "ReferenceError", "SyntaxError", "RangeError",
    "NetworkError", "AbortError", "TimeoutError", "ChunkLoadError",
    "UnhandledRejection", "ResizeObserver loop limit exceeded",
]
EXCEPTION_MESSAGES = [
    "Cannot read properties of undefined (reading 'map')",
    "Cannot read properties of null (reading 'length')",
    "e is not a function",
    "Failed to fetch",
    "Network request failed",
    "Load failed",
    "Unexpected token < in JSON at position 0",
    "ResizeObserver loop completed with undelivered notifications.",
    "Loading chunk 42 failed.",
    "Timeout of 30000ms exceeded",
    "Maximum call stack size exceeded",
    "Permission denied to access property 'document'",
]

# Property name edge cases (for stress testing schema inference)
EDGE_CASE_PROPS = {
    "user.settings.theme": "dark",
    "feature flags": ["beta-ui", "new-onboarding"],
    "conversion_value_\u20ac": 49.99,
    "conversion_value_\u00a5": 7200,
    "\u30a4\u30d9\u30f3\u30c8\u540d": "\u30da\u30fc\u30b8\u30d3\u30e5\u30fc",
    "really_long_property_name_that_tests_column_name_limits_in_warehouses_and_databases_v2": True,
    "metadata.nested.deep.value": 42,
    "$custom_prop_with_dollar": "test",
}

# Page titles
PAGE_TITLES = [
    "Dashboard - PostHog", "Insights - PostHog", "Events - PostHog",
    "Feature Flags - PostHog", "Pricing - PostHog", "Persons & Groups - PostHog",
    "Session Replay - PostHog", "Web Analytics - PostHog", "Experiments - PostHog",
    "Data Management - PostHog", "Settings - PostHog", None,
]

# Pre-computed team/project IDs (a few teams generating the traffic)
TEAM_IDS = [1, 2, 5, 12, 37, 100]
PROJECT_IDS = [1, 2, 3, 5, 10, 42]

# ---------------------------------------------------------------------------
# Power-law user distribution
# ---------------------------------------------------------------------------
# Pre-generate a pool of distinct_ids with a Zipf-like distribution.
# A small fraction of users generate disproportionate event volume.

_NUM_USERS = 2000
_USER_IDS = [f"user-{uuid.UUID(int=i + 1)}" for i in range(_NUM_USERS)]
# Zipf weights: user 0 has weight 1/1, user 1 has weight 1/2, ...
_USER_WEIGHTS = [1.0 / (i + 1) for i in range(_NUM_USERS)]

# Pre-generate session pools per user (each user has a few sessions)
_USER_SESSIONS = {}
_USER_WINDOWS = {}
for _uid in _USER_IDS:
    n_sessions = random.randint(1, 5)
    _USER_SESSIONS[_uid] = [str(uuid.uuid4()) for _ in range(n_sessions)]
    # Each session has 1-3 windows
    _USER_WINDOWS[_uid] = {}
    for sid in _USER_SESSIONS[_uid]:
        _USER_WINDOWS[_uid][sid] = [str(uuid.uuid4()) for _ in range(random.randint(1, 3))]


def _pick_user() -> str:
    """Pick a distinct_id with power-law distribution."""
    return random.choices(_USER_IDS, weights=_USER_WEIGHTS, k=1)[0]


def _pick_session(distinct_id: str) -> tuple:
    """Pick a session_id and window_id for a user."""
    sessions = _USER_SESSIONS.get(distinct_id)
    if not sessions:
        sid = str(uuid.uuid4())
        wid = str(uuid.uuid4())
        return sid, wid
    sid = random.choice(sessions)
    windows = _USER_WINDOWS.get(distinct_id, {}).get(sid, [str(uuid.uuid4())])
    wid = random.choice(windows)
    return sid, wid


def _jittered_timestamp() -> str:
    """Return an ISO timestamp near now, with slight random jitter."""
    # Most events are within the last few seconds, some up to 30s old
    # (simulates client-side batching and network delays)
    jitter_ms = random.expovariate(1.0 / 500)  # mean 500ms, occasionally several seconds
    jitter_ms = min(jitter_ms, 30000)
    ts = datetime.now(timezone.utc) - timedelta(milliseconds=jitter_ms)
    return ts.isoformat()


# ---------------------------------------------------------------------------
# Common properties builder
# ---------------------------------------------------------------------------

def _common_properties(distinct_id: str) -> dict:
    """Properties that appear on most events."""
    session_id, window_id = _pick_session(distinct_id)
    url = random.choice(URLS)
    pathname = url.split("/project/1", 1)[-1] if "/project/1" in url else random.choice(PATHNAMES)

    props = {
        "$current_url": url,
        "$host": "app.posthog.com",
        "$pathname": pathname,
        "$browser": random.choice(BROWSERS),
        "$browser_version": random.choice(BROWSER_VERSIONS),
        "$os": random.choice(OS_LIST),
        "$os_version": random.choice(["10.15.7", "14.0", "11", "17.0", "14", "22H2"]),
        "$device_type": random.choice(DEVICE_TYPES),
        "$screen_height": random.choice([768, 900, 1080, 1440, 2160]),
        "$screen_width": random.choice([1366, 1440, 1920, 2560, 3840]),
        "$viewport_height": random.randint(600, 1200),
        "$viewport_width": random.randint(900, 2560),
        "$lib": random.choice(LIB_NAMES),
        "$lib_version": random.choice(LIB_VERSIONS),
        "$session_id": session_id,
        "$window_id": window_id,
        "$insert_id": str(uuid.uuid4()),
    }

    # Referrer — 60% of events
    if random.random() < 0.6:
        props["$referrer"] = random.choice(REFERRERS)
        ref = props["$referrer"]
        if ref:
            # Extract domain from referrer
            parts = ref.replace("https://", "").replace("http://", "").split("/")
            props["$referring_domain"] = parts[0] if parts[0] else None
        else:
            props["$referring_domain"] = "" if ref == "" else None

    # User agent — 30%
    if random.random() < 0.3:
        props["$user_agent"] = random.choice(USER_AGENTS)

    # UTM — 20%
    if random.random() < 0.2:
        props["utm_source"] = random.choice(UTM_SOURCES)
        props["utm_medium"] = random.choice(UTM_MEDIUMS)
        props["utm_campaign"] = random.choice(UTM_CAMPAIGNS)

    # GeoIP — 40%
    if random.random() < 0.4:
        idx = random.randrange(len(COUNTRIES))
        props["$geoip_country_code"] = COUNTRIES[idx]
        props["$geoip_city_name"] = CITIES[idx]
        props["$geoip_latitude"] = round(random.uniform(-60, 60), 4)
        props["$geoip_longitude"] = round(random.uniform(-180, 180), 4)
        props["$geoip_time_zone"] = random.choice(TIMEZONES)

    # Active feature flags — 50%
    if random.random() < 0.5:
        num_flags = random.randint(1, 6)
        props["$active_feature_flags"] = random.sample(FEATURE_FLAGS, min(num_flags, len(FEATURE_FLAGS)))

    # Group membership — ~20% of events have $groups
    if random.random() < 0.2:
        props["$groups"] = {
            "company": f"company_{random.randint(1, 200)}",
        }
        if random.random() < 0.3:
            props["$groups"]["project"] = f"project_{random.randint(1, 50)}"

    # Edge case properties — 3% of events get one
    if random.random() < 0.03:
        key = random.choice(list(EDGE_CASE_PROPS.keys()))
        props[key] = EDGE_CASE_PROPS[key]

    return props


def _build_elements_chain(elements: list) -> str:
    """Build a realistic PostHog elements_chain string.

    Format: elements separated by ';', each element is:
      tag_name.class1.class2:attr__key="value":attr__key2="value2":nth-child="N":nth-of-type="M":text="..."
    """
    parts = []
    for el in elements:
        segs = [el["tag_name"]]
        # Classes as dot-separated after tag_name
        if "attr__class" in el:
            for cls in el["attr__class"].split(" "):
                if cls:
                    segs[0] += f".{cls}"
        attrs = []
        if "attr__href" in el and el["attr__href"]:
            attrs.append(f'attr__href="{el["attr__href"]}"')
        if "attr__id" in el and el["attr__id"]:
            attrs.append(f'attr__id="{el["attr__id"]}"')
        if "attr__data-attr" in el and el["attr__data-attr"]:
            attrs.append(f'attr__data-attr="{el["attr__data-attr"]}"')
        attrs.append(f'nth-child="{el.get("nth_child", 1)}"')
        attrs.append(f'nth-of-type="{el.get("nth_of_type", 1)}"')
        if "$el_text" in el and el["$el_text"]:
            # Truncate long text, escape quotes
            txt = el["$el_text"][:40].replace('"', '\\"')
            attrs.append(f'text="{txt}"')
        segs.append(":".join(attrs))
        parts.append(":".join(segs))
    return ";".join(parts)


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------

def _make_pageview(distinct_id: str) -> dict:
    """$pageview — most common event, ~35% of traffic."""
    props = _common_properties(distinct_id)
    props["$title"] = random.choice(PAGE_TITLES)
    if random.random() < 0.5:
        props["$prev_pageview_pathname"] = random.choice(PATHNAMES)
    if random.random() < 0.3:
        props["$prev_pageview_duration_seconds"] = round(random.uniform(0.5, 300), 1)
    if random.random() < 0.7:
        props["$screen_name"] = random.choice(["Dashboard", "Insights", "Events", "Persons", None])
    return {
        "uuid": str(uuid.uuid4()),
        "event": "$pageview",
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


def _make_pageleave(distinct_id: str) -> dict:
    """$pageleave — paired with pageview, ~8% of traffic."""
    props = _common_properties(distinct_id)
    props["$title"] = random.choice(PAGE_TITLES)
    props["$prev_pageview_duration_seconds"] = round(random.uniform(1, 600), 1)
    return {
        "uuid": str(uuid.uuid4()),
        "event": "$pageleave",
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


def _make_autocapture(distinct_id: str) -> dict:
    """$autocapture — second most common, ~25%."""
    props = _common_properties(distinct_id)
    props["$event_type"] = random.choice(["click", "click", "click", "submit", "change", "focus"])

    elements = []
    depth = random.randint(2, 6)
    for d in range(depth):
        el = {
            "tag_name": random.choice(TAG_NAMES) if d > 0 else random.choice(["button", "a", "input", "label"]),
            "nth_child": random.randint(1, 12),
            "nth_of_type": random.randint(1, 6),
        }
        # Target element (d=0) usually has text and attrs
        if d == 0:
            if random.random() < 0.7:
                el["$el_text"] = random.choice(BUTTON_TEXTS)
            if random.random() < 0.6:
                el["attr__class"] = " ".join(random.sample(CSS_CLASSES, random.randint(1, 3)))
            if random.random() < 0.3:
                el["attr__data-attr"] = random.choice(DATA_ATTRS)
            if el["tag_name"] == "a" and random.random() < 0.7:
                el["attr__href"] = random.choice(HREFS)
            if random.random() < 0.2:
                el["attr__id"] = random.choice(["main-nav", "sidebar", "content", "modal-root", f"item-{random.randint(1,100)}"])
        else:
            # Parent elements: usually div/span with sparse attrs
            el["tag_name"] = random.choice(["div", "div", "div", "span", "li", "td", "nav", "main", "section"])
            if random.random() < 0.3:
                el["attr__class"] = random.choice(CSS_CLASSES)
            if random.random() < 0.1:
                el["attr__id"] = random.choice(["root", "app", "main-content", "sidebar", f"container-{random.randint(1,20)}"])
        elements.append(el)

    props["$elements"] = elements
    props["$elements_chain"] = _build_elements_chain(elements)

    return {
        "uuid": str(uuid.uuid4()),
        "event": "$autocapture",
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


def _make_identify(distinct_id: str) -> dict:
    """$identify — rare but schema-different, ~3%."""
    props = _common_properties(distinct_id)
    user_num = random.randint(1, 10000)
    first_names = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jake"]
    last_names = ["Chen", "Smith", "Garcia", "Kim", "Patel", "Mueller", "Tanaka", "Okonkwo", "Silva", "Johansen"]

    set_props = {
        "email": f"user{user_num}@example.com",
        "name": f"{random.choice(first_names)} {random.choice(last_names)}",
    }
    # Sparse set properties -- these drift over time
    if random.random() < 0.6:
        set_props["plan"] = random.choice(["free", "growth", "enterprise", "startup"])
    if random.random() < 0.5:
        set_props["company"] = random.choice(COMPANY_NAMES)
    if random.random() < 0.3:
        set_props["phone"] = f"+1-555-{random.randint(1000, 9999)}"
    if random.random() < 0.4:
        set_props["role"] = random.choice(["engineer", "pm", "designer", "founder", "marketing", "data", "devops"])
    if random.random() < 0.2:
        # Type inconsistency: sometimes employee_count is a string
        set_props["employee_count"] = random.choice([random.randint(1, 10000), "1-10", "11-50", "51-200", "201-1000"])
    if random.random() < 0.15:
        set_props["signed_up_at"] = _jittered_timestamp()
    if random.random() < 0.3:
        set_props["industry"] = random.choice(COMPANY_INDUSTRIES)
    if random.random() < 0.2:
        set_props["has_billing"] = random.choice([True, False])
    if random.random() < 0.1:
        set_props["tags"] = random.sample(["power-user", "beta-tester", "enterprise", "churned", "expansion"], random.randint(1, 3))
    if random.random() < 0.1:
        set_props["custom_attributes"] = {
            "onboarding_step": random.randint(1, 5),
            "preferred_language": random.choice(["en", "de", "fr", "ja", "pt"]),
        }

    props["$set"] = set_props
    props["$set_once"] = {
        "initial_referrer": random.choice(REFERRERS),
        "initial_utm_source": random.choice(UTM_SOURCES),
        "initial_url": random.choice(URLS),
    }
    # $anon_distinct_id for identity merging
    if random.random() < 0.5:
        props["$anon_distinct_id"] = f"anon-{uuid.uuid4()}"

    return {
        "uuid": str(uuid.uuid4()),
        "event": "$identify",
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


def _make_groupidentify(distinct_id: str) -> dict:
    """$groupidentify — rare, ~1%. Different schema from other events."""
    props = _common_properties(distinct_id)
    group_type = random.choice(["company", "project", "workspace", "organization"])
    group_key = f"{group_type}_{random.randint(1, 200)}"

    group_set = {
        "name": random.choice(COMPANY_NAMES),
        "created_at": (datetime.now(timezone.utc) - timedelta(days=random.randint(1, 730))).isoformat(),
    }
    if random.random() < 0.7:
        group_set["industry"] = random.choice(COMPANY_INDUSTRIES)
    if random.random() < 0.6:
        group_set["plan"] = random.choice(["free", "growth", "enterprise"])
    if random.random() < 0.5:
        group_set["employee_count"] = random.randint(1, 50000)
    if random.random() < 0.3:
        group_set["arr"] = random.choice([0, 4500, 12000, 50000, 250000])
    if random.random() < 0.2:
        group_set["domain"] = f"{random.choice(['acme', 'initech', 'globex', 'hooli'])}.com"

    props["$group_type"] = group_type
    props["$group_key"] = group_key
    props["$group_set"] = group_set

    return {
        "uuid": str(uuid.uuid4()),
        "event": "$groupidentify",
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


def _make_feature_flag(distinct_id: str) -> dict:
    """$feature_flag_called — moderate frequency, ~8%."""
    props = _common_properties(distinct_id)
    flag = random.choice(FEATURE_FLAGS)
    props["$feature_flag"] = flag
    props["$feature_flag_response"] = random.choice([True, False, "control", "test", "variant-a", "variant-b"])
    if random.random() < 0.4:
        props["$feature_flag_payload"] = {
            "variant": random.choice(["control", "test", "variant-a", "variant-b"]),
            "rollout_percentage": random.randint(0, 100),
        }
    if random.random() < 0.2:
        props["$feature_flag_request_id"] = str(uuid.uuid4())
    return {
        "uuid": str(uuid.uuid4()),
        "event": "$feature_flag_called",
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


def _make_exception(distinct_id: str) -> dict:
    """$exception — error tracking events, ~2%."""
    props = _common_properties(distinct_id)
    exc_type = random.choice(EXCEPTION_TYPES)
    exc_msg = random.choice(EXCEPTION_MESSAGES)

    props["$exception_type"] = exc_type
    props["$exception_message"] = exc_msg
    props["$exception_source"] = random.choice([
        "https://app-static.posthog.com/static/chunk-VENDORS-abc123.js",
        "https://app-static.posthog.com/static/main.4f2a1b.js",
        "https://app-static.posthog.com/static/posthog.js",
        None,
    ])
    props["$exception_lineno"] = random.randint(1, 50000)
    props["$exception_colno"] = random.randint(1, 500)

    # Stack trace (simplified but structurally realistic)
    frames = []
    n_frames = random.randint(2, 8)
    for _ in range(n_frames):
        frames.append({
            "filename": random.choice([
                "https://app-static.posthog.com/static/main.js",
                "https://app-static.posthog.com/static/chunk-123.js",
                "<anonymous>",
            ]),
            "function": random.choice([
                "render", "onClick", "handleSubmit", "fetchData",
                "useEffect", "dispatch", "Object.map", "Array.forEach",
                "async onClick", "processResponse", None,
            ]),
            "lineno": random.randint(1, 50000),
            "colno": random.randint(1, 500),
            "in_app": random.choice([True, True, True, False]),
        })
    props["$exception_stack_trace_raw"] = json.dumps(frames)

    if random.random() < 0.3:
        props["$exception_fingerprint"] = f"{exc_type}:{exc_msg[:30]}"
    if random.random() < 0.5:
        props["$exception_handled"] = random.choice([True, False])
    if random.random() < 0.3:
        props["$exception_level"] = random.choice(["error", "warning", "fatal"])

    return {
        "uuid": str(uuid.uuid4()),
        "event": "$exception",
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


def _make_custom_event(distinct_id: str) -> dict:
    """Custom event — ~13%, each type has its own property set."""
    event_name = random.choice([
        "insight_created", "dashboard_viewed", "recording_watched",
        "export_started", "invite_sent", "billing_upgraded",
        "query_executed", "annotation_created", "survey_shown",
        "survey_dismissed", "survey_sent",
    ])
    props = _common_properties(distinct_id)

    if event_name == "insight_created":
        props["insight_type"] = random.choice(["trends", "funnels", "retention", "paths", "lifecycle", "stickiness", "sql"])
        props["query_time_ms"] = random.randint(50, 30000)
        props["filter_count"] = random.randint(0, 10)
        props["breakdown_type"] = random.choice(["person", "event", "cohort", None])
        if random.random() < 0.3:
            props["is_saved"] = random.choice([True, False])
    elif event_name == "dashboard_viewed":
        props["dashboard_id"] = random.randint(1, 500)
        props["tile_count"] = random.randint(1, 30)
        props["load_time_ms"] = random.randint(200, 15000)
        props["is_shared"] = random.choice([True, False])
    elif event_name == "recording_watched":
        props["recording_duration_ms"] = random.randint(5000, 3600000)
        props["watched_duration_ms"] = random.randint(1000, 600000)
        props["events_count"] = random.randint(5, 5000)
        if random.random() < 0.3:
            props["console_error_count"] = random.randint(0, 50)
        if random.random() < 0.4:
            props["console_log_count"] = random.randint(0, 500)
    elif event_name == "query_executed":
        props["query_type"] = random.choice(["hogql", "sql", "events", "persons"])
        props["query_time_ms"] = random.randint(10, 60000)
        props["rows_returned"] = random.randint(0, 100000)
        props["bytes_scanned"] = random.randint(1000, 1000000000)
        props["query_id"] = random.randint(2**50, 2**53 + 1000)
    elif event_name == "billing_upgraded":
        props["from_plan"] = random.choice(["free", "growth"])
        props["to_plan"] = random.choice(["growth", "enterprise"])
        props["monthly_amount"] = random.choice([0, 450, 2000, random.randint(100, 50000)])
    elif event_name == "invite_sent":
        props["invitee_email"] = f"invited{random.randint(1, 1000)}@example.com"
        props["role"] = random.choice(["admin", "member"])
    elif event_name == "export_started":
        props["export_format"] = random.choice(["csv", "json", "parquet", "xlsx"])
        props["row_count"] = random.randint(100, 10000000)
    elif event_name.startswith("survey_"):
        props["$survey_id"] = str(uuid.uuid4())
        props["$survey_name"] = random.choice(["NPS Q1", "Onboarding feedback", "Feature request", "Churn reason"])
        if event_name == "survey_sent":
            props["$survey_response"] = random.choice([
                "Love the product!", "Needs more integrations", "Too expensive",
                str(random.randint(0, 10)),  # NPS score as string (type inconsistency)
                random.randint(0, 10),  # NPS score as int
            ])

    return {
        "uuid": str(uuid.uuid4()),
        "event": event_name,
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


def _make_snapshot(distinct_id: str) -> dict:
    """$snapshot — session recording metadata events, ~5%.

    These are lightweight metadata events; we do NOT produce actual rrweb
    payloads (those are huge and would just waste Kafka bandwidth in a test).
    """
    session_id, window_id = _pick_session(distinct_id)
    props = {
        "$session_id": session_id,
        "$window_id": window_id,
        "$snapshot_source": random.choice(["web", "web", "web", "mobile"]),
        "$lib": random.choice(["posthog-js", "posthog-ios", "posthog-android"]),
        "$lib_version": random.choice(LIB_VERSIONS),
        "$snapshot_data": {
            "type": random.choice([2, 3, 4, 5]),  # rrweb event types
            "data": {"source": random.randint(0, 14)},
            "timestamp": int(time.time() * 1000),
        },
    }
    return {
        "uuid": str(uuid.uuid4()),
        "event": "$snapshot",
        "distinct_id": distinct_id,
        "timestamp": _jittered_timestamp(),
        "team_id": random.choice(TEAM_IDS),
        "project_id": random.choice(PROJECT_IDS),
        "properties": props,
    }


# ---------------------------------------------------------------------------
# Weighted event type selection: mirrors real PostHog traffic distribution
# ---------------------------------------------------------------------------

_EVENT_GENERATORS = [
    (_make_pageview,       35),
    (_make_autocapture,    25),
    (_make_custom_event,   13),
    (_make_feature_flag,    8),
    (_make_pageleave,       8),
    (_make_snapshot,        5),
    (_make_identify,        3),
    (_make_exception,       2),
    (_make_groupidentify,   1),
]
_GENERATORS, _WEIGHTS = zip(*_EVENT_GENERATORS)


def make_event(i: int) -> dict:
    distinct_id = _pick_user()
    generator = random.choices(_GENERATORS, weights=_WEIGHTS, k=1)[0]
    return generator(distinct_id)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    from confluent_kafka import Producer

    producer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 100000,
        "compression.type": "lz4",
    }
    # Merge KAFKA_PRODUCER_* env vars (e.g. KAFKA_PRODUCER_SECURITY_PROTOCOL=SSL)
    prefix = "KAFKA_PRODUCER_"
    for k, v in os.environ.items():
        if k.startswith(prefix):
            producer_config[k[len(prefix):].lower().replace("_", ".")] = v
    producer = Producer(producer_config)

    infinite = TOTAL_RECORDS == -1
    if infinite:
        print(f"Producing indefinitely to {TOPIC} ({PARTITION_COUNT} partitions)")
    else:
        print(f"Producing {TOTAL_RECORDS} records to {TOPIC} ({PARTITION_COUNT} partitions)")
    print(f"Rate: {RECORDS_PER_SECOND} records/sec")

    sent = 0
    batch_start = time.monotonic()
    batch_size = max(1, RECORDS_PER_SECOND // 10)
    i = 0

    while infinite or i < TOTAL_RECORDS:
        event = make_event(i)
        partition = i % PARTITION_COUNT
        producer.produce(
            TOPIC,
            key=event["distinct_id"].encode(),
            value=json.dumps(event).encode(),
            partition=partition,
        )
        sent += 1
        i += 1

        if sent % batch_size == 0:
            producer.poll(0)  # trigger delivery callbacks without blocking
            elapsed = time.monotonic() - batch_start
            expected = sent / RECORDS_PER_SECOND
            if elapsed < expected:
                time.sleep(expected - elapsed)

        if sent % 5000 == 0:
            producer.flush()
            if infinite:
                print(f"  sent {sent}")
            else:
                print(f"  sent {sent}/{TOTAL_RECORDS}")

    producer.flush()
    print(f"Done. Produced {sent} records.")


if __name__ == "__main__":
    sys.exit(main())
