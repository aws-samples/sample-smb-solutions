# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Lenient catalog parser: raw upstream records into validated ``Event`` models.

This module owns the single mapping function that turns raw records retrieved
from the upstream content directory into validated :class:`~aws_events_mcp.models.Event`
instances. It is deliberately lenient (Requirement 10): a record that is missing
a required field, carries an invalid field value, or is otherwise malformed is
skipped rather than aborting the whole parse, a loguru warning identifying the
skipped record (and the invalid field where applicable) is emitted, and parsing
continues with the remaining records.

The caller (the catalog layer) uses the returned ``warnings`` list together with
:func:`is_partial_parse` to distinguish three outcomes that the design maps onto
different tool responses:

- every record parsed -> success;
- some but not all records parsed -> the partial-parse signal the tool layer
  maps to a ``source_partial`` error (Requirement 11.4);
- non-empty content but zero valid records -> the catalog layer raises
  ``CatalogUnparseableError`` (Requirement 11.3).

Confirmed field mapping (task 8.2)
----------------------------------
The upstream record shape was pinned against the live AWS content-directory API
(see :mod:`aws_events_mcp.consts`). Each raw record handed to the parser is an
unwrapped ``item`` dict carrying a top-level ``id`` plus an ``additionalFields``
sub-mapping of the event content, with the wrapper's ``tags`` list attached
under ``tags`` (done by :func:`aws_events_mcp.source._extract_items`). The
mapping decisions are:

- ``event_id`` <- the top-level ``id`` (always present and unique).
- ``title`` <- ``additionalFields.title`` (falls back to ``heading``).
- ``description`` <- ``additionalFields.body`` (falls back to ``bodyBack``).
- ``start_date`` <- ``additionalFields.date`` when present, else the date
  portion of ``additionalFields.publishedDate`` (an ISO datetime). The value is
  normalized to ``YYYY-MM-DD`` before model validation; a record with neither is
  skipped as missing a required field (Requirement 10.4).
- ``start_time`` / ``time_zone`` <- ``additionalFields.time`` / ``timeZone``
  (absent in the current alias, retained for other directories/locales).
- ``location`` <- ``additionalFields.location`` (free text, e.g. "On-Demand
  Event").
- ``location_mode`` <- **derived**, not a discrete upstream column. Preference
  order: an explicit mode field if present and valid; else the catalog's
  ``GLOBAL#aws-event-type`` tag (``in-person`` -> physical; ``virtual`` /
  ``on-demand`` -> virtual); else inference from the location text; else the
  ``virtual`` default (the catalog is overwhelmingly online webinars, tech
  talks, and on-demand content). Because the model requires this field and the
  upstream has no equivalent column, it is always derived so a record is never
  skipped solely for lacking an explicit mode.
- ``learning_level`` <- ``additionalFields.level`` (lowercase ``foundational`` /
  ``intermediate`` / ``advanced`` / ``expert`` or the numeric 100-400 forms);
  unrecognized values (e.g. ``all``) pass through and leave the level unset.
- ``event_type`` <- the ``GLOBAL#local-tags-content-type`` tag (e.g.
  ``webinar``) when present, else an explicit type field; otherwise ``None``.
- ``partner_name`` <- an explicit partner field when present; otherwise ``None``
  (no partner column in the current alias).
- ``registration_url`` <- ``additionalFields.primaryCTALink`` (the action/
  register link); ``learn_more_url`` <- ``additionalFields.ctaLink`` (the card's
  learn-more link). Site-relative links (``/path``) are resolved to absolute
  ``https://aws.amazon.com`` URLs.

To stay resilient to upstream drift the lookups keep the earlier snake_case and
camelCase aliases in ``_FIELD_KEYS`` (tried in priority order), so both the
confirmed real keys and reasonable variants are accepted. This is the single
place the field mapping is adjusted should AWS change the contract.

General assumptions:

- Each raw record is a mapping (``dict``); anything else is treated as malformed.
- Field values are looked up by trying a small ordered list of candidate keys
  (snake_case, camelCase, and the confirmed upstream keys) and taking the first
  present, non-``None`` value.
- Required fields (``event_id``, ``title``, ``start_date``) that are absent are
  simply omitted from the mapped payload, so pydantic raises a "missing"
  validation error and the record is skipped (Requirement 10.4).

Logging is intentionally not configured here; it is configured once in
``server.py`` and directed to stderr.
"""

from aws_events_mcp.models import Event, LearningLevel, LocationMode
from collections.abc import Mapping
from loguru import logger
from pydantic import ValidationError
from typing import Any


#: Ordered candidate keys for each ``Event`` field, tried in order. The keys
#: confirmed against the live contract (task 8.2) are included alongside the
#: earlier snake_case/camelCase aliases for resilience to upstream drift; this
#: remains the single place to edit when the real record shape changes. The
#: content fields live under the record's ``additionalFields`` sub-mapping,
#: which :func:`_merged_fields` flattens before lookup. ``location_mode``,
#: ``event_type``, and ``partner_name`` have no discrete column in the current
#: alias and are derived from tags / defaults where possible.
_FIELD_KEYS: dict[str, tuple[str, ...]] = {
    'event_id': ('event_id', 'eventId', 'id'),
    'title': ('title', 'heading', 'name', 'headline'),
    'description': ('description', 'summary', 'abstract', 'body', 'bodyBack'),
    'start_date': ('start_date', 'startDate', 'date', 'publishedDate'),
    'start_time': ('start_time', 'startTime', 'time'),
    'time_zone': ('time_zone', 'timeZone', 'timezone', 'tz'),
    'location': ('location', 'venue', 'city'),
    'location_mode': ('location_mode', 'locationMode', 'mode', 'delivery', 'deliveryMode'),
    'learning_level': ('learning_level', 'learningLevel', 'level'),
    'event_type': ('event_type', 'eventType', 'type', 'category'),
    'partner_name': ('partner_name', 'partnerName', 'partner'),
    'registration_url': (
        'registration_url',
        'registrationUrl',
        'register_url',
        'registerUrl',
        'primaryCTALink',
    ),
    'learn_more_url': (
        'learn_more_url',
        'learnMoreUrl',
        'details_url',
        'detailsUrl',
        'ctaLink',
        'secondaryCTALink',
    ),
}

#: Event-field names whose values are URLs; site-relative paths (``/path``) are
#: resolved to absolute ``https://aws.amazon.com`` URLs during mapping.
_URL_FIELDS: tuple[str, ...] = ('registration_url', 'learn_more_url')

#: Base used to resolve site-relative upstream links to absolute URLs.
_ABSOLUTE_URL_BASE = 'https://aws.amazon.com'

#: Fallback details link applied when a record carries no link of its own, so
#: every event in a response always includes at least one actionable URL.
_FALLBACK_EVENT_URL = 'https://aws.amazon.com/events/explore-aws-events/'

#: Tag namespace carrying the catalog's delivery facet (task 8.2). Its tag
#: names (``on-demand`` / ``virtual`` / ``in-person``) drive location-mode
#: derivation when no explicit mode column is present.
_TAG_NS_EVENT_TYPE = 'GLOBAL#aws-event-type'

#: Tag namespace carrying the catalog's content category (e.g. ``webinar``),
#: used to derive ``event_type`` when no explicit type column is present.
_TAG_NS_CONTENT_TYPE = 'GLOBAL#local-tags-content-type'

#: ``GLOBAL#aws-event-type`` tag names that denote a physical (in-person) event.
_PHYSICAL_EVENT_TYPE_TAGS = frozenset({'in-person', 'in person', 'inperson'})

#: ``GLOBAL#aws-event-type`` tag names that denote a virtual (online) event.
_VIRTUAL_EVENT_TYPE_TAGS = frozenset({'virtual', 'on-demand', 'on demand', 'ondemand', 'online'})

#: Default delivery mode when no tag, explicit field, or location text resolves
#: one. The catalog is overwhelmingly online (webinars, tech talks, on-demand).
_DEFAULT_LOCATION_MODE = LocationMode.VIRTUAL.value

#: Location-text substrings that positively indicate a virtual delivery mode.
_VIRTUAL_LOCATION_HINTS: tuple[str, ...] = (
    'on-demand',
    'on demand',
    'online',
    'virtual',
    'webinar',
    'livestream',
    'live stream',
    'twitch',
    'anywhere',
    'digital',
)

#: Recognized ``location_mode`` synonyms normalized to canonical enum values.
_LOCATION_MODE_SYNONYMS: dict[str, str] = {
    'virtual': LocationMode.VIRTUAL.value,
    'online': LocationMode.VIRTUAL.value,
    'physical': LocationMode.PHYSICAL.value,
    'in-person': LocationMode.PHYSICAL.value,
    'in person': LocationMode.PHYSICAL.value,
    'inperson': LocationMode.PHYSICAL.value,
    'venue': LocationMode.PHYSICAL.value,
}

#: Recognized ``learning_level`` synonyms (names and 100/200/300/400) -> enum value.
_LEARNING_LEVEL_SYNONYMS: dict[str, str] = {
    'foundational': LearningLevel.FOUNDATIONAL.value,
    '100': LearningLevel.FOUNDATIONAL.value,
    'intermediate': LearningLevel.INTERMEDIATE.value,
    '200': LearningLevel.INTERMEDIATE.value,
    'advanced': LearningLevel.ADVANCED.value,
    '300': LearningLevel.ADVANCED.value,
    'expert': LearningLevel.EXPERT.value,
    '400': LearningLevel.EXPERT.value,
}


def parse_events(records: list[dict]) -> tuple[list[Event], list[str]]:
    """Parse raw upstream records into validated ``Event`` instances.

    Maps every raw record to an ``Event``, skipping (and warning about) any
    record that is missing a required field, has an invalid field value, or is
    malformed, and continuing past each skip so one bad record never aborts the
    whole parse (Requirements 10.1, 10.4, 10.5, 10.6).

    Args:
        records: The raw records retrieved from the upstream catalog. Each
            record is expected to be a mapping; non-mapping entries are treated
            as malformed and skipped.

    Returns:
        A two-tuple ``(events, warnings)`` where ``events`` is the list of
        successfully parsed ``Event`` instances (in input order) and
        ``warnings`` is a list of human-readable warning strings, one per
        skipped record. A non-empty ``warnings`` list alongside a non-empty
        ``events`` list indicates a partial parse; see :func:`is_partial_parse`.
    """
    events: list[Event] = []
    warnings: list[str] = []

    for index, record in enumerate(records):
        identifier = _record_identifier(record, index)

        if not isinstance(record, Mapping):
            warnings.append(_warn(f'Skipped record {identifier}: malformed (not an object).'))
            continue

        mapped = _map_record(record)

        try:
            events.append(Event.model_validate(mapped))
        except ValidationError as exc:
            reason = _describe_validation_error(exc)
            warnings.append(_warn(f'Skipped record {identifier}: {reason}.'))

    return events, warnings


def is_partial_parse(record_count: int, events: list[Event]) -> bool:
    """Report whether a parse was partial (some but not all records parsed).

    A partial parse is the condition Requirement 11.4 maps to a ``source_partial``
    error: at least one record produced a valid ``Event`` while at least one
    other record was skipped. A wholly empty input, an all-skipped input, and an
    all-parsed input are *not* partial.

    Args:
        record_count: The number of raw records passed to :func:`parse_events`.
        events: The events returned by :func:`parse_events`.

    Returns:
        ``True`` if ``0 < len(events) < record_count``; otherwise ``False``.
    """
    return 0 < len(events) < record_count


def _map_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Build an ``Event`` payload from a raw record using the confirmed mapping.

    Flattens the record's ``additionalFields`` sub-mapping (task 8.2) over its
    top-level keys, looks up each field by its candidate keys, normalizes dates,
    URLs, and the learning-level enum, and always derives a ``location_mode``
    (from an explicit field, the catalog's tags, the location text, or the
    default). Absent fields are omitted so required ones surface as pydantic
    "missing" errors (and the record is skipped) while optional ones fall back to
    their model defaults.

    Args:
        record: A single raw record mapping (an unwrapped ``item`` dict with an
            ``additionalFields`` sub-mapping and an attached ``tags`` list).

    Returns:
        A dict of ``Event`` field names to raw/normalized values, ready to pass
        to ``Event.model_validate``.
    """
    fields = _merged_fields(record)
    tags = record.get('tags') if isinstance(record, Mapping) else None

    mapped: dict[str, Any] = {}
    for field, keys in _FIELD_KEYS.items():
        if field == 'location_mode':
            # Derived below; never read directly as a plain field.
            continue
        value = _first_present(fields, keys)
        if value is None:
            continue
        mapped[field] = value

    if 'start_date' in mapped:
        mapped['start_date'] = _normalize_date(mapped['start_date'])
    if 'learning_level' in mapped:
        mapped['learning_level'] = _normalize_learning_level(mapped['learning_level'])
    if 'event_type' not in mapped:
        derived_type = _derive_event_type(tags)
        if derived_type is not None:
            mapped['event_type'] = derived_type
    for url_field in _URL_FIELDS:
        if url_field in mapped:
            mapped[url_field] = _resolve_url(mapped[url_field])

    # Guarantee every event carries at least one link: when a record has
    # neither a registration nor a learn-more link, fall back to the public
    # AWS Events catalog page so responses are always actionable.
    if not mapped.get('registration_url') and not mapped.get('learn_more_url'):
        mapped['learn_more_url'] = _FALLBACK_EVENT_URL

    mapped['location_mode'] = _derive_location_mode(fields, tags)

    return mapped


def _merged_fields(record: Any) -> dict[str, Any]:
    """Flatten a record's ``additionalFields`` sub-mapping over its top-level keys.

    The confirmed contract nests the event content under ``additionalFields``
    while identifiers/dates live at the top level. This builds a single flat view
    with top-level keys taking precedence, excluding the ``additionalFields`` and
    ``tags`` containers themselves. Records that are already flat (e.g. test
    fixtures with no ``additionalFields``) pass through unchanged.

    Args:
        record: A single raw record (expected to be a mapping).

    Returns:
        A flat mapping of candidate keys to values, or an empty dict when
        ``record`` is not a mapping.
    """
    if not isinstance(record, Mapping):
        return {}
    additional = record.get('additionalFields')
    merged: dict[str, Any] = dict(additional) if isinstance(additional, Mapping) else {}
    for key, value in record.items():
        if key in ('additionalFields', 'tags'):
            continue
        merged[key] = value
    return merged


def _normalize_date(value: Any) -> Any:
    """Reduce an upstream date/datetime string to a ``YYYY-MM-DD`` calendar date.

    The catalog supplies both bare ``YYYY-MM-DD`` dates and full ISO datetimes
    (e.g. ``2025-07-22T10:57:00Z``); pydantic's ``date`` field rejects the
    latter, so the date portion is taken. Non-string values and unparseable
    strings are returned unchanged so model validation skips the record where
    appropriate (Requirement 10.5).

    Args:
        value: The raw start-date value.

    Returns:
        The leading ``YYYY-MM-DD`` portion for datetime strings; otherwise the
        original value unchanged.
    """
    if isinstance(value, str):
        return value.split('T', 1)[0].strip()
    return value


def _resolve_url(value: Any) -> Any:
    """Resolve a site-relative upstream link to an absolute ``aws.amazon.com`` URL.

    Site-relative paths (e.g. ``/training/...``) are prefixed with the AWS base;
    absolute URLs and non-string values are returned unchanged.

    Args:
        value: The raw URL value.

    Returns:
        An absolute URL string when the input was site-relative; otherwise the
        original value.
    """
    if isinstance(value, str) and value.startswith('/') and not value.startswith('//'):
        return f'{_ABSOLUTE_URL_BASE}{value}'
    return value


def _tag_names(tags: Any, namespace: str) -> list[str]:
    """Collect the lowercased tag names under a namespace from a record's tags.

    Args:
        tags: The record's attached ``tags`` list (each a mapping with
            ``tagNamespaceId`` and ``name``); anything else yields no names.
        namespace: The ``tagNamespaceId`` to filter on.

    Returns:
        The lowercased, stripped tag names under ``namespace`` (possibly empty).
    """
    names: list[str] = []
    if not isinstance(tags, list):
        return names
    for tag in tags:
        if not isinstance(tag, Mapping):
            continue
        if tag.get('tagNamespaceId') != namespace:
            continue
        name = tag.get('name')
        if isinstance(name, str) and name.strip():
            names.append(name.strip().lower())
    return names


def _derive_location_mode(fields: Mapping[str, Any], tags: Any) -> str:
    """Derive a valid ``location_mode`` value, never skipping a record for it.

    Preference order (task 8.2): an explicit, recognized mode field; then the
    ``GLOBAL#aws-event-type`` tag (``in-person`` -> physical; ``virtual`` /
    ``on-demand`` -> virtual); then a positive virtual hint in the location text;
    then the ``virtual`` default. The model requires this field and the upstream
    has no equivalent column, so it is always derived.

    Args:
        fields: The flattened record fields.
        tags: The record's attached ``tags`` list.

    Returns:
        A canonical ``LocationMode`` value string (``'virtual'`` or
        ``'physical'``).
    """
    explicit = _first_present(fields, _FIELD_KEYS['location_mode'])
    if explicit is not None:
        normalized = _normalize_location_mode(explicit)
        if normalized in (LocationMode.VIRTUAL.value, LocationMode.PHYSICAL.value):
            return normalized

    event_type_tags = _tag_names(tags, _TAG_NS_EVENT_TYPE)
    if any(name in _PHYSICAL_EVENT_TYPE_TAGS for name in event_type_tags):
        return LocationMode.PHYSICAL.value
    if any(name in _VIRTUAL_EVENT_TYPE_TAGS for name in event_type_tags):
        return LocationMode.VIRTUAL.value

    location = _first_present(fields, _FIELD_KEYS['location'])
    if isinstance(location, str):
        lowered = location.lower()
        if any(hint in lowered for hint in _VIRTUAL_LOCATION_HINTS):
            return LocationMode.VIRTUAL.value

    return _DEFAULT_LOCATION_MODE


def _derive_event_type(tags: Any) -> Any:
    """Derive an ``event_type`` from the content-category tag namespace.

    Uses the first ``GLOBAL#local-tags-content-type`` tag name (e.g. ``webinar``)
    when present; returns ``None`` otherwise so the optional field stays unset.

    Args:
        tags: The record's attached ``tags`` list.

    Returns:
        The content-type tag name, or ``None`` when none is present.
    """
    names = _tag_names(tags, _TAG_NS_CONTENT_TYPE)
    return names[0] if names else None


def _first_present(record: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    """Return the first non-``None`` value among ``keys`` present in ``record``.

    Args:
        record: The raw record mapping to read from.
        keys: Candidate keys to try, in priority order.

    Returns:
        The first non-``None`` value found, or ``None`` if no candidate key is
        present with a value.
    """
    for key in keys:
        if key in record and record[key] is not None:
            return record[key]
    return None


def _normalize_location_mode(value: Any) -> Any:
    """Normalize a recognized location-mode synonym to its canonical enum value.

    Unrecognized or non-string values are returned unchanged so that model
    validation rejects them and the record is skipped (Requirement 10.5).

    Args:
        value: The raw location-mode value.

    Returns:
        The canonical ``LocationMode`` value string when recognized; otherwise
        the original value.
    """
    if not isinstance(value, str):
        return value
    return _LOCATION_MODE_SYNONYMS.get(value.strip().lower(), value)


def _normalize_learning_level(value: Any) -> Any:
    """Normalize a recognized learning-level synonym to its canonical enum value.

    Accepts the canonical names plus the numeric 100/200/300/400 forms (as ints
    or strings). Unrecognized values are returned unchanged so model validation
    rejects them and the record is skipped (Requirement 10.5).

    Args:
        value: The raw learning-level value.

    Returns:
        The canonical ``LearningLevel`` value string when recognized; otherwise
        the original value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        value = str(value)
    if not isinstance(value, str):
        return value
    return _LEARNING_LEVEL_SYNONYMS.get(value.strip().lower(), value)


def _record_identifier(record: Any, index: int) -> str:
    """Build a stable, human-readable identifier for a record in a warning.

    Prefers the record's own identifier/title when available so a skipped record
    can be traced back to the source; always includes the positional index.

    Args:
        record: The raw record (may not be a mapping).
        index: The zero-based position of the record in the input list.

    Returns:
        A short identifier string such as ``at index 3 (id='abc')``.
    """
    if isinstance(record, Mapping):
        fields = _merged_fields(record)
        ident = _first_present(fields, _FIELD_KEYS['event_id'])
        if ident is not None:
            return f'at index {index} (id={ident!r})'
        title = _first_present(fields, _FIELD_KEYS['title'])
        if title is not None:
            return f'at index {index} (title={title!r})'
    return f'at index {index}'


def _describe_validation_error(exc: ValidationError) -> str:
    """Summarize the first validation error, naming the offending field.

    Distinguishes a missing required field from an invalid field value so the
    warning identifies the invalid field where applicable (Requirements 10.4,
    10.5).

    Args:
        exc: The ``ValidationError`` raised by ``Event.model_validate``.

    Returns:
        A short description such as ``missing required field 'title'`` or
        ``invalid value for field 'location_mode'``.
    """
    errors = exc.errors()
    if not errors:
        return 'failed model validation'
    first = errors[0]
    location = first.get('loc') or ()
    field = '.'.join(str(part) for part in location) if location else '<record>'
    if first.get('type') == 'missing':
        return f"missing required field '{field}'"
    return f"invalid value for field '{field}'"


def _warn(message: str) -> str:
    """Emit a loguru warning and return the same message for the warnings list.

    Args:
        message: The warning text identifying the skipped record.

    Returns:
        The unchanged ``message`` so the caller can collect it.
    """
    logger.warning(message)
    return message
