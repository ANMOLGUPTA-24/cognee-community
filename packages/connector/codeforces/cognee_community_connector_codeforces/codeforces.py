"""DLT source for Codeforces practice history (full-snapshot sync + forget-on-delete).

Fetches a list of handles' submissions, rated contests and profiles from the
public Codeforces API and yields them as one dlt resource of text documents
for cognee's ingestion pipeline.

The rows are ingested as *normal documents*: the source declares
``cognee_document_source = "codeforces"``, so ``resolve_dlt_sources`` routes
each row through the standard cognify entity-extraction pipeline instead of the
deterministic dlt-row schema-context path.

Three kinds of document are produced per handle, all in one staging table:

* one per problem the handle attempted, with every attempt on it oldest first
  (verdict, language, the test it failed on, time and memory), so "three WAs on
  test 7, then AC" stays together in one document;
* one per rated contest: name, rank, rating change, and the problems attempted
  during it;
* one profile: current/max rating and rank, plus solved/unsolved and per-tag
  first-try tallies, which a graph search cannot reliably aggregate itself.

The source is a full snapshot (``write_disposition="replace"``). Verdicts can
change after a contest (OK becomes CHALLENGED after a hack, or SKIPPED), so an
append-only sync would keep stale verdicts in memory. A handle's whole history
is one ``user.status`` call, so re-fetching it is cheap, and unchanged
documents keep a stable content-hash ``data_id``, so they are not re-cognified.
A handle removed from the config (or anything that disappears upstream) is
absent from the snapshot, and cognee's ``orphan_cleanup`` forgets it.

Everything is fetched before the first row is yielded, and every handle is
checked with ``user.info`` first, so an unknown handle or an API failure aborts
the run and leaves staging - and memory - untouched instead of syncing a
partial snapshot that would forget live documents.
"""

import hashlib
import os
import secrets
import time
from collections import Counter
from datetime import UTC, datetime
from typing import Any

import httpx
from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR, NODE_SET_COLUMN

logger = get_logger("codeforces_connector")

# dlt resource / staging-table name for Codeforces documents.
CODEFORCES_TABLE_NAME = "codeforces_documents"
CODEFORCES_SOURCE_NAME = "codeforces"

API_URL = "https://codeforces.com/api"
SITE_URL = "https://codeforces.com"

# Codeforces allows roughly one request per two seconds.
_MIN_INTERVAL = 2.0
# Retry budget for "Call limit exceeded" and transient HTTP/network errors.
_MAX_RETRIES = 5
_TIMEOUT = 30.0
# Contest ids from here up are gym contests, which live under /gym/.
_GYM_CONTEST_ID = 100_000

_VERDICTS = {
    "OK": "Accepted",
    "FAILED": "Failed",
    "PARTIAL": "Partial result",
    "COMPILATION_ERROR": "Compilation error",
    "RUNTIME_ERROR": "Runtime error",
    "WRONG_ANSWER": "Wrong answer",
    "PRESENTATION_ERROR": "Presentation error",
    "TIME_LIMIT_EXCEEDED": "Time limit exceeded",
    "MEMORY_LIMIT_EXCEEDED": "Memory limit exceeded",
    "IDLENESS_LIMIT_EXCEEDED": "Idleness limit exceeded",
    "SECURITY_VIOLATED": "Security violated",
    "CRASHED": "Crashed",
    "INPUT_PREPARATION_CRASHED": "Input preparation crashed",
    "CHALLENGED": "Hacked (challenged)",
    "SKIPPED": "Skipped",
    "TESTING": "Still testing",
    "REJECTED": "Rejected",
}
# Verdicts reached on a specific test, so "on test N" is meaningful.
_TEST_VERDICTS = {
    "WRONG_ANSWER",
    "RUNTIME_ERROR",
    "TIME_LIMIT_EXCEEDED",
    "MEMORY_LIMIT_EXCEEDED",
    "IDLENESS_LIMIT_EXCEEDED",
    "PRESENTATION_ERROR",
    "SECURITY_VIOLATED",
    "CRASHED",
}
_PARTICIPATION = {
    "CONTESTANT": "in contest",
    "PRACTICE": "practice",
    "VIRTUAL": "virtual contest",
    "MANAGER": "as manager",
    "OUT_OF_COMPETITION": "out of competition",
}


class CodeforcesAPIError(RuntimeError):
    """The Codeforces API refused a call (``status: FAILED``) for a non-transient reason."""


def codeforces_source(
    handles: list[str] | None = None,
    api_key: str | None = None,
    api_secret: str | None = None,
    client: httpx.Client | None = None,
):
    """Create a dlt source that yields Codeforces history as text documents.

    Args:
        handles: Codeforces handles to sync. Falls back to the comma-separated
            ``CODEFORCES_HANDLES`` environment variable.
        api_key: Optional API key (``CODEFORCES_API_KEY``). Public data needs no
            key; when a key and secret are given every call is signed.
        api_secret: The key's secret (``CODEFORCES_API_SECRET``).
        client: Pre-built ``httpx.Client`` (mainly a test-injection point).

    Returns:
        A dlt source suitable for ``cognee.add(...)`` / ``cognee.remember(...)``.
    """
    import dlt

    resolved = _resolve_handles(handles)
    api_key = api_key or os.environ.get("CODEFORCES_API_KEY")
    api_secret = api_secret or os.environ.get("CODEFORCES_API_SECRET")
    if bool(api_key) != bool(api_secret):
        raise ValueError("Pass both a Codeforces API key and its secret, or neither.")

    @dlt.resource(name=CODEFORCES_TABLE_NAME, primary_key="id", write_disposition="replace")
    def codeforces_documents():
        # Fetch everything before yielding: a failure part-way must abort the
        # run, never commit a snapshot missing a handle's documents.
        if client is not None:
            rows = _fetch_rows(_CodeforcesAPI(client, api_key, api_secret), resolved)
        else:
            with httpx.Client(timeout=_TIMEOUT) as http:
                rows = _fetch_rows(_CodeforcesAPI(http, api_key, api_secret), resolved)
        logger.info("Codeforces: synced %d document(s) for %d handle(s).", len(rows), len(resolved))
        yield from rows

    @dlt.source(name=CODEFORCES_SOURCE_NAME)
    def _codeforces():
        return codeforces_documents

    source = _codeforces()
    # Opt into the document ingestion path (row -> text document -> cognify).
    setattr(source, DOCUMENT_SOURCE_ATTR, CODEFORCES_SOURCE_NAME)
    return source


def _resolve_handles(handles: list[str] | None) -> list[str]:
    """Handles from the argument or ``CODEFORCES_HANDLES``, stripped and de-duplicated."""
    if handles is None:
        handles = os.environ.get("CODEFORCES_HANDLES", "").split(",")
    resolved: list[str] = []
    seen: set[str] = set()
    for handle in handles:
        handle = handle.strip()
        # Handles are case-insensitive on Codeforces.
        if handle and handle.lower() not in seen:
            seen.add(handle.lower())
            resolved.append(handle)
    if not resolved:
        raise ValueError("Codeforces handles required: pass handles= or set CODEFORCES_HANDLES.")
    return resolved


# ---------------------------------------------------------------------------
# Codeforces API client (module-private)
# ---------------------------------------------------------------------------


class _CodeforcesAPI:
    """Throttled, retrying, optionally signed calls to the Codeforces JSON API."""

    def __init__(self, http: httpx.Client, api_key: str | None, api_secret: str | None):
        self._http = http
        self._api_key = api_key
        self._api_secret = api_secret
        self._last_call: float | None = None

    def call(self, method: str, **params: Any) -> Any:
        """Call ``method`` and return its ``result``.

        "Call limit exceeded", 429/5xx and network errors are retried with
        backoff; any other ``FAILED`` response raises ``CodeforcesAPIError``.
        """
        attempt = 0
        while True:
            self._throttle()
            try:
                response = self._http.get(f"{API_URL}/{method}", params=self._sign(method, params))
                payload = response.json()
            except (httpx.TransportError, ValueError) as exc:
                # Network failure, or a non-JSON body (an HTML error page).
                error: Exception = exc
            else:
                if payload.get("status") == "OK":
                    return payload.get("result")
                comment = payload.get("comment") or f"HTTP {response.status_code}"
                if not (_is_call_limit(comment) or response.status_code in (429, 502, 503, 504)):
                    raise CodeforcesAPIError(f"{method}: {comment}")
                error = CodeforcesAPIError(f"{method}: {comment}")
            attempt += 1
            if attempt == _MAX_RETRIES:
                raise error
            delay = _MIN_INTERVAL * 2**attempt
            logger.warning(
                "Codeforces: %s - retrying in %.0fs (%d/%d).", error, delay, attempt, _MAX_RETRIES
            )
            time.sleep(delay)

    def _throttle(self) -> None:
        if self._last_call is not None:
            wait = _MIN_INTERVAL - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
        self._last_call = time.monotonic()

    def _sign(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Add ``apiKey``/``time``/``apiSig`` when a key is configured (see /apiHelp)."""
        params = {key: str(value) for key, value in params.items()}
        if not self._api_key:
            return params
        params["apiKey"] = self._api_key
        params["time"] = str(int(time.time()))
        rand = f"{secrets.randbelow(900_000) + 100_000}"
        query = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
        digest = hashlib.sha512(f"{rand}/{method}?{query}#{self._api_secret}".encode()).hexdigest()
        params["apiSig"] = rand + digest
        return params


def _is_call_limit(comment: str) -> bool:
    return "call limit exceeded" in comment.lower()


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------


def _fetch_rows(api: _CodeforcesAPI, handles: list[str]) -> list[dict]:
    """Fetch every handle and render the full snapshot of document rows."""
    users = _fetch_users(api, handles)
    rows: list[dict] = []
    for user in users:
        handle = user["handle"]
        submissions = api.call("user.status", handle=handle) or []
        rating_changes = api.call("user.rating", handle=handle) or []
        problems = _group_by_problem(submissions)
        rows.extend(_problem_row(handle, problem, attempts) for problem, attempts in problems)
        rows.extend(_contest_row(handle, change, submissions) for change in rating_changes)
        rows.append(_profile_row(user, problems, rating_changes))
    return rows


def _fetch_users(api: _CodeforcesAPI, handles: list[str]) -> list[dict]:
    """``user.info`` for all handles in one call; an unknown handle raises.

    Raising here, before anything is yielded, keeps a typo in the config from
    syncing an empty snapshot that would forget the handle's existing memory.
    """
    try:
        return api.call("user.info", handles=";".join(handles))
    except CodeforcesAPIError as exc:
        if "not found" in str(exc):
            raise ValueError(f"Unknown Codeforces handle - {exc}") from exc
        raise


def _group_by_problem(submissions: list[dict]) -> list[tuple[dict, list[dict]]]:
    """Group submissions by problem, attempts oldest first, problems by first attempt."""
    groups: dict[str, tuple[dict, list[dict]]] = {}
    for submission in submissions:
        problem = submission.get("problem") or {}
        groups.setdefault(_problem_key(problem), (problem, []))[1].append(submission)
    for _, attempts in groups.values():
        attempts.sort(key=_attempt_order)
    return sorted(groups.values(), key=lambda group: _attempt_order(group[1][0]))


def _attempt_order(submission: dict) -> tuple[int, int]:
    return (submission.get("creationTimeSeconds", 0), submission.get("id", 0))


def _problem_key(problem: dict) -> str:
    """``1950A`` for contest problems; problemset problems use the set name."""
    owner = problem.get("contestId") or problem.get("problemsetName") or "unknown"
    return f"{owner}{problem.get('index', '')}"


def _problem_url(problem: dict) -> str | None:
    contest_id, index = problem.get("contestId"), problem.get("index")
    if not index:
        return None
    if contest_id:
        if contest_id >= _GYM_CONTEST_ID:
            return f"{SITE_URL}/gym/{contest_id}/problem/{index}"
        return f"{SITE_URL}/problemset/problem/{contest_id}/{index}"
    if problem.get("problemsetName"):
        return f"{SITE_URL}/problemsets/{problem['problemsetName']}/problem/99999/{index}"
    return None


# ---------------------------------------------------------------------------
# Row rendering. Rows hold only stable fields (no last-online time, no
# contribution), so an unchanged history keeps the same content hash.
# ---------------------------------------------------------------------------


def _problem_row(handle: str, problem: dict, attempts: list[dict]) -> dict:
    key = _problem_key(problem)
    name = problem.get("name") or key
    solved_on = _first_accepted(attempts)

    lines = [f'Codeforces handle {handle} attempted problem {key} "{name}".']
    if problem.get("rating"):
        lines.append(f"Difficulty rating: {problem['rating']}.")
    if problem.get("tags"):
        lines.append(f"Tags: {', '.join(problem['tags'])}.")
    if solved_on is None:
        lines.append(f"Status: not solved after {_plural(len(attempts), 'attempt')}.")
    elif solved_on == 1:
        lines.append("Status: solved on the first attempt.")
    else:
        lines.append(f"Status: solved on attempt {solved_on} of {len(attempts)}.")
    lines.append("")
    lines.append("Attempts, oldest first:")
    lines.extend(f"{n}. {_describe_attempt(s)}" for n, s in enumerate(attempts, 1))

    return _row(
        f"{handle}:problem:{key}",
        f"{handle} - Codeforces {key} {name}",
        "\n".join(lines),
        _problem_url(problem),
        handle,
    )


def _describe_attempt(submission: dict) -> str:
    verdict_code = submission.get("verdict") or "TESTING"
    verdict = _VERDICTS.get(verdict_code, verdict_code.replace("_", " ").capitalize())
    if verdict_code in _TEST_VERDICTS and submission.get("passedTestCount") is not None:
        verdict += f" on test {submission['passedTestCount'] + 1}"

    parts = [_date(submission.get("creationTimeSeconds")), verdict]
    if submission.get("programmingLanguage"):
        parts.append(submission["programmingLanguage"])
    if submission.get("timeConsumedMillis") is not None:
        memory_kb = (submission.get("memoryConsumedBytes") or 0) // 1024
        parts.append(f"{submission['timeConsumedMillis']} ms, {memory_kb} KB")
    participant = (submission.get("author") or {}).get("participantType")
    if participant in _PARTICIPATION:
        parts.append(_PARTICIPATION[participant])
    return " - ".join(parts)


def _contest_row(handle: str, change: dict, submissions: list[dict]) -> dict:
    contest_id = change.get("contestId")
    name = change.get("contestName") or f"Contest {contest_id}"
    old, new = change.get("oldRating"), change.get("newRating")

    lines = [
        f"Codeforces handle {handle} competed in {name} (contest {contest_id}) "
        f"on {_date(change.get('ratingUpdateTimeSeconds'), with_time=False)}.",
        f"Rank: {change.get('rank')}.",
    ]
    if old is not None and new is not None:
        lines.append(f"Rating changed from {old} to {new} ({new - old:+d}).")

    # Problems attempted as a contestant in this contest, with their outcome.
    in_contest = [
        s
        for s in submissions
        if s.get("contestId") == contest_id
        and (s.get("author") or {}).get("participantType") == "CONTESTANT"
    ]
    if in_contest:
        lines.append("")
        lines.append("Problems attempted during the contest:")
        for problem, attempts in _group_by_problem(in_contest):
            solved = _first_accepted(attempts) is not None
            outcome = "solved" if solved else "not solved"
            lines.append(
                f'- {problem.get("index")} "{problem.get("name")}": {outcome}, '
                f"{_plural(len(attempts), 'submission')}"
            )

    return _row(
        f"{handle}:contest:{contest_id}",
        f"{handle} - {name}",
        "\n".join(lines),
        f"{SITE_URL}/contest/{contest_id}" if contest_id else None,
        handle,
    )


def _profile_row(
    user: dict, problems: list[tuple[dict, list[dict]]], rating_changes: list[dict]
) -> dict:
    handle = user["handle"]
    lines = [f"Codeforces profile of handle {handle}."]
    if user.get("rating") is not None:
        lines.append(f"Current rating: {user['rating']} ({user.get('rank', 'unranked')}).")
        lines.append(f"Max rating: {user.get('maxRating')} ({user.get('maxRank', 'unranked')}).")
    else:
        lines.append("Unrated: has not taken part in a rated contest.")
    if user.get("registrationTimeSeconds"):
        lines.append(f"Registered on {_date(user['registrationTimeSeconds'], with_time=False)}.")
    lines.append(f"Rated contests: {len(rating_changes)}.")

    solved = [(p, a) for p, a in problems if _first_accepted(a) is not None]
    unsolved = [(p, a) for p, a in problems if _first_accepted(a) is None]
    first_try = [(p, a) for p, a in solved if _first_accepted(a) == 1]
    submissions = sum(len(a) for _, a in problems)
    lines.append(
        f"Problems attempted: {len(problems)} ({submissions} submissions); "
        f"solved: {len(solved)}; solved on the first try: {len(first_try)}; "
        f"never solved: {len(unsolved)}."
    )
    if unsolved:
        lines.append(
            "Never solved: "
            + ", ".join(f'{_problem_key(p)} "{p.get("name")}"' for p, _ in unsolved)
            + "."
        )

    attempted, solved_tags, first_try_tags = Counter(), Counter(), Counter()
    for problem, attempts in problems:
        accepted_on = _first_accepted(attempts)
        for tag in problem.get("tags") or []:
            attempted[tag] += 1
            solved_tags[tag] += accepted_on is not None
            first_try_tags[tag] += accepted_on == 1
    if attempted:
        lines.append("")
        lines.append("By tag (problems attempted / solved / solved on the first try):")
        for tag, count in sorted(attempted.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- {tag}: {count} / {solved_tags[tag]} / {first_try_tags[tag]}")

    return _row(
        f"{handle}:profile",
        f"{handle} - Codeforces profile",
        "\n".join(lines),
        f"{SITE_URL}/profile/{handle}",
        handle,
    )


def _row(row_id: str, title: str, content: str, url: str | None, handle: str) -> dict:
    # The node set lets a search be scoped to one handle (codeforces:<handle>).
    return {"id": row_id, "title": title, "content": content, "url": url, NODE_SET_COLUMN: [handle]}


def _first_accepted(attempts: list[dict]) -> int | None:
    """1-based number of the first accepted attempt, or None if never accepted."""
    for n, submission in enumerate(attempts, 1):
        if submission.get("verdict") == "OK":
            return n
    return None


def _date(seconds: int | None, with_time: bool = True) -> str:
    if not seconds:
        return "unknown date"
    moment = datetime.fromtimestamp(seconds, UTC)
    return moment.strftime("%Y-%m-%d %H:%M UTC" if with_time else "%Y-%m-%d")


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"
