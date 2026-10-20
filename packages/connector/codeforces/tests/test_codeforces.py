"""Unit and dlt-pipeline tests for the Codeforces connector.

No network: the API is an in-memory fake behind ``httpx.MockTransport``, and
``time.sleep`` is replaced so throttling and backoff are recorded, not waited.

* Rendering: grouping attempts per problem, failed-test numbers, contest and
  profile documents, problem links.
* API client: throttling, call-limit retry, signing, error classification.
* dlt pipeline (temp sqlite): a verdict change on re-sync, a removed handle,
  and an unknown handle or API failure leaving the previous snapshot intact.
"""

import hashlib
from types import SimpleNamespace
from uuid import NAMESPACE_OID, uuid5

import httpx
import pytest
from fake_codeforces import FakeCodeforces, failed, ok, problem

from cognee_community_connector_codeforces import codeforces
from cognee_community_connector_codeforces.codeforces import (
    CODEFORCES_SOURCE_NAME,
    CodeforcesAPIError,
    _CodeforcesAPI,
    _fetch_rows,
    _problem_url,
    _resolve_handles,
    codeforces_source,
)

WATERMELON = problem(4, "A", "Watermelon", 800, ("brute force", "math"))
WORDS = problem(71, "A", "Way Too Long Words", 800, ("strings",))
TREES = problem(1950, "D", "Tree Queries", 1900, ("trees", "dfs and similar"))


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    """Record time.sleep calls instead of sleeping."""
    slept: list[float] = []
    monkeypatch.setattr(codeforces.time, "sleep", slept.append)
    return slept


@pytest.fixture
def cf():
    fake = FakeCodeforces()
    fake.add_user("anmol24")
    return fake


def rows_for(fake, handles=("anmol24",)):
    api = _CodeforcesAPI(fake.client(), None, None)
    return {row["id"]: row for row in _fetch_rows(api, list(handles))}


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def test_attempts_on_one_problem_become_one_document(cf):
    cf.submit("anmol24", WORDS, "WRONG_ANSWER", passed=6, day=0)
    cf.submit("anmol24", WORDS, "WRONG_ANSWER", passed=6, day=1)
    cf.submit("anmol24", WORDS, "WRONG_ANSWER", passed=6, day=2)
    cf.submit("anmol24", WORDS, "OK", day=3)

    rows = rows_for(cf)
    doc = rows["anmol24:problem:71A"]

    assert set(rows) == {"anmol24:problem:71A", "anmol24:profile"}
    assert doc["title"] == "anmol24 - Codeforces 71A Way Too Long Words"
    assert doc["url"] == "https://codeforces.com/problemset/problem/71/A"
    assert "Status: solved on attempt 4 of 4." in doc["content"]
    assert "Tags: strings." in doc["content"]
    # Oldest first, even though the API returns newest first.
    attempts = doc["content"].split("Attempts, oldest first:\n")[1].splitlines()
    assert [("Wrong answer on test 7" in a) for a in attempts] == [True, True, True, False]
    assert "Accepted" in attempts[3]


def test_unsolved_and_first_try_status(cf):
    cf.submit("anmol24", WATERMELON, "OK")
    cf.submit("anmol24", TREES, "TIME_LIMIT_EXCEEDED", passed=11)
    cf.submit("anmol24", TREES, "COMPILATION_ERROR", passed=0)

    rows = rows_for(cf)

    assert "Status: solved on the first attempt." in rows["anmol24:problem:4A"]["content"]
    trees = rows["anmol24:problem:1950D"]["content"]
    assert "Status: not solved after 2 attempts." in trees
    assert "Time limit exceeded on test 12" in trees
    # A compilation error happens on no test.
    assert "Compilation error - " in trees and "Compilation error on test" not in trees


def test_profile_tallies_tags_and_unsolved(cf):
    cf.submit("anmol24", WATERMELON, "OK")
    cf.submit("anmol24", WORDS, "WRONG_ANSWER", passed=0)
    cf.submit("anmol24", WORDS, "OK")
    cf.submit("anmol24", TREES, "WRONG_ANSWER", passed=3)

    profile = rows_for(cf)["anmol24:profile"]["content"]

    assert "Unrated: has not taken part in a rated contest." in profile
    assert (
        "Problems attempted: 3 (4 submissions); solved: 2; solved on the first try: 1; "
        "never solved: 1." in profile
    )
    assert 'Never solved: 1950D "Tree Queries".' in profile
    assert "- strings: 1 / 1 / 0" in profile
    assert "- math: 1 / 1 / 1" in profile
    assert "- trees: 1 / 0 / 0" in profile


def test_rated_contest_document_lists_contest_problems(cf):
    cf.add_user(
        "tourist",
        rating=3500,
        maxRating=4000,
        rank="legendary grandmaster",
        maxRank="legendary grandmaster",
    )
    a = problem(1950, "A", "Stair, Peak, or Neither?")
    cf.submit("tourist", a, "OK", participant="CONTESTANT")
    cf.submit("tourist", TREES, "WRONG_ANSWER", passed=2, participant="CONTESTANT")
    cf.submit("tourist", TREES, "OK", participant="PRACTICE")  # upsolved later
    cf.rate("tourist", 1950, "Codeforces Round 937 (Div. 4)", 3, 3450, 3500)

    rows = rows_for(cf, ["tourist"])
    contest = rows["tourist:contest:1950"]

    assert contest["url"] == "https://codeforces.com/contest/1950"
    assert "Rank: 3." in contest["content"]
    assert "Rating changed from 3450 to 3500 (+50)." in contest["content"]
    assert '- A "Stair, Peak, or Neither?": solved, 1 submission' in contest["content"]
    # Only the in-contest attempt counts towards the contest.
    assert '- D "Tree Queries": not solved, 1 submission' in contest["content"]
    profile = rows["tourist:profile"]["content"]
    assert "Current rating: 3500 (legendary grandmaster)." in profile
    assert "Rated contests: 1." in profile


def test_rows_are_stable_and_carry_the_handle_node_set(cf):
    cf.submit("anmol24", WATERMELON, "OK")
    first, second = rows_for(cf), rows_for(cf)

    # Same history -> identical rows, so the content-hash data_id is stable.
    assert first == second
    assert all(row["cognee_node_set"] == ["anmol24"] for row in first.values())
    # Volatile profile fields never reach the row.
    cf.users["anmol24"]["lastOnlineTimeSeconds"] += 999
    assert rows_for(cf) == first


def test_problem_urls():
    assert _problem_url(WATERMELON) == "https://codeforces.com/problemset/problem/4/A"
    assert (
        _problem_url(problem(104_000, "B", "Gym")) == "https://codeforces.com/gym/104000/problem/B"
    )
    sgu = {"problemsetName": "acmsguru", "index": "100", "name": "A+B"}
    assert _problem_url(sgu) == "https://codeforces.com/problemsets/acmsguru/problem/99999/100"
    assert _problem_url({"name": "no index"}) is None


def test_handle_resolution(monkeypatch):
    assert _resolve_handles([" anmol24 ", "Anmol24", "tourist", ""]) == ["anmol24", "tourist"]
    monkeypatch.setenv("CODEFORCES_HANDLES", "anmol24, tourist")
    assert _resolve_handles(None) == ["anmol24", "tourist"]
    monkeypatch.setenv("CODEFORCES_HANDLES", "")
    with pytest.raises(ValueError, match="handles required"):
        _resolve_handles(None)


def test_key_without_secret_is_rejected():
    with pytest.raises(ValueError, match="key and its secret"):
        codeforces_source(handles=["anmol24"], api_key="key")


def test_source_declares_document_marker_and_routes_through_cognify():
    from cognee.tasks.ingestion.dlt_utils import document_source_tag
    from cognee.tasks.ingestion.resolve_dlt_sources import _build_document_data_item

    source = codeforces_source(handles=["anmol24"])
    assert document_source_tag(source) == CODEFORCES_SOURCE_NAME == "codeforces"

    row = SimpleNamespace(
        row_data={
            "id": "anmol24:problem:4A",
            "title": "anmol24 - Codeforces 4A Watermelon",
            "content": "Status: solved on the first attempt.",
            "url": "https://codeforces.com/problemset/problem/4/A",
            "cognee_node_set": '["anmol24"]',
        },
        content_hash="abc",
        table_name="codeforces_documents",
        primary_key_value="anmol24:problem:4A",
    )
    item = _build_document_data_item(row, uuid5(NAMESPACE_OID, "x"), "codeforces")

    assert item.system_metadata["source"] == "codeforces"
    assert item.system_metadata["external_id"] == "anmol24:problem:4A"
    assert item.data.startswith("# anmol24 - Codeforces 4A Watermelon")
    assert item.node_set == ["codeforces:anmol24"]


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


def test_calls_are_throttled(cf, sleeps, monkeypatch):
    clock = iter([100.0, 100.5, 100.5])
    monkeypatch.setattr(codeforces.time, "monotonic", lambda: next(clock))
    api = _CodeforcesAPI(cf.client(), None, None)

    api.call("user.status", handle="anmol24")
    api.call("user.rating", handle="anmol24")

    # Second call came 0.5s after the first, so it waits out the 2s window.
    assert sleeps == [1.5]


def test_call_limit_is_retried_with_backoff(cf, sleeps, monkeypatch):
    monkeypatch.setattr(codeforces, "_MIN_INTERVAL", 0)
    cf.queued = [failed("Call limit exceeded", 503), httpx.Response(502, content=b"<html>")]
    api = _CodeforcesAPI(cf.client(), None, None)

    assert api.call("user.status", handle="anmol24") == []
    assert len(cf.calls) == 3
    assert len(sleeps) == 2


def test_retries_are_bounded(cf, sleeps, monkeypatch):
    monkeypatch.setattr(codeforces, "_MIN_INTERVAL", 0)
    cf.queued = [failed("Call limit exceeded", 503)] * 10
    api = _CodeforcesAPI(cf.client(), None, None)

    with pytest.raises(CodeforcesAPIError, match="Call limit exceeded"):
        api.call("user.status", handle="anmol24")
    assert len(cf.calls) == codeforces._MAX_RETRIES


def test_permanent_failure_is_not_retried(cf):
    cf.queued = [failed("handle: Field should not be empty", 400)]
    api = _CodeforcesAPI(cf.client(), None, None)

    with pytest.raises(CodeforcesAPIError, match="should not be empty"):
        api.call("user.status", handle="")
    assert len(cf.calls) == 1


def test_network_errors_are_retried(sleeps, monkeypatch):
    monkeypatch.setattr(codeforces, "_MIN_INTERVAL", 0)
    attempts = []

    def flaky(request):
        attempts.append(request)
        if len(attempts) == 1:
            raise httpx.ConnectError("boom")
        return ok([])

    api = _CodeforcesAPI(httpx.Client(transport=httpx.MockTransport(flaky)), None, None)
    assert api.call("user.rating", handle="anmol24") == []
    assert len(attempts) == 2


def test_signed_calls(cf, monkeypatch):
    monkeypatch.setattr(codeforces.time, "time", lambda: 1_700_000_000)
    monkeypatch.setattr(codeforces.secrets, "randbelow", lambda _: 23_456)  # rand = 123456
    api = _CodeforcesAPI(cf.client(), "xxx", "yyy")

    api.call("user.status", handle="anmol24")

    _, params = cf.calls[0]
    expected = hashlib.sha512(
        b"123456/user.status?apiKey=xxx&handle=anmol24&time=1700000000#yyy"
    ).hexdigest()
    assert params["apiKey"] == "xxx"
    assert params["time"] == "1700000000"
    assert params["apiSig"] == "123456" + expected


def test_unknown_handle_raises_before_anything_is_fetched(cf):
    with pytest.raises(ValueError, match=r"Unknown Codeforces handle.*anmol42"):
        rows_for(cf, ["anmol24", "anmol42"])
    assert [method for method, _ in cf.calls] == ["user.info"]


# ---------------------------------------------------------------------------
# dlt pipeline: full snapshot, verdict changes, removed and unknown handles
# ---------------------------------------------------------------------------


def _pipeline(dlt, tmp_path):
    return dlt.pipeline(
        pipeline_name="codeforces_test",
        destination=dlt.destinations.sqlalchemy(f"sqlite:///{(tmp_path / 'cf.db').as_posix()}"),
        dataset_name="codeforces_ds",
        pipelines_dir=str(tmp_path / "state"),
    )


def _sync(dlt, tmp_path, fake, handles):
    pipeline = _pipeline(dlt, tmp_path)
    pipeline.run(codeforces_source(handles=handles, client=fake.client()))
    return pipeline


def _staged(pipeline) -> dict[str, str]:
    with (
        pipeline.sql_client() as client,
        client.execute_query("SELECT id, content FROM codeforces_documents") as cursor,
    ):
        return dict(cursor.fetchall())


@pytest.fixture
def dlt_mod(monkeypatch):
    monkeypatch.setattr(codeforces, "_MIN_INTERVAL", 0)
    return pytest.importorskip("dlt")


def test_verdict_change_is_reflected_on_resync(dlt_mod, tmp_path, cf):
    accepted = cf.submit("anmol24", TREES, "OK", participant="CONTESTANT")
    _sync(dlt_mod, tmp_path, cf, ["anmol24"])

    # Hacked after the contest: the stored verdict must not stay "Accepted".
    accepted["verdict"] = "CHALLENGED"
    staged = _staged(_sync(dlt_mod, tmp_path, cf, ["anmol24"]))

    doc = staged["anmol24:problem:1950D"]
    assert "Hacked (challenged)" in doc
    assert "Accepted" not in doc
    assert "Status: not solved after 1 attempt." in doc


def test_removed_handle_drops_out_of_the_snapshot(dlt_mod, tmp_path, cf):
    cf.add_user("tourist")
    cf.submit("anmol24", WATERMELON, "OK")
    cf.submit("tourist", TREES, "OK")
    assert {"tourist:profile", "tourist:problem:1950D"} <= set(
        _staged(_sync(dlt_mod, tmp_path, cf, ["anmol24", "tourist"]))
    )

    staged = _staged(_sync(dlt_mod, tmp_path, cf, ["anmol24"]))

    # Absent from staging -> cognee's orphan cleanup forgets it downstream.
    assert set(staged) == {"anmol24:problem:4A", "anmol24:profile"}


def test_unknown_handle_keeps_the_previous_snapshot(dlt_mod, tmp_path, cf):
    cf.submit("anmol24", WATERMELON, "OK")
    before = _staged(_sync(dlt_mod, tmp_path, cf, ["anmol24"]))

    with pytest.raises(Exception, match="anmol42"):  # dlt wraps it in PipelineStepFailed
        _sync(dlt_mod, tmp_path, cf, ["anmol24", "anmol42"])

    assert _staged(_pipeline(dlt_mod, tmp_path)) == before


def test_api_failure_mid_sync_keeps_the_previous_snapshot(dlt_mod, tmp_path, cf):
    cf.add_user("tourist")
    cf.submit("anmol24", WATERMELON, "OK")
    cf.submit("tourist", TREES, "OK")
    before = _staged(_sync(dlt_mod, tmp_path, cf, ["anmol24", "tourist"]))

    # user.info and anmol24 succeed, then the API goes down for good.
    cf.calls.clear()
    original = cf._handle
    cf._handle = lambda request: (
        failed("Internal error", 500) if len(cf.calls) >= 3 else original(request)
    )
    with pytest.raises(Exception, match="Internal error"):
        _sync(dlt_mod, tmp_path, cf, ["anmol24", "tourist"])

    assert _staged(_pipeline(dlt_mod, tmp_path)) == before
