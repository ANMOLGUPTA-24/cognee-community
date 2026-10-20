# cognee-community-connector-codeforces

A Codeforces data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync your practice history into memory and ask things like "which tags do I fail most on
the first try?" or "which problems did I never solve?".

It exposes a `dlt` source you hand to `cognee.remember(...)` / `cognee.add(...)`. Rows are
ingested as **normal documents** through cognee's document mode, so they go through
cognify entity extraction rather than the deterministic dlt-row path.

## Install

```bash
uv pip install cognee-community-connector-codeforces
# or, from this monorepo:
cd packages/connector/codeforces && uv sync
```

## Usage

```python
import cognee
from cognee_community_connector_codeforces import codeforces_source

await cognee.remember(
    codeforces_source(handles=["anmol24"]),  # or set CODEFORCES_HANDLES
    dataset_name="codeforces",
)

answer = await cognee.search(
    query_text="Which tags do I fail most often on the first try?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["codeforces"],
)
```

See `examples/example.py` for the full flow.

## What gets ingested

For each handle, from the public [Codeforces API](https://codeforces.com/apiHelp)
(`user.info`, `user.status`, `user.rating`):

| Document | Content |
| --- | --- |
| one per problem attempted | name, contest + index, rating, tags, whether and on which attempt it was solved, and every attempt oldest first: verdict, the test it failed on, language, time, memory, practice or in contest |
| one per rated contest | contest name, rank, old → new rating, and the problems attempted during it |
| one profile | current/max rating and rank (or unrated), solved / unsolved / first-try counts, the list of never-solved problems, and a per-tag attempted / solved / first-try table |

Each document links back to its problem, contest or profile page. Problem statements are
not available through the API, so they are not ingested (no HTML scraping).

Every document carries the node set `codeforces:<handle>`, so a search can be scoped to
one handle when you sync several.

## How sync and forget-on-delete work

The source is a **full snapshot**: `write_disposition="replace"` rewrites staging with the
current history on every run. Verdicts on Codeforces can change after a contest (an
accepted solution can be hacked, or skipped), so an append-only sync would keep stale
verdicts. A handle's whole history is a single `user.status` call, so re-fetching is
cheap, and unchanged documents keep a stable content-hash `data_id`, so they are not
re-cognified.

A handle you remove from the list (or anything that disappears upstream) is absent from
the snapshot, and cognee's `orphan_cleanup` removes it from the graph and vector stores.

**Errors never delete.** Every handle is checked with `user.info` before anything is
fetched, so an unknown or mistyped handle raises instead of syncing an empty snapshot.
The whole snapshot is fetched before the first row is written, so an API failure part-way
aborts the run and leaves memory as it was.

## Configuration

| Argument | Environment variable | |
| --- | --- | --- |
| `handles` | `CODEFORCES_HANDLES` (comma-separated) | required |
| `api_key` | `CODEFORCES_API_KEY` | optional |
| `api_secret` | `CODEFORCES_API_SECRET` | optional, required with a key |

Public data needs no key. With a key and secret (from
[codeforces.com/settings/api](https://codeforces.com/settings/api)) every call is signed
with `apiSig`.

Codeforces allows about one request every two seconds, so calls are throttled to that
rate, and "Call limit exceeded", 429/5xx and network errors are retried with exponential
backoff.

## Testing

```bash
uv run pytest tests/
```

The tests run without network or an LLM key. The Codeforces API is an in-memory fake
behind `httpx.MockTransport`, with response shapes recorded from the live API. They cover
grouping attempts per problem, contest and profile documents, throttling, call-limit
retries, request signing, a verdict changing on re-sync, a removed handle being
forgotten, and an unknown handle or API failure leaving memory untouched.
`tests/test_codeforces_forget.py` runs the real cognee pipeline end to end with a mocked
LLM.
