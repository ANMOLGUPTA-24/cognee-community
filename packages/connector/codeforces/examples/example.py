"""Codeforces connector demo - ask questions about your own practice history.

``codeforces_source`` returns a ``dlt`` source you hand straight to
``cognee.remember``. Each problem you attempted becomes one document with all
your attempts on it, each rated contest becomes one, and your profile adds
solved/unsolved and per-tag first-try tallies.

Each run is a full snapshot: unchanged documents keep a stable id and are not
re-cognified, verdicts that changed after a contest (hacked, skipped) are
updated, and a handle you remove from the list is forgotten on the next sync.

Codeforces data is public, so no API key is needed. Run:

    export LLM_API_KEY="sk-..."
    export CODEFORCES_HANDLES="anmol24"      # comma-separated
    uv run python examples/example.py

Syncing takes a few seconds per handle: the API allows about one request every
two seconds, and each handle needs two requests.
"""

import asyncio
import os

import cognee

from cognee_community_connector_codeforces import codeforces_source

# Keep Codeforces in its own dataset so it is easy to inspect and forget.
DATASET_NAME = "codeforces"

QUESTIONS = (
    "Which tags do I fail most often on the first try?",
    "Which problems did I attempt but never solve?",
)


async def main() -> None:
    if not os.environ.get("CODEFORCES_HANDLES"):
        print("Set CODEFORCES_HANDLES (e.g. CODEFORCES_HANDLES=anmol24) to run this example.")
        return

    print("Syncing Codeforces history into cognee ...")
    await cognee.remember(codeforces_source(), dataset_name=DATASET_NAME)

    for question in QUESTIONS:
        answer = await cognee.search(
            query_text=question,
            query_type=cognee.SearchType.GRAPH_COMPLETION,
            datasets=[DATASET_NAME],
        )
        print(f"\nQ: {question}\nA: {answer}")


if __name__ == "__main__":
    asyncio.run(main())
