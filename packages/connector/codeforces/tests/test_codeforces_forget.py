"""End-to-end: Codeforces changes must reach cognee's graph, not just dlt rows.

Runs the real cognee pipeline (dlt replace -> document ingestion -> cognify)
against the fake Codeforces API, with a mocked LLM and mocked embeddings, so
no network or API key is needed. Each handle solves one problem with a
distinctive name, which the mocked LLM turns into exactly one graph entity.
"""

import importlib

import cognee
import pytest
import pytest_asyncio
from cognee.infrastructure.databases.graph import get_graph_engine
from cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine import (
    LiteLLMEmbeddingEngine,
)
from cognee.infrastructure.llm import LLMGateway
from fake_codeforces import FakeCodeforces, problem

from cognee_community_connector_codeforces import codeforces, codeforces_source

add_data_points_module = importlib.import_module("cognee.tasks.storage.add_data_points")

DATASET = "codeforces_forget_test"
ALPHA = "Alphacorp"
BRAVO = "Bravocorp"
TOKENS = (ALPHA, BRAVO)


def _with_required(model, **values):
    """Build a cognee data model, filling any other required text field."""
    for field_name, field in model.model_fields.items():
        if field.is_required() and field_name not in values:
            values[field_name] = "mock"
    return model(**values)


async def _mock_structured_output(
    text_input=None, system_prompt=None, response_model=str, **_kwargs
):
    """Extract one entity named after whichever token appears in the chunk text."""
    from cognee.shared.data_models import KnowledgeGraph, SummarizedContent
    from cognee.shared.data_models import Node as KGNode

    if response_model is str:
        return "Mocked answer."
    if response_model == SummarizedContent:
        return SummarizedContent(summary="Mock summary", description="Mock summary")
    if response_model == KnowledgeGraph:
        name = next((t for t in TOKENS if text_input and t in text_input), None)
        nodes = (
            [_with_required(KGNode, id=name, name=name, type="Problem", description=name)]
            if name
            else []
        )
        return _with_required(KnowledgeGraph, nodes=nodes, edges=[])
    return response_model()


async def _graph_has(token: str) -> bool:
    nodes, _ = await (await get_graph_engine()).get_graph_data()
    token = token.lower()
    return any(
        token in str(value).lower() for _nid, props in nodes for value in (props or {}).values()
    )


@pytest_asyncio.fixture
async def clean_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("COGNEE_SKIP_CONNECTION_TEST", "true")
    monkeypatch.setattr(codeforces, "_MIN_INTERVAL", 0)
    cognee.config.data_root_directory(str(tmp_path / "data"))
    cognee.config.system_root_directory(str(tmp_path / "system"))
    cognee.config.set_relational_db_config({"db_provider": "sqlite"})

    async def _noop_index(*_a, **_k):
        return None

    monkeypatch.setattr(add_data_points_module, "index_data_points", _noop_index)
    monkeypatch.setattr(add_data_points_module, "index_graph_edges", _noop_index)
    monkeypatch.setattr(LLMGateway, "acreate_structured_output", _mock_structured_output)

    async def _mock_embed_text(self, text):
        return [[0.0] * self.get_vector_size() for _ in text]

    monkeypatch.setattr(LiteLLMEmbeddingEngine, "embed_text", _mock_embed_text)

    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)
    yield
    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)


async def _sync(fake, handles):
    await cognee.add(codeforces_source(handles=handles, client=fake.client()), dataset_name=DATASET)
    await cognee.cognify(datasets=[DATASET])


@pytest.mark.asyncio
async def test_removed_handle_is_forgotten_and_unknown_handle_changes_nothing(clean_environment):
    fake = FakeCodeforces()
    fake.add_user("anmol24")
    fake.add_user("tourist")
    fake.submit("anmol24", problem(1, "A", f"{ALPHA} Watermelon"), "OK")
    fake.submit("tourist", problem(2, "B", f"{BRAVO} Trees"), "OK")

    await _sync(fake, ["anmol24", "tourist"])
    assert await _graph_has(ALPHA)
    assert await _graph_has(BRAVO)

    # A typo'd handle must abort the sync, not wipe anmol24's or tourist's memory.
    with pytest.raises(Exception, match="anmol42"):
        await _sync(fake, ["anmol24", "anmol42"])
    assert await _graph_has(ALPHA)
    assert await _graph_has(BRAVO), "a failed sync must not forget anything"

    # Dropping tourist from the config forgets tourist's documents.
    await _sync(fake, ["anmol24"])
    assert await _graph_has(ALPHA), "an untouched handle must stay"
    assert not await _graph_has(BRAVO), "a removed handle must be forgotten from the graph"
