"""Model definition changes must invalidate the entities that use them."""

import pytest

from mindroom.config.agent import AgentConfig, TeamConfig
from mindroom.config.main import Config
from mindroom.config.models import CompactionConfig, CompactionOverrideConfig, DefaultsConfig, ModelConfig, RouterConfig
from mindroom.constants import ROUTER_AGENT_NAME
from mindroom.orchestration.config_updates import build_config_update_plan, configured_entity_names


def _config() -> Config:
    return Config(
        models={
            name: ModelConfig(provider="openai", id="test-model", context_window=64000)
            for name in ("default", "other", "summary", "fallback", "unused")
        },
        agents={
            "general": AgentConfig(display_name="General"),
            "unaffected": AgentConfig(display_name="Unaffected", model="other"),
        },
        teams={"team": TeamConfig(display_name="Team", role="Coordinate", agents=["general"], model="default")},
        router=RouterConfig(model="default"),
    )


def _restart_set(old: Config, new: Config, existing: set[str] | None = None) -> set[str]:
    entities = set(configured_entity_names(new))
    plan = build_config_update_plan(
        current_config=old,
        new_config=new,
        configured_entities=entities,
        existing_entities=entities if existing is None else existing,
        agent_bots={},
    )
    return plan.entities_to_restart


@pytest.mark.parametrize(
    ("field", "value"),
    [("context_window", 128000), ("id", "replacement-model"), ("extra_kwargs", {"temperature": 0.5})],
)
def test_model_definition_change_restarts_only_referencing_entities(field: str, value: object) -> None:
    """A definition edit reaches default agents, teams and the router, but not another model."""
    old = _config()
    new = old.model_copy(deep=True)
    new.models["default"] = new.models["default"].model_copy(update={field: value})

    assert _restart_set(old, new) == {"general", "team", ROUTER_AGENT_NAME}


@pytest.mark.parametrize("field", ["model", "fallback_model"])
@pytest.mark.parametrize("inherited", [False, True])
def test_compaction_model_definition_change_respects_effective_overrides(field: str, *, inherited: bool) -> None:
    """Summary and fallback references include inheritance and respect explicit null overrides."""
    old = _config()
    override = CompactionOverrideConfig.model_validate({field: "summary"})
    if inherited:
        old.defaults = DefaultsConfig(compaction=CompactionConfig.model_validate({field: "summary"}))
        old.agents["unaffected"].compaction = CompactionOverrideConfig.model_validate({field: None})
    else:
        old.agents["general"].compaction = override
        old.teams["team"].compaction = override
    new = old.model_copy(deep=True)
    new.models["summary"].context_window = 128000

    assert _restart_set(old, new) == {"general", "team"}


def test_unused_model_changes_do_not_restart_entities() -> None:
    """Editing, adding, or removing unused definitions leaves running bots alone."""
    old = _config()
    new = old.model_copy(deep=True)
    new.models.pop("unused")
    new.models["added"] = ModelConfig(provider="openai", id="new-model")
    new.models["summary"].context_window = 128000

    assert _restart_set(old, new) == set()


def test_model_change_does_not_restart_entities_that_are_not_running() -> None:
    """Absent entities retain normal startup planning when a definition changes."""
    old = _config()
    new = old.model_copy(deep=True)
    new.models["default"].context_window = 128000

    assert _restart_set(old, new, existing={"unaffected"}) == set()


def test_unchanged_models_preserve_room_only_reconciliation() -> None:
    """Room edits still reconcile memberships without restarting bots."""
    old = _config()
    new = old.model_copy(deep=True)
    new.agents["general"].rooms = ["new-room"]

    assert _restart_set(old, new) == set()


def test_model_changes_preserve_model_less_team_compaction_references() -> None:
    """A team without a reply model can still reference a summary model."""
    old = _config()
    old.teams["team"].model = None
    old.teams["team"].compaction = CompactionOverrideConfig(model="summary")
    new = old.model_copy(deep=True)
    new.models["summary"].context_window = 128000

    assert _restart_set(old, new) == {"team"}


def test_model_changes_preserve_existing_agent_change_detection() -> None:
    """A simultaneous unrelated model edit cannot hide an agent's own changes."""
    old = _config()
    new = old.model_copy(deep=True)
    new.models["unused"].context_window = 128000
    new.agents["unaffected"].display_name = "Renamed"

    assert _restart_set(old, new) == {"unaffected"}
