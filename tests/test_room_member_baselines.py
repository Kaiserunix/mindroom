"""Nio membership snapshots suppress false onboarding without retiring owed hooks."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from mindroom.hooks import EVENT_ROOM_MEMBER_JOINED, HookRegistry, RoomMemberJoinedContext, hook
from mindroom.matrix import room_member_joins
from tests.test_durable_ingestion_runtime import ROOM, _consume_frame, _joined_frame, _owned_session
from tests.test_room_member_hooks import _plugin, _room_member_event, _router_bot

if TYPE_CHECKING:
    from pathlib import Path

    import nio


def _frame(
    token: str,
    *,
    timeline: tuple[nio.RoomMemberEvent, ...] = (),
    state: tuple[nio.RoomMemberEvent, ...] = (),
) -> bytes:
    frame = json.loads(_joined_frame([event.source for event in timeline]))
    frame["next_batch"] = token
    frame["rooms"]["join"][ROOM]["state"]["events"] = [event.source for event in state]
    return json.dumps(frame).encode()


def _registry(seen: list[str]) -> HookRegistry:
    @hook(EVENT_ROOM_MEMBER_JOINED)
    async def joined(context: RoomMemberJoinedContext) -> None:
        seen.append(context.event_id)

    return HookRegistry.from_plugins([_plugin("onboarding", [joined])])


@pytest.mark.asyncio
@pytest.mark.parametrize("baseline", ["timeline", "state", "restored_state"])
async def test_profile_update_after_baseline_does_not_onboard_existing_member(tmp_path: Path, baseline: str) -> None:
    """Actual Nio HISTORY/state baselines are silent; a new live member still gets a hook."""
    bot = _router_bot(tmp_path)
    seen: list[str] = []
    bot.hook_registry = _registry(seen)
    try:
        if baseline == "restored_state":
            async with _owned_session(bot) as session:
                await _consume_frame(bot, session, _frame("before-restart"))
        async with _owned_session(bot) as session:
            existing = _room_member_event(event_id="$baseline", prev_membership=None)
            frame = (
                _frame("baseline", timeline=(existing,))
                if baseline == "timeline"
                else _frame("baseline", state=(existing,))
            )
            await _consume_frame(bot, session, frame)
            await bot._journal_dispatcher.drain_once()
            assert seen == []
            profile = _room_member_event(event_id="$profile", prev_membership=None, display_name="Alice New")
            newcomer = _room_member_event(event_id="$new-join", user_id="@bob:localhost", prev_membership=None)
            await _consume_frame(bot, session, _frame("live", timeline=(profile, newcomer)))
            await bot._journal_dispatcher.drain_once()
            assert seen == ["$new-join"]
    finally:
        await bot._journal_store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot_event_id", ["$snapshot", "$genuine-join"])
async def test_snapshot_does_not_suppress_a_pending_genuine_join(tmp_path: Path, snapshot_event_id: str) -> None:
    """A state snapshot cannot turn an unfinished live hook into completed baseline history."""
    bot = _router_bot(tmp_path)
    seen: list[str] = []
    bot.hook_registry = _registry(seen)
    try:
        async with _owned_session(bot) as session:
            await _consume_frame(bot, session, _frame("initial"))
            joined = _room_member_event(event_id="$genuine-join", prev_membership=None)
            await _consume_frame(bot, session, _frame("joined", timeline=(joined,)))
            assert await bot.journal_principal().is_pending(joined.event_id)
            snapshot = _room_member_event(event_id=snapshot_event_id, prev_membership=None)
            await _consume_frame(bot, session, _frame("snapshot", state=(snapshot,)))
            await bot._journal_dispatcher.drain_once()
            assert seen == ["$genuine-join"]
            assert not await bot.journal_principal().is_pending(joined.event_id)
            profile = _room_member_event(event_id="$profile", prev_membership=None)
            await _consume_frame(bot, session, _frame("profile", timeline=(profile,)))
            await bot._journal_dispatcher.drain_once()
            assert seen == ["$genuine-join"]
    finally:
        await bot._journal_store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("baseline", ["timeline", "state"])
async def test_baseline_persistence_retries_before_nio_ack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    baseline: str,
) -> None:
    """A committed journal receipt cannot make an interrupted baseline write disappear."""
    bot = _router_bot(tmp_path)
    seen: list[str] = []
    bot.hook_registry = _registry(seen)
    save = room_member_joins._save_room_member_joins
    calls = 0

    def interrupted_save(path: Path, members: dict[str, set[str]]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            message = "baseline write interrupted"
            raise RuntimeError(message)
        save(path, members)

    monkeypatch.setattr(room_member_joins, "_save_room_member_joins", interrupted_save)
    try:
        async with _owned_session(bot) as session:
            existing = _room_member_event(event_id="$baseline", prev_membership=None)
            frame = (
                _frame("baseline", timeline=(existing,))
                if baseline == "timeline"
                else _frame("baseline", state=(existing,))
            )
            with pytest.raises(RuntimeError, match="baseline write interrupted"):
                await _consume_frame(bot, session, frame)
            await _consume_frame(bot, session, frame)
            assert calls == 2
            profile = _room_member_event(event_id="$profile", prev_membership=None)
            await _consume_frame(bot, session, _frame("profile", timeline=(profile,)))
            await bot._journal_dispatcher.drain_once()
            assert seen == []
    finally:
        await bot._journal_store.close()
