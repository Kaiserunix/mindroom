"""Runtime regressions across the real nio producer and MindRoom consumer."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock
from uuid import uuid4

import nio
import pytest
from nio.durable import DurableSyncConfig, RecordKind, SyncRecord
from nio.durable.transport import HttpError

from mindroom.bot import AgentBot
from mindroom.constants import ROUTER_AGENT_NAME
from mindroom.event_journal import DeliveryStage, DepartureSource, RoomMembershipPosition
from mindroom.matrix._owned_session import MatrixCredentials, open_owned_matrix_session
from mindroom.matrix.client_session import create_authenticated_client
from mindroom.matrix.durable_ingestion import consume_one_ingestion_batch
from mindroom.orchestrator import _MultiAgentOrchestrator
from tests.test_bot_ready_hook import _agent_bot
from tests.test_durable_ingestion_admission import ROOM
from tests.test_event_journal_store import admit, interactive_edit, interactive_prompt
from tests.test_room_invites import _handle_invite, _live_router_invite_scenario, _pending_room_invites

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from nio.durable import DurableSync

    from mindroom.event_journal.store import PrincipalStore


@asynccontextmanager
async def _owned_session(bot: AgentBot) -> AsyncIterator[DurableSync]:
    opened = await open_owned_matrix_session(
        "https://localhost",
        MatrixCredentials(bot.agent_user.user_id, "DEVICE", "token"),
        bot.runtime_paths,
        consumer_store=bot.journal_principal(),
        new_consumer_generation=uuid4(),
        config=DurableSyncConfig(),
    )
    bot.client = opened.client
    bot._ingestion_session = opened.session
    try:
        yield opened.session
    finally:
        await opened.session.close()
        await opened.client.close()


def _joined_frame(events: list[dict[str, object]]) -> bytes:
    return json.dumps(
        {
            "next_batch": "next",
            "rooms": {"join": {ROOM: {"state": {"events": []}, "timeline": {"limited": False, "events": events}}}},
        },
    ).encode()


async def _consume_frame(bot: AgentBot, session: DurableSync, frame: bytes) -> None:
    frames: asyncio.Queue[bytes] = asyncio.Queue()
    frames.put_nowait(frame)

    async def request(*_args: object, **_kwargs: object) -> bytes:
        return await frames.get()

    session._transport.request = request
    session._maintain_crypto = AsyncMock()
    runner = asyncio.create_task(session.run())
    completed = False

    async def after_sync() -> None:
        nonlocal completed
        completed = True

    try:
        async with asyncio.timeout(2):
            while not completed:
                facts = await consume_one_ingestion_batch(
                    session,
                    bot.journal_principal(),
                    account_id=bot.agent_user.user_id,
                    after_admission=bot._after_ingestion_admission,
                    after_sync=after_sync,
                )
                if facts is None:
                    await session.wait_for_work()
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 429, 503])
async def test_failed_durable_join_retains_pending_invitation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    """An ambiguous HTTP failure must retain both the invitation and decrypt fence."""
    config, bot, room, event = _live_router_invite_scenario(tmp_path)
    bot._change_local_membership = AgentBot._change_local_membership.__get__(bot)
    bot._room_lifecycle.deps = replace(bot._room_lifecycle.deps, change_membership=bot._change_local_membership)
    monkeypatch.setattr(bot._room_lifecycle, "_send_invite_welcome", AsyncMock())
    async with _owned_session(bot) as session:
        bot.client.invited_rooms[room.room_id] = room
        session._transport.request = AsyncMock(side_effect=HttpError(status))
        with pytest.raises(RuntimeError, match="Failed to join invited room"):
            await _handle_invite(bot, room, event)
        assert room.room_id in _pending_room_invites(config, ROUTER_AGENT_NAME)
        assert bot._room_lifecycle.decrypt_notice_is_fenced(room.room_id)


@pytest.mark.asyncio
async def test_malformed_message_does_not_block_following_valid_message(tmp_path: Path) -> None:
    """An ordinary malformed payload must not poison durable batch replay."""
    bot = _agent_bot(tmp_path)
    async with _owned_session(bot) as session:
        await _consume_frame(
            bot,
            session,
            _joined_frame(
                [
                    {
                        "type": "m.room.message",
                        "event_id": "$bad",
                        "sender": "@alice:example.org",
                        "origin_server_ts": 100,
                        "content": {"msgtype": "m.image", "body": "missing URL"},
                    },
                    {
                        "type": "m.room.message",
                        "event_id": "$good",
                        "sender": "@alice:example.org",
                        "origin_server_ts": 101,
                        "content": {"msgtype": "m.text", "body": "hello"},
                    },
                ],
            ),
        )
        assert await bot.journal_principal().load_event("$bad") is None
        assert await bot.journal_principal().load_event("$good") is not None
    async with _owned_session(bot) as session:
        assert await session.next_batch() is None


@pytest.mark.asyncio
async def test_quiesce_is_bounded_when_delivery_projection_cannot_recover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Source drain must let shutdown continue while retaining blocked input."""
    bot = _agent_bot(tmp_path)
    principal = bot.journal_principal()
    account = bot.agent_user.user_id
    await admit(principal, "$turn", sender="@alice:example.org")
    await admit(
        principal,
        "$prompt",
        sender=account,
        content=interactive_prompt("Old?", "old", source_event_id="$turn"),
    )
    await principal.enqueue_matrix_delivery(
        delivery_id="$edit",
        stage=DeliveryStage.FINAL,
        room_id=ROOM,
        thread_id=None,
        payload=interactive_edit("$prompt", "New?", "new", source_event_id="$turn"),
        edits_event_id="$prompt",
    )
    await principal.claim_matrix_delivery(delivery_id="$edit", stage=DeliveryStage.FINAL)
    async with _owned_session(bot) as session:
        with session._store.transaction():
            session._store.publish(
                (
                    SyncRecord(
                        RecordKind.TIMELINE,
                        ROOM,
                        {
                            "type": "m.reaction",
                            "event_id": "$reaction",
                            "sender": "@alice:example.org",
                            "origin_server_ts": 3000,
                            "content": {
                                "m.relates_to": {"rel_type": "m.annotation", "event_id": "$prompt", "key": "1"},
                            },
                        },
                        provenance=nio.TimelineEventProvenance.RECOVERED,
                    ),
                ),
            )
        recovery_attempted = asyncio.Event()

        async def unavailable_delivery() -> bool:
            recovery_attempted.set()
            return False

        monkeypatch.setattr(bot, "_recover_unacknowledged_matrix_deliveries", unavailable_delivery)
        sync = asyncio.create_task(bot.sync_forever())
        try:
            await asyncio.wait_for(recovery_attempted.wait(), timeout=2)
            await asyncio.wait_for(bot._quiesce_matrix_ingestion(), timeout=5.2)
        finally:
            bot._sync_shutting_down = True
            bot._delivery_recovery_wake.set()
            sync.cancel()
            await asyncio.gather(sync, return_exceptions=True)
            if bot._delivery_recovery_task is not None:
                await asyncio.gather(bot._delivery_recovery_task, return_exceptions=True)
    async with _owned_session(bot) as session:
        retained = await session.next_batch()
        assert retained is not None
        assert retained.records[0].source["event_id"] == "$reaction"
        assert await principal.load_event("$reaction") is None


@pytest.mark.asyncio
async def test_ordinary_store_adoption_preserves_existing_journal_tenure(tmp_path: Path) -> None:
    """Fresh producer epochs must not reset or block existing journal tenure."""
    bot = _agent_bot(tmp_path)
    principal = bot.journal_principal()
    await principal.fence_departure(ROOM, source=DepartureSource.LOCAL)
    await principal.note_membership_restarted(ROOM)
    await admit(principal, "$existing", sender="@alice:example.org")
    await principal.enqueue_matrix_delivery(
        delivery_id="$existing",
        stage=DeliveryStage.FINAL,
        room_id=ROOM,
        thread_id=None,
        payload={"msgtype": "m.text", "body": "answer"},
    )
    legacy = create_authenticated_client(
        "https://localhost",
        bot.agent_user.user_id,
        "DEVICE",
        "token",
        bot.runtime_paths,
    )
    await legacy.close()
    assert legacy.store is not None
    legacy.store.database.close()
    bot._local_departures_awaiting_sync.add(ROOM)
    async with _owned_session(bot) as session:
        await _consume_frame(bot, session, _joined_frame([]))
        assert ROOM not in bot._local_departures_awaiting_sync
        assert await principal.membership_epoch(ROOM) == 1
        assert await principal.load_event("$existing") is not None
        delivery = await principal.load_matrix_delivery(delivery_id="$existing", stage=DeliveryStage.FINAL)
        assert delivery is not None
        assert not delivery.retired
        assert delivery.membership_epoch == 1
        assert await principal.ingestion_membership_position(ROOM) == RoomMembershipPosition("join", 0)
        session._transport.request = AsyncMock(return_value=b"{}")
        assert await AgentBot._change_local_membership(bot, ROOM, "leave")
        await consume_one_ingestion_batch(session, principal, account_id=bot.agent_user.user_id)
        assert await principal.membership_position(ROOM) == RoomMembershipPosition("leave", 2)
        delivery = await principal.load_matrix_delivery(delivery_id="$existing", stage=DeliveryStage.FINAL)
        assert delivery is not None
        assert delivery.retired
        leave_frame = json.loads(_joined_frame([]))
        leave_frame["rooms"]["leave"] = leave_frame["rooms"].pop("join")
        await _consume_frame(bot, session, json.dumps(leave_frame).encode())
    async with _owned_session(bot) as session:
        assert await principal.ingestion_membership_position(ROOM) == RoomMembershipPosition("leave", 1)
        session._transport.request = AsyncMock(return_value=b"{}")
        assert await AgentBot._change_local_membership(bot, ROOM, "join")
        await consume_one_ingestion_batch(session, principal, account_id=bot.agent_user.user_id)
        assert await principal.membership_position(ROOM) == RoomMembershipPosition("join", 2)
        await _consume_frame(bot, session, _joined_frame([]))
        assert await session.next_batch() is None


@pytest.mark.asyncio
async def test_startup_cleanup_leaves_room_before_first_membership_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown producer state cannot certify that a server-joined room was left."""
    bot = _agent_bot(tmp_path)
    principal = bot.journal_principal()
    await principal.fence_departure(ROOM, source=DepartureSource.LOCAL)
    await principal.note_membership_restarted(ROOM)
    bot._room_lifecycle.deps = replace(
        bot._room_lifecycle.deps,
        change_membership=AgentBot._change_local_membership.__get__(bot),
    )
    monkeypatch.setattr("mindroom.matrix.rooms.is_dm_room", AsyncMock(return_value=False))
    first_poll = asyncio.Event()
    leave_requests: list[str] = []

    async def request(_method: str, path: str, *_args: object, **_kwargs: object) -> bytes:
        if "/sync" in path:
            first_poll.set()
            await asyncio.Event().wait()
        assert path.partition("?")[0].endswith("/leave")
        leave_requests.append(path)
        return b"{}"

    async with _owned_session(bot) as session:
        assert bot.client is not None
        monkeypatch.setattr(bot.client, "joined_rooms", AsyncMock(return_value=nio.JoinedRoomsResponse(rooms=[ROOM])))
        session._transport.request = request
        session._maintain_crypto = AsyncMock()
        runner = asyncio.create_task(session.run())
        try:
            await asyncio.wait_for(first_poll.wait(), timeout=2)
            await asyncio.wait_for(bot.leave_unconfigured_rooms(), timeout=2)
            assert len(leave_requests) == 1
            facts = await consume_one_ingestion_batch(session, principal, account_id=bot.agent_user.user_id)
            assert facts is not None
            assert await principal.membership_position(ROOM) == RoomMembershipPosition("leave", 2)
            assert await principal.ingestion_membership_position(ROOM) == RoomMembershipPosition("leave", 0)
        finally:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)


async def _retain_membership_before_admission(bot: AgentBot, retained: str) -> None:
    """Stop a real source after committing membership but before app admission."""
    principal = bot.journal_principal()
    async with _owned_session(bot) as session:
        frame = json.loads(_joined_frame([]))
        if retained == "leave":
            await _consume_frame(bot, session, _joined_frame([]))
            frame["rooms"]["leave"] = frame["rooms"].pop("join")
            frame["next_batch"] = "departed"
        session._transport.request = AsyncMock(return_value=json.dumps(frame).encode())
        session._maintain_crypto = AsyncMock()
        runner = asyncio.create_task(session.run())
        try:
            await asyncio.wait_for(session.wait_for_work(), timeout=2)
            assert await principal.ingestion_membership_position(ROOM) == (
                RoomMembershipPosition("join", 0) if retained == "leave" else None
            )
        finally:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_entity_removal_leaves_with_retained_input_before_closing_stores(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removal keeps the real source and pump available until the leave finishes."""
    bot = _agent_bot(tmp_path)
    await _retain_membership_before_admission(bot, "join")
    bot._room_lifecycle.deps = replace(
        bot._room_lifecycle.deps,
        change_membership=AgentBot._change_local_membership.__get__(bot),
    )
    monkeypatch.setattr("mindroom.matrix.rooms.is_dm_room", AsyncMock(return_value=False))
    monkeypatch.setattr(bot, "_on_ingestion_frame_completion", AsyncMock())
    leaves: list[str] = []

    async def request(_method: str, path: str, *_args: object, **_kwargs: object) -> bytes:
        if "/sync" in path:
            await asyncio.Event().wait()
        assert path.partition("?")[0].endswith("/leave")
        leaves.append(path)
        return b"{}"

    async with _owned_session(bot) as session:
        assert bot.client is not None
        monkeypatch.setattr(bot.client, "joined_rooms", AsyncMock(return_value=nio.JoinedRoomsResponse(rooms=[ROOM])))
        session._transport.request = request
        session._maintain_crypto = AsyncMock()
        orchestrator = _MultiAgentOrchestrator(runtime_paths=bot.runtime_paths)
        orchestrator.agent_bots[bot.agent_name] = bot
        orchestrator._approval_transport.reconcile_unavailable_entities = AsyncMock()
        sync = asyncio.create_task(bot.sync_forever())
        orchestrator._sync_tasks[bot.agent_name] = sync
        try:
            await asyncio.wait_for(orchestrator._remove_deleted_entities({bot.agent_name}), timeout=2)
            assert len(leaves) == 1
            assert bot.agent_name not in orchestrator.agent_bots
            assert bot.agent_name not in orchestrator._sync_tasks
            assert sync.done()
        finally:
            sync.cancel()
            await asyncio.gather(sync, return_exceptions=True)


@pytest.mark.asyncio
async def test_removal_cleanup_bounds_wait_for_stopped_ingestion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unrecoverable retained input must not leave removal waiting forever."""
    bot = _agent_bot(tmp_path)
    await _retain_membership_before_admission(bot, "join")
    bot._change_local_membership = AgentBot._change_local_membership.__get__(bot)
    bot._room_lifecycle.deps = replace(bot._room_lifecycle.deps, change_membership=bot._change_local_membership)
    monkeypatch.setattr("mindroom.matrix.rooms.is_dm_room", AsyncMock(return_value=False))
    async with _owned_session(bot) as session:
        assert bot.client is not None
        monkeypatch.setattr(bot.client, "joined_rooms", AsyncMock(return_value=nio.JoinedRoomsResponse(rooms=[ROOM])))
        session._transport.request = AsyncMock()
        cleanup = asyncio.create_task(bot.leave_rooms())
        try:
            done, _pending = await asyncio.wait([cleanup], timeout=5.5)
            assert cleanup in done, "room cleanup waited forever for a stopped admission pump"
            await cleanup
            session._transport.request.assert_not_awaited()
            assert await session.next_batch() is not None
        finally:
            for task in asyncio.all_tasks():
                if task.get_name() == "matrix_leave_room_and_cleanup":
                    task.cancel()
            cleanup.cancel()
            await asyncio.gather(cleanup, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["join", "leave"])
@pytest.mark.parametrize("retained", ["join", "leave"])
async def test_startup_reconciles_membership_saved_before_application_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    retained: str,
) -> None:
    """Restarted room maintenance must wait for retained producer membership."""
    bot = _agent_bot(tmp_path)
    principal = bot.journal_principal()
    await _retain_membership_before_admission(bot, retained)
    bot.rooms = [ROOM] if target == "join" else []
    configured_setup = AsyncMock()
    bot._room_lifecycle.deps = replace(
        bot._room_lifecycle.deps,
        change_membership=AgentBot._change_local_membership.__get__(bot),
        on_configured_room_joined=configured_setup,
    )
    monkeypatch.setattr("mindroom.matrix.rooms.is_dm_room", AsyncMock(return_value=False))
    monkeypatch.setattr(bot, "_on_ingestion_frame_completion", AsyncMock())
    position_read = asyncio.Event()
    read_position = type(principal).ingestion_membership_position

    async def observe_position(store: PrincipalStore, room_id: str) -> RoomMembershipPosition | None:
        position = await read_position(store, room_id)
        position_read.set()
        return position

    monkeypatch.setattr(type(principal), "ingestion_membership_position", observe_position)
    membership_requests: list[str] = []

    async def request(_method: str, path: str, *_args: object, **_kwargs: object) -> bytes:
        if "/sync" in path:
            await asyncio.Event().wait()
        action = "join" if "/join/" in path else path.partition("?")[0].rsplit("/", 1)[-1]
        assert action in {"join", "leave"}
        membership_requests.append(action)
        return b"{}"

    async with _owned_session(bot) as session:
        assert bot.client is not None
        monkeypatch.setattr(bot.client, "joined_rooms", AsyncMock(return_value=nio.JoinedRoomsResponse(rooms=[ROOM])))
        session._transport.request = request
        session._maintain_crypto = AsyncMock()
        maintenance = asyncio.create_task(bot.ensure_rooms())
        sync: asyncio.Task[None] | None = None
        try:
            await asyncio.wait_for(position_read.wait(), timeout=2)
            sync = asyncio.create_task(bot.sync_forever())
            await asyncio.wait_for(maintenance, timeout=2)
            if target == "join":
                configured_setup.assert_awaited_once_with(ROOM)
            else:
                configured_setup.assert_not_awaited()
            assert membership_requests == ([target] if target != retained else [])
        finally:
            maintenance.cancel()
            if sync is not None:
                sync.cancel()
            await asyncio.gather(maintenance, *([sync] if sync is not None else []), return_exceptions=True)
