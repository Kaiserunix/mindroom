"""Real nio member records carry tenure effects alongside their event disposition."""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock
from uuid import uuid4

import nio
import pytest
from nio.durable import open_durable_sync

from mindroom.config.access import ResponderAccessConfig
from mindroom.event_journal import DeliveryProjectionPendingError, EventKind
from mindroom.event_journal import store as journal_store
from mindroom.matrix.durable_ingestion import consume_one_ingestion_batch
from mindroom.matrix.state import MatrixState
from tests.conftest import install_call_manager_mock, make_matrix_client_mock
from tests.test_bot_ready_hook import _router_bot_with_orchestrator

if TYPE_CHECKING:
    from pathlib import Path

    from mindroom.event_journal import AdmissionFacts, InboundEvent, IngestionRecordAdmission
    from mindroom.event_journal.backend import Transaction

ROOM = "!grant:localhost"
SENDER = "@alice:localhost"


def _member(event_id: str, user_id: str, membership: str) -> dict[str, object]:
    return {
        "type": "m.room.member",
        "event_id": event_id,
        "sender": user_id,
        "state_key": user_id,
        "origin_server_ts": 100,
        "content": {"membership": membership},
    }


def _message(event_id: str) -> dict[str, object]:
    return {
        "type": "m.room.message",
        "event_id": event_id,
        "sender": SENDER,
        "origin_server_ts": 100,
        "content": {"msgtype": "m.text", "body": "hello"},
    }


def _response(cursor: str, state: list[dict[str, object]], timeline: list[dict[str, object]]) -> bytes:
    return json.dumps(
        {
            "next_batch": cursor,
            "rooms": {
                "join": {
                    ROOM: {
                        "state": {"events": state},
                        "timeline": {"events": timeline, "limited": False},
                    },
                },
            },
        },
    ).encode()


@pytest.mark.asyncio
@pytest.mark.parametrize("block_leave_once", [False, True])
async def test_real_member_state_and_timeline_preserve_tenure_and_hooks(  # noqa: C901, PLR0915 - complete real source lifecycle
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    block_leave_once: bool,
) -> None:
    """State join and semantic leave/rejoin each carry their own atomic tenure effect."""
    bot, _orchestrator = _router_bot_with_orchestrator(tmp_path)
    account = bot.agent_user.user_id
    bot.config.router.access = ResponderAccessConfig(members_of_rooms=["grant"])
    state = MatrixState.load(runtime_paths=bot.runtime_paths)
    state.add_room("grant", ROOM, "#grant:localhost", "Grant")
    state.save(runtime_paths=bot.runtime_paths)
    principal = bot.journal_principal()
    consumer = await principal.load_or_create_ingestion_consumer(new_generation=uuid4())
    client = nio.AsyncClient("https://localhost", account, device_id="DEVICE")
    client.restore_login(account, "DEVICE", "token")
    session = open_durable_sync(client, consumer_id=consumer.generation, store_path=tmp_path / "crypto")
    await principal.bind_ingestion_stream(generation=consumer.generation, stream_id=session.stream_id)
    bot.client = client
    call_manager = AsyncMock()
    install_call_manager_mock(bot, call_manager)
    source: asyncio.Queue[bytes] = asyncio.Queue()

    async def request(*_args: object, **_kwargs: object) -> bytes:
        return await source.get()

    session._transport.request = request
    session._maintain_crypto = AsyncMock()
    completed = 0
    observed: list[tuple[str, bool, str, int]] = []
    blocked_leaves = 0
    snapshot_source = journal_store._snapshot_interactive_source

    def snapshot(transaction: Transaction, principal_id: str, event: InboundEvent) -> None:
        nonlocal blocked_leaves
        if block_leave_once and event.event_id == "$leave" and blocked_leaves == 0:
            blocked_leaves += 1
            message = "own leave projection pending"
            raise DeliveryProjectionPendingError(message)
        snapshot_source(transaction, principal_id, event)

    monkeypatch.setattr(journal_store, "_snapshot_interactive_source", snapshot)

    async def complete() -> None:
        nonlocal completed
        completed += 1

    async def after(
        record: IngestionRecordAdmission,
        facts: AdmissionFacts,
        provenance: nio.TimelineEventProvenance | None,
    ) -> None:
        await bot._after_ingestion_admission(record, facts, provenance)
        event = record.event
        if event is not None:
            position = await principal.membership_position(ROOM)
            allowed = bot._runtime_view.agent_reply_memberships.is_allowed(
                SENDER,
                ["grant"],
                bot.config,
                bot.runtime_paths,
            )
            observed.append((event.event_id, allowed, position.membership, position.membership_epoch))

    async def drain_until(target: int) -> None:
        async with asyncio.timeout(3):
            while completed < target:
                try:
                    facts = await consume_one_ingestion_batch(
                        session,
                        principal,
                        account_id=account,
                        before_admission=bot._before_ingestion_admission,
                        after_admission=after,
                        after_sync=complete,
                    )
                except DeliveryProjectionPendingError:
                    position = await principal.membership_position(ROOM)
                    assert (position.membership, position.membership_epoch) == ("join", 0)
                    assert await principal.load_event("$leave") is None
                    assert "$before" in [event.event_id for event in await principal.pending()]
                    continue
                if facts is None:
                    await session.wait_for_work()

    runner = asyncio.create_task(session.run())
    try:
        await source.put(_response("one", [_member("$own-state", account, "join")], []))
        await drain_until(1)
        position = await principal.membership_position(ROOM)
        assert (position.membership, position.membership_epoch) == ("join", 0)
        call_manager.on_sync_room_membership.assert_awaited_once_with(joined_room_ids={ROOM}, left_room_ids=set())
        snapshot = make_matrix_client_mock(user_id=account)
        snapshot.joined_rooms.return_value = nio.JoinedRoomsResponse(rooms=[ROOM])
        snapshot.joined_members.return_value = nio.JoinedMembersResponse(
            members=[nio.RoomMember(account, None, None)],
            room_id=ROOM,
        )
        await bot._runtime_view.agent_reply_memberships.refresh(bot.config, bot.runtime_paths, snapshot)
        await source.put(
            _response(
                "two",
                [],
                [
                    _member("$grant", SENDER, "join"),
                    _message("$before"),
                    _member("$leave", account, "leave"),
                    _message("$fenced"),
                    _member("$rejoin", account, "join"),
                    _message("$after"),
                ],
            ),
        )
        await drain_until(2)
        assert ("$before", True, "join", 0) in observed
        assert ("$leave", False, "leave", 1) in observed
        assert ("$fenced", False, "leave", 1) in observed
        assert ("$rejoin", False, "join", 1) in observed
        position = await principal.membership_position(ROOM)
        assert (position.membership, position.membership_epoch) == ("join", 1)
        pending = await principal.pending()
        # Rejoining does not authorize the remaining captured history as live.
        assert [event.event_id for event in pending if event.kind is EventKind.MESSAGE] == []
        assert (await principal.load_event("$leave")).kind is EventKind.ROOM_LIFECYCLE
        assert (await principal.load_event("$rejoin")).kind is EventKind.ROOM_LIFECYCLE
        assert call_manager.on_sync_room_membership.await_count == 3
        assert blocked_leaves == int(block_leave_once)
        await source.put(
            _response("three", [_member("$rejoin", account, "join")], []),
        )
        await drain_until(3)
        await bot._runtime_view.agent_reply_memberships.refresh(bot.config, bot.runtime_paths, snapshot)
        await source.put(_response("four", [], [_member("$live-grant", SENDER, "join"), _message("$live")]))
        await drain_until(4)
        assert ("$live", True, "join", 1) in observed
        assert [event.event_id for event in await principal.pending() if event.kind is EventKind.MESSAGE] == ["$live"]
    finally:
        runner.cancel()
        with suppress(asyncio.CancelledError):
            await runner
        await session.close()
        await client.close()
