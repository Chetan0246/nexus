"""WebSocket router: authenticated rooms and live pub/sub broadcast."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from nexus.api.ws import ConnectionHub
from nexus.schemas import WSInMessage, WSOutMessage
from nexus.security import decode_token

router = APIRouter(tags=["websocket"])


@router.websocket("/ws/{room}")
async def ws_endpoint(
    websocket: WebSocket,
    room: str,
    token: str = Query(...),
) -> None:
    try:
        payload = decode_token(token)
        user_id = str(payload.get("sub", ""))
        if not user_id:
            await websocket.close(code=4401)
            return
    except Exception:
        await websocket.close(code=4401)
        return

    hub: ConnectionHub = websocket.app.state.hub
    await websocket.accept()
    sub = await hub.subscribe(room)

    join_msg = WSOutMessage(
        room=room,
        user_id="system",
        text=f"User {user_id} joined room '{room}'",
        ts=datetime.now(UTC),
    )
    await hub.publish(room, join_msg.model_dump_json())

    async def sender_pump() -> None:
        try:
            while True:
                msg = await sub.queue.get()
                await websocket.send_text(msg)
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass

    sender_task = asyncio.create_task(sender_pump())

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                in_msg = WSInMessage.model_validate_json(raw)
            except ValidationError as exc:
                err_msg = WSOutMessage(
                    room=room,
                    user_id="system",
                    text=f"Validation error: {exc.errors()[0]['msg']}",
                    ts=datetime.now(UTC),
                )
                await websocket.send_text(err_msg.model_dump_json())
                continue

            out_msg = WSOutMessage(
                room=in_msg.room,
                user_id=user_id,
                text=in_msg.text,
                ts=datetime.now(UTC),
            )
            await hub.publish(in_msg.room, out_msg.model_dump_json())

    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    finally:
        sender_task.cancel()
        await hub.unsubscribe(room, sub)
        leave_msg = WSOutMessage(
            room=room,
            user_id="system",
            text=f"User {user_id} left room '{room}'",
            ts=datetime.now(UTC),
        )
        await hub.publish(room, leave_msg.model_dump_json())
