"""Rewrite pyrogram.get_chat_history"""

from collections.abc import AsyncGenerator
from datetime import datetime

import pyrogram
from pyrogram import raw, types, utils


async def get_chunk_v2(
    *,
    client: pyrogram.Client,
    chat_id: int | str,
    limit: int = 0,
    offset: int = 0,
    max_id: int = 0,
    from_message_id: int = 0,
    from_date: datetime | None = None,
    reverse: bool = False,
):
    """get chunk"""
    if from_date is None:
        from_date = utils.zero_datetime()
    from_message_id = from_message_id or (1 if reverse else 0)

    messages = await utils.parse_messages(
        client,
        await client.invoke(
            raw.functions.messages.GetHistory(
                peer=await client.resolve_peer(chat_id),  # ty:ignore[invalid-argument-type]
                offset_id=from_message_id,
                offset_date=utils.datetime_to_timestamp(from_date),  # ty:ignore[invalid-argument-type]
                add_offset=offset * (-1 if reverse else 1) - (limit if reverse else 0),
                limit=limit,
                max_id=max_id,
                min_id=0,
                hash=0,
            ),
            sleep_threshold=60,
        ),
        replies=0,
    )

    if reverse:
        messages.reverse()

    return messages


async def get_chat_history_v2(
    self: pyrogram.Client,
    chat_id: int | str,
    limit: int = 0,
    max_id: int = 0,
    offset: int = 0,
    offset_id: int = 0,
    offset_date: datetime | None = None,
    reverse: bool = False,
) -> AsyncGenerator["types.Message", None] | None:
    """Get messages from a chat history."""
    if offset_date is None:
        offset_date = utils.zero_datetime()
    current = 0
    total = limit or (1 << 31) - 1
    limit = min(100, total)

    while True:
        messages = await get_chunk_v2(
            client=self,
            chat_id=chat_id,
            limit=limit,
            offset=offset,
            max_id=max_id + 1 if max_id else 0,
            from_message_id=offset_id,
            from_date=offset_date,
            reverse=reverse,
        )

        if not messages:
            return

        offset_id = messages[-1].id + (1 if reverse else 0)

        for message in messages:
            yield message

            current += 1

            if current >= total:
                return
