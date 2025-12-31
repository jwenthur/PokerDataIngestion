from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Optional, Tuple

from sqlalchemy import text
from db import queries as q


def find_existing_session_within_gap(
    conn,
    site: str,
    ts_local: datetime,
    gap_minutes: int,
) -> Tuple[Optional[str], Optional[datetime]]:
    gap = timedelta(minutes=gap_minutes)

    prev = conn.execute(
        text(q.SQL_PREV_WITHIN_GAP),
        {"site": site, "ts": ts_local, "gap": gap},
    ).fetchone()
    if prev and prev.session_id:
        return str(prev.session_id), prev.session_start_ts_local

    nxt = conn.execute(
        text(q.SQL_NEXT_WITHIN_GAP),
        {"site": site, "ts": ts_local, "gap": gap},
    ).fetchone()
    if nxt and nxt.session_id:
        return str(nxt.session_id), nxt.session_start_ts_local

    return None, None


def ensure_session_and_index(
    conn,
    site: str,
    ts_local: datetime,
    gap_minutes: int,
) -> Tuple[str, datetime, int]:
    """
    Returns:
      (session_id, session_start_ts_local, session_tournament_index)

    - If ts falls within gap of an existing session, re-use it.
    - Else create a new UUID session.
    - If inserting in the middle of an existing session, bump indices.
    """
    session_id, session_start = find_existing_session_within_gap(conn, site, ts_local, gap_minutes)

    if session_id is None:
        new_session_id = str(uuid.uuid4())
        return new_session_id, ts_local, 1

    # Determine the index position
    count_before = conn.execute(
        text(q.SQL_COUNT_BEFORE_IN_SESSION),
        {"site": site, "session_id": session_id, "ts": ts_local},
    ).fetchone().c

    new_index = int(count_before) + 1

    # Bump existing indices that occur at/after this timestamp in the same session
    conn.execute(
        text(q.SQL_BUMP_INDICES_AT_OR_AFTER_TS),
        {"site": site, "session_id": session_id, "ts": ts_local},
    )

    # Session start should be the earliest tournament in that session
    min_start = conn.execute(
        text(q.SQL_MIN_SESSION_START),
        {"site": site, "session_id": session_id},
    ).fetchone().min_ts

    session_start = min_start if min_start else session_start
    return session_id, session_start, new_index
