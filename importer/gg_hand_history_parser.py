# importer/gg_hand_history_parser.py
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import List, Optional, Tuple

from models import ParsedHand
from utils.hashing import sha256_file  # adjust if your hashing util name differs


HAND_START_RE = re.compile(r"^Poker Hand #(?P<hand_id>\S+): Tournament #(?P<tournament_id>\d+), (?P<game_type>.+?) Hold'em", re.M)
LEVEL_RE = re.compile(r"Level(?P<level>\d+)\((?P<sb>\d+)/(?P<bb>\d+)\)")
TS_RE = re.compile(r"- (?P<ts>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})")
TABLE_RE = re.compile(r"^Table '(?P<table_id>[^']+)' (?P<max_players>\d+)-max Seat #(?P<button_seat>\d+) is the button", re.M)
SEAT_RE = re.compile(r"^Seat (?P<seat>\d+): (?P<name>.+?) \((?P<stack>\d+) in chips\)", re.M)
DEALT_HERO_RE = re.compile(r"^Dealt to Hero \[(?P<c1>[2-9TJQKA][shdc]) (?P<c2>[2-9TJQKA][shdc])\]", re.M)

HOLE_CARDS_MARK = "*** HOLE CARDS ***"
FLOP_MARK = "*** FLOP ***"
SUMMARY_MARK = "*** SUMMARY ***"


def _split_into_hand_blocks(text: str) -> List[str]:
    """
    Split a file into individual hand blocks. Each hand starts with 'Poker Hand #...'
    """
    starts = [m.start() for m in re.finditer(r"^Poker Hand #", text, re.M)]
    if not starts:
        return []
    blocks = []
    for i, s in enumerate(starts):
        e = starts[i + 1] if i + 1 < len(starts) else len(text)
        blocks.append(text[s:e].strip())
    return blocks


def _parse_positions(active_seats: List[int], button_seat: int) -> dict[int, str]:
    """
    Returns map seat -> position.
    For 3-handed: BTN (button), SB (next), BB (next next) in seat-order around the table.
    For HU: button seat is SB, other seat is BB.
    """
    seats_sorted = sorted(active_seats)
    if len(seats_sorted) == 2:
        # HU: button = SB, other = BB
        other = [s for s in seats_sorted if s != button_seat][0]
        return {button_seat: "SB", other: "BB"}

    # 3-handed or more (we only expect up to 3 right now)
    if button_seat not in seats_sorted:
        # Fallback
        return {s: None for s in seats_sorted}  # type: ignore

    btn_idx = seats_sorted.index(button_seat)
    sb_seat = seats_sorted[(btn_idx + 1) % len(seats_sorted)]
    bb_seat = seats_sorted[(btn_idx + 2) % len(seats_sorted)]
    return {button_seat: "BTN", sb_seat: "SB", bb_seat: "BB"}


def _bucket_effective_stack(eff_bb: float) -> str:
    # Your default scheme (we can tweak later)
    if eff_bb <= 5:
        return "0-5"
    if eff_bb <= 8:
        return "6-8"
    if eff_bb <= 12:
        return "9-12"
    if eff_bb <= 17:
        return "13-17"
    if eff_bb <= 25:
        return "18-25"
    return "26+"


def _extract_preflop_block(hand_text: str) -> str:
    """
    Extract lines from HOLE CARDS through just before FLOP, or SUMMARY if no flop.
    """
    if HOLE_CARDS_MARK not in hand_text:
        return ""

    start = hand_text.index(HOLE_CARDS_MARK)
    end = None
    if FLOP_MARK in hand_text:
        end = hand_text.index(FLOP_MARK)
    elif SUMMARY_MARK in hand_text:
        end = hand_text.index(SUMMARY_MARK)
    else:
        end = len(hand_text)

    return hand_text[start:end].strip()


def parse_file(path: str, site: str = "GG") -> List[ParsedHand]:
    """
    Parse a GG hand history file into ParsedHand objects.
    Phase 1: preflop-first extraction; hero_net_chips can be added later.
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    file_hash = sha256_file(path)
    file_name = os.path.basename(path)

    hand_blocks = _split_into_hand_blocks(text)
    parsed: List[ParsedHand] = []

    for block in hand_blocks:
        m = HAND_START_RE.search(block)
        if not m:
            continue

        hand_id = m.group("hand_id")
        tournament_id = int(m.group("tournament_id"))
        # game_type_text = m.group("game_type").strip()  # optional if you want it later

        # Timestamp
        ts_m = TS_RE.search(block)
        if not ts_m:
            continue
        hand_ts_local = datetime.strptime(ts_m.group("ts"), "%Y/%m/%d %H:%M:%S")

        # Level / blinds
        level = None
        sb_amount = None
        bb_amount = None
        lvl_m = LEVEL_RE.search(block)
        if lvl_m:
            level = int(lvl_m.group("level"))
            sb_amount = int(lvl_m.group("sb"))
            bb_amount = int(lvl_m.group("bb"))
        else:
            # blinds are essential to compute effective stack; skip if missing
            continue

        # Table / button
        table_id = None
        max_players = 3
        button_seat = None
        t_m = TABLE_RE.search(block)
        if t_m:
            table_id = t_m.group("table_id")
            max_players = int(t_m.group("max_players"))
            button_seat = int(t_m.group("button_seat"))

        # Seats and stacks at start
        seats = []
        hero_seat = None
        hero_stack_start = None

        for sm in SEAT_RE.finditer(block):
            seat = int(sm.group("seat"))
            name = sm.group("name").strip()
            stack = int(sm.group("stack"))
            seats.append(seat)
            if name == "Hero":
                hero_seat = seat
                hero_stack_start = stack

        players_in_hand = len(seats)
        is_hu = players_in_hand == 2

        hero_position = None
        if button_seat is not None and hero_seat is not None and seats:
            pos_map = _parse_positions(seats, button_seat)
            hero_position = pos_map.get(hero_seat)

        # Hero cards
        hero_cards = None
        dealt = DEALT_HERO_RE.search(block)
        if dealt:
            hero_cards = f"{dealt.group('c1')}{dealt.group('c2')}"

        # Effective stack in BB (start-of-hand approximation; we’ll refine to decision-point later)
        effective_stack_bb = None
        stack_bucket = None
        if hero_stack_start is not None and bb_amount:
            # Approx effective stack = hero stack / BB for now.
            # Later: use min(hero, villain) and decision-point.
            effective_stack_bb = round(hero_stack_start / bb_amount, 2)
            stack_bucket = _bucket_effective_stack(effective_stack_bb)

        preflop_block = _extract_preflop_block(block)

        parsed.append(
            ParsedHand(
                site=site,
                tournament_id=tournament_id,
                hand_id=hand_id,
                hand_ts_local=hand_ts_local,
                table_id=table_id,
                max_players=max_players,
                players_in_hand=players_in_hand,
                is_hu=is_hu,
                level=level,
                sb_amount=sb_amount,
                bb_amount=bb_amount,
                button_seat=button_seat,
                hero_seat=hero_seat,
                hero_position=hero_position,
                effective_stack_bb=effective_stack_bb,
                stack_bucket=stack_bucket,
                hero_cards=hero_cards,
                preflop_spot_type=None,     # classifier fills this
                hero_preflop_action=None,   # classifier fills this
                faced_action_type=None,     # classifier fills this
                is_all_in_preflop=None,     # classifier fills this
                preflop_action_line=preflop_block or None,
                hero_stack_start=hero_stack_start,
                hero_stack_end=None,        # later
                hero_net_chips=None,        # later
                source_file_name=file_name,
                source_file_hash=file_hash,
            )
        )

    return parsed
