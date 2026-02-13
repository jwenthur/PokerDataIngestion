# importer/gg_hand_history_parser.py
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from models import ParsedHand
from utils.hashing import sha256_file

HAND_START_RE = re.compile(
    r"^Poker Hand #(?P<hand_id>\S+): Tournament #(?P<tournament_id>\d+), "
    r"(?P<game_type>.+?) Hold'em",
    re.M
)
LEVEL_RE = re.compile(r"Level(?P<level>\d+)\((?P<sb>[\d,]+)/(?P<bb>[\d,]+)\)")
TS_RE = re.compile(r"- (?P<ts>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})")
TABLE_RE = re.compile(
    r"^Table '(?P<table_id>[^']+)' (?P<max_players>\d+)-max Seat #(?P<button_seat>\d+) is the button",
    re.M
)
SEAT_RE = re.compile(
    r"^Seat (?P<seat>\d+): (?P<name>.+?) \((?P<stack>[\d,]+) in chips\)",
    re.M
)
DEALT_HERO_RE = re.compile(
    r"^Dealt to Hero \[(?P<c1>[2-9TJQKA][shdc]) (?P<c2>[2-9TJQKA][shdc])\]",
    re.M
)

# Summary extraction for hero results
SUMMARY_HERO_RESULT_RE = re.compile(
    r"^Seat \d+: Hero .* (?:won|lost|collected) \((?P<result>[\d,]+)\)",
    re.M
)
SHOWDOWN_COLLECT_RE = re.compile(
    r"^(?P<name>.+?) collected (?P<amount>[\d,]+) from pot",
    re.M
)

HOLE_CARDS_MARK = "*** HOLE CARDS ***"
FLOP_MARK = "*** FLOP ***"
SUMMARY_MARK = "*** SUMMARY ***"


def _parse_int(value: str) -> int:
    """Parse integer, handling commas."""
    return int(value.replace(",", ""))


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
    For HU (2 players): button seat is SB, other seat is BB.
    For 3-handed: BTN (button), SB (next), BB (next next) in seat-order around the table.

    Args:
        active_seats: List of seat numbers present in the hand (can be non-contiguous)
        button_seat: The seat number of the button
    """
    seats_sorted = sorted(active_seats)

    if len(seats_sorted) == 2:
        # HU: button = SB, other = BB
        other_seats = [s for s in seats_sorted if s != button_seat]
        if not other_seats:
            # Defensive: button not in active seats
            return {s: None for s in seats_sorted}  # type: ignore
        other = other_seats[0]
        return {button_seat: "SB", other: "BB"}

    # 3-handed: BTN, SB, BB
    if button_seat not in seats_sorted:
        # Fallback: button not found in active seats
        return {s: None for s in seats_sorted}  # type: ignore

    btn_idx = seats_sorted.index(button_seat)
    sb_seat = seats_sorted[(btn_idx + 1) % len(seats_sorted)]
    bb_seat = seats_sorted[(btn_idx + 2) % len(seats_sorted)]
    return {button_seat: "BTN", sb_seat: "SB", bb_seat: "BB"}


def _bucket_effective_stack(eff_bb: float) -> str:
    """Categorize effective stack into buckets."""
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


def _calculate_hero_total_bet(hand_text: str) -> int:
    """
    Calculate the total amount Hero bet/put into the pot during the hand.

    This tracks:
    - Blinds posted
    - Calls
    - Raises (the additional amount added)
    - All-ins

    Returns:
        Total chips Hero committed to the pot
    """
    total_bet = 0

    # Get all Hero action lines
    hero_actions = []
    for match in re.finditer(r"^Hero: (.*)", hand_text, re.M):
        action_line = match.group(1)
        hero_actions.append(action_line)

    # Parse each action
    for action in hero_actions:
        # "posts small blind 15"
        if m := re.match(r"posts (?:small blind|big blind|ante) (?P<amt>[\d,]+)", action):
            total_bet += _parse_int(m.group("amt"))
        # "calls 110"
        elif m := re.match(r"calls (?P<amt>[\d,]+)", action):
            total_bet += _parse_int(m.group("amt"))
        # "raises 290 to 380 and is all-in"
        # The "to 380" is total IN THE POT at that point from Hero
        # We need to add the NET increase from this raise
        elif m := re.match(r"raises (?P<raise_amt>[\d,]+) to (?P<to_amt>[\d,]+)", action):
            raise_amt = _parse_int(m.group("raise_amt"))
            total_bet += raise_amt
        # "bets 60"
        elif m := re.match(r"bets (?P<amt>[\d,]+)", action):
            total_bet += _parse_int(m.group("amt"))
        # "checks" or "folds" - no money
        elif action.startswith("checks") or action.startswith("folds"):
            pass

    # Check for uncalled bets returned to Hero
    uncalled_match = re.search(r"Uncalled bet \((?P<amt>[\d,]+)\) returned to Hero", hand_text, re.M)
    if uncalled_match:
        uncalled = _parse_int(uncalled_match.group("amt"))
        total_bet -= uncalled

    return total_bet


def _compute_hero_net_chips(
        hand_text: str,
        hero_stack_start: int,
        hero_seat: int
) -> Tuple[Optional[int], Optional[int]]:
    """
    Compute hero's ending stack and net chips for the hand.

    Returns:
        (hero_stack_end, hero_net_chips)
    """
    # Try to find hero's result in summary
    summary_match = re.search(
        rf"^Seat {hero_seat}: Hero .* (?:and won|and lost|collected) \((?P<amount>[\d,]+)\)",
        hand_text,
        re.M
    )

    if summary_match:
        # Hero won/collected chips - the amount is the total pot won
        collected = _parse_int(summary_match.group("amount"))
        hero_stack_end = collected
        hero_net_chips = collected - hero_stack_start
        return hero_stack_end, hero_net_chips

    # Check if hero lost (showed and lost pattern)
    lost_match = re.search(
        rf"^Seat {hero_seat}: Hero .* and lost",
        hand_text,
        re.M
    )

    if lost_match:
        # Hero lost the pot - check if Hero went all-in
        allin_match = re.search(r"^Hero:.*and is all-in", hand_text, re.M)

        if allin_match:
            # Hero lost while all-in, stack is now 0
            hero_stack_end = 0
            hero_net_chips = -hero_stack_start
        else:
            # Hero lost but wasn't all-in - calculate actual loss by tracking bets
            total_bet = _calculate_hero_total_bet(hand_text)
            hero_stack_end = hero_stack_start - total_bet
            hero_net_chips = -total_bet

        return hero_stack_end, hero_net_chips

    # Check showdown for collected amounts
    collect_match = re.search(
        r"^Hero collected (?P<amount>[\d,]+) from pot",
        hand_text,
        re.M
    )

    if collect_match:
        collected = _parse_int(collect_match.group("amount"))
        # The collected amount is the total pot Hero won
        # Hero's ending stack is the collected amount (since it includes Hero's own bets back)
        # Hero's net chips is collected minus what Hero started with
        hero_stack_end = collected
        hero_net_chips = collected - hero_stack_start
        return hero_stack_end, hero_net_chips

    # Check if Hero folded (and didn't win/lose at showdown)
    fold_match = re.search(
        rf"^Seat {hero_seat}: Hero .* folded",
        hand_text,
        re.M
    )

    if fold_match:
        # Hero folded - calculate what Hero lost (blinds/bets before folding)
        total_bet = _calculate_hero_total_bet(hand_text)
        hero_stack_end = hero_stack_start - total_bet
        hero_net_chips = -total_bet
        return hero_stack_end, hero_net_chips

    # No clear result found - still try to calculate from actions
    # This handles edge cases like Hero folding without a summary line
    total_bet = _calculate_hero_total_bet(hand_text)
    if total_bet > 0:
        # Hero put chips in but we don't have a clear win/loss indicator
        # Assume Hero lost what was bet (most common case for missing patterns)
        hero_stack_end = hero_stack_start - total_bet
        hero_net_chips = -total_bet
        return hero_stack_end, hero_net_chips

    # Truly no action found - return None
    return None, None


def parse_file(path: str, site: str = "GG") -> List[ParsedHand]:
    """
    Parse a GG hand history file into ParsedHand objects.

    Handles:
    - Hands in any order (reverse chronological or chronological)
    - Heads-up hands with only 2 seats (any seat numbers)
    - Commas in numeric values
    - Missing players (busted players not in seat list)

    Args:
        path: Path to the hand history file
        site: Site identifier (default "GG")

    Returns:
        List of ParsedHand objects, one per hand in the file
    """
    path_obj = Path(path)

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    file_hash = sha256_file(path_obj)
    file_name = path_obj.name

    hand_blocks = _split_into_hand_blocks(text)
    parsed: List[ParsedHand] = []

    for block in hand_blocks:
        try:
            # === HEADER PARSING ===
            m = HAND_START_RE.search(block)
            if not m:
                continue

            hand_id = m.group("hand_id")
            tournament_id = int(m.group("tournament_id"))

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
                sb_amount = _parse_int(lvl_m.group("sb"))
                bb_amount = _parse_int(lvl_m.group("bb"))
            else:
                # Blinds are essential; skip if missing
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

            # === SEATS AND STACKS ===
            seats: List[int] = []
            seat_stacks: dict[int, int] = {}
            hero_seat = None
            hero_stack_start = None

            for sm in SEAT_RE.finditer(block):
                seat = int(sm.group("seat"))
                name = sm.group("name").strip()
                stack = _parse_int(sm.group("stack"))

                seats.append(seat)
                seat_stacks[seat] = stack

                if name == "Hero":
                    hero_seat = seat
                    hero_stack_start = stack

            players_in_hand = len(seats)
            is_hu = players_in_hand == 2

            # === POSITION CALCULATION ===
            hero_position = None
            if button_seat is not None and hero_seat is not None and seats:
                pos_map = _parse_positions(seats, button_seat)
                hero_position = pos_map.get(hero_seat)

            # === HERO CARDS ===
            hero_cards = None
            dealt = DEALT_HERO_RE.search(block)
            if dealt:
                hero_cards = f"{dealt.group('c1')}{dealt.group('c2')}"

            # === EFFECTIVE STACK CALCULATION ===
            # Effective stack = min(hero_stack, villain_stack) at start of hand
            effective_stack_bb = None
            stack_bucket = None

            if hero_stack_start is not None and bb_amount:
                if is_hu and len(seat_stacks) == 2:
                    # HU: effective stack is min of both stacks
                    villain_stacks = [stack for seat, stack in seat_stacks.items() if seat != hero_seat]
                    if villain_stacks:
                        effective_stack = min(hero_stack_start, villain_stacks[0])
                        effective_stack_bb = round(effective_stack / bb_amount, 2)
                        stack_bucket = _bucket_effective_stack(effective_stack_bb)
                else:
                    # 3-handed: use minimum of all stacks
                    all_stacks = list(seat_stacks.values())
                    if all_stacks:
                        effective_stack = min(all_stacks)
                        effective_stack_bb = round(effective_stack / bb_amount, 2)
                        stack_bucket = _bucket_effective_stack(effective_stack_bb)

            # === PREFLOP ACTION LINE ===
            preflop_block = _extract_preflop_block(block)

            # === HERO NET CHIPS ===
            hero_stack_end, hero_net_chips = _compute_hero_net_chips(
                block, hero_stack_start or 0, hero_seat or 0
            )

            # === BUILD PARSED HAND ===
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
                    preflop_spot_type=None,  # classifier fills this
                    hero_preflop_action=None,  # classifier fills this
                    faced_action_type=None,  # classifier fills this
                    is_all_in_preflop=None,  # classifier fills this
                    preflop_action_line=preflop_block or None,
                    hero_stack_start=hero_stack_start,
                    hero_stack_end=hero_stack_end,
                    hero_net_chips=hero_net_chips,
                    source_file_name=file_name,
                    source_file_hash=file_hash,
                )
            )

        except Exception as e:
            # Log parse error but continue with other hands
            print(f"Warning: Failed to parse hand block: {type(e).__name__}: {e}")
            continue

    return parsed