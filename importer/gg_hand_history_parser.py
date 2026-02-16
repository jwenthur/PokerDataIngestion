# importer/gg_hand_history_parser.py
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import List, Optional, Tuple, Dict

from models import ParsedHand
from utils.hashing import sha256_file
from utils.equity_calculator import calculate_equity

HAND_START_RE = re.compile(
    r"^Poker Hand #(?P<hand_id>\S+): Tournament #(?P<tournament_id>\d+), (?P<game_type>.+?) Hold'em", re.M)
LEVEL_RE = re.compile(r"Level(?P<level>\d+)\((?P<sb>\d+)/(?P<bb>\d+)\)")
TS_RE = re.compile(r"- (?P<ts>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})")
TABLE_RE = re.compile(r"^Table '(?P<table_id>[^']+)' (?P<max_players>\d+)-max Seat #(?P<button_seat>\d+) is the button",
                      re.M)
SEAT_RE = re.compile(r"^Seat (?P<seat>\d+): (?P<name>.+?) \((?P<stack>[\d,]+) in chips\)", re.M)
DEALT_HERO_RE = re.compile(r"^Dealt to Hero \[(?P<c1>[2-9TJQKA][shdc]) (?P<c2>[2-9TJQKA][shdc])\]", re.M)

# Patterns for equity calculation
SHOWS_RE = re.compile(r"^(?P<n>.+?): shows \[(?P<c1>[2-9TJQKA][shdc]) (?P<c2>[2-9TJQKA][shdc])\]", re.M)
BOARD_RE = re.compile(r"^Board \[(?P<cards>.*?)\]", re.M)
COLLECTED_RE = re.compile(r"^(?P<n>.+?) collected (?P<amount>[\d,]+) from pot", re.M)
ALLIN_RE = re.compile(r"^(?P<n>.+?):.*?(?:and is )?all-in", re.M)

HOLE_CARDS_MARK = "*** HOLE CARDS ***"
FLOP_MARK = "*** FLOP ***"
TURN_MARK = "*** TURN ***"
RIVER_MARK = "*** RIVER ***"
SHOWDOWN_MARK = "*** SHOWDOWN ***"
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


def _parse_int(s: str) -> int:
    """Parse integer removing commas: '1,120' -> 1120"""
    return int(s.replace(",", ""))


def _extract_showdown_cards(hand_text: str) -> Dict[str, str]:
    """
    Extract shown hole cards from showdown.
    Returns {player_name: "AsKh"}
    """
    shown_cards = {}
    for m in SHOWS_RE.finditer(hand_text):
        name = m.group("n").strip()
        cards = m.group("c1") + m.group("c2")
        shown_cards[name] = cards
    return shown_cards


def _extract_board_cards(hand_text: str) -> Optional[str]:
    """Extract board cards from summary. Returns 'As Kh Qd Jc Tc' format"""
    m = BOARD_RE.search(hand_text)
    if m:
        return m.group("cards")
    return None


def _detect_allin_street(hand_text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Detect which street the all-in occurred and what board was present.
    Returns (street, board_at_allin) where:
      - street: 'preflop', 'flop', 'turn', 'river', or None
      - board_at_allin: Cards on board when all-in occurred (e.g., '2c 5h 9c')
    """
    # Find all-in action
    allin_match = ALLIN_RE.search(hand_text)
    if not allin_match:
        return None, None

    allin_pos = allin_match.start()

    # Determine which street based on position
    flop_pos = hand_text.find(FLOP_MARK) if FLOP_MARK in hand_text else len(hand_text)
    turn_pos = hand_text.find(TURN_MARK) if TURN_MARK in hand_text else len(hand_text)
    river_pos = hand_text.find(RIVER_MARK) if RIVER_MARK in hand_text else len(hand_text)

    if allin_pos < flop_pos:
        return 'preflop', ''
    elif allin_pos < turn_pos:
        # All-in on flop - extract flop cards
        flop_match = re.search(r"\*\*\* FLOP \*\*\* \[(?P<cards>[^\]]+)\]", hand_text)
        if flop_match:
            return 'flop', flop_match.group("cards")
        return 'flop', ''
    elif allin_pos < river_pos:
        # All-in on turn - extract flop + turn
        turn_match = re.search(r"\*\*\* TURN \*\*\* \[([^\]]+)\] \[(?P<turn>[^\]]+)\]", hand_text)
        if turn_match:
            # Get full board through turn from the TURN line
            full_match = re.search(r"\*\*\* TURN \*\*\* \[(?P<board>[^\]]+)\]", hand_text)
            if full_match:
                return 'turn', full_match.group("board")
        return 'turn', ''
    else:
        # All-in on river - extract full board minus river
        river_match = re.search(r"\*\*\* RIVER \*\*\* \[([^\]]+)\] \[(?P<river>[^\]]+)\]", hand_text)
        if river_match:
            full_match = re.search(r"\*\*\* RIVER \*\*\* \[(?P<board>[^\]]+)\]", hand_text)
            if full_match:
                # Remove the last card (river card)
                board = full_match.group("board")
                cards = board.split()
                if len(cards) >= 4:
                    return 'river', ' '.join(cards[:4])
        return 'river', ''


def _calculate_equity_and_adjusted_chips(
        hand_text: str,
        hero_cards: Optional[str],
        hero_seat: Optional[int]
) -> Tuple[bool, Optional[str], Optional[str], Optional[int], Optional[float], Optional[int], Optional[str]]:
    """
    Calculate all-in equity and adjusted chips.

    Returns:
        (went_to_showdown, allin_street, board_at_allin, pot_at_allin,
         hero_equity_at_allin, allin_adjusted_chips, villain_cards)
    """
    # Check if hand went to showdown
    went_to_showdown = SHOWDOWN_MARK in hand_text

    if not went_to_showdown or not hero_cards:
        return False, None, None, None, None, None, None

    # Extract all shown cards
    shown_cards = _extract_showdown_cards(hand_text)

    # CRITICAL CHECK: Was Hero in the showdown?
    # If Hero folded before showdown, Hero won't be in shown_cards
    if "Hero" not in shown_cards:
        # Hero folded - don't calculate equity for a pot Hero isn't in
        return True, None, None, None, None, None, None

    # Detect all-in street and board
    allin_street, board_at_allin = _detect_allin_street(hand_text)

    if not allin_street:
        # No all-in detected, but went to showdown - still went to showdown
        return True, None, None, None, None, None, None

    # Get villain cards (Hero was in showdown)
    villain_cards = {k: v for k, v in shown_cards.items() if k != "Hero"}

    if not villain_cards:
        # No villain cards shown, can't calculate equity
        return True, allin_street, board_at_allin, None, None, None, None

    # Get pot size from summary
    pot_match = re.search(r"Total pot ([\d,]+)", hand_text)
    if not pot_match:
        return True, allin_street, board_at_allin, None, None, None, None

    pot_at_allin = _parse_int(pot_match.group(1))

    # Calculate equity
    villain_card_list = list(villain_cards.values())
    board_str = board_at_allin.replace(" ", "") if board_at_allin else ""

    equity = calculate_equity(
        hero_cards=hero_cards,
        villain_cards=villain_card_list,
        board=board_str,
        num_simulations=5000  # Balance speed vs accuracy
    )

    if equity is None:
        # Equity calculation failed
        villain_cards_str = "|".join([f"{k}:{v}" for k, v in villain_cards.items()])
        return True, allin_street, board_at_allin, pot_at_allin, None, None, villain_cards_str

    # Calculate adjusted chips
    # Hero's EV in this pot = equity * pot_size
    allin_adjusted_chips = int(equity * pot_at_allin)

    # Format villain cards for storage: "player1:AhKd|player2:QsJs"
    villain_cards_str = "|".join([f"{k}:{v}" for k, v in villain_cards.items()])

    return True, allin_street, board_at_allin, pot_at_allin, equity, allin_adjusted_chips, villain_cards_str


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
            stack = _parse_int(sm.group("stack"))  # Handle commas in stack sizes
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

        # Effective stack in BB (start-of-hand approximation; we'll refine to decision-point later)
        effective_stack_bb = None
        stack_bucket = None
        if hero_stack_start is not None and bb_amount:
            # Approx effective stack = hero stack / BB for now.
            # Later: use min(hero, villain) and decision-point.
            effective_stack_bb = round(hero_stack_start / bb_amount, 2)
            stack_bucket = _bucket_effective_stack(effective_stack_bb)

        preflop_block = _extract_preflop_block(block)

        # Calculate equity and adjusted chips
        (went_to_showdown, allin_street, board_at_allin, pot_at_allin,
         hero_equity_at_allin, allin_adjusted_chips, villain_cards) = _calculate_equity_and_adjusted_chips(
            block, hero_cards, hero_seat
        )

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
                hero_stack_end=None,  # later
                hero_net_chips=None,  # later
                # ChipEV fields
                went_to_showdown=went_to_showdown,
                allin_street=allin_street,
                board_at_allin=board_at_allin,
                pot_at_allin=pot_at_allin,
                hero_equity_at_allin=hero_equity_at_allin,
                allin_adjusted_chips=allin_adjusted_chips,
                villain_cards=villain_cards,
                source_file_name=file_name,
                source_file_hash=file_hash,
            )
        )

    return parsed