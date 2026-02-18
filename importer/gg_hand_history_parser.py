# importer/gg_hand_history_parser.py
# ENHANCED VERSION with intelligent all-in detection
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
    """Split a file into individual hand blocks"""
    starts = [m.start() for m in re.finditer(r"^Poker Hand #", text, re.M)]
    if not starts:
        return []
    blocks = []
    for i, s in enumerate(starts):
        e = starts[i + 1] if i + 1 < len(starts) else len(text)
        blocks.append(text[s:e].strip())
    return blocks


def _parse_positions(active_seats: List[int], button_seat: int) -> dict[int, str]:
    """Returns map seat -> position"""
    seats_sorted = sorted(active_seats)
    if len(seats_sorted) == 2:
        other = [s for s in seats_sorted if s != button_seat][0]
        return {button_seat: "SB", other: "BB"}

    if button_seat not in seats_sorted:
        return {s: None for s in seats_sorted}  # type: ignore

    btn_idx = seats_sorted.index(button_seat)
    sb_seat = seats_sorted[(btn_idx + 1) % len(seats_sorted)]
    bb_seat = seats_sorted[(btn_idx + 2) % len(seats_sorted)]
    return {button_seat: "BTN", sb_seat: "SB", bb_seat: "BB"}


def _bucket_effective_stack(eff_bb: float) -> str:
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
    """Extract lines from HOLE CARDS through just before FLOP"""
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
    """Parse integer removing commas"""
    return int(s.replace(",", ""))


def _extract_showdown_cards(hand_text: str) -> Dict[str, str]:
    """Extract shown hole cards from showdown. Returns {player_name: "AsKh"}"""
    shown_cards = {}
    for m in SHOWS_RE.finditer(hand_text):
        name = m.group("n").strip()
        cards = m.group("c1") + m.group("c2")
        shown_cards[name] = cards
    return shown_cards


def _extract_board_cards(hand_text: str) -> Optional[str]:
    """Extract board cards from summary"""
    m = BOARD_RE.search(hand_text)
    if m:
        return m.group("cards")
    return None


def _detect_allin_street(hand_text: str, starting_stacks: Dict[str, int]) -> Tuple[Optional[str], Optional[str]]:
    """
    ENHANCED all-in detection using THREE methods:
    1. Explicit "and is all-in" markers
    2. Cards shown before streets complete (implicit all-in)
    3. Bet/raise equals player's entire starting stack

    Returns (street, board_at_allin)
    """
    # Method 1: Check for cards shown before flop (most reliable for preflop all-ins)
    shows_pattern = re.compile(r': shows \[', re.M)
    shows_matches = list(shows_pattern.finditer(hand_text))

    if shows_matches:
        flop_pos = hand_text.find(FLOP_MARK) if FLOP_MARK in hand_text else len(hand_text)

        for show_match in shows_matches:
            if show_match.start() < flop_pos:
                # Cards shown before flop = preflop all-in
                return 'preflop', ''

    # Method 2: Explicit all-in marker
    allin_match = ALLIN_RE.search(hand_text)

    if allin_match:
        allin_pos = allin_match.start()

        # Determine street
        flop_pos = hand_text.find(FLOP_MARK) if FLOP_MARK in hand_text else len(hand_text)
        turn_pos = hand_text.find(TURN_MARK) if TURN_MARK in hand_text else len(hand_text)
        river_pos = hand_text.find(RIVER_MARK) if RIVER_MARK in hand_text else len(hand_text)

        if allin_pos < flop_pos:
            return 'preflop', ''
        elif allin_pos < turn_pos:
            flop_match = re.search(r"\*\*\* FLOP \*\*\* \[(?P<cards>[^\]]+)\]", hand_text)
            if flop_match:
                return 'flop', flop_match.group("cards")
            return 'flop', ''
        elif allin_pos < river_pos:
            full_match = re.search(r"\*\*\* TURN \*\*\* \[(?P<board>[^\]]+)\]", hand_text)
            if full_match:
                return 'turn', full_match.group("board")
            return 'turn', ''
        else:
            river_match = re.search(r"\*\*\* RIVER \*\*\* \[(?P<board>[^\]]+)\]", hand_text)
            if river_match:
                board = river_match.group("board")
                cards = board.split()
                if len(cards) >= 4:
                    return 'river', ' '.join(cards[:4])
            return 'river', ''

    # Method 3: Check for implicit all-ins by comparing bet to stack
    # This catches cases where GG doesn't write "and is all-in"
    lines = hand_text.split('\n')

    for line in lines:
        # Stop at flop
        if FLOP_MARK in line:
            break

        # Check for raises that equal player's starting stack
        raise_match = re.match(r'^(\w+): raises [\d,]+ to ([\d,]+)', line)
        if raise_match:
            player = raise_match.group(1)
            amount = _parse_int(raise_match.group(2))

            if player in starting_stacks and starting_stacks[player] == amount:
                # Bet equals entire starting stack = all-in!
                return 'preflop', ''

    return None, None


def _calculate_hero_chip_result(hand_text: str, hero_stack_start: Optional[int]) -> Tuple[Optional[int], Optional[int]]:
    """
    Calculate hero's ending stack and net chips from hand actions.
    FIXED: Properly handles multi-street pots with raises.
    """
    if hero_stack_start is None:
        return None, None

    POST_BLIND_RE = re.compile(r"^Hero: posts (?:small blind|big blind) ([\d,]+)", re.M)
    CALL_RE = re.compile(r"^Hero: calls ([\d,]+)", re.M)
    RAISE_RE = re.compile(r"^Hero: raises [\d,]+ to ([\d,]+)", re.M)
    BET_RE = re.compile(r"^Hero: bets ([\d,]+)", re.M)
    COLLECTED_RE = re.compile(r"^Hero collected ([\d,]+) from pot", re.M)
    UNCALLED_RE = re.compile(r"^Uncalled bet \(([\d,]+)\) returned to Hero", re.M)

    chips_invested = 0
    chips_won = 0

    lines = hand_text.split('\n')

    for line in lines:
        m = POST_BLIND_RE.match(line)
        if m:
            amount = _parse_int(m.group(1))
            chips_invested += amount
            continue

        m = CALL_RE.match(line)
        if m:
            amount = _parse_int(m.group(1))
            chips_invested += amount
            continue

        # CRITICAL FIX: Add full street amount to cumulative total
        m = RAISE_RE.match(line)
        if m:
            street_total = _parse_int(m.group(1))
            chips_invested += street_total
            continue

        m = BET_RE.match(line)
        if m:
            amount = _parse_int(m.group(1))
            chips_invested += amount
            continue

        m = UNCALLED_RE.match(line)
        if m:
            amount = _parse_int(m.group(1))
            chips_invested -= amount
            continue

        m = COLLECTED_RE.match(line)
        if m:
            amount = _parse_int(m.group(1))
            chips_won = amount
            continue

    hero_stack_end = hero_stack_start - chips_invested + chips_won
    hero_net_chips = hero_stack_end - hero_stack_start

    return hero_stack_end, hero_net_chips


def _calculate_equity_and_adjusted_chips(
        hand_text: str,
        hero_cards: Optional[str],
        hero_seat: Optional[int],
        starting_stacks: Dict[str, int]
) -> Tuple[bool, Optional[str], Optional[str], Optional[int], Optional[float], Optional[int], Optional[str]]:
    """Calculate all-in equity and adjusted chips with ENHANCED all-in detection"""

    went_to_showdown = SHOWDOWN_MARK in hand_text

    if not went_to_showdown or not hero_cards:
        return False, None, None, None, None, None, None

    shown_cards = _extract_showdown_cards(hand_text)

    if "Hero" not in shown_cards:
        return True, None, None, None, None, None, None

    # ENHANCED: Pass starting stacks to all-in detector
    allin_street, board_at_allin = _detect_allin_street(hand_text, starting_stacks)

    if not allin_street:
        return True, None, None, None, None, None, None

    villain_cards = {k: v for k, v in shown_cards.items() if k != "Hero"}

    if not villain_cards:
        return True, allin_street, board_at_allin, None, None, None, None

    pot_match = re.search(r"Total pot ([\d,]+)", hand_text)
    if not pot_match:
        return True, allin_street, board_at_allin, None, None, None, None

    pot_at_allin = _parse_int(pot_match.group(1))

    villain_card_list = list(villain_cards.values())
    board_str = board_at_allin.replace(" ", "") if board_at_allin else ""

    equity = calculate_equity(
        hero_cards=hero_cards,
        villain_cards=villain_card_list,
        board=board_str,
        num_simulations=5000
    )

    if equity is None:
        villain_cards_str = "|".join([f"{k}:{v}" for k, v in villain_cards.items()])
        return True, allin_street, board_at_allin, pot_at_allin, None, None, villain_cards_str

    allin_adjusted_chips = int(equity * pot_at_allin)
    villain_cards_str = "|".join([f"{k}:{v}" for k, v in villain_cards.items()])

    return True, allin_street, board_at_allin, pot_at_allin, equity, allin_adjusted_chips, villain_cards_str


def parse_file(path: str, site: str = "GG") -> List[ParsedHand]:
    """Parse a GG hand history file into ParsedHand objects with ENHANCED all-in detection"""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    file_hash = sha256_file(path)
    file_name = os.path.basename(path)

    hand_blocks = _split_into_hand_blocks(text)
    parsed: List[ParsedHand] = []

    for block in hand_blocks:
        try:
            m = HAND_START_RE.search(block)
            if not m:
                continue

            hand_id = m.group("hand_id")
            tournament_id = int(m.group("tournament_id"))

            ts_m = TS_RE.search(block)
            if not ts_m:
                continue
            hand_ts_local = datetime.strptime(ts_m.group("ts"), "%Y/%m/%d %H:%M:%S")

            level = None
            sb_amount = None
            bb_amount = None
            lvl_m = LEVEL_RE.search(block)
            if lvl_m:
                level = int(lvl_m.group("level"))
                sb_amount = int(lvl_m.group("sb"))
                bb_amount = int(lvl_m.group("bb"))
            else:
                continue

            table_id = None
            max_players = 3
            button_seat = None
            t_m = TABLE_RE.search(block)
            if t_m:
                table_id = t_m.group("table_id")
                max_players = int(t_m.group("max_players"))
                button_seat = int(t_m.group("button_seat"))

            # Parse seats and build starting_stacks dict
            seats = []
            hero_seat = None
            hero_stack_start = None
            starting_stacks: Dict[str, int] = {}  # For all-in detection

            for sm in SEAT_RE.finditer(block):
                seat = int(sm.group("seat"))
                name = sm.group("name").strip()
                stack = _parse_int(sm.group("stack"))
                seats.append(seat)
                starting_stacks[name] = stack  # Track all stacks

                if name == "Hero":
                    hero_seat = seat
                    hero_stack_start = stack

            players_in_hand = len(seats)
            is_hu = players_in_hand == 2

            hero_position = None
            if button_seat is not None and hero_seat is not None and seats:
                pos_map = _parse_positions(seats, button_seat)
                hero_position = pos_map.get(hero_seat)

            hero_cards = None
            dealt = DEALT_HERO_RE.search(block)
            if dealt:
                hero_cards = f"{dealt.group('c1')}{dealt.group('c2')}"

            effective_stack_bb = None
            stack_bucket = None
            if hero_stack_start is not None and bb_amount:
                effective_stack_bb = round(hero_stack_start / bb_amount, 2)
                stack_bucket = _bucket_effective_stack(effective_stack_bb)

            preflop_block = _extract_preflop_block(block)

            # ENHANCED: Pass starting_stacks to equity calculator
            (went_to_showdown, allin_street, board_at_allin, pot_at_allin,
             hero_equity_at_allin, allin_adjusted_chips, villain_cards) = _calculate_equity_and_adjusted_chips(
                block, hero_cards, hero_seat, starting_stacks
            )

            hero_stack_end, hero_net_chips = _calculate_hero_chip_result(block, hero_stack_start)

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
                    preflop_spot_type=None,
                    hero_preflop_action=None,
                    faced_action_type=None,
                    is_all_in_preflop=None,
                    preflop_action_line=preflop_block or None,
                    hero_stack_start=hero_stack_start,
                    hero_stack_end=hero_stack_end,
                    hero_net_chips=hero_net_chips,
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

        except Exception as e:
            print(f"Warning: Failed to parse hand block: {type(e).__name__}: {e}")
            continue

    return parsed