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
TABLE_RE = re.compile(
    r"^Table '(?P<table_id>[^']+)' (?P<max_players>\d+)-max Seat #(?P<button_seat>\d+) is the button", re.M)
SEAT_RE = re.compile(
    r"^Seat (?P<seat>\d+): (?P<name>.+?) \((?P<stack>[\d,]+) in chips\)", re.M)
DEALT_HERO_RE = re.compile(
    r"^Dealt to Hero \[(?P<c1>[2-9TJQKA][shdc]) (?P<c2>[2-9TJQKA][shdc])\]", re.M)
SHOWS_RE = re.compile(
    r"^(?P<player>.+?): shows \[(?P<c1>[2-9TJQKA][shdc]) (?P<c2>[2-9TJQKA][shdc])\]", re.M)
ALLIN_RE = re.compile(r"^(?P<player>.+?):.*?(?:and is )?all-in", re.M)

HOLE_CARDS_MARK = "*** HOLE CARDS ***"
FLOP_MARK       = "*** FLOP ***"
TURN_MARK       = "*** TURN ***"
RIVER_MARK      = "*** RIVER ***"
SHOWDOWN_MARK   = "*** SHOWDOWN ***"
SUMMARY_MARK    = "*** SUMMARY ***"

# Street boundary markers for per-street investment tracking.
# HOLE_CARDS_MARK is intentionally excluded so that blind posts (which appear
# before that marker) are captured in the preflop segment.
_STREET_MARKS = [FLOP_MARK, TURN_MARK, RIVER_MARK]


def _split_into_hand_blocks(text: str) -> List[str]:
    starts = [m.start() for m in re.finditer(r"^Poker Hand #", text, re.M)]
    if not starts:
        return []
    blocks = []
    for i, s in enumerate(starts):
        e = starts[i + 1] if i + 1 < len(starts) else len(text)
        blocks.append(text[s:e].strip())
    return blocks


def _parse_positions(active_seats: List[int], button_seat: int) -> dict:
    seats_sorted = sorted(active_seats)
    if len(seats_sorted) == 2:
        other = [s for s in seats_sorted if s != button_seat][0]
        return {button_seat: "SB", other: "BB"}
    if button_seat not in seats_sorted:
        return {s: None for s in seats_sorted}
    btn_idx = seats_sorted.index(button_seat)
    sb_seat = seats_sorted[(btn_idx + 1) % len(seats_sorted)]
    bb_seat = seats_sorted[(btn_idx + 2) % len(seats_sorted)]
    return {button_seat: "BTN", sb_seat: "SB", bb_seat: "BB"}


def _bucket_effective_stack(eff_bb: float) -> str:
    if eff_bb <= 5:  return "0-5"
    if eff_bb <= 8:  return "6-8"
    if eff_bb <= 12: return "9-12"
    if eff_bb <= 17: return "13-17"
    if eff_bb <= 25: return "18-25"
    return "26+"


def _extract_preflop_block(hand_text: str) -> str:
    if HOLE_CARDS_MARK not in hand_text:
        return ""
    start = hand_text.index(HOLE_CARDS_MARK)
    if FLOP_MARK in hand_text:
        end = hand_text.index(FLOP_MARK)
    elif SUMMARY_MARK in hand_text:
        end = hand_text.index(SUMMARY_MARK)
    else:
        end = len(hand_text)
    return hand_text[start:end].strip()


def _parse_int(s: str) -> int:
    return int(s.replace(",", ""))


def _split_into_street_segments(hand_text: str) -> List[str]:
    """
    Split hand text into per-street segments for investment tracking.

    Starts at position 0 (not HOLE_CARDS_MARK) so blind posts are included
    in the preflop segment. "raises X to Y" resets only the current street
    counter because Y is street-relative in GGPoker format.
    """
    positions = []
    for mark in _STREET_MARKS:
        pos = hand_text.find(mark)
        if pos != -1:
            positions.append(pos)
    positions = [0] + sorted(positions) + [len(hand_text)]
    segments = [hand_text[positions[i]:positions[i + 1]] for i in range(len(positions) - 1)]
    return segments if segments else [hand_text]


def _calculate_hero_chips_invested(hand_text: str) -> int:
    """
    Total chips Hero committed to the pot.

    Per-street segmentation ensures "raises X to Y" resets only the
    current street total (Y is street-relative, not hand-cumulative).
    """
    grand_total = 0
    for segment in _split_into_street_segments(hand_text):
        street_total = 0
        for m in re.finditer(r"^Hero: (.*)", segment, re.M):
            action = m.group(1)
            if ma := re.match(r"posts (?:small blind|big blind|ante) ([\d,]+)", action):
                street_total += _parse_int(ma.group(1))
            elif ma := re.match(r"calls ([\d,]+)", action):
                street_total += _parse_int(ma.group(1))
            elif ma := re.match(r"raises [\d,]+ to ([\d,]+)", action):
                street_total = _parse_int(ma.group(1))   # reset — street-relative
            elif ma := re.match(r"bets ([\d,]+)", action):
                street_total += _parse_int(ma.group(1))
        grand_total += street_total

    unc = re.search(r"Uncalled bet \(([\d,]+)\) returned to Hero", hand_text, re.M)
    if unc:
        grand_total -= _parse_int(unc.group(1))

    return max(0, grand_total)


def _extract_showdown_cards(hand_text: str) -> Dict[str, str]:
    shown_cards = {}
    for m in SHOWS_RE.finditer(hand_text):
        name = m.group("player").strip()
        shown_cards[name] = m.group("c1") + m.group("c2")
    return shown_cards


def _detect_allin_street(
        hand_text: str,
        starting_stacks: Dict[str, int]
) -> Tuple[Optional[str], Optional[str]]:
    """
    Detect all-in street using three methods:
    1. Cards shown before flop (GGPoker often omits explicit all-in text preflop).
    2. Explicit "and is all-in" marker.
    3. Raise-to amount equals starting stack (stack-comparison fallback).
    """
    flop_pos = hand_text.find(FLOP_MARK) if FLOP_MARK in hand_text else len(hand_text)

    # Method 1: cards shown before flop = preflop all-in
    for show_m in re.finditer(r": shows \[", hand_text, re.M):
        if show_m.start() < flop_pos:
            return "preflop", ""

    # Method 2: explicit all-in marker
    allin_match = ALLIN_RE.search(hand_text)
    if allin_match:
        allin_pos = allin_match.start()
        turn_pos  = hand_text.find(TURN_MARK)  if TURN_MARK  in hand_text else len(hand_text)
        river_pos = hand_text.find(RIVER_MARK) if RIVER_MARK in hand_text else len(hand_text)

        if allin_pos < flop_pos:
            return "preflop", ""
        elif allin_pos < turn_pos:
            fm = re.search(r"\*\*\* FLOP \*\*\* \[(?P<cards>[^\]]+)\]", hand_text)
            return "flop", fm.group("cards") if fm else ""
        elif allin_pos < river_pos:
            tm = re.search(r"\*\*\* TURN \*\*\* \[(?P<board>[^\]]+)\]", hand_text)
            return "turn", tm.group("board") if tm else ""
        else:
            rm = re.search(r"\*\*\* RIVER \*\*\* \[(?P<board>[^\]]+)\]", hand_text)
            if rm:
                cards = rm.group("board").split()
                if len(cards) >= 4:
                    return "river", " ".join(cards[:4])
            return "river", ""

    # Method 3: raise-to equals starting stack (preflop only)
    for line in hand_text.split("\n"):
        if FLOP_MARK in line:
            break
        raise_m = re.match(r"^(\w+): raises [\d,]+ to ([\d,]+)", line)
        if raise_m:
            player = raise_m.group(1)
            amount = _parse_int(raise_m.group(2))
            if player in starting_stacks and starting_stacks[player] == amount:
                return "preflop", ""

    return None, None


def _calculate_hero_chip_result(
        hand_text: str,
        hero_stack_start: Optional[int]
) -> Tuple[Optional[int], Optional[int]]:
    if hero_stack_start is None:
        return None, None

    chips_invested = _calculate_hero_chips_invested(hand_text)
    chips_collected = 0
    collect_m = re.search(r"^Hero collected ([\d,]+) from pot", hand_text, re.M)
    if collect_m:
        chips_collected = _parse_int(collect_m.group(1))

    hero_stack_end = hero_stack_start - chips_invested + chips_collected
    return hero_stack_end, hero_stack_end - hero_stack_start


def _calculate_equity_and_adjusted_chips(
        hand_text: str,
        hero_cards: Optional[str],
        hero_seat: Optional[int],
        starting_stacks: Dict[str, int]
) -> Tuple[bool, Optional[str], Optional[str], Optional[int],
           Optional[float], Optional[int], Optional[int], Optional[str]]:
    """
    Returns:
        (went_to_showdown, allin_street, board_at_allin, pot_at_allin,
         hero_equity_at_allin, hero_chips_invested, allin_adjusted_chips,
         villain_cards_str)

    allin_adjusted_chips = int(hero_equity * pot_at_allin) - hero_chips_invested
    """
    went_to_showdown = SHOWDOWN_MARK in hand_text

    if not went_to_showdown or not hero_cards:
        return False, None, None, None, None, None, None, None

    shown_cards = _extract_showdown_cards(hand_text)
    if "Hero" not in shown_cards:
        return True, None, None, None, None, None, None, None

    allin_street, board_at_allin = _detect_allin_street(hand_text, starting_stacks)
    if not allin_street:
        return True, None, None, None, None, None, None, None

    villain_cards = {k: v for k, v in shown_cards.items() if k != "Hero"}
    if not villain_cards:
        return True, allin_street, board_at_allin, None, None, None, None, None

    pot_match = re.search(r"Total pot ([\d,]+)", hand_text)
    if not pot_match:
        return True, allin_street, board_at_allin, None, None, None, None, None

    pot_at_allin = _parse_int(pot_match.group(1))
    villain_cards_str = "|".join(f"{k}:{v}" for k, v in villain_cards.items())

    board_str = board_at_allin.replace(" ", "") if board_at_allin else ""
    equity = calculate_equity(
        hero_cards=hero_cards,
        villain_cards=list(villain_cards.values()),
        board=board_str,
        num_simulations=5000
    )

    hero_chips_invested = _calculate_hero_chips_invested(hand_text)

    if equity is None:
        return (True, allin_street, board_at_allin, pot_at_allin,
                None, hero_chips_invested, None, villain_cards_str)

    allin_adjusted_chips = int(equity * pot_at_allin) - hero_chips_invested
    return (True, allin_street, board_at_allin, pot_at_allin,
            equity, hero_chips_invested, allin_adjusted_chips, villain_cards_str)


def parse_file(path: str, site: str = "GG") -> List[ParsedHand]:
    """Parse a GG hand history file into ParsedHand objects."""
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

            hand_id       = m.group("hand_id")
            tournament_id = int(m.group("tournament_id"))

            ts_m = TS_RE.search(block)
            if not ts_m:
                continue
            hand_ts_local = datetime.strptime(ts_m.group("ts"), "%Y/%m/%d %H:%M:%S")

            lvl_m = LEVEL_RE.search(block)
            if not lvl_m:
                continue
            level     = int(lvl_m.group("level"))
            sb_amount = int(lvl_m.group("sb"))
            bb_amount = int(lvl_m.group("bb"))

            table_id = None; max_players = 3; button_seat = None
            t_m = TABLE_RE.search(block)
            if t_m:
                table_id    = t_m.group("table_id")
                max_players = int(t_m.group("max_players"))
                button_seat = int(t_m.group("button_seat"))

            seats: List[int] = []
            hero_seat = None; hero_stack_start = None
            starting_stacks: Dict[str, int] = {}

            for sm in SEAT_RE.finditer(block):
                seat  = int(sm.group("seat"))
                name  = sm.group("name").strip()
                stack = _parse_int(sm.group("stack"))
                seats.append(seat)
                starting_stacks[name] = stack
                if name == "Hero":
                    hero_seat        = seat
                    hero_stack_start = stack

            players_in_hand = len(seats)
            is_hu = players_in_hand == 2

            hero_position = None
            if button_seat is not None and hero_seat is not None and seats:
                hero_position = _parse_positions(seats, button_seat).get(hero_seat)

            hero_cards = None
            dealt = DEALT_HERO_RE.search(block)
            if dealt:
                hero_cards = f"{dealt.group('c1')}{dealt.group('c2')}"

            effective_stack_bb = None; stack_bucket = None
            if hero_stack_start is not None and bb_amount:
                effective_stack_bb = round(hero_stack_start / bb_amount, 2)
                stack_bucket       = _bucket_effective_stack(effective_stack_bb)

            preflop_block = _extract_preflop_block(block)

            (went_to_showdown, allin_street, board_at_allin, pot_at_allin,
             hero_equity_at_allin, hero_chips_invested, allin_adjusted_chips,
             villain_cards) = _calculate_equity_and_adjusted_chips(
                block, hero_cards, hero_seat, starting_stacks)

            hero_stack_end, hero_net_chips = _calculate_hero_chip_result(
                block, hero_stack_start)

            parsed.append(ParsedHand(
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
                went_to_showdown=went_to_showdown,
                allin_street=allin_street,
                board_at_allin=board_at_allin,
                pot_at_allin=pot_at_allin,
                hero_equity_at_allin=hero_equity_at_allin,
                hero_chips_invested=hero_chips_invested,
                allin_adjusted_chips=allin_adjusted_chips,
                villain_cards=villain_cards,
                source_file_name=file_name,
                source_file_hash=file_hash,
            ))

        except Exception as e:
            print(f"Warning: Failed to parse hand block: {type(e).__name__}: {e}")
            continue

    return parsed