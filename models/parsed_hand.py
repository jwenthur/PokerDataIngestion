from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class ParsedHand:
    site: str
    tournament_id: int
    hand_id: str
    hand_ts_local: datetime

    table_id: Optional[str]
    max_players: int
    players_in_hand: int
    is_hu: bool

    level: Optional[int]
    sb_amount: int
    bb_amount: int

    button_seat: Optional[int]
    hero_seat: Optional[int]

    hero_position: Optional[str]
    effective_stack_bb: Optional[float]
    stack_bucket: Optional[str]

    hero_cards: Optional[str]

    preflop_spot_type: Optional[str]
    hero_preflop_action: Optional[str]
    faced_action_type: Optional[str]
    is_all_in_preflop: Optional[bool]
    preflop_action_line: Optional[str]

    hero_stack_start: Optional[int]
    hero_stack_end: Optional[int]
    hero_net_chips: Optional[int]

    # ChipEV calculation fields
    went_to_showdown: Optional[bool]
    allin_street: Optional[str]  # 'preflop', 'flop', 'turn', 'river', or None
    board_at_allin: Optional[str]  # Board cards when all-in occurred
    pot_at_allin: Optional[int]
    hero_equity_at_allin: Optional[float]  # 0-1 range (e.g., 0.6234 = 62.34%)
    allin_adjusted_chips: Optional[int]

    source_file_name: Optional[str]
    source_file_hash: Optional[str]