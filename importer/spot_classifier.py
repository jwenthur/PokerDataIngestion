# importer/spot_classifier.py
from __future__ import annotations

import re
from dataclasses import replace
from typing import Optional, Tuple

from models import ParsedHand, SpotType, FacedActionType, ActionType


HERO_ACTION_RE = re.compile(r"^Hero:\s+(?P<action>folds|checks|calls|raises)\b(?P<rest>.*)$", re.M)
VILLAIN_ACTION_RE = re.compile(r"^(?!Hero:)(?P<name>[^:]+):\s+(?P<action>folds|checks|calls|raises)\b(?P<rest>.*)$", re.M)


def _parse_action_type(action_word: str, rest: str) -> ActionType:
    action_word = action_word.lower()
    rest_l = rest.lower()

    if action_word == "folds":
        return ActionType.FOLD
    if action_word == "checks":
        return ActionType.CHECK
    if action_word == "calls":
        return ActionType.CALL
    if action_word == "raises":
        # If it includes "all-in" then treat as JAM
        if "all-in" in rest_l:
            return ActionType.JAM
        return ActionType.RAISE

    # fallback
    return ActionType.CHECK


def _is_open_jam(action_word: str, rest: str) -> bool:
    return action_word.lower() == "raises" and "all-in" in rest.lower()


def _detect_faced_action(preflop_text: str, sb_amount: int, bb_amount: int) -> FacedActionType:
    """
    Determine what hero faced BEFORE hero’s first action, in a simple way.
    We assume HU scenarios and look for the first non-hero action in preflop block.
    """
    if not preflop_text:
        return FacedActionType.NONE

    # Find first villain action line
    for m in VILLAIN_ACTION_RE.finditer(preflop_text):
        action = m.group("action").lower()
        rest = (m.group("rest") or "").strip().lower()

        if action == "calls":
            # In HU, SB limp is usually "calls <sb>" after posting.
            # We don't trust the exact amount string; we just treat a call as a limp if no raise happened first.
            return FacedActionType.LIMP

        if action == "raises":
            if "all-in" in rest:
                return FacedActionType.OPEN_JAM

            # Detect minraise by "to <amount>"
            # Example: "raises 80 to 160"
            to_m = re.search(r"\bto\s+(?P<to_amt>\d+)\b", rest)
            if to_m:
                to_amt = int(to_m.group("to_amt"))
                if to_amt == 2 * bb_amount:
                    return FacedActionType.MINRAISE
            return FacedActionType.RAISE

        # folds/checks from SB means hero faced NONE as BB
        return FacedActionType.NONE

    return FacedActionType.NONE


def classify_preflop(hand: ParsedHand) -> ParsedHand:
    """
    Enrich ParsedHand with preflop spot classification for Phase 1.
    """
    pre = hand.preflop_action_line or ""
    if not pre:
        return replace(
            hand,
            preflop_spot_type=SpotType.UNKNOWN.value,
            faced_action_type=FacedActionType.NONE.value,
        )

    # Hero first action
    hero_m = HERO_ACTION_RE.search(pre)
    hero_action = None
    is_all_in_pre = None
    if hero_m:
        hero_action = _parse_action_type(hero_m.group("action"), hero_m.group("rest") or "").value
        is_all_in_pre = (hero_action == ActionType.JAM.value)

    faced = FacedActionType.NONE
    if hand.sb_amount and hand.bb_amount:
        faced = _detect_faced_action(pre, hand.sb_amount, hand.bb_amount)

    # Spot type logic (Phase 1)
    spot = SpotType.UNKNOWN
    if hand.is_hu:
        if hand.hero_position == "SB":
            spot = SpotType.HU_SB_FTA
        elif hand.hero_position == "BB":
            if faced == FacedActionType.LIMP:
                spot = SpotType.HU_BB_VS_SB_LIMP
            elif faced == FacedActionType.MINRAISE:
                spot = SpotType.HU_BB_VS_SB_MINRAISE
            elif faced == FacedActionType.OPEN_JAM:
                spot = SpotType.HU_BB_VS_SB_OPEN_JAM
            else:
                spot = SpotType.UNKNOWN
    else:
        # Minimal 3-handed scaffolding
        if hand.hero_position == "BTN":
            spot = SpotType.TH_BTN_FTA
        else:
            spot = SpotType.UNKNOWN

    return replace(
        hand,
        preflop_spot_type=spot.value,
        faced_action_type=faced.value,
        hero_preflop_action=hero_action,
        is_all_in_preflop=is_all_in_pre,
    )
