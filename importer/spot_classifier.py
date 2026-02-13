# importer/spot_classifier.py
from __future__ import annotations

import re
from dataclasses import replace
from typing import Optional

from models import ParsedHand, SpotType, FacedActionType, ActionType

HERO_ACTION_RE = re.compile(
    r"^Hero:\s+(?P<action>folds|checks|calls|raises)\b(?P<rest>.*)$",
    re.M
)
# Match villain actions - exclude Hero and prevent matching across newlines
VILLAIN_ACTION_RE = re.compile(
    r"^(?!Hero:)(?P<n>[^\n:]+?):\s+(?P<action>folds|checks|calls|raises)\b(?P<rest>.*)$",
    re.M
)


def _parse_action_type(action_word: str, rest: str) -> ActionType:
    """Parse action word and context to determine ActionType."""
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


def _detect_faced_action(
        preflop_text: str,
        sb_amount: int,
        bb_amount: int,
        hero_position: Optional[str]
) -> FacedActionType:
    """
    Determine what hero faced BEFORE hero's first action.

    Logic:
    - If hero is SB (first to act): faced NONE
    - If hero is BB/BTN: look at first villain action before hero acts
    """
    if not preflop_text or not hero_position:
        return FacedActionType.NONE

    # SB acts first, so faces NONE
    if hero_position == "SB":
        return FacedActionType.NONE

    # Find all action lines in order
    all_actions = []

    # Parse all actions (both hero and villain)
    for line in preflop_text.split('\n'):
        line = line.strip()
        if not line or line.startswith('***') or line.startswith('Dealt') or line.startswith('Uncalled'):
            continue

        # Check if it's a hero action
        hero_m = re.match(r"^Hero:\s+(?P<action>folds|checks|calls|raises)\b(?P<rest>.*)$", line)
        if hero_m:
            all_actions.append(("Hero", hero_m.group("action"), hero_m.group("rest") or ""))
            continue

        # Check if it's a villain action
        villain_m = re.match(r"^([^:]+?):\s+(?P<action>folds|checks|calls|raises)\b(?P<rest>.*)$", line)
        if villain_m:
            all_actions.append((villain_m.group(1).strip(), villain_m.group("action"), villain_m.group("rest") or ""))

    if not all_actions:
        return FacedActionType.NONE

    # Find first villain action (before hero acts)
    for name, action, rest in all_actions:
        if name == "Hero":
            # Hero acted - stop here
            break

        # This is a villain action before hero
        action = action.lower()
        rest_l = rest.lower()

        if action == "calls":
            # In HU SB position, a "call" is typically a limp/complete
            # e.g., "SB: calls 10" means SB completed to BB
            return FacedActionType.LIMP

        if action == "raises":
            if "all-in" in rest_l:
                return FacedActionType.OPEN_JAM

            # Detect minraise: "raises X to Y" where Y == 2*BB
            to_m = re.search(r"\bto\s+([\d,]+)\b", rest)
            if to_m:
                to_amt_str = to_m.group(1).replace(",", "")
                to_amt = int(to_amt_str)
                if to_amt == 2 * bb_amount:
                    return FacedActionType.MINRAISE

            return FacedActionType.RAISE

        if action == "folds":
            # Villain folded before hero - hero faces NONE
            return FacedActionType.NONE

    return FacedActionType.NONE


def classify_preflop(hand: ParsedHand) -> ParsedHand:
    """
    Enrich ParsedHand with preflop spot classification.

    Classifies:
    - preflop_spot_type: The specific spot (e.g., HU_SB_FTA, HU_BB_VS_SB_MINRAISE)
    - faced_action_type: What action hero faced
    - hero_preflop_action: Hero's first action
    - is_all_in_preflop: Whether hero went all-in preflop
    """
    pre = hand.preflop_action_line or ""
    if not pre:
        return replace(
            hand,
            preflop_spot_type=SpotType.UNKNOWN.value,
            faced_action_type=FacedActionType.NONE.value,
        )

    # === HERO'S FIRST ACTION ===
    hero_m = HERO_ACTION_RE.search(pre)
    hero_action = None
    is_all_in_pre = None
    if hero_m:
        hero_action = _parse_action_type(
            hero_m.group("action"),
            hero_m.group("rest") or ""
        ).value
        is_all_in_pre = (hero_action == ActionType.JAM.value)

    # === FACED ACTION ===
    faced = FacedActionType.NONE
    if hand.sb_amount and hand.bb_amount and hand.hero_position:
        faced = _detect_faced_action(
            pre,
            hand.sb_amount,
            hand.bb_amount,
            hand.hero_position
        )

    # === SPOT TYPE CLASSIFICATION ===
    spot = SpotType.UNKNOWN

    if hand.is_hu:
        # Heads-up spots
        if hand.hero_position == "SB":
            # SB first to act
            spot = SpotType.HU_SB_FTA
        elif hand.hero_position == "BB":
            # BB facing SB action
            if faced == FacedActionType.LIMP:
                spot = SpotType.HU_BB_VS_SB_LIMP
            elif faced == FacedActionType.MINRAISE:
                spot = SpotType.HU_BB_VS_SB_MINRAISE
            elif faced == FacedActionType.RAISE:
                # For now, treat any raise (including 3x, 4x, etc) as part of minraise category
                # Or we could add a new spot type for "HU_BB_VS_SB_RAISE"
                spot = SpotType.HU_BB_VS_SB_MINRAISE  # Group all raises together
            elif faced == FacedActionType.OPEN_JAM:
                spot = SpotType.HU_BB_VS_SB_OPEN_JAM
            else:
                spot = SpotType.UNKNOWN
    else:
        # 3-handed spots
        if hand.hero_position == "BTN":
            spot = SpotType.TH_BTN_FTA
        elif hand.hero_position == "SB":
            # SB vs BTN open (if BTN raised)
            if faced in (FacedActionType.RAISE, FacedActionType.MINRAISE, FacedActionType.OPEN_JAM):
                spot = SpotType.TH_SB_VS_BTN_OPEN
            else:
                spot = SpotType.UNKNOWN
        elif hand.hero_position == "BB":
            # BB vs BTN open
            if faced in (FacedActionType.RAISE, FacedActionType.MINRAISE, FacedActionType.OPEN_JAM):
                spot = SpotType.TH_BB_VS_BTN_OPEN
            else:
                spot = SpotType.UNKNOWN

    return replace(
        hand,
        preflop_spot_type=spot.value,
        faced_action_type=faced.value,
        hero_preflop_action=hero_action,
        is_all_in_preflop=is_all_in_pre,
    )