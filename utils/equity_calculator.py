"""
Poker equity calculator using Monte Carlo simulation.
Calculates hero's equity in heads-up and 3-way all-in situations.
"""
from __future__ import annotations

import random
from typing import List, Tuple
from itertools import combinations

# Card rank and suit mappings
RANKS = '23456789TJQKA'
SUITS = 'shdc'
RANK_VALUES = {r: i for i, r in enumerate(RANKS, start=2)}
RANK_VALUES['T'] = 10
RANK_VALUES['J'] = 11
RANK_VALUES['Q'] = 12
RANK_VALUES['K'] = 13
RANK_VALUES['A'] = 14


def card_to_tuple(card: str) -> Tuple[int, str]:
    """Convert card string like 'Ks' to (rank_value, suit)"""
    rank = card[0]
    suit = card[1]
    return (RANK_VALUES[rank], suit)


def cards_to_tuples(cards_str: str) -> List[Tuple[int, str]]:
    """Convert cards string like 'KsJc' to list of tuples"""
    cards = []
    i = 0
    while i < len(cards_str):
        if i + 1 < len(cards_str):
            cards.append(card_to_tuple(cards_str[i:i + 2]))
            i += 2
        else:
            i += 1
    return cards


def hand_rank(cards: List[Tuple[int, str]]) -> Tuple[int, List[int]]:
    """
    Evaluate a 5-card poker hand.
    Returns (hand_type, kickers) where hand_type is 0-8.
    """
    if len(cards) != 5:
        raise ValueError("Must have exactly 5 cards")

    ranks = sorted([c[0] for c in cards], reverse=True)
    suits = [c[1] for c in cards]

    is_flush = len(set(suits)) == 1

    # Check for straight
    is_straight = False
    if ranks == list(range(ranks[0], ranks[0] - 5, -1)):
        is_straight = True
    elif ranks == [14, 5, 4, 3, 2]:  # Wheel
        is_straight = True
        ranks = [5, 4, 3, 2, 1]

    rank_counts = {}
    for r in ranks:
        rank_counts[r] = rank_counts.get(r, 0) + 1

    counts = sorted(rank_counts.values(), reverse=True)
    unique_ranks = sorted(rank_counts.keys(), key=lambda r: (rank_counts[r], r), reverse=True)

    if is_straight and is_flush:
        return (8, ranks[:1])
    if counts == [4, 1]:
        return (7, unique_ranks)
    if counts == [3, 2]:
        return (6, unique_ranks)
    if is_flush:
        return (5, ranks)
    if is_straight:
        return (4, ranks[:1])
    if counts == [3, 1, 1]:
        return (3, unique_ranks)
    if counts == [2, 2, 1]:
        return (2, unique_ranks)
    if counts == [2, 1, 1, 1]:
        return (1, unique_ranks)

    return (0, ranks)


def best_five_card_hand(cards: List[Tuple[int, str]]) -> Tuple[int, List[int]]:
    """Find the best 5-card hand from 5, 6, or 7 cards"""
    if len(cards) == 5:
        return hand_rank(cards)

    best = None
    for combo in combinations(cards, 5):
        rank = hand_rank(list(combo))
        if best is None or rank > best:
            best = rank
    return best


def create_deck() -> List[Tuple[int, str]]:
    """Create a standard 52-card deck"""
    deck = []
    for rank in RANKS:
        for suit in SUITS:
            deck.append((RANK_VALUES[rank], suit))
    return deck


def calculate_equity(
        hero_cards: str,
        villain_cards: List[str],
        board: str = "",
        num_simulations: int = 5000
) -> float:
    """
    Calculate hero's equity using Monte Carlo simulation.

    Args:
        hero_cards: Hero's hole cards, e.g., "KsJc"
        villain_cards: List of villain hole cards, e.g., ["Ts9s"] for HU, ["Ts9s", "AhKh"] for 3-way
        board: Community cards dealt so far, e.g., "2c5h9c" for flop, "" for preflop
        num_simulations: Number of random runouts to simulate

    Returns:
        Hero's equity as a float between 0 and 1 (e.g., 0.6234 = 62.34%)
    """
    try:
        hero = cards_to_tuples(hero_cards)
        villains = [cards_to_tuples(v) for v in villain_cards]
        board_cards = cards_to_tuples(board) if board else []

        # Create deck and remove known cards
        deck = create_deck()
        known_cards = hero + board_cards
        for v in villains:
            known_cards.extend(v)

        deck = [c for c in deck if c not in known_cards]

        # Number of cards to deal to complete the board
        cards_to_deal = 5 - len(board_cards)

        wins = 0
        ties = 0

        for _ in range(num_simulations):
            # Shuffle and deal remaining board cards
            random.shuffle(deck)
            simulated_board = board_cards + deck[:cards_to_deal]

            # Evaluate all hands
            hero_hand = best_five_card_hand(hero + simulated_board)
            villain_hands = [best_five_card_hand(v + simulated_board) for v in villains]

            # Determine winner
            best_villain = max(villain_hands)

            if hero_hand > best_villain:
                wins += 1
            elif hero_hand == best_villain:
                # Count how many players tied
                num_tied = 1 + sum(1 for vh in villain_hands if vh == hero_hand)
                ties += 1.0 / num_tied

        equity = (wins + ties) / num_simulations
        return equity

    except Exception as e:
        # If equity calculation fails, return None
        return None