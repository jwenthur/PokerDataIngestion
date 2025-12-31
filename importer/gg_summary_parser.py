from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple


@dataclass(frozen=True)
class ParsedTournament:
    site: str
    tournament_id: int
    tournament_start_ts_local: datetime
    hero_name: str

    tournament_name: Optional[str]
    game_type: Optional[str]
    player_count: Optional[int]
    currency: str

    buy_in_amount: float
    prize_pool_amount: Optional[float]
    payout_amount: float
    profit_amount: float
    finish_place: Optional[int]


def parse_money_usd(token: str) -> Optional[float]:
    """
    Accepts: "$3", "$3.00", "$1,200.50"
    Returns float or None if not a cash $ amount.
    """
    token = token.strip()
    if not token.startswith("$"):
        return None
    number_part = token[1:].replace(",", "").strip()
    if not re.fullmatch(r"\d+(\.\d+)?", number_part):
        return None
    return float(number_part)


class GGSummaryParser:
    RE_TOURNAMENT_LINE = re.compile(
        r"^\s*Tournament\s*#(?P<tid>\d+)\s*,\s*(?P<tname>.+?)\s*,\s*(?P<gtype>.+?)\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    RE_BUYIN = re.compile(
        r"^\s*Buy-in:\s*(?P<amt>\$[0-9,]+(?:\.[0-9]+)?)\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    RE_PLAYERS = re.compile(r"^\s*(?P<n>\d+)\s+Players\s*$", re.IGNORECASE | re.MULTILINE)
    RE_POOL = re.compile(
        r"^\s*Total\s+Prize\s+Pool:\s*(?P<amt>\$[0-9,]+(?:\.[0-9]+)?)\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    RE_STARTED = re.compile(
        r"^\s*Tournament\s+started\s+(?P<dt>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    RE_FINISH = re.compile(
        r"^\s*You\s+finished\s+in\s+(?P<place>\d+)\s+place\.\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    RE_PLACEMENT_LINE = re.compile(
        r"^\s*(?P<place>\d+)(?:st|nd|rd|th)\s*:\s*(?P<name>.+?)\s*,\s*(?P<payout>.+?)\s*$",
        re.IGNORECASE | re.MULTILINE,
    )

    def parse(self, site: str, text: str) -> Tuple[Optional[ParsedTournament], Optional[str]]:
        """
        Returns (ParsedTournament or None, reason_if_none).
        Reasons starting with:
          - "needs_review:" route to Needs Review
          - "parse_error:" route to Needs Review as well (but logged as error)
        """
        m = self.RE_TOURNAMENT_LINE.search(text)
        if not m:
            return None, "parse_error:missing_tournament_header"

        tournament_id = int(m.group("tid"))
        tournament_name = m.group("tname").strip()
        game_type = m.group("gtype").strip()

        m_buy = self.RE_BUYIN.search(text)
        if not m_buy:
            return None, "parse_error:missing_buy_in"
        buy_in = parse_money_usd(m_buy.group("amt"))
        if buy_in is None:
            return None, "needs_review:non_cash_buy_in"

        m_p = self.RE_PLAYERS.search(text)
        player_count = int(m_p.group("n")) if m_p else None

        m_pool = self.RE_POOL.search(text)
        prize_pool = parse_money_usd(m_pool.group("amt")) if m_pool else None

        m_s = self.RE_STARTED.search(text)
        if not m_s:
            return None, "parse_error:missing_start_time"
        start_dt = datetime.strptime(m_s.group("dt"), "%Y/%m/%d %H:%M:%S")

        m_f = self.RE_FINISH.search(text)
        finish_place = int(m_f.group("place")) if m_f else None

        hero_name = "Hero"
        payout_amount: Optional[float] = None

        for pm in self.RE_PLACEMENT_LINE.finditer(text):
            name = pm.group("name").strip()
            if name.lower() == hero_name.lower():
                payout_token = pm.group("payout").strip()
                payout_amount = parse_money_usd(payout_token)
                if payout_amount is None:
                    return None, "needs_review:non_cash_payout"
                break

        if payout_amount is None:
            return None, "parse_error:missing_hero_payout_line"

        profit_amount = round(payout_amount - buy_in, 2)

        return ParsedTournament(
            site=site,
            tournament_id=tournament_id,
            tournament_start_ts_local=start_dt,
            hero_name=hero_name,
            tournament_name=tournament_name,
            game_type=game_type,
            player_count=player_count,
            currency="USD",
            buy_in_amount=round(buy_in, 2),
            prize_pool_amount=round(prize_pool, 2) if prize_pool is not None else None,
            payout_amount=round(payout_amount, 2),
            profit_amount=profit_amount,
            finish_place=finish_place,
        ), None
