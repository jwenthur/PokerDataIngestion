# Central place for SQL text blocks so they don't get duplicated across modules.
# Keep it boring and explicit.

SQL_HASH_EXISTS = """
SELECT 1
FROM tournament_results
WHERE source_file_hash = :file_hash
LIMIT 1
"""

SQL_INSERT_TOURNAMENT = """
INSERT INTO tournament_results (
    site, tournament_id, tournament_start_ts_local, hero_name,
    source_file_name, source_file_hash,
    tournament_name, game_type, player_count, currency,
    buy_in_amount, prize_pool_amount, payout_amount, profit_amount,
    finish_place,
    session_id, session_start_ts_local, session_tournament_index,
    imported_at, modified_at, notes
) VALUES (
    :site, :tournament_id, :start_ts, :hero_name,
    :source_file_name, :source_file_hash,
    :tournament_name, :game_type, :player_count, :currency,
    :buy_in_amount, :prize_pool_amount, :payout_amount, :profit_amount,
    :finish_place,
    :session_id, :session_start_ts_local, :session_tournament_index,
    now(), NULL, NULL
)
"""

SQL_PREV_WITHIN_GAP = """
SELECT session_id, session_start_ts_local, tournament_start_ts_local
FROM tournament_results
WHERE site = :site
  AND tournament_start_ts_local <= :ts
  AND (:ts - tournament_start_ts_local) <= :gap
ORDER BY tournament_start_ts_local DESC
LIMIT 1
"""

SQL_NEXT_WITHIN_GAP = """
SELECT session_id, session_start_ts_local, tournament_start_ts_local
FROM tournament_results
WHERE site = :site
  AND tournament_start_ts_local >= :ts
  AND (tournament_start_ts_local - :ts) <= :gap
ORDER BY tournament_start_ts_local ASC
LIMIT 1
"""

SQL_COUNT_BEFORE_IN_SESSION = """
SELECT COUNT(*) AS c
FROM tournament_results
WHERE site = :site
  AND session_id = :session_id
  AND tournament_start_ts_local < :ts
"""

SQL_BUMP_INDICES_AT_OR_AFTER_TS = """
UPDATE tournament_results
SET session_tournament_index = session_tournament_index + 1,
    modified_at = now(),
    notes = CASE
        WHEN notes IS NULL OR notes = '' THEN 'index_shifted'
        ELSE notes || ' | ' || 'index_shifted'
    END
WHERE site = :site
  AND session_id = :session_id
  AND tournament_start_ts_local >= :ts
"""

SQL_MIN_SESSION_START = """
SELECT MIN(tournament_start_ts_local) AS min_ts
FROM tournament_results
WHERE site = :site AND session_id = :session_id
"""
