from enum import Enum


class ActionType(str, Enum):
    FOLD = "FOLD"
    CALL = "CALL"
    CHECK = "CHECK"
    RAISE = "RAISE"
    JAM = "JAM"


class FacedActionType(str, Enum):
    NONE = "NONE"
    LIMP = "LIMP"
    MINRAISE = "MINRAISE"
    RAISE = "RAISE"
    OPEN_JAM = "OPEN_JAM"
    VS_JAM = "VS_JAM"


class SpotType(str, Enum):
    HU_SB_FTA = "HU_SB_FTA"
    HU_BB_VS_SB_LIMP = "HU_BB_VS_SB_LIMP"
    HU_BB_VS_SB_MINRAISE = "HU_BB_VS_SB_MINRAISE"
    HU_BB_VS_SB_OPEN_JAM = "HU_BB_VS_SB_OPEN_JAM"

    TH_BTN_FTA = "3H_BTN_FTA"
    TH_SB_VS_BTN_OPEN = "3H_SB_VS_BTN_OPEN"
    TH_BB_VS_BTN_OPEN = "3H_BB_VS_BTN_OPEN"

    UNKNOWN = "UNKNOWN"