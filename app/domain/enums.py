from enum import Enum


class Action(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Mode(str, Enum):
    FIRST_TIME = "FIRST_TIME"   # no holdings yet: BUY only
    REBALANCE = "REBALANCE"     # explicit SELL / BUY(new) / ADJUST instructions


class Category(str, Enum):
    NEW = "NEW"        # buy a stock not currently held
    EXIT = "EXIT"      # sell the whole position
    ADJUST = "ADJUST"  # buy/sell part of an existing position


class OrderStatus(str, Enum):
    SUCCESS = "SUCCESS"      # filled
    SUBMITTED = "SUBMITTED"  # accepted by broker, fill not confirmed yet
    FAILED = "FAILED"


class ExecutionStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"
