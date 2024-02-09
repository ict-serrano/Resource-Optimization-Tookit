from enum import IntEnum


class ExecutionStatus(IntEnum):
    PENDING = 1
    STARTED = 2
    FAILED = 3
    DONE = 4
    RE = 5
    TERMINATED = 6


class ResponseStatus(IntEnum):
    ACTIVE = 1
    COMPLETED = 2
    FAILED = 3
    REJECTED = 4
    CANCELLED = 5
