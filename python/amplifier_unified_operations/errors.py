from contextlib import contextmanager

class ScheduleRefused(ValueError):
    """The requested schedule mutation did not commit; no work was admitted."""

@contextmanager
def schedule_validation():
    # Only surround preflight or Foundation's atomic mutation transaction.
    # Never wrap response publication after a committed result.
    try:yield
    except ValueError as error:raise ScheduleRefused(str(error)) from error
