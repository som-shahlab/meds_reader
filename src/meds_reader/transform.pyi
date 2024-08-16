from __future__ import annotations

import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import meds_reader

class MutableSubject(meds_reader.Subject):
    """A subject consists of a subject_id and a sequence of Events"""

    def __init__(self, subject_id, events: List[MutableEvent] = []): ...

    subject_id: int
    "The unique identifier for this subject"

    events: List[MutableEvent]
    "Items that have happened to a subject"

class MutableEvent(meds_reader.Event):
    """An event represents a single unit of information about a subject. It contains a time and code, and potentially more properties."""

    def __init__(self, time: datetime.datetime, code: str, properties: Dict[str, Any] = {}): ...

    time: datetime.datetime
    "The time the event occurred"

    code: str
    "An identifier for the type of event that occured"

    def __getattr__(self, name: str) -> Any:
        """Events can contain arbitrary additional properties. This retrieves the specified property, or returns None"""
        ...

    def __setattr__(self, name: str, value: Any) -> None:
        """Assign an arbitrary property to this event."""
        ...

    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        """Iterate over all non-None properties within this event."""

def transform_meds_dataset(
    source_dataset_path: str,
    target_dataset_path: str,
    transform_func: Callable[[MutableSubject], Optional[MutableSubject]],
    num_threads: int = 1,
):
    """Transform a MEDS dataset using the provided transform function, potentially using multiple threads."""
    ...
